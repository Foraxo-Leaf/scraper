import requests
import logging
import os
import hashlib
import time
from urllib.parse import urlparse

# Define default status constants if not imported from db_manager
# These should ideally be consistent with DatabaseManagerBR constants
FILE_DOWNLOAD_SUCCESS = "success"
FILE_DOWNLOAD_FAILED = "failed"
FILE_DOWNLOAD_SKIPPED_EXIST = "skipped_exist"
FILE_TYPE_PDF = "pdf"
FILE_TYPE_HTML_SNAPSHOT = "html_snapshot"

class ResourceDownloaderBR:
    """
    Handles downloading resources like PDFs and HTML snapshots for the Brazil scraper.
    Manages file paths, retries, and MD5 hash calculation.
    """

    def __init__(self, config, logger=None, db_manager=None):
        """
        Initializes the ResourceDownloaderBR.

        Args:
            config (dict): Configuration dictionary.
            logger (logging.Logger, optional): Logger instance.
            db_manager (DatabaseManagerBR, optional): Database manager for logging download results.
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.db_manager = db_manager # For logging results back to the DB

        self.base_output_dir = config.get('base_output_directory', 'BR/output')
        self.pdf_dir_name = config.get('pdf_subdirectory', 'pdfs')
        self.html_dir_name = config.get('html_snapshot_subdirectory', 'html_snapshot')
        
        self.download_retries = config.get('download_retries', 3)
        self.download_delay_retry_seconds = config.get('download_delay_retry_seconds', 5)
        self.download_timeout = config.get('download_timeout_seconds', 120) # Generous timeout for PDFs
        self.user_agent = config.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36')
        
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})
        
        # Ensure base output directories exist
        self._ensure_directory_exists(os.path.join(self.base_output_dir, self.pdf_dir_name))
        self._ensure_directory_exists(os.path.join(self.base_output_dir, self.html_dir_name))

    def _ensure_directory_exists(self, dir_path):
        """Creates a directory if it doesn't exist."""
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path, exist_ok=True)
                self.logger.info(f"Created directory: {dir_path}")
            except OSError as e:
                self.logger.error(f"Error creating directory {dir_path}: {e}")
                raise # Or handle more gracefully if preferred

    def _get_local_file_path(self, item_id, resource_url, resource_type):
        """
        Determines the local file path for a downloaded resource.
        Creates a subdirectory for each item_id to organize files.

        Args:
            item_id (int or str): The ID of the item this resource belongs to.
            resource_url (str): The URL of the resource, used to derive a filename.
            resource_type (str): 'pdf' or 'html_snapshot'.

        Returns:
            str: The absolute local file path.
        """
        if resource_type == FILE_TYPE_PDF:
            type_dir = self.pdf_dir_name
            file_extension = ".pdf"
        elif resource_type == FILE_TYPE_HTML_SNAPSHOT:
            type_dir = self.html_dir_name
            file_extension = ".html"
        else:
            self.logger.warning(f"Unknown resource type: {resource_type}. Defaulting to '.dat' extension and 'unknown_type' directory.")
            type_dir = "unknown_type"
            file_extension = ".dat"

        # Create a directory for the item if it doesn't exist
        item_specific_dir = os.path.join(self.base_output_dir, type_dir, str(item_id))
        self._ensure_directory_exists(item_specific_dir)

        # Generate a filename from the URL
        try:
            parsed_url = urlparse(resource_url)
            base_filename = os.path.basename(parsed_url.path)
            if not base_filename or not base_filename.endswith(file_extension):
                 # If basename is empty or doesn't have the expected extension, create a generic one
                 # or use a hash of the URL if filenames are not predictable
                if resource_type == FILE_TYPE_PDF and not base_filename.lower().endswith('.pdf'):
                    base_filename = f"{hashlib.md5(resource_url.encode('utf-8')).hexdigest()}{file_extension}"
                elif resource_type == FILE_TYPE_HTML_SNAPSHOT:
                     base_filename = f"snapshot_{hashlib.md5(resource_url.encode('utf-8')).hexdigest()}{file_extension}"
                else: # Fallback for other types or if still no good name
                    base_filename = f"downloaded_resource{file_extension}"
            
            # Sanitize filename (simple version, could be more robust)
            safe_filename = "".join(c for c in base_filename if c.isalnum() or c in ('.', '-', '_')).strip()
            if not safe_filename:
                safe_filename = f"default_filename{file_extension}"

        except Exception as e:
            self.logger.error(f"Error parsing resource URL '{resource_url}' for filename: {e}")
            safe_filename = f"error_filename_{hashlib.md5(resource_url.encode('utf-8')).hexdigest()}{file_extension}"
            
        return os.path.join(item_specific_dir, safe_filename)

    def calculate_md5(self, file_path):
        """Calculates the MD5 hash of a file."""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except IOError as e:
            self.logger.error(f"Error reading file {file_path} for MD5 calculation: {e}")
            return None

    def download_resource(self, item_id, resource_url, resource_type, force_download=False):
        """
        Downloads a resource (PDF or HTML) from a given URL.

        Args:
            item_id (int): The ID of the item this resource belongs to.
            resource_url (str): The URL of the resource to download.
            resource_type (str): Type of resource ('pdf', 'html_snapshot').
            force_download (bool): If True, download even if file exists (and potentially verified).

        Returns:
            tuple: (success (bool), local_path (str) or None, md5_hash (str) or None, file_size (int) or None)
        """
        if not resource_url or not resource_url.startswith(('http://', 'https://')):
            self.logger.error(f"Invalid or missing resource URL provided for item {item_id}: '{resource_url}'")
            if self.db_manager:
                # Need a placeholder local_path for logging failure if URL is truly bad
                placeholder_path = os.path.join(self.base_output_dir, resource_type, str(item_id), "download_failed_invalid_url.err")
                self.db_manager.log_file_download(item_id, resource_type, resource_url, placeholder_path, FILE_DOWNLOAD_FAILED)
            return False, None, None, None

        local_path = self._get_local_file_path(item_id, resource_url, resource_type)
        md5sum = None
        file_size = None
        download_outcome_status = FILE_DOWNLOAD_FAILED # Default to failure

        # Check if file exists and if we should skip download
        if os.path.exists(local_path) and not force_download:
            self.logger.info(f"Resource already exists at {local_path}. Verifying...")
            md5sum = self.calculate_md5(local_path)
            file_size = os.path.getsize(local_path)
            # Here, you might want to check against DB if MD5 matches a previously successful download.
            # For simplicity, if it exists and not forcing, we assume it's okay.
            self.logger.info(f"Skipping download for existing file: {local_path}. MD5: {md5sum}, Size: {file_size} bytes.")
            download_outcome_status = FILE_DOWNLOAD_SKIPPED_EXIST
            if self.db_manager:
                 self.db_manager.log_file_download(item_id, resource_type, resource_url, local_path, 
                                                 download_outcome_status, md5sum, file_size)
            return True, local_path, md5sum, file_size # Considered success as file is present

        self.logger.info(f"Attempting to download {resource_type} for item {item_id} from {resource_url} to {local_path}")
        
        for attempt in range(self.download_retries):
            try:
                # Use stream=True for large files like PDFs to avoid loading entirely into memory
                is_streaming = resource_type == FILE_TYPE_PDF
                response = self.session.get(resource_url, timeout=self.download_timeout, stream=is_streaming, allow_redirects=True)
                response.raise_for_status()

                # Check content type, especially for PDFs
                content_type = response.headers.get('Content-Type', '').lower()
                if resource_type == FILE_TYPE_PDF and 'application/pdf' not in content_type:
                    self.logger.warning(f"Content-Type for {resource_url} is '{content_type}', not 'application/pdf'. File may not be a PDF.")
                    # Decide if to proceed or fail. For now, proceed but log.
                elif resource_type == FILE_TYPE_HTML_SNAPSHOT and 'text/html' not in content_type:
                     self.logger.warning(f"Content-Type for {resource_url} is '{content_type}', not 'text/html'. File may not be HTML.")

                with open(local_path, 'wb') as f:
                    if is_streaming:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk: # filter out keep-alive new chunks
                                f.write(chunk)
                    else: # For HTML or small files if not streaming
                        f.write(response.content)
                
                file_size = os.path.getsize(local_path)
                md5sum = self.calculate_md5(local_path)
                self.logger.info(f"Successfully downloaded {resource_type} to {local_path} (Size: {file_size} bytes, MD5: {md5sum})")
                download_outcome_status = FILE_DOWNLOAD_SUCCESS
                if self.db_manager:
                    self.db_manager.log_file_download(item_id, resource_type, resource_url, local_path, 
                                                     download_outcome_status, md5sum, file_size)
                return True, local_path, md5sum, file_size

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Download attempt {attempt + 1}/{self.download_retries} for {resource_url} failed: {e}")
                if attempt < self.download_retries - 1:
                    time.sleep(self.download_delay_retry_seconds)
                else:
                    self.logger.error(f"All {self.download_retries} download attempts failed for {resource_url}.")
                    download_outcome_status = FILE_DOWNLOAD_FAILED
            except IOError as e:
                 self.logger.error(f"File I/O error during download of {resource_url} to {local_path}: {e}")
                 download_outcome_status = FILE_DOWNLOAD_FAILED
                 break # Don't retry on local file system errors
            except Exception as e:
                self.logger.error(f"An unexpected error occurred during download of {resource_url}: {e}")
                download_outcome_status = FILE_DOWNLOAD_FAILED
                break # Don't retry on unknown errors

        # If loop finishes without successful download
        if self.db_manager:
             self.db_manager.log_file_download(item_id, resource_type, resource_url, local_path, 
                                             download_outcome_status, None, None)
        return False, local_path, None, None # Return local_path even on failure for logging purposes

    def get_html_content(self, url, item_id_for_snapshot=None, save_snapshot=False):
        """
        Fetches HTML content from a URL. Optionally saves a snapshot.
        This method is for cases where KeywordSearcher or HTMLMetadataExtractor might need direct HTML content.
        It's distinct from download_resource which is for primary PDF/HTML downloads logged to DB.

        Args:
            url (str): The URL to fetch.
            item_id_for_snapshot (int, optional): If provided and save_snapshot is True,
                                                 the HTML will be saved as a snapshot for this item_id.
            save_snapshot (bool): Whether to save the fetched HTML as a snapshot.

        Returns:
            str: The HTML content as text, or None on failure.
        """
        try:
            response = self.session.get(url, timeout=self.download_timeout) # Shorter timeout for general HTML?
            response.raise_for_status()
            response.encoding = response.apparent_encoding if response.apparent_encoding else 'utf-8'
            html_text = response.text

            if save_snapshot and item_id_for_snapshot is not None:
                self.logger.debug(f"Saving HTML snapshot for item {item_id_for_snapshot} from {url}")
                # Use download_resource to leverage its path generation and (optional) DB logging
                # Here, we are just saving it. DB logging for snapshots is typically handled by the caller if needed.
                snapshot_local_path = self._get_local_file_path(item_id_for_snapshot, url, FILE_TYPE_HTML_SNAPSHOT)
                try:
                    with open(snapshot_local_path, 'w', encoding='utf-8') as f:
                        f.write(html_text)
                    self.logger.info(f"HTML snapshot saved to {snapshot_local_path}")
                except IOError as e:
                    self.logger.error(f"Failed to save HTML snapshot to {snapshot_local_path}: {e}")
            return html_text
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get HTML content from {url}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in get_html_content for {url}: {e}")
            return None

# Example Usage (Illustrative)
if __name__ == '__main__':
    # Setup basic logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    example_logger = logging.getLogger("ResourceDownloaderBRExample")

    # Mock configuration
    mock_config_data = {
        'base_output_directory': 'BR_downloader_test_output',
        'pdf_subdirectory': 'test_pdfs',
        'html_snapshot_subdirectory': 'test_html',
        'download_retries': 2,
        'download_delay_retry_seconds': 1,
        'download_timeout_seconds': 10,
        'user_agent': 'TestDownloader/1.0'
    }

    # Mock DB Manager (optional, for full testing of DB logging)
    class MockDBForDownloader:
        def log_file_download(self, item_id, file_type, file_url, local_path, download_status, md5_hash=None, file_size_bytes=None):
            example_logger.info(f"DB_LOG: item_id={item_id}, type={file_type}, url={file_url}, path={local_path}, status={download_status}, md5={md5_hash}, size={file_size_bytes}")

    # Clean up previous test output if it exists
    if os.path.exists(mock_config_data['base_output_directory']):
        import shutil
        shutil.rmtree(mock_config_data['base_output_directory'])
        example_logger.info(f"Removed previous test output directory: {mock_config_data['base_output_directory']}")

    downloader = ResourceDownloaderBR(config=mock_config_data, logger=example_logger, db_manager=MockDBForDownloader())

    example_logger.info("ResourceDownloaderBR initialized for example.")

    # Test PDF download (using a public domain PDF for testing)
    # Replace with a real, stable PDF URL from Embrapa if possible for more relevant testing
    # For this example, using a known small public PDF often used for tests.
    test_pdf_url = "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf" 
    test_item_id_pdf = 101

    example_logger.info(f"Attempting to download PDF for item {test_item_id_pdf} from {test_pdf_url}")
    success_pdf, path_pdf, md5_pdf, size_pdf = downloader.download_resource(test_item_id_pdf, test_pdf_url, FILE_TYPE_PDF)

    if success_pdf:
        example_logger.info(f"PDF Download successful: Path={path_pdf}, MD5={md5_pdf}, Size={size_pdf}")
        assert os.path.exists(path_pdf), "Downloaded PDF file does not exist where expected."
        # Test redownload (should skip or be forced based on logic)
        example_logger.info("Attempting to download the same PDF again (should be skipped or logged as existing)...")
        s2, p2, m2, sz2 = downloader.download_resource(test_item_id_pdf, test_pdf_url, FILE_TYPE_PDF)
        assert s2 and p2 == path_pdf, "Second download attempt (skip existing) failed or path changed."
    else:
        example_logger.error(f"PDF Download failed for {test_pdf_url}")

    # Test HTML snapshot download
    test_html_url = "http://example.com" # A simple, stable HTML page
    test_item_id_html = 202
    example_logger.info(f"Attempting to download HTML snapshot for item {test_item_id_html} from {test_html_url}")
    # Using get_html_content for snapshot saving as per its design
    # html_text_content = downloader.get_html_content(test_html_url, item_id_for_snapshot=test_item_id_html, save_snapshot=True)
    # if html_text_content:
    #    example_logger.info(f"HTML content fetched (and snapshot saved if configured).")
    #    # Path would be built by _get_local_file_path internally by get_html_content if save_snapshot was true
    #    # constructed_html_path = downloader._get_local_file_path(test_item_id_html, test_html_url, FILE_TYPE_HTML_SNAPSHOT)
    #    # assert os.path.exists(constructed_html_path), "Saved HTML snapshot does not exist."
    # else:
    #    example_logger.error(f"HTML Snapshot fetch failed for {test_html_url}")
    # The `download_resource` method is more for primary downloads logged to files table.
    # If an HTML snapshot is a primary artifact to be tracked like a PDF:
    success_html, path_html, md5_html, size_html = downloader.download_resource(test_item_id_html, test_html_url, FILE_TYPE_HTML_SNAPSHOT)
    if success_html:
        example_logger.info(f"HTML Snapshot download successful (via download_resource): Path={path_html}, MD5={md5_html}, Size={size_html}")
        assert os.path.exists(path_html), "Downloaded HTML file does not exist where expected."
    else:
        example_logger.error(f"HTML Snapshot download failed for {test_html_url} (via download_resource)")

    example_logger.info("ResourceDownloaderBR example finished.")
    example_logger.info(f"Please check the '{mock_config_data['base_output_directory']}' directory for output.")
    pass
