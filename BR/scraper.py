import logging
import json
import os
import argparse
import yaml # For loading selectors and potentially config
from datetime import datetime

# Import an_scraper classes
from .database_manager_br import DatabaseManagerBR, FILE_DOWNLOAD_SUCCESS, FILE_TYPE_PDF, STATUS_PROCESSED, STATUS_PENDING_PDF_DOWNLOAD, STATUS_PENDING_HTML_PROCESSING, STATUS_PENDING_PDF_LINK
from .html_metadata_extractor_br import HTMLMetadataExtractorBR
from .resource_downloader_br import ResourceDownloaderBR
from .keyword_searcher_br import KeywordSearcherBR
from .oai_harvester_br import OAIHarvesterBR

# Default configuration (can be overridden by a config file or command-line args)
DEFAULT_CONFIG_BR = {
    "base_url": "https://www.embrapa.br",
    "country_code": "BR",
    "db_file": "BR/db/scraper_br.db",
    "log_file": "BR/logs/BR-SCRAPER.log",
    "log_level": "INFO", # DEBUG, INFO, WARNING, ERROR, CRITICAL
    "user_agent": "Mozilla/5.0 (compatible; EmbrapaScraper/1.0; +http://example.com/botinfo.html)",
    
    "base_output_directory": "BR/output",
    "pdf_subdirectory": "pdfs",
    "html_snapshot_subdirectory": "html_snapshots",
    "state_file_json": "BR/output/state_br.json",
    "test_results_file_json": "BR/output/test_results_br.json", # For VERIFY task compliance
    "sample_pdfs_dir_for_doc": "BR/docs/sample_pdfs/", # For DOC task compliance

    "selectors_file": "BR/selectors.yaml",

    "oai_harvesting_enabled": True,
    "oai_repositories": [
        {
            "name": "alice",
            "base_url": "https://www.alice.cnptia.embrapa.br/oai/request",
            "metadata_prefix": "oai_dc",
            # "set_spec": "com_123456789_1" # Example: filter by a specific set if needed
        },
        {
            "name": "infoteca-e",
            "base_url": "https://www.infoteca.cnptia.embrapa.br/infoteca/oai",
            "metadata_prefix": "oai_dc"
        }
    ],
    "max_records_oai_per_repo": 20, # Max records to fetch per OAI repo (set low for testing)
    "oai_request_timeout": 60, # seconds
    "oai_request_delay_seconds": 3,

    "keyword_searching_enabled": True,
    "keywords_to_search": ["milho", "soja", "inteligência artificial"], # Corn, Soya, AI
    "search_config": {
        "base_url": "https://www.embrapa.br/busca-de-publicacoes", # From analysis
        "query_param": "_resultadoBuscaPortlet_WAR_buscaportlet_q",
        "pagination_param": "_resultadoBuscaPortlet_WAR_buscaportlet_cur",
        "start_page_number": 1,
        "max_search_pages_per_keyword": 2, # Max search result pages to crawl per keyword
        "search_delay_seconds": 5,
        "selenium_timeout": 45,
        "repository_source_keyword_search": "embrapa_portal_search",
        "fixed_params": { # Parameters that seem fixed in Embrapa's search URL structure
            "p_p_id": "resultadoBuscaPortlet_WAR_buscaportlet",
            "p_p_lifecycle": "0",
            "p_p_state": "normal",
            "p_p_mode": "view",
            "p_p_col_id": "column-1",
            "p_p_col_count": "1",
            "_resultadoBuscaPortlet_WAR_buscaportlet_delta": "10" # Results per page
        }
    },
    "webdriver_path": "chromedriver", # Path to chromedriver executable, or just 'chromedriver' if in PATH

    "process_html_for_pdf_link_limit": 50, # Max items to process from DB for PDF link extraction per run
    "download_pdfs_limit": 20, # Max PDFs to download per run
    "download_retries": 3,
    "download_delay_retry_seconds": 10,
    "download_timeout_seconds": 180, # Longer timeout for potentially large PDF files

    "generate_state_json": True,
    "generate_test_results_json": True, # For VERIFY task, create a small sample
    "max_items_for_test_results": 5, # Number of successfully downloaded PDFs for test_results.json
}

class ScraperBR:
    """
    Main class for the Embrapa (Brazil) Scraper.
    Orchestrates the OAI harvesting, keyword searching, metadata extraction, 
    resource downloading, and database management.
    """

    def __init__(self, config):
        """
        Initializes the ScraperBR with a given configuration.
        """
        self.config = config
        self.logger = self._setup_logger()
        self.selectors = self._load_selectors()
        self.db_manager = DatabaseManagerBR(config['db_file'], logger=self.logger)
        self.downloader = ResourceDownloaderBR(config, logger=self.logger, db_manager=self.db_manager)
        self.html_extractor = HTMLMetadataExtractorBR(config, logger=self.logger, downloader=self.downloader)
        self.oai_harvester = OAIHarvesterBR(config, logger=self.logger, db_manager=self.db_manager, selectors_dict=self.selectors)
        self.keyword_searcher = KeywordSearcherBR(config, logger=self.logger, db_manager=self.db_manager, downloader=self.downloader, selectors_dict=self.selectors)

        self._ensure_output_dirs_exist()

    def _setup_logger(self):
        """Configures and returns a logger instance."""
        logger = logging.getLogger("ScraperBR")
        level = logging.getLevelName(self.config.get('log_level', 'INFO').upper())
        logger.setLevel(level)
        
        # Prevent multiple handlers if already configured (e.g., in tests or multiple runs)
        if not logger.handlers:
            # File Handler
            log_file_path = self.config['log_file']
            log_dir = os.path.dirname(log_file_path)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            fh = logging.FileHandler(log_file_path, mode='a') # Append mode
            fh.setLevel(level)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s')
            fh.setFormatter(formatter)
            logger.addHandler(fh)

            # Console Handler (optional, for immediate feedback)
            ch = logging.StreamHandler()
            ch.setLevel(level) # Or a higher level like INFO for console
            ch.setFormatter(formatter)
            logger.addHandler(ch)
        
        logger.info("Logger initialized.")
        return logger

    def _load_selectors(self):
        """Loads XPath/CSS selectors from the YAML file specified in config."""
        selectors_file_path = self.config.get('selectors_file')
        if selectors_file_path and os.path.exists(selectors_file_path):
            try:
                with open(selectors_file_path, 'r', encoding='utf-8') as f:
                    selectors = yaml.safe_load(f)
                self.logger.info(f"Selectors loaded successfully from {selectors_file_path}")
                return selectors
            except yaml.YAMLError as e:
                self.logger.error(f"Error parsing YAML selectors file {selectors_file_path}: {e}")
            except IOError as e:
                self.logger.error(f"Error reading selectors file {selectors_file_path}: {e}")
        else:
            self.logger.warning(f"Selectors file not found or not specified: {selectors_file_path}. Using empty selectors dict.")
        return {} # Return empty dict if loading fails or no file specified

    def _ensure_output_dirs_exist(self):
        """Ensures that all configured base output directories exist."""
        dirs_to_check = [
            self.config['base_output_directory'],
            os.path.join(self.config['base_output_directory'], self.config['pdf_subdirectory']),
            os.path.join(self.config['base_output_directory'], self.config['html_snapshot_subdirectory']),
            os.path.dirname(self.config.get('state_file_json', '')),
            os.path.dirname(self.config.get('test_results_file_json', '')),
            self.config.get('sample_pdfs_dir_for_doc')
        ]
        for dir_path in dirs_to_check:
            if dir_path and not os.path.exists(dir_path):
                try:
                    os.makedirs(dir_path, exist_ok=True)
                    self.logger.info(f"Created output directory: {dir_path}")
                except OSError as e:
                    self.logger.error(f"Error creating output directory {dir_path}: {e}")

    def _run_oai_harvest(self):
        """Runs the OAI harvesting process if enabled."""
        if self.config.get('oai_harvesting_enabled', False):
            self.logger.info("Starting OAI Harvesting Phase...")
            try:
                new_oai_items = self.oai_harvester.run_harvest()
                self.logger.info(f"OAI Harvesting Phase completed. Found {new_oai_items} new items.")
            except Exception as e:
                self.logger.error(f"Error during OAI harvesting phase: {e}", exc_info=True)
        else:
            self.logger.info("OAI Harvesting is disabled in the configuration.")

    def _run_keyword_search(self):
        """Runs the keyword searching process if enabled."""
        if self.config.get('keyword_searching_enabled', False):
            self.logger.info("Starting Keyword Searching Phase...")
            keywords = self.config.get('keywords_to_search', [])
            if not keywords:
                self.logger.warning("Keyword searching enabled, but no keywords provided in config.")
                return
            
            total_new_keyword_items = 0
            try:
                for keyword in keywords:
                    self.logger.info(f"Initiating search for keyword: '{keyword}'")
                    total_new_keyword_items += self.keyword_searcher.search_by_keyword(keyword)
            except Exception as e:
                self.logger.error(f"Error during keyword searching phase: {e}", exc_info=True)
            finally:
                self.keyword_searcher.quit_webdriver() # Ensure webdriver is closed
            self.logger.info(f"Keyword Searching Phase completed. Found {total_new_keyword_items} new items.")
        else:
            self.logger.info("Keyword Searching is disabled in the configuration.")

    def _process_items_for_pdf_links(self):
        """
        Processes items from the database that are pending PDF link extraction.
        These items typically come from OAI harvesting (status: pending_pdf_link) or 
        keyword search if HTML needs full processing (status: pending_html_processing).
        """
        limit = self.config.get('process_html_for_pdf_link_limit', 50)
        # Items from OAI might directly need PDF link extraction if not found in OAI record identifiers
        # Items from keyword search are initially marked pending_html_processing, then might go to pending_pdf_link
        statuses_to_process = [STATUS_PENDING_PDF_LINK, STATUS_PENDING_HTML_PROCESSING]
        
        items_to_process = self.db_manager.get_items_by_status(statuses_to_process, limit=limit)
        if not items_to_process:
            self.logger.info("No items found in DB needing PDF link extraction or HTML processing.")
            return

        self.logger.info(f"Found {len(items_to_process)} items for PDF link extraction/HTML processing.")
        processed_count = 0
        for item_row in items_to_process:
            item_id = item_row['item_id']
            item_page_url = item_row['item_page_url']
            current_status = item_row['processing_status']
            pdf_url_harvested = item_row['pdf_url_harvested'] # Already found in OAI or initial search

            self.logger.info(f"Processing item_id: {item_id}, URL: {item_page_url}, Current Status: {current_status}")

            if pdf_url_harvested: # If PDF URL was already found (e.g. from OAI identifier)
                self.logger.info(f"Item {item_id} already has a harvested PDF URL: {pdf_url_harvested}. Updating status for download.")
                self.db_manager.log_item_metadata(item_id, pdf_url_harvested=pdf_url_harvested) # Ensure it's logged
                self.db_manager.update_item_status(item_id, STATUS_PENDING_PDF_DOWNLOAD)
                processed_count += 1
                continue

            if not item_page_url:
                self.logger.warning(f"Item {item_id} has no item_page_url. Cannot extract PDF link. Setting status to error.")
                self.db_manager.update_item_status(item_id, 'processing_error_missing_url')
                continue
            
            # Fetch HTML and extract PDF link using HTMLMetadataExtractorBR
            # The extractor can fetch HTML itself if not provided
            pdf_link = self.html_extractor.extract_pdf_link(item_page_url)

            if pdf_link:
                self.logger.info(f"Successfully extracted PDF link for item {item_id}: {pdf_link}")
                self.db_manager.log_item_metadata(item_id, pdf_url_harvested=pdf_link)
                self.db_manager.update_item_status(item_id, STATUS_PENDING_PDF_DOWNLOAD)
            else:
                self.logger.warning(f"Failed to extract PDF link for item {item_id} from {item_page_url}. Status remains {current_status} or updated to error.")
                # Optionally, change status to a specific error state like 'pdf_link_extraction_failed'
                # For now, we leave it or db_manager can update based on attempts.
                # If it was pending_html_processing, it might stay there, or go to a specific error.
                # If it was pending_pdf_link, it indicates a failure to find the link from its page.
                self.db_manager.update_item_status(item_id, 'error_pdf_link_not_found')
            processed_count += 1
        self.logger.info(f"Finished processing {processed_count} items for PDF links.")

    def _download_pending_pdfs(self):
        """Downloads PDF files for items that are pending download."""
        limit = self.config.get('download_pdfs_limit', 20)
        items_to_download = self.db_manager.get_items_by_status(STATUS_PENDING_PDF_DOWNLOAD, limit=limit)
        
        if not items_to_download:
            self.logger.info("No items found in DB pending PDF download.")
            return

        self.logger.info(f"Found {len(items_to_download)} items pending PDF download.")
        downloaded_count = 0
        for item_row in items_to_download:
            item_id = item_row['item_id']
            # The PDF URL should have been stored in 'pdf_url_harvested' field by previous step
            pdf_url = item_row['pdf_url_harvested'] 

            if not pdf_url:
                self.logger.error(f"Item {item_id} is pending PDF download, but has no pdf_url_harvested. Setting to error.")
                self.db_manager.update_item_status(item_id, 'download_error_missing_url')
                continue
            
            self.logger.info(f"Attempting to download PDF for item {item_id} from URL: {pdf_url}")
            success, local_path, md5, size = self.downloader.download_resource(item_id, pdf_url, FILE_TYPE_PDF)

            if success:
                self.logger.info(f"PDF for item {item_id} downloaded/verified: {local_path}")
                # DB log for file is handled by downloader, now update item status
                self.db_manager.update_item_status(item_id, STATUS_PROCESSED)
                downloaded_count += 1
            else:
                self.logger.error(f"Failed to download PDF for item {item_id} from {pdf_url}. Status logged by downloader.")
                # Update item status to reflect download error
                self.db_manager.update_item_status(item_id, 'download_error_failed_attempt')
        
        self.logger.info(f"Finished PDF download phase. Successfully downloaded/verified {downloaded_count} PDFs.")

    def _generate_state_json(self):
        """Generates a JSON file summarizing the state of all processed items."""
        if not self.config.get('generate_state_json', False):
            self.logger.info("State JSON generation is disabled.")
            return

        state_file_path = self.config.get('state_file_json')
        if not state_file_path:
            self.logger.error("state_file_json path not configured. Cannot generate state JSON.")
            return

        self.logger.info(f"Generating state JSON file at: {state_file_path}")
        all_items_data = []
        try:
            # Fetch all items (or a representative sample if too many) for the state file
            # This could be very large for a full scrape, consider criteria or sampling
            # For now, fetching all might be slow but complete for smaller tests.
            # A more scalable approach might be to query items page by page.
            all_db_items = self.db_manager.conn.execute("SELECT * FROM items ORDER BY item_id DESC").fetchall()
            
            for item_row in all_db_items:
                item_data = dict(item_row) # Convert sqlite3.Row to dict
                # Get associated files for this item
                files_for_item = self.db_manager.get_files_for_item(item_row['item_id'])
                item_data['files'] = [dict(f_row) for f_row in files_for_item]
                # Convert timestamps to ISO format strings for JSON serialization
                for key, value in item_data.items():
                    if isinstance(value, datetime):
                        item_data[key] = value.isoformat()
                for file_entry in item_data['files']:
                    for key, value in file_entry.items():
                        if isinstance(value, datetime):
                            file_entry[key] = value.isoformat()
                all_items_data.append(item_data)

            with open(state_file_path, 'w', encoding='utf-8') as f:
                json.dump(all_items_data, f, indent=4, ensure_ascii=False)
            self.logger.info(f"State JSON file generated successfully with {len(all_items_data)} items.")

        except Exception as e:
            self.logger.error(f"Error generating state JSON file: {e}", exc_info=True)

    def _generate_test_results_json(self):
        """
        Generates a test_results.json file as per VERIFY task requirements.
        Includes a small sample of successfully downloaded PDFs with their metadata.
        """
        if not self.config.get('generate_test_results_json', False):
            self.logger.info("Test results JSON generation is disabled.")
            return

        test_results_file = self.config.get('test_results_file_json')
        max_items_for_report = self.config.get('max_items_for_test_results', 5)
        sample_pdfs_output_dir = self.config.get('sample_pdfs_dir_for_doc')

        if not test_results_file:
            self.logger.error("test_results_file_json path not configured. Cannot generate test results.")
            return
        
        self.logger.info(f"Generating test results JSON: {test_results_file}")
        results_data = []
        try:
            # Get items that are processed and have a successfully downloaded PDF
            # Query items and their PDF files directly
            query = f"""
            SELECT i.item_id, i.title, i.publication_date, i.doi, i.item_page_url, f.local_path, f.md5_hash, f.file_size_bytes
            FROM items i
            JOIN files f ON i.item_id = f.item_id
            WHERE i.processing_status = '{STATUS_PROCESSED}' 
              AND f.file_type = '{FILE_TYPE_PDF}' 
              AND f.download_status = '{FILE_DOWNLOAD_SUCCESS}'
            ORDER BY i.item_id DESC -- Get recent ones
            LIMIT ?
            """
            cursor = self.db_manager.conn.cursor()
            processed_items_with_pdfs = cursor.execute(query, (max_items_for_report,)).fetchall()

            for item_row in processed_items_with_pdfs:
                # Get authors for this item
                cursor.execute("""
                SELECT a.name FROM authors a
                JOIN item_authors ia ON a.author_id = ia.author_id
                WHERE ia.item_id = ?
                """, (item_row['item_id'],))
                authors = [row['name'] for row in cursor.fetchall()]

                item_info = {
                    "item_id_db": item_row['item_id'],
                    "title": item_row['title'],
                    "authors": authors,
                    "publication_date": item_row['publication_date'],
                    "doi": item_row['doi'],
                    "source_url": item_row['item_page_url'],
                    "local_pdf_path": item_row['local_path'],
                    "md5_hash": item_row['md5_hash'],
                    "file_size_bytes": item_row['file_size_bytes']
                }
                results_data.append(item_info)

                # Copy this PDF to sample_pdfs_dir if it exists and sample dir is configured
                if sample_pdfs_output_dir and item_row['local_path'] and os.path.exists(item_row['local_path']):
                    self._ensure_output_dirs_exist() # Ensures sample_pdfs_dir_for_doc exists
                    try:
                        import shutil
                        dest_file_name = f"item_{item_row['item_id']}_{os.path.basename(item_row['local_path'])}"
                        dest_path = os.path.join(sample_pdfs_output_dir, dest_file_name)
                        shutil.copy2(item_row['local_path'], dest_path)
                        self.logger.info(f"Copied sample PDF for DOC task: {item_row['local_path']} to {dest_path}")
                    except Exception as copy_e:
                        self.logger.error(f"Failed to copy sample PDF {item_row['local_path']} to {sample_pdfs_output_dir}: {copy_e}")
            
            cursor.close()
            with open(test_results_file, 'w', encoding='utf-8') as f:
                json.dump(results_data, f, indent=4, ensure_ascii=False)
            self.logger.info(f"Test results JSON generated with {len(results_data)} items.")

        except Exception as e:
            self.logger.error(f"Error generating test results JSON: {e}", exc_info=True)
            if cursor: cursor.close()

    def run(self):
        """Runs the full scraping process."""
        self.logger.info(f"Starting Embrapa Scraper run at {datetime.now().isoformat()}")
        start_time = time.time()

        # Phase 1: OAI Harvesting (if enabled)
        self._run_oai_harvest()

        # Phase 2: Keyword Searching (if enabled)
        self._run_keyword_search()

        # Phase 3: Process items for PDF links (from OAI or Keyword Search that need it)
        self.logger.info("Starting phase: Process items for PDF links.")
        self._process_items_for_pdf_links()

        # Phase 4: Download actual PDFs for items ready for download
        self.logger.info("Starting phase: Download pending PDFs.")
        self._download_pending_pdfs()

        # Phase 5: Generate summary/state files
        self.logger.info("Starting phase: Generate report files.")
        self._generate_state_json()
        self._generate_test_results_json() # For VERIFY task

        end_time = time.time()
        self.logger.info(f"Embrapa Scraper run finished in {end_time - start_time:.2f} seconds.")
        
        # Log final stats from DB
        item_stats = self.db_manager.get_item_stats()
        file_stats = self.db_manager.get_file_stats()
        self.logger.info(f"Final Item Stats: {item_stats}")
        self.logger.info(f"Final File Stats: {file_stats}")

        # Close DB connection when done with all operations
        self.db_manager.close_connection()
        self.logger.info("Database connection closed.")

def main(args):
    """Main execution function when script is called directly."""
    config = DEFAULT_CONFIG_BR.copy() # Start with defaults

    # --- Configuration loading and overriding --- 
    # 1. Load from a YAML config file if specified (e.g., args.config_file)
    # This part can be expanded. For now, using defaults.
    # if args.config_file and os.path.exists(args.config_file):
    #     try:
    #         with open(args.config_file, 'r') as f:
    #             yaml_config = yaml.safe_load(f)
    #             config.update(yaml_config) # Override defaults with file config
    #         print(f"Loaded configuration from {args.config_file}")
    #     except Exception as e:
    #         print(f"Error loading config file {args.config_file}: {e}. Using defaults.")

    # 2. Override with command-line arguments (if any are defined and parsed)
    # Example: if args.log_level: config['log_level'] = args.log_level
    # Example: if args.keywords: config['keywords_to_search'] = args.keywords.split(',')
    if args.keyword:
        config['keywords_to_search'] = [args.keyword]
        config['keyword_searching_enabled'] = True
        # Potentially disable OAI if only a specific keyword search is requested
        # config['oai_harvesting_enabled'] = False 
        print(f"Running keyword search for specific keyword: {args.keyword}")
    
    if args.max_oai_records:
        config['max_records_oai_per_repo'] = args.max_oai_records
        print(f"Max OAI records per repo set to: {args.max_oai_records}")
        
    if args.max_keyword_items:
        # This would require a new config key like 'max_total_keyword_items' or similar
        # and KeywordSearcher would need to respect it globally, not just per keyword page.
        # For now, we use 'max_search_pages_per_keyword' as the main control.
        print(f"Note: --max-keyword-items is illustrative. Control via 'max_search_pages_per_keyword' in config.")

    if args.source:
        if args.source == 'oai':
            config['keyword_searching_enabled'] = False
            config['oai_harvesting_enabled'] = True
            print("Running OAI harvesting only.")
        elif args.source == 'keyword':
            config['oai_harvesting_enabled'] = False
            config['keyword_searching_enabled'] = True
            print("Running keyword searching only.")
            if not config['keywords_to_search'] and not args.keyword:
                print("Warning: Keyword source selected but no keywords defined. Using defaults or provide --keyword.")
                if not config['keywords_to_search']:
                     config['keywords_to_search'] = ["agricultura"] # Default fallback if none set

    scraper = ScraperBR(config)
    scraper.run()

if __name__ == '__main__':
    # Setup command line argument parsing
    parser = argparse.ArgumentParser(description="Scraper for Embrapa (Brazil) publications.")
    parser.add_argument("--config", dest="config_file", help="Path to a YAML configuration file.")
    parser.add_argument("--keyword", type=str, help="A specific keyword to search for. Overrides keywords in config.")
    parser.add_argument("--max-oai-records", type=int, help="Maximum OAI records to fetch per repository.")
    parser.add_argument("--max-keyword-items", type=int, help="Illustrative: Maximum total items to process from keyword search.")
    parser.add_argument("--source", choices=['all', 'oai', 'keyword'], default='all', 
                        help="Specify data source to process: 'oai', 'keyword', or 'all' (default)." )
    # Add more arguments as needed (e.g., for log level, specific actions)

    args = parser.parse_args()
    main(args)
