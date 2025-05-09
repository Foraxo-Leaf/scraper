import requests
import logging
from lxml import html
from urllib.parse import urljoin

class HTMLMetadataExtractorBR:
    """
    Extracts metadata and PDF links from HTML pages of Brazilian repository items.
    Relies on XPath selectors defined in a configuration dictionary.
    """

    def __init__(self, config, logger=None, downloader=None):
        """
        Initializes the HTMLMetadataExtractorBR.

        Args:
            config (dict): Configuration dictionary, expected to contain 'selectors'.
            logger (logging.Logger, optional): Logger instance. Defaults to None.
            downloader (ResourceDownloaderBR, optional): Instance for fetching HTML content.
                                                        If None, a simple requests.get will be used.
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.selectors = config.get('selectors', {}).get('metadata_selectors', {})
        self.pdf_link_selectors = config.get('selectors', {}).get('pdf_link_selectors', [])
        self.downloader = downloader
        self.requests_timeout = config.get('requests_timeout', 30)
        self.session = requests.Session() # Use a session for potential cookie persistence and connection pooling
        headers = {
            'User-Agent': config.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36')
        }
        self.session.headers.update(headers)

    def _fetch_html_content(self, url):
        """
        Fetches HTML content from a given URL.
        Uses the downloader if available, otherwise uses simple requests.
        """
        if self.downloader:
            # Assuming downloader has a method like fetch_html or similar that returns text content
            # and handles its own retries/logging for the fetch operation.
            # For this example, let's assume it has a `download_resource` that can save to a temp path
            # and we can then read it, or a method that returns content directly.
            # If downloader.download_resource returns content directly:
            # success, content_or_path, _ = self.downloader.download_resource(url, is_temp=True, resource_type='html')
            # if success and isinstance(content_or_path, str): return content_or_path else: return None
            
            # Simpler: assume a method that just gets content
            try:
                return self.downloader.get_html_content(url) # This method needs to exist in ResourceDownloaderBR
            except Exception as e:
                self.logger.error(f"Error fetching HTML via downloader for {url}: {e}")
                return None
        else:
            try:
                response = self.session.get(url, timeout=self.requests_timeout)
                response.raise_for_status() # Raise an exception for HTTP errors
                # Ensure content is decoded correctly, try common encodings if default is wrong
                response.encoding = response.apparent_encoding if response.apparent_encoding else 'utf-8'
                return response.text
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching HTML content from {url}: {e}")
                return None

    def extract_pdf_link(self, item_page_url, html_content=None):
        """
        Extracts the direct PDF link from the HTML content of an item page.

        Args:
            item_page_url (str): The URL of the item page (used to resolve relative links).
            html_content (str, optional): The HTML content of the page. 
                                         If None, it will be fetched using item_page_url.

        Returns:
            str: The absolute URL to the PDF, or None if not found.
        """
        if not html_content:
            self.logger.debug(f"HTML content not provided for {item_page_url}, fetching...")
            html_content = self._fetch_html_content(item_page_url)
            if not html_content:
                self.logger.warning(f"Could not fetch HTML content for PDF link extraction from {item_page_url}")
                return None
        
        try:
            tree = html.fromstring(html_content)
        except Exception as e:
            self.logger.error(f"Failed to parse HTML content from {item_page_url}: {e}")
            return None

        for selector_info in self.pdf_link_selectors:
            try:
                path = selector_info.get('path')
                if not path: continue
                
                elements = tree.xpath(path)
                if elements:
                    # The selector might return a list of elements or a list of strings (attribute values)
                    # We want the first non-empty string which looks like a PDF link
                    for element in elements:
                        # If element is already a string (e.g. from @href)
                        if isinstance(element, str):
                            pdf_url = element.strip()
                        # If element is an lxml element, get its href or text content
                        # This part might be redundant if XPath always extracts the attribute string, but good for safety
                        elif hasattr(element, 'get'): # Check if it's an lxml element
                            pdf_url = element.get('href', '').strip() # Common for <a> tags
                        else:
                            continue # Not a string or recognizable element

                        if pdf_url and '.pdf' in pdf_url.lower(): # Basic check
                            absolute_pdf_url = urljoin(item_page_url, pdf_url) # Ensure it's absolute
                            self.logger.info(f"Found PDF link: {absolute_pdf_url} using selector: {path} on {item_page_url}")
                            return absolute_pdf_url
            except Exception as e:
                self.logger.error(f"Error applying PDF selector '{path}' on {item_page_url}: {e}")
                continue # Try next selector
        
        self.logger.warning(f"No PDF link found on {item_page_url} using configured selectors.")
        return None

    def extract_all_metadata(self, item_page_url, html_content=None):
        """
        Extracts all defined metadata from the HTML content of an item page.
        This is a placeholder, as for Embrapa, OAI is the primary metadata source.
        This would be more relevant if scraping HTML was the main strategy.

        Args:
            item_page_url (str): The URL of the item page.
            html_content (str, optional): The HTML content. Fetched if None.

        Returns:
            dict: A dictionary containing the extracted metadata.
                  Keys are metadata field names (e.g., 'title', 'author').
                  Values can be strings or lists of strings.
        """
        if not self.selectors:
            self.logger.info("No metadata selectors configured. Skipping HTML metadata extraction.")
            return {}

        # For Embrapa, OAI is primary. This function might only be used for supplementary data
        # or if OAI fails for a particular item and HTML scraping is a fallback.
        # The current implementation of ScraperBR prioritizes OAI and uses this class mainly for PDF link extraction.
        self.logger.warning(f"extract_all_metadata called for {item_page_url}, but OAI is primary for Embrapa. "
                           "This function is currently a placeholder or for supplementary use.")
        
        # Example of how it *would* work if fully utilized:
        # if not html_content:
        #     html_content = self._fetch_html_content(item_page_url)
        #     if not html_content: return {}
        # try:
        #     tree = html.fromstring(html_content)
        # except Exception as e:
        #     self.logger.error(f"Failed to parse HTML for metadata extraction from {item_page_url}: {e}")
        #     return {}
        # metadata = {}
        # for field, selector_list in self.selectors.items():
        #     for selector_details in selector_list:
        #         path = selector_details.get('path')
        #         try:
        #             elements = tree.xpath(path)
        #             if elements:
        #                 # Clean and store data (e.g., join list of strings, take first non-empty)
        #                 # This part needs careful handling based on selector type and expected output
        #                 data = [str(e).strip() for e in elements if str(e).strip()]
        #                 if data:
        #                     metadata[field] = data if len(data) > 1 else data[0]
        #                     break # Found data for this field with one selector
        #         except Exception as e:
        #             self.logger.error(f"Error applying metadata selector for {field} ('{path}') on {item_page_url}: {e}")
        # return metadata
        return {}

# Example usage (typically not run directly from here but from the main scraper script)
if __name__ == '__main__':
    # Example Usage (Illustrative)
    # This is a basic example and assumes you have a config dict and selectors.yaml structure.
    
    # Setup basic logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("HTMLMetadataExtractorBRExample")

    # Mock configuration (replace with actual loading from a config file or dict)
    mock_config = {
        'selectors': {
            'pdf_link_selectors': [
                {'path': "//meta[@name='citation_pdf_url']/@content"},
                {'path': "//a[contains(@href, '.pdf')]/@href"} # Generic PDF link
            ],
            'metadata_selectors': {
                'title': [{'path': "//meta[@name='DC.title']/@content"}],
                'author': [{'path': "//meta[@name='DC.creator']/@content"}]
            }
        },
        'user_agent': 'TestScraper/1.0'
    }

    extractor = HTMLMetadataExtractorBR(config=mock_config, logger=logger)

    # Example URL (replace with a real item page URL from Embrapa for testing)
    # This URL is a placeholder and will likely not work directly.
    test_item_page_url = "https://www.alice.cnptia.embrapa.br/handle/doc/1141355" # Example from Alice

    logger.info(f"Attempting to extract PDF link from: {test_item_page_url}")
    
    # You would need to provide actual HTML content or ensure _fetch_html_content works
    # For a standalone test, you might fetch HTML here:
    # html_content_example = None
    # try:
    #     response = requests.get(test_item_page_url, headers={'User-Agent': 'TestScraper/1.0'}, timeout=10)
    #     response.raise_for_status()
    #     html_content_example = response.text
    #     logger.info(f"Successfully fetched HTML for {test_item_page_url}")
    # except requests.RequestException as e:
    #     logger.error(f"Failed to fetch {test_item_page_url} for example: {e}")

    # if html_content_example:
    #    pdf_link = extractor.extract_pdf_link(test_item_page_url, html_content=html_content_example)
    #    if pdf_link:
    #        logger.info(f"Extracted PDF link: {pdf_link}")
    #    else:
    #        logger.warning("Could not extract PDF link from the example page.")
    # else:
    #    logger.warning("HTML content not available for example, skipping PDF link extraction test.")

    # The primary purpose within the Embrapa scraper is PDF link extraction.
    # Metadata is primarily from OAI.
    logger.info("HTMLMetadataExtractorBR example finished. Note: Full functionality depends on actual HTML content and selectors.")
    pass
