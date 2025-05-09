import logging
import time
import re
from urllib.parse import urlencode, urljoin

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException

from lxml import html

# Mock classes for testing if real dependencies are not available or for simple examples
class MockLogger:
    def info(self, msg): print(f"INFO: {msg}")
    def warning(self, msg): print(f"WARNING: {msg}")
    def error(self, msg, exc_info=False): print(f"ERROR: {msg}")
    def debug(self, msg): print(f"DEBUG: {msg}")

class MockDBManager:
    def get_or_create_item(self, repository_source, oai_identifier=None, item_page_url=None, initial_status='pending_html_processing'):
        print(f"DB: Get/Create item for source={repository_source}, page_url={item_page_url}")
        # Simulate returning a new item ID and that it's new
        return (int(time.time() * 1000) % 100000, True) 
    def update_item_status(self, item_id, status):
        print(f"DB: Update item_id={item_id} to status={status}")
    def log_item_metadata(self, item_id, title=None, pdf_url_harvested=None, **kwargs):
        print(f"DB: Log metadata for item_id={item_id}, title={title}, pdf_url={pdf_url_harvested}")

class KeywordSearcherBR:
    """
    Performs keyword searches on the Embrapa main portal (or other configured web source)
    and extracts links to item detail pages.
    """

    def __init__(self, config, logger=None, db_manager=None, downloader=None, selectors_dict=None):
        """
        Initializes the KeywordSearcherBR.

        Args:
            config (dict): Configuration dictionary.
            logger (logging.Logger, optional): Logger instance.
            db_manager (DatabaseManagerBR, optional): Database manager instance.
            downloader (ResourceDownloaderBR, optional): Resource downloader for HTML (not used if Selenium is primary).
            selectors_dict (dict, optional): Dictionary of selectors, typically from selectors.yaml.
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.db_manager = db_manager
        self.downloader = downloader # May not be used if Selenium fetches directly
        
        self.search_config = config.get('search_config', {})
        self.base_search_url = self.search_config.get('base_url')
        self.search_query_param = self.search_config.get('query_param', 'q') # Common query param
        self.pagination_param = self.search_config.get('pagination_param', 'page') # Common pagination param
        self.start_page_number = self.search_config.get('start_page_number', 0) # Or 1, depends on site
        self.max_search_pages = self.search_config.get('max_search_pages_per_keyword', 5)
        self.search_results_per_page = self.search_config.get('results_per_page', 10) # Typical default
        self.search_delay_seconds = self.search_config.get('search_delay_seconds', 5)
        self.selenium_timeout = self.search_config.get('selenium_timeout', 30)
        self.webdriver_path = config.get('webdriver_path', 'chromedriver') # Or path to geckodriver etc.
        self.user_agent = config.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

        if selectors_dict is None:
            self.logger.warning("Selectors dictionary not provided to KeywordSearcherBR. Parsing will likely fail.")
            self.selectors = {}
        else:
            self.selectors = selectors_dict.get('web_search_results_selectors', {})
            if not self.selectors:
                 self.logger.warning("'web_search_results_selectors' not found or empty in selectors_dict.")

        self.item_container_xpath = self.selectors.get('item_container')
        self.title_xpath = self.selectors.get('title')
        self.link_to_item_page_xpath = self.selectors.get('link_to_item_page')
        self.next_page_link_xpath = self.selectors.get('next_page_link')

        if not all([self.item_container_xpath, self.title_xpath, self.link_to_item_page_xpath]):
            self.logger.error("Critical search selectors (item_container, title, link_to_item_page) are missing. Keyword searching will be impaired.")

        self.driver = None

    def _configure_webdriver_options(self):
        """Configures Chrome options for Selenium WebDriver."""
        options = ChromeOptions()
        options.add_argument("--headless") # Run headless
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument(f"user-agent={self.user_agent}")
        options.add_argument("--window-size=1920x1080")
        # options.add_experimental_option("excludeSwitches", ["enable-automation"]) # More stealth
        # options.add_experimental_option('useAutomationExtension', False)
        # options.add_argument('--disable-blink-features=AutomationControlled')
        return options

    def _init_webdriver(self):
        """Initializes the Selenium WebDriver."""
        if self.driver:
            return True
        try:
            options = self._configure_webdriver_options()
            # For now, assuming ChromeDriver. Could be made configurable for Firefox (geckodriver) etc.
            # If webdriver_path is just 'chromedriver', it needs to be in PATH.
            # Otherwise, it should be the full path to the executable.
            if self.webdriver_path and self.webdriver_path != 'chromedriver':
                 # Using Service object is the modern way
                from selenium.webdriver.chrome.service import Service as ChromeService
                service = ChromeService(executable_path=self.webdriver_path)
                self.driver = webdriver.Chrome(service=service, options=options)
            else:
                self.driver = webdriver.Chrome(options=options) # Assumes chromedriver is in PATH
            
            self.driver.set_page_load_timeout(self.selenium_timeout)
            self.logger.info("Selenium WebDriver initialized successfully.")
            return True
        except WebDriverException as e:
            self.logger.error(f"Failed to initialize Selenium WebDriver: {e}. Ensure WebDriver (e.g., chromedriver) is correctly installed and in PATH or path is configured.")
            self.driver = None
            return False
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during WebDriver initialization: {e}")
            self.driver = None
            return False

    def _get_html_with_selenium(self, url):
        """Fetches HTML content of a URL using Selenium, handling dynamic content."""
        if not self.driver and not self._init_webdriver():
            return None
        try:
            self.logger.debug(f"Fetching URL with Selenium: {url}")
            self.driver.get(url)
            # Wait for a general condition, e.g., body tag to be present, or a specific element for search results
            WebDriverWait(self.driver, self.selenium_timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            # Optionally, add a small delay for JavaScript to execute if WebDriverWait isn't enough
            # time.sleep(2) # Use with caution, prefer explicit waits
            return self.driver.page_source
        except TimeoutException:
            self.logger.error(f"Timeout occurred while loading page: {url}")
            return None
        except WebDriverException as e:
            self.logger.error(f"WebDriverException while fetching {url}: {e}")
            # Consider re-initializing driver or quitting if it's a persistent issue
            # self.quit_webdriver() # or self.driver = None
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching with Selenium {url}: {e}")
            return None

    def _build_search_url(self, keyword, page_number):
        """
        Builds the search URL for a given keyword and page number.
        Example for Embrapa main site (hypothetical, based on common patterns):
        https://www.embrapa.br/busca-de-publicacoes?p_p_id=resultadoBuscaPortlet_WAR_buscaportlet&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&p_p_col_id=column-1&p_p_col_count=1&_resultadoBuscaPortlet_WAR_buscaportlet_q=milho&_resultadoBuscaPortlet_WAR_buscaportlet_delta=10&_resultadoBuscaPortlet_WAR_buscaportlet_cur=1
        Here, 'q' is the keyword, 'cur' is the page number (1-indexed), 'delta' is results per page.
        """
        if not self.base_search_url:
            self.logger.error("Base search URL is not configured. Cannot build search URL.")
            return None

        # This is a simplified example. Real Embrapa URL might be more complex.
        # The actual params (q, page, etc.) come from self.search_query_param, self.pagination_param
        params = {
            self.search_query_param: keyword,
            # self.search_config.get('results_per_page_param', 'delta'): self.search_results_per_page, # If site uses this
            self.pagination_param: page_number
        }
        # Add any fixed parameters from config
        fixed_params = self.search_config.get('fixed_params', {})
        params.update(fixed_params)

        # The base_search_url might already contain some query parameters.
        # A robust way is to parse the base_url, add/update params, then reconstruct.
        # For simplicity here, we assume base_search_url is the part before '?' or has a trailing '&' if params exist.
        if '?' in self.base_search_url:
            separator = '&'
        else:
            separator = '?'
        
        # Properly encode parameters
        query_string = urlencode(params)
        full_url = f"{self.base_search_url.rstrip('&')}{separator}{query_string}"
        self.logger.debug(f"Built search URL: {full_url}")
        return full_url

    def _parse_search_results(self, html_content, search_url):
        """
        Parses the HTML of a search results page to extract item details.

        Args:
            html_content (str): The HTML content of the search results page.
            search_url (str): The URL of the search page (for resolving relative links).

        Returns:
            tuple: (list_of_items, has_next_page)
                     list_of_items is a list of dicts, each with 'title' and 'item_page_url'.
                     has_next_page is a boolean.
        """
        if not html_content:
            self.logger.warning("HTML content is empty for parsing search results.")
            return [], False
        
        if not self.item_container_xpath or not self.title_xpath or not self.link_to_item_page_xpath:
            self.logger.error("Cannot parse search results: critical selectors are missing.")
            return [], False
        
        try:
            tree = html.fromstring(html_content)
        except Exception as e:
            self.logger.error(f"Failed to parse HTML for search results from {search_url}: {e}")
            return [], False

        extracted_items = []
        item_elements = tree.xpath(self.item_container_xpath)
        self.logger.info(f"Found {len(item_elements)} potential item containers on {search_url} using xpath: {self.item_container_xpath}")

        if not item_elements and html_content: # If no items, log a snippet for debugging selectors
            snippet = html_content[:1000].replace('\n',' ')
            self.logger.warning(f"No item containers found. HTML snippet from {search_url}: {snippet}...")

        for element in item_elements:
            title = ""
            item_page_url = ""
            try:
                title_elements = element.xpath(self.title_xpath)
                if title_elements:
                    # Handle cases where title might be split across multiple text nodes or within sub-tags
                    title = " ".join([t.strip() for t in title_elements if isinstance(t, str) or hasattr(t, 'text_content')]).strip()
                    if not title and hasattr(title_elements[0], 'text_content'): # Fallback for complex elements
                         title = title_elements[0].text_content().strip()
                else:
                    self.logger.warning(f"Title not found for an item on {search_url} using xpath: {self.title_xpath} relative to container.")

                link_elements = element.xpath(self.link_to_item_page_xpath)
                if link_elements:
                    # link_elements could be a string (from @href) or an element
                    raw_link = link_elements[0] if isinstance(link_elements[0], str) else link_elements[0].get('href')
                    if raw_link:
                        item_page_url = urljoin(search_url, raw_link.strip()) # Ensure absolute URL
                else:
                    self.logger.warning(f"Item page link not found for an item on {search_url} using xpath: {self.link_to_item_page_xpath} relative to container.")

                if title and item_page_url:
                    # Basic validation of URL (optional)
                    if not (item_page_url.startswith('http://') or item_page_url.startswith('https://')):
                        self.logger.warning(f"Extracted item page URL seems invalid: {item_page_url}. Skipping.")
                        continue
                    
                    extracted_items.append({
                        'title': title,
                        'item_page_url': item_page_url
                    })
                    self.logger.debug(f"Extracted item: Title='{title}', URL='{item_page_url}'")
                else:
                    self.logger.debug(f"Skipping item due to missing title or URL. Title found: '{bool(title)}', URL found: '{bool(item_page_url)}'")
            
            except Exception as e:
                self.logger.error(f"Error parsing a specific item element on {search_url}: {e}", exc_info=False)
                # Log the problematic element's HTML for debugging
                try:
                    problematic_html = html.tostring(element, pretty_print=True).decode('utf-8')[:500]
                    self.logger.debug(f"Problematic item HTML snippet: {problematic_html}...")
                except Exception as e_log:
                    self.logger.error(f"Could not serialize problematic element: {e_log}")
                continue
        
        has_next_page = False
        if self.next_page_link_xpath:
            next_page_elements = tree.xpath(self.next_page_link_xpath)
            if next_page_elements:
                # Check if the element is not disabled or marked as current page link in some way
                # This logic might need to be very site-specific.
                # For now, just presence implies next page.
                has_next_page = True
                self.logger.info(f"'Next page' link found on {search_url}.")
            else:
                self.logger.info(f"No 'Next page' link found on {search_url} (or XPath {self.next_page_link_xpath} failed).")
        else:
            self.logger.debug("'next_page_link_xpath' not configured, assuming no pagination check needed from parser.")

        return extracted_items, has_next_page

    def search_by_keyword(self, keyword):
        """
        Performs a search for a given keyword and processes the results.

        Args:
            keyword (str): The keyword to search for.

        Returns:
            int: Count of new items found and added/updated in the DB.
        """
        if not self.base_search_url or not self.item_container_xpath:
            self.logger.error(f"Keyword search cannot proceed: base_search_url or item_container_xpath not configured for keyword: '{keyword}'")
            return 0
        
        if not self.db_manager:
            self.logger.error("Database manager not available. Keyword search results cannot be saved.")
            return 0

        if not self._init_webdriver(): # Ensure WebDriver is ready
            self.logger.error("WebDriver could not be initialized. Aborting keyword search.")
            return 0

        self.logger.info(f"Starting keyword search for: '{keyword}'")
        total_new_items_found_for_keyword = 0
        processed_urls_this_session = set() # Avoid processing same URL multiple times if it appears on different pages

        for page_num in range(self.start_page_number, self.start_page_number + self.max_search_pages):
            self.logger.info(f"Searching for '{keyword}', page: {page_num}") 
            search_url = self._build_search_url(keyword, page_num)
            if not search_url:
                self.logger.error("Failed to build search URL. Skipping page.")
                continue

            html_content = self._get_html_with_selenium(search_url)
            if not html_content:
                self.logger.warning(f"No HTML content received for {search_url}. Skipping page.")
                # Potentially a sign of being blocked or end of results if it keeps happening
                # Could implement a counter for consecutive empty pages to break early
                continue

            items_on_page, has_next = self._parse_search_results(html_content, search_url)

            if not items_on_page:
                self.logger.info(f"No items found on page {page_num} for keyword '{keyword}'. URL: {search_url}")
                # If this is the first page (page_num == self.start_page_number) and no items, maybe no results at all.
                if page_num == self.start_page_number:
                    self.logger.info(f"No items found on the first results page for '{keyword}'. Assuming no results or selector issue.")
                    break # No point in continuing for this keyword if first page is empty
                # If not first page, and no items, could be end of results or parsing issue.
                # If has_next is also false, then it is likely end of results.
                if not has_next:
                    self.logger.info(f"No items and no 'next page' link found on page {page_num} for '{keyword}'. Ending search for this keyword.")
                    break
                # If no items but has_next is true, it might be an intermittent issue or selector problem on this specific page.
                # We continue to the next page based on has_next, but log it.
                self.logger.warning(f"No items found on page {page_num} for '{keyword}', but a 'next page' link was detected. Proceeding to next page.")
            
            page_items_processed = 0
            for item_data in items_on_page:
                item_page_url = item_data.get('item_page_url')
                item_title = item_data.get('title', '[No Title Found]')
                
                if not item_page_url:
                    self.logger.warning(f"Skipping item with no page URL. Title: {item_title}")
                    continue
                
                if item_page_url in processed_urls_this_session:
                    self.logger.debug(f"Skipping already processed URL in this session: {item_page_url}")
                    continue
                processed_urls_this_session.add(item_page_url)

                # Use a generic repository_source for items found via web search
                repo_source = self.search_config.get('repository_source_keyword_search', 'embrapa_keyword_search')
                item_id, is_new = self.db_manager.get_or_create_item(
                    repository_source=repo_source,
                    item_page_url=item_page_url,
                    initial_status='pending_html_processing' # Items from keyword search need HTML processing for PDF link
                )

                if item_id:
                    page_items_processed += 1
                    if is_new:
                        total_new_items_found_for_keyword += 1
                        self.logger.info(f"New item from keyword search '{keyword}': ID {item_id}, URL: {item_page_url}")
                        # Log basic metadata found on search page (title)
                        self.db_manager.log_item_metadata(item_id, title=item_title)
                    else:
                        self.logger.info(f"Existing item found from keyword search '{keyword}': ID {item_id}, URL: {item_page_url}. Will check/update status.")
                        # Optionally, update title if it changed or wasn't logged before
                        # self.db_manager.log_item_metadata(item_id, title=item_title) 
                else:
                    self.logger.error(f"Failed to get/create DB entry for item URL: {item_page_url}")
            
            self.logger.info(f"Processed {page_items_processed} items from page {page_num} for keyword '{keyword}'.")

            if not has_next:
                self.logger.info(f"No 'next page' link detected on page {page_num} for '{keyword}'. Ending search for this keyword.")
                break # End of results for this keyword
            
            if page_num < self.start_page_number + self.max_search_pages - 1: # if not the last configured page to check
                self.logger.info(f"Pausing for {self.search_delay_seconds} seconds before next search page...")
                time.sleep(self.search_delay_seconds)
            else:
                self.logger.info(f"Reached max_search_pages ({self.max_search_pages}) for keyword '{keyword}'.")

        self.logger.info(f"Keyword search for '{keyword}' completed. Found and registered {total_new_items_found_for_keyword} new items.")
        return total_new_items_found_for_keyword

    def quit_webdriver(self):
        """Closes the WebDriver session if it's active."""
        if self.driver:
            try:
                self.logger.info("Quitting Selenium WebDriver.")
                self.driver.quit()
            except Exception as e:
                self.logger.error(f"Error quitting WebDriver: {e}")
            finally:
                self.driver = None

# Example Usage (Illustrative)
if __name__ == '__main__':
    # Setup basic logging for the example
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main_logger = logging.getLogger("KeywordSearcherBRExample")

    # Mock configuration (replace with actual loading from a config file or dict)
    # IMPORTANT: The selectors here are CRITICAL and ENTIRELY HYPOTHETICAL.
    # They MUST be derived from actual analysis of Embrapa's search results page.
    mock_config_dict = {
        'search_config': {
            'base_url': 'https://www.embrapa.br/busca-de-publicacoes', # Example, needs verification
            'query_param': '_resultadoBuscaPortlet_WAR_buscaportlet_q', # From observed URL
            'pagination_param': '_resultadoBuscaPortlet_WAR_buscaportlet_cur', # From observed URL
            'start_page_number': 1, # Embrapa seems 1-indexed for 'cur'
            'max_search_pages_per_keyword': 2, # Limit for example
            'search_delay_seconds': 2,
            'selenium_timeout': 20,
            'repository_source_keyword_search': 'embrapa_portal_search',
            'fixed_params': { # Parameters that seem fixed in Embrapa's search URL structure
                'p_p_id': 'resultadoBuscaPortlet_WAR_buscaportlet',
                'p_p_lifecycle': '0',
                'p_p_state': 'normal',
                'p_p_mode': 'view',
                'p_p_col_id': 'column-1',
                'p_p_col_count': '1',
                '_resultadoBuscaPortlet_WAR_buscaportlet_delta': '10' # Results per page
            }
        },
        'webdriver_path': 'chromedriver',  # Or specify full path if not in PATH
        'user_agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)' # Example User Agent
    }

    # HYPOTHETICAL SELECTORS - THESE WILL NOT WORK WITHOUT REAL ANALYSIS
    mock_selectors = {
        'web_search_results_selectors': {
            'item_container': "//div[contains(@class, 'resultado_busca_item') or contains(@class, 'search-result-item')]", # Placeholder
            'title': ".//h3/a/text() | .//div[contains(@class, 'title')]/a/text()", # Placeholder
            'link_to_item_page': ".//h3/a/@href | .//div[contains(@class, 'title')]/a/@href", # Placeholder
            'next_page_link': "//a[contains(text(), 'Próxima') or contains(@class, 'next') or contains(text(), '>') and not(contains(@class, 'disabled'))]/@href" # Placeholder
        }
    }

    main_logger.info("Initializing KeywordSearcherBR for example...")
    # Using mock DB and logger for this standalone example
    searcher = KeywordSearcherBR(
        config=mock_config_dict, 
        logger=main_logger, # Using main_logger here
        db_manager=MockDBManager(), 
        selectors_dict=mock_selectors
    )

    test_keyword = "milho" # Corn in Portuguese
    main_logger.info(f"Starting example search for keyword: '{test_keyword}'")

    # Ensure webdriver_path in mock_config_dict points to your actual chromedriver or is in PATH
    # This example will attempt to run Selenium.
    # If selectors are not correct for Embrapa, it will likely find 0 items or fail parsing.
    try:
        # items_found = searcher.search_by_keyword(test_keyword)
        # main_logger.info(f"Example search for '{test_keyword}' found {items_found} new items.")
        main_logger.info("Example search call is commented out to prevent actual web requests during non-interactive tests.")
        main_logger.info("To run a real test, uncomment the search_by_keyword call and ensure correct selectors and WebDriver setup.")
        
        # Example of how to build a URL (for testing the builder)
        # url_test = searcher._build_search_url("soja", 2)
        # main_logger.info(f"Test URL build: {url_test}")

    except Exception as e:
        main_logger.error(f"An error occurred during the example: {e}", exc_info=True)
    finally:
        searcher.quit_webdriver() # Important to clean up WebDriver session
        main_logger.info("KeywordSearcherBR example finished.")

    pass
