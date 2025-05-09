import logging
import re
import time
import os
from urllib.parse import urljoin, urlencode

from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# Assuming downloader and db_manager will be provided from the main scraper
# from .resource_downloader_br import ResourceDownloaderBR (Adjust if necessary)
# from .database_manager_br import DatabaseManagerBR (Adjust if necessary)

class KeywordSearcherBR:
    def __init__(self, config, logger, db_manager, downloader, selectors_dict):
        """
        Initializes the KeywordSearcherBR with configuration, logger, database manager, downloader,
        and the parsed selectors dictionary.
        
        Args:
            config (dict): Configuration dictionary.
            logger (logging.Logger): Logger instance.
            db_manager: Instance of DatabaseManagerBR.
            downloader: Instance of ResourceDownloaderBR.
            selectors_dict (dict): Parsed dictionary of selectors from selectors.yaml.
        """
        self.config = config
        self.logger = logger
        self.db_manager = db_manager
        self.downloader = downloader
        self.base_url = self.config.get('base_url_embrapa_search', 'https://www.embrapa.br/busca-de-publicacoes')
        self.selectors = selectors_dict.get('keyword_search_embrapa', {})
        self.webdriver_options = self._configure_webdriver_options()
        # self.webdriver_path = self.config.get('chromedriver_path', 'chromedriver') # O usar webdriver-manager

        if not self.selectors:
            self.logger.warning("Keyword search selectors ('keyword_search_embrapa') not found in provided selectors dictionary.")

    def _configure_webdriver_options(self):
        """Configura las opciones para el WebDriver de Chrome."""
        options = webdriver.ChromeOptions()
        # options.add_argument("--headless") # Desactivar headless para observar
        # options.add_argument("--disable-gpu") # Desactivar temporalmente
        options.add_argument("--window-size=1920x1080")
        options.add_argument(f"user-agent={self.config.get('user_agent')}")
        # options.add_argument("--no-sandbox") # Desactivar temporalmente
        # options.add_argument("--disable-dev-shm-usage") # Desactivar temporalmente
        options.add_experimental_option('excludeSwitches', ['enable-logging']) # Mantener para limpiar logs
        return options

    def _get_html_with_selenium(self, url):
        """Obtiene el contenido HTML de una URL usando Selenium, esperando que los resultados carguen."""
        html_content = None
        driver = None
        webdriver_path = self.config.get('chromedriver_path')
        
        try:
            if webdriver_path:
                self.logger.debug(f"Usando chromedriver desde path especificado: {webdriver_path}")
                service = ChromeService(executable_path=webdriver_path)
                driver = webdriver.Chrome(service=service, options=self.webdriver_options)
            else:
                self.logger.debug("No se especificó chromedriver_path, asumiendo que está en el PATH del sistema.")
                driver = webdriver.Chrome(options=self.webdriver_options)

            self.logger.debug(f"Navegando a {url} con Selenium...")
            driver.get(url)

            # --- Inicio: Depuración Adicional --- 
            # Guardar HTML inicial antes de esperar
            try:
                initial_html_path = os.path.join(self.config.get('output_dir', 'BR/output'), 'debug_search_results_before_wait.html')
                with open(initial_html_path, 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                self.logger.info(f"DEBUG: HTML inicial guardado en {initial_html_path}")
            except Exception as e_write:
                self.logger.error(f"DEBUG: Error guardando HTML inicial: {e_write}")
            
            # Listar frames
            try:
                iframes = driver.find_elements(By.TAG_NAME, 'iframe')
                if iframes:
                    self.logger.info(f"DEBUG: Se encontraron {len(iframes)} iframes en la página.")
                    for i, frame in enumerate(iframes):
                        try:
                            frame_id = frame.get_attribute('id') or f'(sin id, índice {i})'
                            frame_name = frame.get_attribute('name') or '(sin name)'
                            frame_src = frame.get_attribute('src') or '(sin src)'
                            self.logger.info(f"  Frame {i}: ID='{frame_id}', Name='{frame_name}', Src='{frame_src[:100]}...'")
                        except Exception as e_frame_info:
                             self.logger.warning(f"DEBUG: No se pudo obtener info del frame {i}: {e_frame_info}")
                else:
                    self.logger.info("DEBUG: No se encontraron iframes en la página.")
            except Exception as e_frames:
                self.logger.error(f"DEBUG: Error al buscar iframes: {e_frames}")
            # --- Fin: Depuración Adicional ---

            # Esperar a que aparezca un elemento clave de la página de búsqueda renderizada
            wait_timeout = self.config.get('selenium_wait_timeout', 60)
            wait_xpath = "//input[@id='_buscapublicacao_WAR_pcebusca6_1portlet_palavrasChaveNovaBuscaPublicacao']"
            self.logger.debug(f"Esperando (hasta {wait_timeout}s) a que aparezca el campo de búsqueda: {wait_xpath}")
            
            WebDriverWait(driver, wait_timeout).until(
                EC.presence_of_element_located((By.XPATH, wait_xpath))
            )
            self.logger.debug("Elemento clave de la página encontrado. Dando un breve tiempo adicional para renderizado...")
            time.sleep(5) 
            
            self.logger.info("Obteniendo código fuente final...")
            html_content = driver.page_source
            # Guardar el HTML final también (sobrescribe el anterior si tuvo éxito)
            final_html_path = os.path.join(self.config.get('output_dir', 'BR/output'), 'debug_search_results_final.html')
            try:
                 with open(final_html_path, 'w', encoding='utf-8') as f:
                     f.write(html_content)
                 self.logger.info(f"DEBUG: HTML final guardado en {final_html_path}")
            except Exception as e_write:
                 self.logger.error(f"DEBUG: Error guardando HTML final: {e_write}")

        except TimeoutException:
            self.logger.error(f"Timeout esperando el elemento clave ({wait_xpath}) en {url}")
            # --- Inicio: Guardar HTML en Timeout ---
            try:
                timeout_html_path = os.path.join(self.config.get('output_dir', 'BR/output'), 'debug_search_results_on_timeout.html')
                timeout_html = driver.page_source
                with open(timeout_html_path, 'w', encoding='utf-8') as f:
                    f.write(timeout_html)
                self.logger.info(f"DEBUG: HTML en el momento del timeout guardado en {timeout_html_path}")
            except Exception as e_write_timeout:
                self.logger.error(f"DEBUG: Error guardando HTML en timeout: {e_write_timeout}")
            # --- Fin: Guardar HTML en Timeout ---
        except WebDriverException as e:
            self.logger.error(f"Error de WebDriver al procesar {url}: {e}")
            if "net::ERR_NAME_NOT_RESOLVED" in str(e) or "net::ERR_CONNECTION_REFUSED" in str(e):
                 self.logger.error("Error de red o DNS. Verifica la conexión y la URL.")
            elif "session not created" in str(e) or "unable to find binary" in str(e):
                 self.logger.error("Error al iniciar Chrome/ChromeDriver. Verifica la instalación y el path.")
            # Otros errores específicos de WebDriver
        except Exception as e:
            self.logger.error(f"Error inesperado usando Selenium para {url}: {e}", exc_info=True)
        finally:
            if driver:
                try:
                    driver.quit()
                    self.logger.debug("WebDriver cerrado.")
                except Exception as e_quit:
                    self.logger.error(f"Error al cerrar WebDriver: {e_quit}")
            
        return html_content

    def _build_search_url(self, keyword, page_number=1):
        """
        Builds the search URL for a given keyword and page number.

        Args:
            keyword (str): The keyword to search for.
            page_number (int): The page number to retrieve.

        Returns:
            str: The fully constructed search URL.
        """
        # URL structure: https://www.embrapa.br/busca-de-publicacoes/-/publicacao/busca/{keyword}
        # Params: _buscapublicacao_WAR_pcebusca6_1portlet_cur={page}
        #         _buscapublicacao_WAR_pcebusca6_1portlet_delta={page_size}
        #         (and potentially others like ordenarPor, direcao, etc.)
        
        # Sanitize keyword for URL path (basic sanitization)
        safe_keyword = keyword.replace(' ', '-').lower() # Example, might need more robust slugify
        
        # --- Construcción manual de URL --- 
        # Asegurar que la base no tenga / al final y el segmento no tenga / al inicio
        base_search_url = self.base_url.rstrip('/') 
        search_segment = f'/-/publicacao/busca/{safe_keyword}'
        # Construir la URL completa del path
        url = f"{base_search_url}/{search_segment}"
        # --- Fin construcción manual ---

        items_per_page = self.config.get('keyword_search_items_per_page', 10)
        
        # Essential parameters identified
        query_params = {
            '_buscapublicacao_WAR_pcebusca6_1portlet_cur': page_number,
            '_buscapublicacao_WAR_pcebusca6_1portlet_delta': items_per_page,
            # Tentative additional parameters (can be moved to config if needed)
            '_buscapublicacao_WAR_pcebusca6_1portlet_isRoute': 'false',
            '_buscapublicacao_WAR_pcebusca6_1portlet_advancedSearch': 'false',
            '_buscapublicacao_WAR_pcebusca6_1portlet_andOperator': 'true',
            '_buscapublicacao_WAR_pcebusca6_1portlet_ordenarPor': self.config.get('keyword_search_order_by', 'relevancia-ordenacao'),
            '_buscapublicacao_WAR_pcebusca6_1portlet_direcao': self.config.get('keyword_search_direction', 'asc'),
            '_buscapublicacao_WAR_pcebusca6_1portlet_resetCur': 'false'
        }
        
        # Filter out any params with None values if we make some optional later
        # query_params = {k: v for k, v in query_params.items() if v is not None}

        if query_params:
            return f"{url}?{urlencode(query_params)}"
        return url

    def _parse_search_results(self, html_content):
        """
        Parses the HTML content of a search results page to extract item details and pagination info.

        Args:
            html_content (str): The HTML content of the search results page.

        Returns:
            tuple: (list_of_items, next_page_url_or_none)
                     Each item in list_of_items is a dict: {'title': str, 'item_page_url': str}
        """
        if not html_content:
            self.logger.warning("HTML content is empty, cannot parse search results.")
            return [], None

        try:
            tree = html.fromstring(html_content)
        except Exception as e:
            self.logger.error(f"Failed to parse HTML content: {e}")
            return [], None

        items = []
        item_container_xpath = self.selectors.get('result_item_container')
        title_link_xpath = self.selectors.get('result_item_title_link')
        url_xpath = self.selectors.get('result_item_url')

        if not all([item_container_xpath, title_link_xpath, url_xpath]):
            self.logger.error("Missing one or more critical selectors for parsing search results.")
            return [], None

        for container in tree.xpath(item_container_xpath):
            title_elements = container.xpath(title_link_xpath)
            url_elements = container.xpath(url_xpath)

            if title_elements and url_elements:
                title = title_elements[0].text_content().strip()
                item_page_path = url_elements[0].strip()
                item_page_url = urljoin(self.base_url, item_page_path) # Ensure it's absolute
                
                if title and item_page_url:
                    items.append({
                        'title': title,
                        'item_page_url': item_page_url,
                        'keyword_source': True # Flag to indicate it came from keyword search
                    })
                else:
                    self.logger.debug(f"Found item container but title or URL was empty. Title: '{title}', URL: '{item_page_url}'") 
            else:
                self.logger.debug("Item container found but could not extract title_link or url_link element.")
        
        next_page_url = None
        next_page_link_xpath = self.selectors.get('next_page_link')
        if next_page_link_xpath:
            next_page_elements = tree.xpath(next_page_link_xpath)
            if next_page_elements and next_page_elements[0].get('href'):
                # The href might be a full URL or just a path, ensure it's absolute
                # Also, it might be javascript:; if it's the last page and link is disabled
                next_href = next_page_elements[0].get('href')
                if next_href and not next_href.startswith('javascript:'):
                    next_page_url = urljoin(self.base_url, next_href) 
                    self.logger.debug(f"Found next page link: {next_page_url}")
                else:
                    self.logger.info("No more 'Próximo' link or it is disabled.")
            else:
                self.logger.info("No 'Próximo' link found on the page.")

        # Placeholder for total results extraction - may not be strictly needed if we rely on next_page_link
        # total_results_text_xpath = self.selectors.get('total_results_text')
        # if total_results_text_xpath:
        #     text_elements = tree.xpath(total_results_text_xpath)
        #     if text_elements:
        #         raw_text = text_elements[0].strip()
        #         # Add regex to extract number: e.g., re.search(r'de\s*(\d{1,3}(?:\.\d{3})*)\s*resultados', raw_text)
        #         self.logger.debug(f"Raw total results text: {raw_text}")

        return items, next_page_url

    def search_and_register_keyword(self, keyword, max_pages=None, max_items_per_keyword=None):
        """
        Searches for a keyword, extracts item URLs, and registers them in the database.

        Args:
            keyword (str): The keyword to search for.
            max_pages (int, optional): Maximum number of result pages to process. Defaults to None (all pages).
            max_items_per_keyword (int, optional): Maximum number of items to save for this keyword. Defaults to None (all items).
        """
        self.logger.info(f"Starting keyword search for: '{keyword}' using Selenium")
        current_page_number = 1
        processed_item_count = 0
        
        while True:
            if max_pages is not None and current_page_number > max_pages:
                self.logger.info(f"Reached max_pages limit ({max_pages}) for keyword '{keyword}'.")
                break
            
            if max_items_per_keyword is not None and processed_item_count >= max_items_per_keyword:
                self.logger.info(f"Reached max_items_per_keyword limit ({max_items_per_keyword}) for keyword '{keyword}'.")
                break

            search_url = self._build_search_url(keyword, current_page_number)
            self.logger.info(f"Fetching search results page {current_page_number} for '{keyword}' with Selenium: {search_url}")
            
            html_content = self._get_html_with_selenium(search_url)

            # --- Inicio: Guardar HTML para Depuración ---
            if current_page_number == 1 and html_content: # Guardar solo la primera página para análisis
                debug_file_path = os.path.join(self.config.get('output_dir', 'BR/output'), 'debug_search_results_page_1.html')
                try:
                    os.makedirs(os.path.dirname(debug_file_path), exist_ok=True)
                    with open(debug_file_path, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                    self.logger.info(f"DEBUG: HTML de la página 1 de resultados guardado en {debug_file_path}")
                except Exception as e_write:
                    self.logger.error(f"DEBUG: Error al guardar el HTML de depuración: {e_write}")
            # --- Fin: Guardar HTML para Depuración ---

            if not html_content:
                self.logger.warning(f"No content fetched for {search_url}. Stopping search for '{keyword}' at page {current_page_number}.")
                break

            items_on_page, next_page_url_from_parser = self._parse_search_results(html_content)

            if not items_on_page and not next_page_url_from_parser and current_page_number > 1:
                # If no items and no next page, and not the first page, likely end of results
                self.logger.info(f"No items found and no next page link on page {current_page_number} for '{keyword}'. Assuming end of results.")
                break
            elif not items_on_page and current_page_number == 1:
                 self.logger.info(f"No items found on the first page for keyword '{keyword}'.")
                 # break or continue based on next_page_url_from_parser might be desired if empty first pages can happen

            for item_data in items_on_page:
                if max_items_per_keyword is not None and processed_item_count >= max_items_per_keyword:
                    break # Break inner loop as well
                
                # Construct a unique identifier if not directly available
                # For now, item_page_url can serve as a unique key for "discovered" items.
                # We create an oai_identifier based on the source and URL for keyword items
                repo_source = 'keyword_search_embrapa' # Definir una fuente
                oai_identifier = f"{repo_source}:{item_data['item_page_url']}" 
                
                # Usar get_or_create_item para registrar o encontrar el ítem
                item_id, current_status = self.db_manager.get_or_create_item(
                    item_page_url=item_data['item_page_url'],
                    repository_source=repo_source, # Guardar la fuente
                    oai_identifier=oai_identifier, # Guardar el ID generado
                    discovery_mode='keyword_search', # Indicar modo descubrimiento
                    search_keyword=keyword, # Guardar la keyword que lo encontró
                    initial_status='pending_html_processing' # ESTADO INICIAL CORRECTO
                )
                
                # Solo proceder a loguear metadatos iniciales si es realmente nuevo o si queremos actualizar
                # El status devuelto por get_or_create_item nos dice si ya existía
                if item_id and current_status == 'pending_html_processing': # Loguear solo si acaba de ser creado con este status
                    self.logger.debug(f"Nuevo ítem ID {item_id} encontrado por keyword '{keyword}', registrando metadatos iniciales: {item_data['title']}")
                    self.db_manager.log_item_metadata(
                        item_id=item_id, 
                        metadata_dict={'title': item_data['title'], 'item_page_url': item_data['item_page_url'], 'source': 'keyword_search'}, 
                        # No HTML path yet
                    )
                    processed_item_count += 1
                elif item_id:
                    self.logger.debug(f"Item {item_data['item_page_url']} (ID: {item_id}) ya existía con estado '{current_status}', no se registra como nuevo para la keyword '{keyword}'.")
                else:
                     self.logger.warning(f"No se pudo obtener/crear item_id para {item_data['item_page_url']} desde keyword '{keyword}'.")
            
            self.logger.info(f"Processed {len(items_on_page)} items from page {current_page_number} for keyword '{keyword}'. Total for keyword: {processed_item_count}")

            if not next_page_url_from_parser:
                self.logger.info(f"No more pages found for keyword '{keyword}' after page {current_page_number}.")
                break
            
            current_page_number += 1
            # Optional: add a delay between requests
            # time.sleep(self.config.get('keyword_search_delay', 1))

        self.logger.info(f"Finished keyword search for '{keyword}'. Found and registered {processed_item_count} new items.")
        return processed_item_count


if __name__ == '__main__':
    # Basic test setup (requires mock objects or a simple config)
    print("Running basic KeywordSearcherBR tests...")

    class MockLogger:
        def info(self, msg): print(f"INFO: {msg}")
        def warning(self, msg): print(f"WARN: {msg}")
        def error(self, msg): print(f"ERROR: {msg}")
        def debug(self, msg): print(f"DEBUG: {msg}")

    class MockDBManager:
        def __init__(self):
            self.items = set()
        def check_item_exists(self, oai_identifier):
            return oai_identifier in self.items
        def register_item(self, oai_identifier, item_page_url, status, metadata_dict=None):
            self.items.add(oai_identifier)
            print(f"DB: Registered {oai_identifier} - {item_page_url} - {status}")
        def log_item_metadata(self, oai_identifier, metadata_dict, status):
            print(f"DB_LOG: {oai_identifier} - {metadata_dict} - {status}")

    class MockDownloader:
        def fetch_content(self, url, is_html_snapshot=True):
            print(f"DOWNLOADER: Fetching {url} (snapshot: {is_html_snapshot})")
            if "page=1" in url and "testkeyword" in url: # Simulate first page with results
                return """
                <html><body>
                    <div class='results publicacao-de-busca'>
                        <table><tbody>
                            <tr><td><a href='/-/publicacao/item1'>Item 1 Title</a></td></tr>
                            <tr><td><a href='/-/publicacao/item2'>Item 2 Title</a></td></tr>
                        </tbody></table>
                    </div>
                    <div class='pagination-bar'><a href='page=2_link'>Próximo</a></div>
                    <div class='pagination-results'>Mostrando 1 - 2 de 4 resultados.</div>
                </body></html>
                """
            elif "page=2" in url and "testkeyword" in url: # Simulate second page with results
                return """
                <html><body>
                    <div class='results publicacao-de-busca'>
                        <table><tbody>
                            <tr><td><a href='/-/publicacao/item3'>Item 3 Title</a></td></tr>
                        </tbody></table>
                    </div>
                    <div class='pagination-bar'><a href='javascript:;'>Próximo</a></div>
                     <div class='pagination-results'>Mostrando 3 - 3 de 3 resultados.</div>
                </body></html>
                """
            elif "emptykeyword" in url: 
                 return "<html><body><div class='pagination-results'>Mostrando 0 - 0 de 0 resultados.</div></body></html>"
            return ""

    test_config = {
        'base_url_embrapa_search': 'https://www.embrapa.br/busca-de-publicacoes',
        'selectors': {
            'keyword_search_embrapa': {
                'result_item_container': "//div[contains(@class, 'results') and contains(@class, 'publicacao-de-busca')]//table/tbody/tr",
                'result_item_title_link': ".//td//a[contains(@href, '/-/publicacao/') and normalize-space(text())][1]",
                'result_item_url': ".//td//a[contains(@href, '/-/publicacao/') and normalize-space(text())][1]/@href",
                'next_page_link': "//div[contains(@class, 'pagination-bar')]//a[normalize-space(text())='Próximo']",
                'total_results_text': "//div[contains(@class, 'pagination-results')]/text()"
            }
        },
        'keyword_search_items_per_page': 2, # For test
        'keyword_search_order_by': 'relevancia-ordenacao',
        'keyword_search_direction': 'asc'
    }

    logger = MockLogger()
    db_manager = MockDBManager()
    downloader = MockDownloader()

    searcher = KeywordSearcherBR(test_config, logger, db_manager, downloader, test_config['selectors'])
    
    # Test URL building
    built_url = searcher._build_search_url("test keyword", page_number=2)
    print(f"Built URL: {built_url}")
    # Expected: https://www.embrapa.br/busca-de-publicacoes/-/publicacao/busca/test-keyword?_buscapublicacao_WAR_pcebusca6_1portlet_cur=2&_buscapublicacao_WAR_pcebusca6_1portlet_delta=2&_buscapublicacao_WAR_pcebusca6_1portlet_isRoute=false&_buscapublicacao_WAR_pcebusca6_1portlet_advancedSearch=false&_buscapublicacao_WAR_pcebusca6_1portlet_andOperator=true&_buscapublicacao_WAR_pcebusca6_1portlet_ordenarPor=relevancia-ordenacao&_buscapublicacao_WAR_pcebusca6_1portlet_direcao=asc&_buscapublicacao_WAR_pcebusca6_1portlet_resetCur=false
    # Check if it matches expectations
    
    # Test parsing (using mock downloader content)
    print("\n--- Testing Parser ---")
    mock_html_page1 = downloader.fetch_content("https://www.embrapa.br/testkeyword?page=1", False)
    parsed_items, next_page = searcher._parse_search_results(mock_html_page1)
    print(f"Parsed Items (Page 1): {parsed_items}")
    print(f"Next Page URL (Page 1): {next_page}")
    assert len(parsed_items) == 2
    assert parsed_items[0]['title'] == 'Item 1 Title'
    assert next_page == 'https://www.embrapa.br/page=2_link'

    mock_html_page2 = downloader.fetch_content("https://www.embrapa.br/testkeyword?page=2", False)
    parsed_items_p2, next_page_p2 = searcher._parse_search_results(mock_html_page2)
    print(f"Parsed Items (Page 2): {parsed_items_p2}")
    print(f"Next Page URL (Page 2): {next_page_p2}")
    assert len(parsed_items_p2) == 1
    assert next_page_p2 is None # Next link is javascript:;

    # Test full search and register
    print("\n--- Testing Full Search and Register (testkeyword) ---")
    db_manager.items = set() # Reset DB for this test
    count = searcher.search_and_register_keyword("testkeyword", max_pages=5)
    print(f"Total items registered for 'testkeyword': {count}")
    assert count == 3 # 2 from page 1, 1 from page 2

    print("\n--- Testing Full Search and Register (emptykeyword) ---")
    db_manager.items = set() # Reset DB for this test
    count_empty = searcher.search_and_register_keyword("emptykeyword", max_pages=5)
    print(f"Total items registered for 'emptykeyword': {count_empty}")
    assert count_empty == 0

    print("\n--- Testing Max Items per Keyword ---")
    db_manager.items = set() # Reset DB
    count_max_items = searcher.search_and_register_keyword("testkeyword", max_items_per_keyword=1)
    print(f"Total items registered for 'testkeyword' (max_items=1): {count_max_items}")
    assert count_max_items == 1

    print("\nKeywordSearcherBR basic tests completed.")
