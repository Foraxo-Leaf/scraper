import time
import logging
from urllib.parse import urlencode, urljoin, quote_plus

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
# from webdriver_manager.chrome import ChromeDriverManager # Opcional, para gestión automática del driver

from lxml import html # Para parsear el HTML obtenido por Selenium

class KeywordSearcherBR:
    """
    Clase para buscar publicaciones por palabras clave en portales web,
    adaptada para el scraper de Brasil (BR).
    Utiliza Selenium para la navegación y obtención de HTML si es necesario.
    """

    def __init__(self, config, logger, db_manager, downloader, selectors_dict):
        """
        Inicializa el KeywordSearcherBR.

        Args:
            config (dict): Configuración del scraper.
            logger (logging.Logger): Instancia del logger.
            db_manager (DatabaseManagerBR): Instancia del gestor de base de datos.
            downloader (ResourceDownloaderBR): Instancia para descargar recursos.
            selectors_dict (dict): Diccionario con los selectores XPath/CSS.
        """
        self.config = config
        self.logger = logger if logger else logging.getLogger(__name__)
        self.db_manager = db_manager
        self.downloader = downloader # Podría usarse para descargar snapshots HTML de resultados
        self.selectors_dict = selectors_dict.get('web_search_results', {})

        self.search_config = self.config.get('search_config', {})
        self.base_search_url = self.search_config.get('base_url')
        self.query_param = self.search_config.get('query_param', 'termo') # ej. 'q', 'query', 'palavras_chave', 'termo'
        self.page_param = self.search_config.get('page_param', 'pagina') # ej. 'page', 'p', 'start', 'pagina'
        self.results_per_page = self.search_config.get('results_per_page', 20) # Estimado o fijo
        self.max_pages_to_search = self.search_config.get('max_pages_per_keyword', 5)
        self.max_items_to_process = self.config.get('max_items_keyword_search', 100)
        
        self.use_selenium = self.search_config.get('use_selenium', True) # Si el sitio requiere JS
        self.selenium_timeout = self.search_config.get('selenium_page_load_timeout', 30)
        self.selenium_driver_path = self.config.get('selenium_chrome_driver_path', None) # ej. '/usr/bin/chromedriver'
        self.selenium_implicit_wait = self.search_config.get('selenium_implicit_wait', 10)
        self.selenium_wait_for_element_xpath = self.selectors_dict.get('item_container', None) # Esperar a que los contenedores de items carguen
        self.search_delay_between_pages = self.search_config.get('delay_between_search_pages', 3) # segundos
        self.user_agent = self.config.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')

        self.driver = None # Se inicializará si se usa Selenium

    def _configure_webdriver_options(self):
        """Configura las opciones para el WebDriver de Chrome."""
        options = webdriver.ChromeOptions()
        options.add_argument("--headless") # Ejecutar en modo headless
        options.add_argument("--no-sandbox") # Necesario para ejecutar como root en algunos entornos (ej. Docker)
        options.add_argument("--disable-dev-shm-usage") # Superar recursos limitados
        options.add_argument("--disable-gpu") # A veces necesario en headless
        options.add_argument(f"user-agent={self.user_agent}")
        options.add_argument("--lang=pt-BR,pt;q=0.9,es;q=0.8,en;q=0.7") # Preferir portugués
        options.add_argument("--window-size=1920x1080")
        # options.add_experimental_option('excludeSwitches', ['enable-logging']) # Para limpiar logs de consola
        return options

    def _init_webdriver(self):
        """Inicializa el WebDriver de Selenium."""
        if not self.use_selenium:
            self.logger.info("Selenium no está habilitado para la búsqueda por palabra clave.")
            return False
        
        if self.driver:
            return True # Ya inicializado

        self.logger.info("Inicializando WebDriver de Selenium (Chrome)...")
        try:
            options = self._configure_webdriver_options()
            if self.selenium_driver_path:
                service = ChromeService(executable_path=self.selenium_driver_path)
                self.driver = webdriver.Chrome(service=service, options=options)
            else:
                # Intentar usar webdriver_manager si está disponible y configurado (no por defecto aquí)
                # self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
                # O asumir que chromedriver está en el PATH
                self.logger.info("No se especificó selenium_chrome_driver_path. Asumiendo que chromedriver está en el PATH.")
                self.driver = webdriver.Chrome(options=options)
            
            self.driver.implicitly_wait(self.selenium_implicit_wait)
            self.driver.set_page_load_timeout(self.selenium_timeout)
            self.logger.info("WebDriver de Selenium inicializado exitosamente.")
            return True
        except WebDriverException as e:
            self.logger.error(f"Error inicializando WebDriver de Selenium: {e}")
            self.logger.error("Asegúrate de que ChromeDriver esté instalado y en el PATH, o especifica 'selenium_chrome_driver_path' en la config.")
            self.driver = None
            return False
        except Exception as e:
            self.logger.error(f"Error inesperado inicializando WebDriver: {e}")
            self.driver = None
            return False

    def _get_html_with_selenium(self, url):
        """
        Obtiene el contenido HTML de una URL usando Selenium.
        """
        if not self.driver:
            if not self._init_webdriver(): # Intenta inicializar si aún no lo está
                return None
        
        self.logger.info(f"Navegando a (Selenium): {url}")
        try:
            self.driver.get(url)
            # Esperar a que un elemento clave de los resultados esté presente (si se configuró)
            if self.selenium_wait_for_element_xpath:
                WebDriverWait(self.driver, self.selenium_timeout).until(
                    EC.presence_of_all_elements_located((By.XPATH, self.selenium_wait_for_element_xpath))
                )
                self.logger.debug(f"Elemento esperado ({self.selenium_wait_for_element_xpath}) encontrado en la página.")
            else: # Espera genérica si no hay selector específico
                time.sleep(self.config.get('selenium_default_load_wait', 5)) 

            html_content = self.driver.page_source
            self.logger.info(f"HTML obtenido con Selenium desde {url} (longitud: {len(html_content)})")
            return html_content
        except TimeoutException:
            self.logger.error(f"Timeout esperando a que cargue la página o el elemento en (Selenium): {url}")
        except WebDriverException as e:
            self.logger.error(f"Error de WebDriver obteniendo HTML de {url}: {e}")
        except Exception as e:
            self.logger.error(f"Error inesperado obteniendo HTML con Selenium de {url}: {e}")
        return None

    def _get_html_with_requests(self, url):
        """
        Obtiene el contenido HTML de una URL usando Requests (si Selenium no es necesario).
        """
        self.logger.info(f"Obteniendo HTML con Requests desde: {url}")
        headers = {
            'User-Agent': self.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,es;q=0.8,en;q=0.7',
        }
        try:
            response = requests.get(url, headers=headers, timeout=self.config.get('requests_timeout', 30))
            response.raise_for_status()
            response.encoding = response.apparent_encoding if response.apparent_encoding else 'utf-8'
            html_content = response.text
            self.logger.info(f"HTML obtenido con Requests desde {url} (longitud: {len(html_content)})")
            return html_content
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Error HTTP {e.response.status_code} obteniendo {url} con Requests: {e.response.text[:200]}...")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error de Requests obteniendo HTML de {url}: {e}")
        return None
    
    def _fetch_page_html(self, url):
        """Decide si usar Selenium o Requests para obtener el HTML."""
        if self.use_selenium:
            return self._get_html_with_selenium(url)
        else:
            return self._get_html_with_requests(url)

    def _build_search_url(self, keyword, page_number=1):
        """
        Construye la URL de búsqueda para una palabra clave y número de página dados.
        El portal de Embrapa parece usar `https://www.embrapa.br/busca-de-publicacoes?termo=<keyword>&pagina=<num>`
        pero esto podría variar o necesitar ajustes.
        """
        if not self.base_search_url:
            self.logger.error("URL base de búsqueda no configurada ('search_config.base_url').")
            return None
        
        # Codificar la palabra clave para la URL
        encoded_keyword = quote_plus(keyword)
        
        params = {self.query_param: encoded_keyword}
        
        # El manejo de la paginación varía mucho:
        # 1. Parámetro de página (ej. page=2)
        # 2. Parámetro de offset/start (ej. start=20 si results_per_page=20)
        # 3. Parte de la URL path (ej. /search/keyword/page/2)
        # Asumimos un parámetro de página por defecto.
        if page_number > 1:
            # El portal de Embrapa parece usar 'pagina' y es 0-indexed o 1-indexed.
            # Si 'pagina' es el parámetro y es 1-indexed:
            # params[self.page_param] = page_number
            # Si es 0-indexed, sería page_number - 1.
            # La URL de ejemplo de Embrapa usa `pagina=2` para la segunda página, así que parece 1-indexed.
            params[self.page_param] = page_number
        
        # Construir la URL con los parámetros
        # No usamos urlencode directamente en base_search_url si ya tiene un ' ? '
        if '?' in self.base_search_url:
            url_parts = list(urlparse(self.base_search_url))
            query = parse_qs(url_parts[4])
            query.update(params)
            url_parts[4] = urlencode(query, doseq=True)
            search_url = urlunparse(url_parts)
        else:
            search_url = f"{self.base_search_url}?{urlencode(params)}"
            
        self.logger.debug(f"URL de búsqueda construida: {search_url}")
        return search_url

    def _parse_search_results(self, html_content, search_url_base_for_relative_links):
        """
        Parsea el contenido HTML de una página de resultados de búsqueda.

        Args:
            html_content (str): Contenido HTML de la página.
            search_url_base_for_relative_links (str): URL base para resolver enlaces relativos.

        Returns:
            tuple: (list_of_items, next_page_url)
                   list_of_items: lista de diccionarios, cada uno con 'title' y 'item_page_url'.
                   next_page_url: URL de la siguiente página de resultados, o None.
        """
        if not html_content:
            return [], None
        
        try:
            doc = html.fromstring(html_content)
        except Exception as e:
            self.logger.error(f"Error parseando HTML de resultados de búsqueda: {e}")
            return [], None

        found_items = []
        item_container_xpath = self.selectors_dict.get('item_container')
        item_link_xpath = self.selectors_dict.get('item_link')
        item_title_xpath = self.selectors_dict.get('item_title') # Opcional, para logueo

        if not item_container_xpath or not item_link_xpath:
            self.logger.error("Selectores críticos para resultados de búsqueda no configurados: 'item_container' o 'item_link'. Verifica 'web_search_results' en selectors.yaml")
            return [], None

        self.logger.debug(f"Usando selector de contenedor: {item_container_xpath}")
        for container in doc.xpath(item_container_xpath):
            try:
                link_elements = container.xpath(item_link_xpath)
                if link_elements:
                    relative_item_url = str(link_elements[0]).strip()
                    item_page_url = urljoin(search_url_base_for_relative_links, relative_item_url)
                    
                    title = "N/A"
                    if item_title_xpath:
                        title_elements = container.xpath(item_title_xpath)
                        if title_elements:
                            title = str(title_elements[0]).strip()
                    
                    found_items.append({'title': title, 'item_page_url': item_page_url})
                    self.logger.debug(f"Ítem encontrado: '{title[:50]}...' en {item_page_url}")
                else:
                    self.logger.warning(f"Contenedor de ítem encontrado pero sin enlace usando selector: {item_link_xpath}")
            except Exception as e:
                self.logger.error(f"Error extrayendo datos de un ítem individual en resultados de búsqueda: {e}")
        
        self.logger.info(f"Se encontraron {len(found_items)} ítems en esta página de resultados.")

        # Encontrar enlace a la siguiente página
        next_page_url = None
        next_page_selector = self.selectors_dict.get('next_page_link')
        if next_page_selector:
            next_page_elements = doc.xpath(next_page_selector)
            if next_page_elements:
                relative_next_url = str(next_page_elements[0]).strip()
                next_page_url = urljoin(search_url_base_for_relative_links, relative_next_url)
                self.logger.info(f"Enlace a la siguiente página de resultados encontrado: {next_page_url}")
            else:
                self.logger.info("No se encontró enlace a la siguiente página de resultados usando el selector.")
        else:
            self.logger.info("No hay selector configurado para 'next_page_link'. Asumiendo que no hay más páginas o se maneja diferente.")
            
        return found_items, next_page_url

    def search_by_keyword(self, keyword):
        """
        Realiza una búsqueda por palabra clave y procesa los resultados.

        Args:
            keyword (str): La palabra clave a buscar.

        Returns:
            int: Número de ítems procesados y guardados/actualizados en la BD.
        """
        if not self.base_search_url:
            self.logger.error("Búsqueda por palabra clave no configurada (falta 'base_search_url').")
            return 0
        
        if self.use_selenium and not self.driver:
            if not self._init_webdriver(): # Asegurar que el driver esté listo
                self.logger.error("No se pudo inicializar Selenium. Abortando búsqueda por palabra clave.")
                return 0
        
        self.logger.info(f"--- Iniciando búsqueda por palabra clave: '{keyword}' ---")
        
        current_page_number = 1
        processed_item_count = 0
        consecutive_empty_pages = 0
        max_consecutive_empty_before_stop = 2 # Detener si N páginas seguidas no devuelven ítems

        while current_page_number <= self.max_pages_to_search:
            if processed_item_count >= self.max_items_to_process:
                self.logger.info(f"Se alcanzó el límite de {self.max_items_to_process} ítems a procesar para la palabra clave '{keyword}'.")
                break

            search_url = self._build_search_url(keyword, current_page_number)
            if not search_url:
                self.logger.error("No se pudo construir la URL de búsqueda. Abortando.")
                break
            
            self.logger.info(f"Buscando en: {search_url} (Página {current_page_number}/{self.max_pages_to_search})")
            html_content = self._fetch_page_html(search_url)
            
            if not html_content:
                self.logger.warning(f"No se pudo obtener HTML para {search_url}. Intentando siguiente página si aplica.")
                # Podríamos considerar esto como una página vacía o un error más serio.
                # Por ahora, si falla la obtención, podría ser el fin o un problema temporal.
                consecutive_empty_pages += 1 
                if consecutive_empty_pages >= max_consecutive_empty_before_stop:
                    self.logger.warning(f"{max_consecutive_empty_before_stop} páginas consecutivas sin resultados/HTML. Deteniendo búsqueda para '{keyword}'.")
                    break
                current_page_number += 1
                if current_page_number <= self.max_pages_to_search and self.search_delay_between_pages > 0:
                    self.logger.info(f"Esperando {self.search_delay_between_pages}s antes de la siguiente página de búsqueda...")
                    time.sleep(self.search_delay_between_pages)
                continue # Saltar al siguiente ciclo del while para la próxima página

            # Usar la URL base del portal para resolver enlaces relativos de los items
            # Si base_search_url es algo como https://example.com/search, la base es https://example.com
            parsed_search_url = urlparse(self.base_search_url)
            base_url_for_relative_links = f"{parsed_search_url.scheme}://{parsed_search_url.netloc}"

            items_on_page, next_page_link_from_parser = self._parse_search_results(html_content, base_url_for_relative_links)

            if not items_on_page:
                self.logger.info(f"No se encontraron ítems en la página {current_page_number} para '{keyword}'.")
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_consecutive_empty_before_stop:
                    self.logger.warning(f"{max_consecutive_empty_before_stop} páginas consecutivas sin ítems. Deteniendo búsqueda para '{keyword}'.")
                    break
            else:
                consecutive_empty_pages = 0 # Resetear contador si se encontraron ítems

            for item_data in items_on_page:
                if processed_item_count >= self.max_items_to_process:
                    self.logger.info(f"Límite de {self.max_items_to_process} ítems alcanzado a mitad de página.")
                    break # Salir del bucle for de items
                try:
                    self.logger.debug(f"Procesando ítem de búsqueda: Título='{item_data['title'][:50]}...', URL='{item_data['item_page_url']}'")
                    item_id, created = self.db_manager.get_or_create_item(
                        item_page_url=item_data['item_page_url'],
                        repository_source=f"keyword_search_{urlparse(self.base_search_url).netloc}", # Fuente: ej. keyword_search_www.embrapa.br
                        # oai_identifier será None para items de búsqueda por palabra clave inicialmente
                    )
                    if created:
                        self.logger.info(f"Nuevo ítem (de búsqueda) creado en BD con ID {item_id} para URL: {item_data['item_page_url']}")
                        # Guardar metadatos básicos si los tenemos (título)
                        self.db_manager.log_item_metadata(item_id, {'title': item_data['title'], 'item_page_url': item_data['item_page_url']}, source_type='keyword_search_result')
                        self.db_manager.update_item_status(item_id, 'pending_html_processing') # Necesita que se procese su HTML para metadatos y PDF
                    else:
                        self.logger.info(f"Ítem existente (de búsqueda o OAI previo) encontrado en BD con ID {item_id} para URL: {item_data['item_page_url']}")
                        # Si ya existe, no actualizamos estado a menos que queramos forzar reprocesamiento
                    
                    processed_item_count += 1
                except Exception as e:
                    self.logger.error(f"Error procesando/guardando en BD el ítem de búsqueda {item_data.get('item_page_url', 'URL_DESCONOCIDA')}: {e}")
            
            if processed_item_count >= self.max_items_to_process:
                break # Salir del bucle while si se alcanzó el límite después de procesar la página

            if next_page_link_from_parser: # Si el parser encontró un enlace a la siguiente página
                # No necesitamos construir la URL, ya la tenemos. Simplemente avanzamos.
                # Podríamos querer validar si esta URL es la misma que construiríamos nosotros, pero por ahora confiamos en el parser.
                self.logger.info(f"Avanzando a la siguiente página de resultados: {next_page_link_from_parser}")
                # En este caso, no usamos current_page_number para construir la URL, pero sí para el log y el límite de max_pages
                current_page_number += 1
            elif self.page_param: # Si no hay enlace explícito pero sí un parámetro de página, intentamos la siguiente
                current_page_number += 1
            else: # No hay cómo saber la siguiente página
                self.logger.info(f"No hay enlace a la siguiente página ni parámetro de página configurado. Fin de la búsqueda para '{keyword}'.")
                break
            
            if current_page_number <= self.max_pages_to_search and self.search_delay_between_pages > 0:
                self.logger.info(f"Esperando {self.search_delay_between_pages}s antes de la siguiente página de búsqueda...")
                time.sleep(self.search_delay_between_pages)
        
        self.logger.info(f"--- Búsqueda por palabra clave '{keyword}' finalizada. Total ítems procesados/registrados: {processed_item_count} ---")
        return processed_item_count

    def close(self):
        """Cierra el WebDriver de Selenium si está abierto."""
        if self.driver:
            try:
                self.logger.info("Cerrando WebDriver de Selenium...")
                self.driver.quit()
                self.driver = None
                self.logger.info("WebDriver de Selenium cerrado.")
            except Exception as e:
                self.logger.error(f"Error cerrando WebDriver de Selenium: {e}")

# Ejemplo de uso (requiere mocks o una BD, config, etc. reales)
if __name__ == '__main__':
    # --- Mockups para prueba --- 
    class MockLogger:
        def info(self, msg): print(f"INFO: {msg}")
        def error(self, msg): print(f"ERROR: {msg}")
        def warning(self, msg): print(f"WARNING: {msg}")
        def debug(self, msg): print(f"DEBUG: {msg}")

    class MockDBManager:
        def get_or_create_item(self, item_page_url, repository_source, oai_identifier=None):
            print(f"DB: Get/Create item URL {item_page_url} from {repository_source}")
            return f"fake_item_id_for_{item_page_url.split('/')[-1]}", True # Simula creación
        def log_item_metadata(self, item_id, metadata_dict, source_type):
            print(f"DB: Logged metadata for item {item_id} (source: {source_type}), Title: {metadata_dict.get('title', 'N/A')[:30]}")
        def update_item_status(self, item_id, status):
            print(f"DB: Updated status for item {item_id} to {status}")

    mock_logger = MockLogger()
    mock_db_manager = MockDBManager()
    # Configuración de ejemplo para el portal de Embrapa
    mock_config = {
        'search_config': {
            'base_url': 'https://www.embrapa.br/busca-de-publicacoes',
            'query_param': 'termo',
            'page_param': 'pagina',
            'use_selenium': True, # Cambiar a False para probar con Requests si el sitio lo permite
            'max_pages_per_keyword': 2, # Limitar para prueba
        },
        'max_items_keyword_search': 5, # Limitar para prueba
        'user_agent': 'TestKeywordSearcher/1.0',
        # 'selenium_chrome_driver_path': '/ruta/a/tu/chromedriver' # Descomentar y poner ruta real si es necesario
        'selectors_file_br': 'selectors.yaml' # Nombre del archivo, no el dict aquí
    }
    # Para la prueba, necesitaríamos cargar los selectores de un archivo o mockearlos
    mock_selectors_dict_for_searcher = {
        'web_search_results': {
            'item_container': "//div[contains(@class, 'conteudoListaResultado')]//li[contains(@class, ' ResultadoBuscaPublicacao')]",
            'item_link': ".//h3[contains(@class, 'titulo')]/a/@href",
            'item_title': ".//h3[contains(@class, 'titulo')]/a/text()",
            'next_page_link': "//a[@class='proximo' and contains(text(), 'Próxima')]/@href"
        }
    }
    # --- Fin Mockups ---

    searcher = KeywordSearcherBR(mock_config, mock_logger, mock_db_manager, None, mock_selectors_dict_for_searcher)

    mock_logger.info("--- Iniciando prueba del KeywordSearcherBR ---")
    keyword_to_test = "maiz" # "soja", "trigo", "algodon"
    
    if not searcher.use_selenium or searcher._init_webdriver(): # Solo buscar si selenium se inicializa bien (o no se usa)
        items_found = searcher.search_by_keyword(keyword_to_test)
        mock_logger.info(f"Búsqueda por '{keyword_to_test}' completada. Ítems procesados/registrados: {items_found}")
    else:
        mock_logger.error("No se pudo inicializar Selenium, la prueba de búsqueda no se ejecutará.")

    searcher.close() # Importante para cerrar el navegador Selenium
    mock_logger.info("--- Prueba del KeywordSearcherBR finalizada ---")
