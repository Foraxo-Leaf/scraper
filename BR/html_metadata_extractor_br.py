import requests
from lxml import html
import logging

class HTMLMetadataExtractorBR:
    """
    Clase para extraer metadatos de páginas HTML, adaptada para el scraper de Brasil (BR).
    Se enfoca principalmente en extraer enlaces a PDFs, asumiendo que la mayoría de los
    metadatos bibliográficos provienen de OAI o de una fase de extracción más completa.
    """

    def __init__(self, config, logger, selectors_dict):
        """
        Inicializa el extractor de metadatos HTML.

        Args:
            config (dict): Configuración general del scraper.
            logger (logging.Logger): Instancia del logger.
            selectors_dict (dict): Diccionario con los selectores XPath/CSS.
        """
        self.config = config
        self.logger = logger if logger else logging.getLogger(__name__)
        self.selectors_dict = selectors_dict
        self.download_timeout = self.config.get('download_timeout', 30)
        self.max_retries = self.config.get('max_download_retries', 3)
        self.retry_delay = self.config.get('download_retry_delay', 5) # segundos

    def _fetch_html_content(self, url):
        """
        Obtiene el contenido HTML de una URL con manejo de reintentos y errores.

        Args:
            url (str): La URL de la cual obtener el HTML.

        Returns:
            lxml.html.HtmlElement: El contenido HTML parseado como un objeto lxml,
                                   o None si la descarga falla.
        """
        self.logger.info(f"Intentando obtener HTML de: {url}")
        user_agent = self.config.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Connection': 'keep-alive'
        }

        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=self.download_timeout, allow_redirects=True)
                response.raise_for_status() # Lanza HTTPError para respuestas 4xx/5xx
                
                # Verificar si el contenido es HTML
                content_type = response.headers.get('Content-Type', '')
                if 'text/html' not in content_type.lower():
                    self.logger.warning(f"La URL {url} no devolvió contenido HTML. Content-Type: {content_type}")
                    return None

                # Decodificar correctamente según el charset detectado o UTF-8 por defecto
                response.encoding = response.apparent_encoding if response.apparent_encoding else 'utf-8'
                html_content = response.text

                if not html_content:
                    self.logger.warning(f"Contenido HTML vacío para {url}.")
                    return None

                parsed_html = html.fromstring(html_content)
                self.logger.info(f"HTML obtenido y parseado exitosamente de {url}")
                return parsed_html
            except requests.exceptions.HTTPError as e:
                self.logger.error(f"Error HTTP {e.response.status_code} obteniendo {url}: {e}")
                if e.response.status_code in [403, 404, 503]: # Errores que no justifican reintento inmediato
                    break
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error de red obteniendo {url} (intento {attempt + 1}/{self.max_retries}): {e}")
            
            if attempt < self.max_retries - 1:
                self.logger.info(f"Reintentando en {self.retry_delay} segundos...")
                time.sleep(self.retry_delay)
            else:
                self.logger.error(f"Fallaron todos los intentos para obtener HTML de {url}")
        return None

    def extract_pdf_link(self, item_page_url, html_doc=None):
        """
        Extrae el enlace directo al PDF de una página de ítem.

        Args:
            item_page_url (str): URL de la página del ítem.
            html_doc (lxml.html.HtmlElement, optional): Documento HTML ya parseado.
                                                       Si es None, se descargará de item_page_url.

        Returns:
            str: La URL del PDF, o None si no se encuentra.
        """
        if not html_doc:
            html_doc = self._fetch_html_content(item_page_url)
        
        if html_doc is None:
            self.logger.warning(f"No se pudo obtener/procesar el HTML de {item_page_url} para extraer el enlace PDF.")
            return None

        pdf_selectors = self.selectors_dict.get('pdf_link_selectors', [])
        if not pdf_selectors:
            self.logger.warning("No se encontraron 'pdf_link_selectors' en la configuración de selectores.")
            return None

        for selector_set in pdf_selectors:
            if isinstance(selector_set, str): # Si es un solo selector
                selectors_to_try = [selector_set]
            elif isinstance(selector_set, list): # Si es una lista de selectores (intentar en orden)
                selectors_to_try = selector_set
            else:
                selectors_to_try = [str(selector_set)]

            for selector_xpath in selectors_to_try:
                try:
                    self.logger.debug(f"Intentando selector XPath para PDF: {selector_xpath} en {item_page_url}")
                    elements = html_doc.xpath(selector_xpath)
                    if elements:
                        pdf_url = elements[0]
                        if isinstance(pdf_url, html.HtmlElement):
                            pdf_url = pdf_url.text_content().strip()
                        else:
                            pdf_url = str(pdf_url).strip()
                        
                        # Resolver URL relativa
                        if not pdf_url.startswith(('http://', 'https://')):
                            from urllib.parse import urljoin
                            pdf_url = urljoin(item_page_url, pdf_url)
                        
                        self.logger.info(f"Enlace PDF encontrado con selector '{selector_xpath}': {pdf_url}")
                        return pdf_url
                except Exception as e:
                    self.logger.error(f"Error aplicando selector XPath para PDF '{selector_xpath}' en {item_page_url}: {e}")
        
        self.logger.warning(f"No se encontró enlace PDF para {item_page_url} con los selectores proporcionados.")
        return None

    def extract_all_metadata(self, item_page_url, html_doc=None):
        """
        Extrae todos los metadatos configurados de una página de ítem.
        NOTA: Para el scraper de Brasil, la fuente principal de metadatos es OAI.
        Esta función sería un complemento o para casos donde OAI no esté disponible
        o no provea toda la información necesaria.

        Args:
            item_page_url (str): URL de la página del ítem.
            html_doc (lxml.html.HtmlElement, optional): Documento HTML ya parseado.

        Returns:
            dict: Un diccionario con los metadatos extraídos.
        """
        if not html_doc:
            html_doc = self._fetch_html_content(item_page_url)
        
        if html_doc is None:
            self.logger.warning(f"No se pudo obtener/procesar el HTML de {item_page_url} para extraer metadatos.")
            return {}

        metadata_results = {}
        metadata_selectors_config = self.selectors_dict.get('metadata_selectors', {})

        if not metadata_selectors_config:
            self.logger.info(f"No hay 'metadata_selectors' definidos para {item_page_url}. La extracción de metadatos HTML se omitirá o será limitada.")
            # Aún así, intentamos extraer el enlace PDF si no se hizo antes
            pdf_link = self.extract_pdf_link(item_page_url, html_doc)
            if pdf_link:
                metadata_results['pdf_url_extracted_from_html'] = pdf_link
            return metadata_results

        self.logger.info(f"Extrayendo metadatos de {item_page_url} usando selectores HTML.")
        for field, selectors in metadata_selectors_config.items():
            if not isinstance(selectors, list):
                selectors = [selectors] # Asegurar que sea una lista
            
            for selector_xpath in selectors:
                try:
                    elements = html_doc.xpath(selector_xpath)
                    if elements:
                        # Tomar el primer resultado no vacío y limpiarlo
                        # Puede haber múltiples elementos (ej. varios autores), se concatenarían si es necesario
                        # o se tomaría el primero según la lógica del scraper principal.
                        # Aquí, para simplificar, tomamos el primero no vacío o concatenamos si son múltiples líneas de un mismo campo.
                        value = ' '.join([el.strip() if isinstance(el, str) else el.text_content().strip() for el in elements if (isinstance(el, str) and el.strip()) or (not isinstance(el, str) and el.text_content() and el.text_content().strip())])
                        if value:
                            metadata_results[field] = value
                            self.logger.debug(f"Metadato '{field}' encontrado con selector '{selector_xpath}': {value[:100]}...")
                            break # Tomar el primer selector exitoso para este campo
                except Exception as e:
                    self.logger.error(f"Error aplicando selector XPath para metadato '{field}' ('{selector_xpath}') en {item_page_url}: {e}")
        
        # Asegurar que el enlace al PDF también se extraiga si no se ha hecho
        if 'pdf_url_extracted_from_html' not in metadata_results and 'pdf_link_selectors' in self.selectors_dict:
             pdf_link = self.extract_pdf_link(item_page_url, html_doc)
             if pdf_link:
                metadata_results['pdf_url_extracted_from_html'] = pdf_link

        if not metadata_results:
            self.logger.warning(f"No se extrajeron metadatos significativos de {item_page_url} usando selectores HTML.")
        else:
            self.logger.info(f"Metadatos extraídos de {item_page_url}: {list(metadata_results.keys())}")
            
        return metadata_results

# Ejemplo de uso (requiere configuración y selectores)
if __name__ == '__main__':
    # Mock logger y config para pruebas básicas
    logging.basicConfig(level=logging.DEBUG)
    mock_logger = logging.getLogger("HTMLMetaTest")
    mock_config = {
        'download_timeout': 10,
        'max_download_retries': 2,
        'download_retry_delay': 1,
        'user_agent': 'TestScraper/1.0'
    }
    mock_selectors = {
        'pdf_link_selectors': [
            "//a[contains(@href, '.pdf')]/@href" # Selector muy genérico
        ],
        'metadata_selectors': {
            'title': ["//title/text()"],
            'description': ["//meta[@name='description']/@content"]
        }
    }

    # extractor = HTMLMetadataExtractorBR(mock_config, mock_logger, mock_selectors)
    
    # test_url = "URL_DE_PRUEBA_DE_UNA_PAGINA_DE_ITEM"
    # if test_url != "URL_DE_PRUEBA_DE_UNA_PAGINA_DE_ITEM": # Solo si se pone una URL real
    #     pdf = extractor.extract_pdf_link(test_url)
    #     mock_logger.info(f"PDF Link: {pdf}")
    #     metadata = extractor.extract_all_metadata(test_url)
    #     mock_logger.info(f"Metadata: {metadata}")
    # else:
    #     mock_logger.info("Por favor, edita el script y proporciona una URL de prueba real en 'test_url' para probar el extractor.")
    mock_logger.info("El extractor HTML está listo para ser usado por el scraper principal.")
    mock_logger.info("Recuerda que su función principal en este scraper es encontrar el enlace al PDF, ya que los metadatos provienen de OAI.")
