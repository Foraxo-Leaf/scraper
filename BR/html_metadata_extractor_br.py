# BR/html_metadata_extractor_br.py
import requests
import logging
from lxml import html
from urllib.parse import urljoin, urlparse
import time # Para delays
import os

class HTMLMetadataExtractorBR:
    def __init__(self, config, logger, selectors_config, downloader_instance=None):
        self.config = config
        self.logger = logger
        self.selectors_config = selectors_config if isinstance(selectors_config, dict) else {}
        self.item_page_selectors = self.selectors_config.get('dspace_item_page', {})
        
        # Cargar la LISTA de XPaths para el enlace PDF
        pdf_xpaths_from_config = self.item_page_selectors.get('pdf_link_xpaths', [])
        if not pdf_xpaths_from_config:
            self.logger.warning("No se encontraron 'pdf_link_xpaths' en la configuración de selectores para 'dspace_item_page'. La extracción de PDF podría fallar.")
            self.pdf_link_xpaths = []
        elif isinstance(pdf_xpaths_from_config, list):
            self.pdf_link_xpaths = pdf_xpaths_from_config
        else:
            self.logger.warning(f"'pdf_link_xpaths' debería ser una lista, pero se encontró {type(pdf_xpaths_from_config)}. Se usará como lista de un solo elemento si no está vacía.")
            self.pdf_link_xpaths = [pdf_xpaths_from_config] if pdf_xpaths_from_config else []

        self.request_timeout = self.config.get('request_timeout', 60)
        self.downloader = downloader_instance # Para guardar snapshots HTML
        self.user_agent = self.config.get('user_agent', 'HTMLMetadataExtractor/1.0')
        
        self.save_item_page_snapshot = self.config.get('save_item_page_snapshot', True)

    def _fetch_html_content(self, url, item_id_for_log=None):
        log_prefix = f"[Item {item_id_for_log}] " if item_id_for_log else ""
        self.logger.info(f"{log_prefix}Obteniendo contenido HTML desde: {url}")
        html_content_str = None
        try:
            headers = {'User-Agent': self.user_agent}
            response = requests.get(url, timeout=self.request_timeout, headers=headers)
            response.raise_for_status()
            
            detected_encoding = response.encoding if response.encoding else 'utf-8'
            try:
                html_content_str = response.content.decode(detected_encoding, errors='replace')
            except UnicodeDecodeError:
                self.logger.warning(f"{log_prefix}Fallo decodificación HTML con {detected_encoding} para {url}, intentando iso-8859-1...")
                try:
                    html_content_str = response.content.decode('iso-8859-1', errors='replace')
                except Exception as decode_err_iso:
                    self.logger.error(f"{log_prefix}Error final decodificando HTML de {url} con iso-8859-1: {decode_err_iso}")
                    return None

            if html_content_str and self.save_item_page_snapshot and self.downloader and item_id_for_log:
                 snapshot_path = self.downloader.save_html_snapshot(item_id_for_log, url, html_content_str)
                 if snapshot_path:
                     self.logger.info(f"{log_prefix}Snapshot HTML guardado en: {snapshot_path}")
                 else:
                     self.logger.warning(f"{log_prefix}No se pudo guardar el snapshot HTML para {url}")
            return html_content_str
        except requests.exceptions.Timeout:
            self.logger.error(f"{log_prefix}Timeout obteniendo página HTML {url}")
            return None
        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code if http_err.response is not None else 'N/A'
            self.logger.error(f"{log_prefix}Error HTTP {status_code} obteniendo página HTML {url}: {http_err.response.text[:200] if http_err.response is not None else http_err}")
            return None
        except requests.exceptions.RequestException as req_err:
            self.logger.error(f"{log_prefix}Error de red obteniendo página HTML {url}: {req_err}")
            return None
        except Exception as e:
            self.logger.error(f"{log_prefix}Error inesperado obteniendo HTML desde {url}: {e}", exc_info=True)
            return None

    def extract_pdf_link(self, item_page_url, item_id_for_log=None):
        log_prefix = f"[Item {item_id_for_log}] " if item_id_for_log else ""
        html_string = self._fetch_html_content(item_page_url, item_id_for_log)

        if not html_string:
            self.logger.warning(f"{log_prefix}No se pudo obtener contenido HTML de {item_page_url}, no se puede extraer enlace PDF.")
            return None

        if not self.pdf_link_xpaths: # Chequeo adicional por si la lista quedó vacía
            self.logger.error(f"{log_prefix}La lista de pdf_link_xpaths está vacía. No se puede intentar la extracción de PDF.")
            return None
            
        try:
            tree = html.fromstring(html_string)
            
            for i, pdf_xpath in enumerate(self.pdf_link_xpaths):
                self.logger.debug(f"{log_prefix}Intentando XPath para PDF #{i+1}: {pdf_xpath}")
                try:
                    hrefs = tree.xpath(pdf_xpath)
                    if hrefs:
                        raw_href = hrefs[0]
                        if isinstance(raw_href, str):
                            pdf_relative_url = raw_href.strip()
                            if pdf_relative_url:
                                pdf_absolute_url = urljoin(item_page_url, pdf_relative_url)
                                self.logger.info(f"{log_prefix}Enlace PDF encontrado con XPath #{i+1} ({pdf_xpath}): {pdf_absolute_url}")
                                return pdf_absolute_url
                        else:
                             self.logger.warning(f"{log_prefix}XPath #{i+1} ({pdf_xpath}) devolvió un tipo inesperado: {type(raw_href)}. Se esperaba str.")
                except Exception as xpath_err:
                    self.logger.warning(f"{log_prefix}Error aplicando XPath para PDF #{i+1} ({pdf_xpath}): {xpath_err}") # Cambiado a warning para que no pare la iteración por un mal xpath
            
            self.logger.warning(f"{log_prefix}No se encontró el enlace PDF en {item_page_url} después de probar {len(self.pdf_link_xpaths)} XPath(s).")
            return None

        except Exception as e:
            self.logger.error(f"{log_prefix}Error parseando HTML o extrayendo enlace PDF de {item_page_url}: {e}", exc_info=True)
            return None

    def extract_all_metadata(self, item_page_url, item_id_for_log=None):
        # Esta función podría usarse si necesitáramos extraer más metadatos desde el HTML,
        # además del enlace PDF. Actualmente OAI es la fuente primaria.
        # Su implementación sería similar a extract_pdf_link pero usando otros selectores del YAML.
        log_prefix = f"[Item {item_id_for_log}] " if item_id_for_log else ""
        self.logger.info(f"{log_prefix}Extracción de metadatos HTML completos no implementada en detalle (OAI es primario).")
        return {}


if __name__ == '__main__':
    # Bloque de prueba simple
    print("Probando HTMLMetadataExtractorBR...")
    test_logger = logging.getLogger("HTMLMetaTest")
    test_logger.setLevel(logging.DEBUG)
    test_handler = logging.StreamHandler()
    test_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    test_handler.setFormatter(test_formatter)
    if not test_logger.handlers: test_logger.addHandler(test_handler)

    # Configuración simulada
    mock_config = {
        'request_timeout': 10,
        'user_agent': 'TestMetaExtractor/1.0',
        'output_dir': 'BR/output', 
        'save_item_page_snapshot': True,
        'repositories': {} 
    }
    
    mock_selectors_config = {
        'dspace_item_page': {
            'pdf_link_xpaths': [
                "//table[.//tr/th[1][normalize-space(text())='Fichero'] or .//tr/td[1][normalize-space(text())='Fichero']]//tr[td[a[contains(@href, '/bitstream/')]]]/td[1]//a[contains(@href,'.pdf')]/@href",
                "//a[contains(@href, 'type=application/pdf')]/@href", 
                "//a[contains(@id, 'download-link-does-not-exist')]/@href"
            ],
            'title': "meta[name='DC.title']@content"
        }
    }

    class MockDownloader:
        def __init__(self, config, logger):
            self.output_dir = config.get('output_dir', 'test_output')
            self.logger = logger
            # Asegurar que el directorio base para snapshots de prueba exista
            os.makedirs(os.path.join(self.output_dir, "html_snapshot"), exist_ok=True)

        def _build_local_path(self, item_id, file_type_group, remote_url_for_filename):
            import os
            from urllib.parse import urlparse
            import hashlib
            base_output_dir = os.path.join(self.output_dir, file_type_group, str(item_id))
            os.makedirs(base_output_dir, exist_ok=True)
            try:
                filename_base = os.path.basename(urlparse(remote_url_for_filename).path)
            except Exception:
                filename_base = ""
            safe_filename = "".join([c for c in filename_base if c.isalnum() or c in ('-', '_', '.')]).rstrip()
            if not safe_filename:
                safe_filename = f"snapshot_{hashlib.md5(remote_url_for_filename.encode()).hexdigest()[:8]}"
            if not safe_filename.lower().endswith('.html'):
                safe_filename += ".html"
            return os.path.join(base_output_dir, safe_filename)

        def save_html_snapshot(self, item_id, url, html_content_str):
            snapshot_path = self._build_local_path(str(item_id), "html_snapshot", url) # item_id a str
            try:
                with open(snapshot_path, 'w', encoding='utf-8') as f:
                    f.write(html_content_str)
                self.logger.info(f"Snapshot HTML de prueba guardado en: {snapshot_path}")
                return snapshot_path
            except Exception as e:
                self.logger.error(f"Error guardando snapshot HTML de prueba: {e}")
                return None

    mock_downloader = MockDownloader(mock_config, test_logger)
    extractor = HTMLMetadataExtractorBR(mock_config, test_logger, mock_selectors_config, mock_downloader)

    test_url_alice = "http://www.alice.cnptia.embrapa.br/alice/handle/doc/84107"
    test_logger.info(f"--- Extrayendo PDF link de ALICE: {test_url_alice} ---")
    pdf_link = extractor.extract_pdf_link(test_url_alice, item_id_for_log="test_alice_84107")
    if pdf_link:
        test_logger.info(f"Enlace PDF encontrado para Alice: {pdf_link}")
        assert "/bitstream/" in pdf_link
    else:
        test_logger.error("No se encontró enlace PDF para Alice.")

    test_url_infoteca = "http://www.infoteca.cnptia.embrapa.br/infoteca/handle/doc/85742"
    test_logger.info(f"--- Extrayendo PDF link de INFOTECA-E: {test_url_infoteca} ---")
    pdf_link_infoteca = extractor.extract_pdf_link(test_url_infoteca, item_id_for_log="test_infoteca_85742")
    if pdf_link_infoteca:
        test_logger.info(f"Enlace PDF encontrado para Infoteca-e: {pdf_link_infoteca}")
        assert "/bitstream/" in pdf_link_infoteca
    else:
        test_logger.error("No se encontró enlace PDF para Infoteca-e.")

    test_logger.info("Pruebas de HTMLMetadataExtractorBR finalizadas.") 
