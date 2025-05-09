import logging
import logging.config
import os
import json
import yaml # Para cargar selectores y configuración si se usa YAML
import argparse
from datetime import datetime

# Importar módulos específicos del scraper de Brasil (BR)
from .database_manager_br import DatabaseManagerBR
from .oai_harvester_br import OAIHarvesterBR
from .keyword_searcher_br import KeywordSearcherBR
from .html_metadata_extractor_br import HTMLMetadataExtractorBR
from .resource_downloader_br import ResourceDownloaderBR

# Configuración de logging por defecto (puede ser sobreescrita por un archivo de config)
DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO',
        },
        'file': {
            'class': 'logging.FileHandler',
            'formatter': 'standard',
            'level': 'DEBUG',
            'filename': 'BR/logs/BR-SCRAPER.log', # Ruta por defecto del log
            'mode': 'a', # 'a' para append, 'w' para overwrite
        }
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'DEBUG',
    }
}

# Configuración por defecto del scraper (específica para Brasil)
DEFAULT_CONFIG_BR = {
    'app_name': 'ScraperBR',
    'country_code': 'BR',
    'base_url': 'https://www.embrapa.br', # URL principal de Embrapa
    'db_file': 'BR/db/scraper_br.db',
    'log_config': DEFAULT_LOGGING_CONFIG, # Usar la configuración de logging definida arriba
    'selectors_file_br': 'BR/selectors.yaml',
    'output_path': 'BR/output',
    'state_file_br': 'BR/output/state_br.json',
    'test_results_file_br': 'BR/output/test_results_br.json',
    'user_agent': 'Mozilla/5.0 (compatible; EmbrapaScraperBR/1.0; +http://example.com/scraperinfo)',

    # Configuraciones de OAI Harvester
    'oai_harvesting_enabled': True,
    'oai_repositories': [
        {
            'name': 'Alice (Embrapa)',
            'url': 'https://www.alice.cnptia.embrapa.br/oai/request', # URL base del endpoint OAI
            'set_spec': None, # Opcional: para cosechar un conjunto específico
            'max_records': 100000 # Límite de registros a obtener de este repo OAI (ej. 10 para pruebas)
        },
        {
            'name': 'Infoteca-e (Embrapa)',
            'url': 'https://www.infoteca.cnptia.embrapa.br/infoteca-e/oai/request',
            'set_spec': None,
            'max_records': 100000 # Límite de registros (ej. 10 para pruebas)
        }
    ],
    'oai_metadata_prefix': 'oai_dc', # Formato de metadatos a solicitar
    'oai_request_timeout': 60,
    'oai_max_retries': 3,
    'oai_retry_delay': 10, # segundos
    'oai_max_records_total_per_repo': None, # Límite global si no se especifica por repo
    # 'oai_from_date': '2023-01-01', # Formato YYYY-MM-DD, para cosechas incrementales
    # 'oai_until_date': '2023-12-31',

    # Configuraciones de Keyword Searcher
    'keyword_searching_enabled': True,
    'keywords_br': ["maize", "soja", "algodão", "pecuária de corte", "agricultura familiar"], # Palabras clave de ejemplo
    'search_config': {
        'base_url': 'https://www.embrapa.br/busca-de-publicacoes', # URL base para la búsqueda
        'query_param': 'termo',          # Nombre del parámetro de query para la palabra clave
        'page_param': 'pagina',           # Nombre del parámetro para la paginación
        'results_per_page': 20,          # Número estimado/fijo de resultados por página
        'max_pages_per_keyword': 5,    # Máximo de páginas a scrapear por palabra clave
        'use_selenium': True,            # Si el sitio requiere JS para cargar resultados
        'selenium_page_load_timeout': 45,
        'selenium_implicit_wait': 15,
        'delay_between_search_pages': 5, # Segundos de espera entre páginas de búsqueda
    },
    'max_items_keyword_search': 20,  # Límite total de ítems a procesar desde búsqueda por keyword
    'selenium_chrome_driver_path': None, # Ruta al ejecutable de ChromeDriver. Si es None, se busca en PATH.
                                        # Alternativamente, se podría usar webdriver-manager.
    
    # Configuraciones de HTML Metadata Extractor y Resource Downloader
    'download_timeout': 60, # Timeout general para descargas (PDFs, HTML)
    'max_download_retries': 3,
    'download_retry_delay': 10, # segundos
    'base_download_path': 'BR/output', # Directorio base para todos los archivos descargados
    'pdf_path_segment': 'pdfs',        # Subdirectorio para PDFs dentro de base_download_path
    'html_snapshot_path_segment': 'html_snapshot', # Subdirectorio para snapshots HTML
    'save_html_snapshots': False, # Si se deben guardar snapshots HTML de las páginas de ítems

    # Límites generales del scraper
    'max_items_to_process_total': 200, # Límite total de ítems a procesar en una ejecución (OAI + Keyword)
    'max_pdfs_to_download_total': 50, # Límite total de PDFs a descargar en una ejecución
}

class ScraperBR:
    """
    Clase principal para el scraper de Embrapa (Brasil).
    Orquesta la cosecha OAI, búsqueda por palabras clave, extracción de metadatos,
    descarga de PDFs y gestión de estado en base de datos.
    """

    def __init__(self, config=None, logger=None):
        """
        Inicializa el ScraperBR.

        Args:
            config (dict, optional): Configuración personalizada. Si es None, usa DEFAULT_CONFIG_BR.
            logger (logging.Logger, optional): Instancia del logger. Si es None, se configura uno.
        """
        self.config = config if config else DEFAULT_CONFIG_BR
        self.logger = logger if logger else self._setup_logging()
        self.selectors = self._load_selectors()
        
        # Contadores de la ejecución actual
        self.items_processed_this_run = 0
        self.pdfs_downloaded_this_run = 0

        # Inicializar componentes
        self.db_manager = DatabaseManagerBR(self.config['db_file'], self.logger)
        self.html_extractor = HTMLMetadataExtractorBR(self.config, self.logger, self.selectors)
        self.resource_downloader = ResourceDownloaderBR(self.config, self.logger, self.db_manager)
        self.oai_harvester = OAIHarvesterBR(self.config, self.logger, self.db_manager)
        self.keyword_searcher = KeywordSearcherBR(self.config, self.logger, self.db_manager, self.resource_downloader, self.selectors)
        
        self.logger.info(f"ScraperBR inicializado para {self.config['country_code']} con configuración: {self.config['app_name']}")
        self._ensure_output_dirs()

    def _setup_logging(self):
        """Configura el logging según el archivo de configuración o por defecto."""
        log_config_dict = self.config.get('log_config', DEFAULT_LOGGING_CONFIG)
        
        # Asegurar que el directorio de logs exista si se especifica en filename
        if 'file' in log_config_dict.get('handlers', {}):
            log_file_path = log_config_dict['handlers']['file'].get('filename')
            if log_file_path:
                log_dir = os.path.dirname(log_file_path)
                if log_dir and not os.path.exists(log_dir):
                    try:
                        os.makedirs(log_dir)
                    except OSError as e:
                        # Si no se puede crear, al menos loguear a consola
                        print(f"Advertencia: No se pudo crear el directorio de logs {log_dir}: {e}")
                        # Podríamos remover el handler de archivo si falla la creación del dir
                        # del log_config_dict['handlers']['file']
                        # y de log_config_dict['root']['handlers']
                        pass 

        logging.config.dictConfig(log_config_dict)
        return logging.getLogger(self.config.get('app_name', 'ScraperBR'))

    def _load_selectors(self):
        """Carga los selectores desde el archivo YAML especificado en la configuración."""
        selectors_file = self.config.get('selectors_file_br')
        if not selectors_file or not os.path.exists(selectors_file):
            self.logger.warning(f"Archivo de selectores '{selectors_file}' no encontrado o no especificado. La extracción HTML puede fallar.")
            return {}
        try:
            with open(selectors_file, 'r', encoding='utf-8') as f:
                selectors = yaml.safe_load(f)
            self.logger.info(f"Selectores cargados exitosamente desde {selectors_file}")
            return selectors
        except yaml.YAMLError as e:
            self.logger.error(f"Error cargando archivo de selectores YAML '{selectors_file}': {e}")
        except Exception as e:
            self.logger.error(f"Error inesperado cargando '{selectors_file}': {e}")
        return {}

    def _ensure_output_dirs(self):
        """Asegura que los directorios de salida principales existan."""
        output_path = self.config.get('output_path', 'BR/output')
        if not os.path.exists(output_path):
            try:
                os.makedirs(output_path)
                self.logger.info(f"Directorio de salida principal creado: {output_path}")
            except OSError as e:
                self.logger.error(f"Error creando directorio de salida {output_path}: {e}")
                # Esto podría ser crítico, considerar si se debe detener el scraper.
        # Los subdirectorios (pdfs, html_snapshot) son creados por ResourceDownloaderBR

    def _run_oai_harvest(self):
        """Ejecuta la fase de cosecha OAI-PMH."""
        if not self.config.get('oai_harvesting_enabled', False):
            self.logger.info("Cosecha OAI-PMH deshabilitada en la configuración.")
            return

        self.logger.info("--- Iniciando Fase de Cosecha OAI-PMH ---")
        oai_repos = self.config.get('oai_repositories', [])
        if not oai_repos:
            self.logger.warning("No hay repositorios OAI configurados en 'oai_repositories'.")
            return

        total_oai_items_processed = 0
        for repo_config in oai_repos:
            repo_name = repo_config.get('name', repo_config.get('url'))
            if self.items_processed_this_run >= self.config.get('max_items_to_process_total', float('inf')):
                self.logger.info(f"Límite total de ítems ({self.config.get('max_items_to_process_total')}) alcanzado. Saltando cosecha OAI de {repo_name}.")
                continue
            
            # Pasar el límite restante al harvester
            max_records_for_this_repo = repo_config.get('max_records') 
            # Si hay un max_items_to_process_total, ajustar max_records_for_this_repo
            # para no exceder el límite global con lo que falta procesar.
            remaining_global_item_limit = self.config.get('max_items_to_process_total', float('inf')) - self.items_processed_this_run
            if max_records_for_this_repo is None or max_records_for_this_repo > remaining_global_item_limit:
                 max_records_for_this_repo = remaining_global_item_limit
            
            # Actualizar la config del repo con el límite ajustado si es necesario
            # Esto es un poco hacky, idealmente el harvester manejaría esto internamente
            # pero por simplicidad, pasamos un repo_config modificado.
            current_repo_config = repo_config.copy()
            if max_records_for_this_repo < float('inf'): # Solo si hay un límite real
                current_repo_config['max_records'] = max_records_for_this_repo
            
            if max_records_for_this_repo <= 0:
                 self.logger.info(f"Límite total de ítems ya alcanzado. No hay cupo para cosechar de {repo_name}.")
                 continue

            try:
                _, processed_count, _ = self.oai_harvester.harvest_repository(current_repo_config)
                total_oai_items_processed += processed_count
                self.items_processed_this_run += processed_count
            except Exception as e:
                self.logger.error(f"Error catastrófico durante la cosecha OAI del repositorio {repo_name}: {e}", exc_info=True)
        
        self.logger.info(f"--- Fase de Cosecha OAI-PMH Finalizada. Total ítems OAI procesados/registrados en esta ejecución: {total_oai_items_processed} ---")

    def _run_keyword_search(self):
        """Ejecuta la fase de búsqueda por palabras clave."""
        if not self.config.get('keyword_searching_enabled', False):
            self.logger.info("Búsqueda por palabra clave deshabilitada en la configuración.")
            return

        self.logger.info("--- Iniciando Fase de Búsqueda por Palabras Clave ---")
        keywords = self.config.get('keywords_br', [])
        if not keywords:
            self.logger.warning("No hay palabras clave configuradas en 'keywords_br'.")
            return

        total_keyword_items_processed = 0
        for keyword in keywords:
            if self.items_processed_this_run >= self.config.get('max_items_to_process_total', float('inf')):
                self.logger.info(f"Límite total de ítems ({self.config.get('max_items_to_process_total')}) alcanzado. Saltando búsqueda para keyword: '{keyword}'.")
                continue
            
            # Ajustar el max_items_to_process para esta keyword basado en el límite global restante
            # Esto se maneja dentro de keyword_searcher.search_by_keyword si le pasamos el límite global
            # o si la config 'max_items_keyword_search' se respeta junto con el chequeo de self.items_processed_this_run.
            # Por ahora, el searcher tiene su propio 'max_items_to_process' (que es 'max_items_keyword_search')
            # y nosotros chequeamos el global aquí.
            
            try:
                # El searcher debería idealmente saber cuántos ítems más puede procesar
                # para no exceder el límite global. Modificaremos KeywordSearcher o su llamada.
                # Por ahora, contamos con el chequeo externo.
                processed_count = self.keyword_searcher.search_by_keyword(keyword)
                total_keyword_items_processed += processed_count
                self.items_processed_this_run += processed_count # Asumimos que search_by_keyword devuelve los *nuevos* items procesados en esta llamada
            except Exception as e:
                self.logger.error(f"Error catastrófico durante la búsqueda por palabra clave '{keyword}': {e}", exc_info=True)
        
        # Asegurar que el driver de Selenium se cierre si se usó
        self.keyword_searcher.close()
        self.logger.info(f"--- Fase de Búsqueda por Palabras Clave Finalizada. Total ítems de búsqueda procesados/registrados en esta ejecución: {total_keyword_items_processed} ---")

    def _process_items_for_pdf_links(self):
        """Procesa ítems que están pendientes de extracción de enlace PDF (ej. de OAI o búsqueda web)."""
        self.logger.info("--- Iniciando Fase de Extracción de Enlaces PDF ---")
        # Obtener ítems que necesitan que se les extraiga el enlace al PDF
        # Esto incluye ítems de OAI ('pending_pdf_link') y de búsqueda web ('pending_html_processing')
        # que podrían no tener el enlace PDF directo.
        items_to_process_pdf_link = self.db_manager.get_items_by_status('pending_pdf_link')
        items_to_process_html = self.db_manager.get_items_by_status('pending_html_processing')
        
        # Unir y evitar duplicados si un ítem está en ambas listas (poco probable con estados distintos)
        all_items_for_pdf_extraction = {item['item_id']: item for item in items_to_process_pdf_link}
        for item in items_to_process_html:
            if item['item_id'] not in all_items_for_pdf_extraction:
                all_items_for_pdf_extraction[item['item_id']] = item
        
        items_to_scan = list(all_items_for_pdf_extraction.values())
        self.logger.info(f"Se encontraron {len(items_to_scan)} ítems para escanear en busca de enlaces PDF.")
        
        processed_count = 0
        for item_info in items_to_scan:
            item_id = item_info['item_id']
            item_page_url = item_info.get('item_page_url') # Puede ser de OAI o de búsqueda
            
            if not item_page_url:
                # Si un ítem OAI no tiene item_page_url en dc:identifier, ¿cómo encontramos el PDF?
                # Esto depende del repositorio. Algunos pueden tener el PDF en dc:relation o dc:identifier (como URN que resuelve a PDF).
                # Por ahora, si no hay item_page_url, lo marcamos como que no se puede encontrar el PDF.
                self.logger.warning(f"Ítem {item_id} (OAI: {item_info.get('oai_identifier')}) no tiene item_page_url. No se puede buscar PDF desde HTML.")
                # Podríamos intentar buscar en los metadatos ya guardados si hay algún enlace PDF directo.
                existing_meta = self.db_manager.get_item_metadata(item_id)
                pdf_url_from_meta = None
                for key, value in existing_meta.items():
                    if isinstance(value, str) and value.startswith('http') and '.pdf' in value.lower():
                        pdf_url_from_meta = value
                        break
                    elif isinstance(value, list):
                        for v_item in value:
                             if isinstance(v_item, str) and v_item.startswith('http') and '.pdf' in v_item.lower():
                                pdf_url_from_meta = v_item
                                break
                        if pdf_url_from_meta: break
                
                if pdf_url_from_meta:
                    self.logger.info(f"Enlace PDF encontrado directamente en metadatos OAI para ítem {item_id}: {pdf_url_from_meta}")
                    self.db_manager.log_download_attempt_for_item(item_id, pdf_url_from_meta, 'pdf', None, 0, None, 'pending') # Listo para descargar
                    self.db_manager.update_item_status(item_id, 'pending_pdf_download')
                else:
                    self.db_manager.update_item_status(item_id, 'failed_pdf_link_not_found', 'No item_page_url y no PDF en metadatos OAI')
                continue

            self.logger.info(f"Procesando ítem {item_id} (URL: {item_page_url}) para extraer enlace PDF.")
            try:
                # Si el estado era pending_html_processing, también extraemos otros metadatos si es posible
                # html_doc = self.html_extractor._fetch_html_content(item_page_url) # El extractor lo hace interno
                if item_info.get('processing_status') == 'pending_html_processing':
                    # Extraer todos los metadatos HTML (incluyendo el enlace PDF si se encuentra)
                    html_metadata = self.html_extractor.extract_all_metadata(item_page_url)
                    if html_metadata:
                        self.db_manager.log_item_metadata(item_id, html_metadata, source_type='html_content')
                        self.logger.info(f"Metadatos HTML extraídos y guardados para ítem {item_id}.")
                    pdf_url = html_metadata.get('pdf_url_extracted_from_html')
                else: # Solo buscar el enlace PDF (para items de OAI que ya tienen metadatos)
                    pdf_url = self.html_extractor.extract_pdf_link(item_page_url)

                if pdf_url:
                    self.logger.info(f"Enlace PDF encontrado para ítem {item_id}: {pdf_url}")
                    # Registrar el archivo PDF como pendiente de descarga
                    self.db_manager.log_download_attempt_for_item(item_id, pdf_url, 'pdf', None, 0, None, 'pending')
                    self.db_manager.update_item_status(item_id, 'pending_pdf_download')
                else:
                    self.logger.warning(f"No se encontró enlace PDF para ítem {item_id} en {item_page_url}.")
                    self.db_manager.update_item_status(item_id, 'failed_pdf_link_not_found', f'No se encontró PDF en {item_page_url}')
                processed_count += 1
            except Exception as e:
                self.logger.error(f"Error extrayendo enlace PDF para ítem {item_id} (URL: {item_page_url}): {e}", exc_info=True)
                self.db_manager.update_item_status(item_id, 'failed_pdf_link_extraction', str(e))
        
        self.logger.info(f"--- Fase de Extracción de Enlaces PDF Finalizada. Ítems escaneados: {processed_count} ---")

    def _download_pending_pdfs(self):
        """Descarga todos los PDFs que están pendientes."""
        if self.pdfs_downloaded_this_run >= self.config.get('max_pdfs_to_download_total', float('inf')):
            self.logger.info(f"Límite total de PDFs a descargar ({self.config.get('max_pdfs_to_download_total')}) ya alcanzado. Saltando fase de descarga.")
            return

        self.logger.info("--- Iniciando Fase de Descarga de PDFs ---")
        # Obtener ítems que tienen PDFs pendientes de descarga
        # Podríamos buscar por items con estado 'pending_pdf_download'
        # o directamente buscar en la tabla 'files' los que están 'pending' y son 'pdf'
        items_with_pending_pdfs = self.db_manager.get_items_by_status('pending_pdf_download')
        
        pdfs_attempted_this_phase = 0
        pdfs_successfully_downloaded_this_phase = 0

        for item_info in items_with_pending_pdfs:
            if self.pdfs_downloaded_this_run >= self.config.get('max_pdfs_to_download_total', float('inf')):
                self.logger.info(f"Límite total de PDFs ({self.config.get('max_pdfs_to_download_total')}) alcanzado. Deteniendo descargas.")
                break

            item_id = item_info['item_id']
            # Un ítem puede tener múltiples archivos PDF asociados (raro, pero posible)
            # Obtener los archivos PDF pendientes para este ítem
            pending_files = self.db_manager.get_files_for_item_by_type_and_status(item_id, 'pdf', 'pending', limit=1) # Descargar uno a la vez por ítem por ahora
            
            if not pending_files:
                self.logger.warning(f"Ítem {item_id} está en estado 'pending_pdf_download' pero no tiene archivos PDF en estado 'pending'. Revisar lógica.")
                self.db_manager.update_item_status(item_id, 'processed', 'No PDF pendiente encontrado a pesar del estado.') # O a un estado de error
                continue

            for file_entry in pending_files:
                if self.pdfs_downloaded_this_run >= self.config.get('max_pdfs_to_download_total', float('inf')):
                    self.logger.info(f"Límite total de PDFs ({self.config.get('max_pdfs_to_download_total')}) alcanzado (a mitad de archivos de un ítem). Deteniendo.")
                    break # Salir del bucle de archivos de este ítem

                file_id = file_entry['file_id']
                pdf_url = file_entry['file_url']
                self.logger.info(f"Intentando descargar PDF para ítem {item_id} (File ID: {file_id}) desde: {pdf_url}")
                pdfs_attempted_this_phase += 1
                try:
                    local_path, md5_hash, size_bytes, status_msg = self.resource_downloader.download_resource(
                        item_id, pdf_url, resource_type='pdf', file_id_in_db=file_id
                    )
                    if status_msg == 'downloaded' or status_msg == 'verified': # 'verified' si ResourceDownloader también verifica
                        self.logger.info(f"PDF descargado y registrado para ítem {item_id}. Path: {local_path}")
                        pdfs_successfully_downloaded_this_phase += 1
                        self.pdfs_downloaded_this_run += 1
                        # Marcar el ítem principal como 'processed' si este era el (único) PDF pendiente.
                        # Una lógica más compleja podría verificar si hay OTROS PDFs pendientes para este ítem.
                        # Por ahora, si uno se descarga, el ítem se considera procesado.
                        self.db_manager.update_item_status(item_id, 'processed', f'PDF {file_id} descargado.')
                    else:
                        self.logger.error(f"Fallo la descarga del PDF para ítem {item_id} (File ID: {file_id}). Estado final del archivo: {status_msg}")
                        # El estado del ítem principal podría ser 'failed_pdf_download' si todas las descargas fallan.
                        # Si hay múltiples PDFs, el estado del ítem es más complejo.
                        # Por ahora, si falla la descarga de UN PDF, el ítem podría quedar en 'pending_pdf_download'
                        # o cambiar a un estado de fallo si es el único o el último intento.
                        # La función download_resource ya actualiza el estado del *archivo* en la BD.
                        # Aquí decidimos qué hacer con el estado del *ítem*.
                        # Si fue un fallo, y no hay otros PDFs pendientes para este ítem, marcar el ítem como fallido.
                        other_pending_files = self.db_manager.get_files_for_item_by_type_and_status(item_id, 'pdf', 'pending')
                        if not other_pending_files:
                             self.db_manager.update_item_status(item_id, 'failed_pdf_download', f'Fallo al descargar PDF {file_id}: {status_msg}')
                except Exception as e:
                    self.logger.error(f"Error catastrófico descargando PDF para ítem {item_id} (File ID: {file_id}, URL: {pdf_url}): {e}", exc_info=True)
                    self.db_manager.log_file_download_attempt(file_id, None, 0, None, 'failed_exception', pdf_url, 'pdf')
                    # Similar al caso anterior, ver si hay otros PDFs pendientes para este ítem.
                    other_pending_files = self.db_manager.get_files_for_item_by_type_and_status(item_id, 'pdf', 'pending')
                    if not other_pending_files:
                        self.db_manager.update_item_status(item_id, 'failed_pdf_download', f'Excepción al descargar PDF {file_id}')
            
            if self.pdfs_downloaded_this_run >= self.config.get('max_pdfs_to_download_total', float('inf')):
                break # Salir del bucle de ítems

        self.logger.info(f"--- Fase de Descarga de PDFs Finalizada. PDFs intentados: {pdfs_attempted_this_phase}. Descargados exitosamente: {pdfs_successfully_downloaded_this_phase} ---")

    def _generate_state_json(self):
        """Genera un archivo JSON con el resumen del estado de los ítems procesados."""
        state_file_path = self.config.get('state_file_br')
        if not state_file_path:
            self.logger.warning("No se especificó 'state_file_br' en la configuración. No se generará JSON de estado.")
            return
        
        self.logger.info(f"Generando archivo de estado JSON en: {state_file_path}")
        try:
            # Obtener un resumen de todos los ítems procesados o fallidos de la BD
            # Limitar la cantidad de datos si es necesario para no hacer el JSON demasiado grande
            summary_limit = self.config.get('state_json_summary_limit', 1000) # ej. últimos 1000 actualizados
            items_summary = self.db_manager.get_all_processed_items_summary(limit=summary_limit)
            
            output_data = {
                'scraper_run_timestamp': datetime.utcnow().isoformat(),
                'total_items_in_summary': len(items_summary),
                'items': items_summary
            }
            
            with open(state_file_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            self.logger.info(f"Archivo de estado JSON generado exitosamente con {len(items_summary)} ítems.")
        except Exception as e:
            self.logger.error(f"Error generando archivo de estado JSON: {e}", exc_info=True)

    def _generate_test_results_json(self):
        """
        Genera un archivo JSON con resultados de prueba (ej. para la tarea VERIFY de Cursor Rules).
        Esto implicaría seleccionar algunos PDFs descargados y verificar sus metadatos básicos.
        Para este scraper, podríamos tomar una muestra de los PDFs descargados exitosamente.
        """
        test_results_file = self.config.get('test_results_file_br')
        if not test_results_file:
            self.logger.info("No se especificó 'test_results_file_br'. No se generará JSON de resultados de prueba.")
            return

        self.logger.info(f"Generando archivo de resultados de prueba JSON en: {test_results_file}")
        try:
            # Obtener una muestra de ítems procesados que tengan PDFs descargados
            # Esta es una simplificación. Una tarea VERIFY real podría implicar más.
            sample_size = self.config.get('test_results_sample_size', 5)
            processed_items_with_pdfs = []
            all_processed = self.db_manager.get_all_processed_items_summary(limit=sample_size * 5) # Tomar más para filtrar
            
            for item_summary in all_processed:
                if len(processed_items_with_pdfs) >= sample_size:
                    break
                if item_summary.get('processing_status') == 'processed' and item_summary.get('pdf_files_info'):
                    # Tomar solo el primer PDF descargado para el resumen de prueba
                    first_pdf_info = next((pdf for pdf in item_summary['pdf_files_info'] if pdf.get('status') in ['downloaded', 'verified']), None)
                    if first_pdf_info:
                        processed_items_with_pdfs.append({
                            'item_id_db': item_summary.get('item_id'),
                            'oai_identifier': item_summary.get('oai_identifier'),
                            'item_page_url': item_summary.get('item_page_url'),
                            'title_from_db': item_summary.get('titles'), # Puede ser lista
                            'pdf_downloaded_path': first_pdf_info.get('local_path'),
                            'pdf_md5_hash': first_pdf_info.get('md5'),
                            'pdf_size_bytes': first_pdf_info.get('size')
                            # Aquí se podrían añadir metadatos extraídos del PDF si se implementa esa lógica
                        })
            
            output_data = {
                'test_run_timestamp': datetime.utcnow().isoformat(),
                'sample_size_requested': sample_size,
                'verified_items_count': len(processed_items_with_pdfs),
                'verified_items': processed_items_with_pdfs
            }
            with open(test_results_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            self.logger.info(f"Archivo de resultados de prueba JSON generado con {len(processed_items_with_pdfs)} ítems de muestra.")
        except Exception as e:
            self.logger.error(f"Error generando archivo de resultados de prueba JSON: {e}", exc_info=True)

    def run(self):
        """Ejecuta el proceso completo del scraper."""
        self.logger.info(f"--- Iniciando ejecución del ScraperBR ({self.config['app_name']}) --- Timestamp: {datetime.utcnow().isoformat()}")
        start_time = time.time()
        self.items_processed_this_run = 0
        self.pdfs_downloaded_this_run = 0

        try:
            # 1. Cosecha OAI (si está habilitada)
            if self.config.get('oai_harvesting_enabled', True):
                self._run_oai_harvest()
            else:
                self.logger.info("OAI Harvesting está deshabilitado.")

            # 2. Búsqueda por Palabras Clave (si está habilitada)
            if self.items_processed_this_run < self.config.get('max_items_to_process_total', float('inf')) and \
               self.config.get('keyword_searching_enabled', True):
                self._run_keyword_search()
            else:
                self.logger.info("Keyword Searching está deshabilitado o se alcanzó el límite de ítems.")

            # 3. Procesar ítems para extraer enlaces PDF (de OAI o Búsqueda)
            # Esta fase es crucial para los ítems que no tienen el enlace PDF directo de la cosecha/búsqueda inicial.
            self._process_items_for_pdf_links()

            # 4. Descargar PDFs pendientes
            if self.pdfs_downloaded_this_run < self.config.get('max_pdfs_to_download_total', float('inf')):
                self._download_pending_pdfs()
            else:
                self.logger.info("Fase de descarga de PDFs omitida, límite ya alcanzado.")

            # 5. Generar reportes/archivos de estado final
            self._generate_state_json()
            self._generate_test_results_json() # Para simular la tarea VERIFY

        except Exception as e:
            self.logger.critical(f"Error crítico no manejado en la ejecución principal del scraper: {e}", exc_info=True)
        finally:
            # Cerrar conexiones, etc.
            if self.db_manager:
                self.db_manager.close()
            if self.keyword_searcher:
                 self.keyword_searcher.close() # Asegurar que el driver de Selenium se cierre
            
            end_time = time.time()
            duration = end_time - start_time
            self.logger.info(f"--- Ejecución del ScraperBR Finalizada --- Duración: {duration:.2f} segundos.")
            self.logger.info(f"Resumen de esta ejecución: Ítems procesados/registrados: {self.items_processed_this_run}, PDFs descargados: {self.pdfs_downloaded_this_run}")

def main():
    """Punto de entrada principal para ejecutar el scraper desde la línea de comandos."""
    parser = argparse.ArgumentParser(description="Scraper para el portal de Embrapa (Brasil).")
    # Aquí se podrían añadir argumentos para sobrescribir configuraciones, ej:
    # parser.add_argument("--keyword", type=str, help="Palabra clave específica para buscar.")
    # parser.add_argument("--max-items", type=int, help="Máximo de ítems a procesar en total.")
    # parser.add_argument("--config-file", type=str, help="Ruta a un archivo de configuración JSON/YAML.")
    # parser.add_argument("--log-level", type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help="Nivel de logging para la consola.")

    args = parser.parse_args()
    
    # Cargar config desde archivo si se especifica, o usar DEFAULT_CONFIG_BR
    # Por ahora, usaremos la configuración por defecto.
    config = DEFAULT_CONFIG_BR.copy()

    # TODO: Lógica para actualizar 'config' con args de línea de comando si es necesario.
    # Ejemplo: if args.log_level: config['log_config']['handlers']['console']['level'] = args.log_level

    scraper = ScraperBR(config=config)
    scraper.run()

if __name__ == "__main__":
    # Esto permite ejecutar el scraper como un script: python -m BR.scraper
    # o (si este archivo está en el directorio raíz del proyecto BR) python scraper.py
    main()
