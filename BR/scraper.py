# BR/scraper.py
import argparse
import logging
import os
import yaml
import json
import time
import sqlite3 # Importar sqlite3 para la consulta de depuración
import shutil # Importar shutil para copiar archivos
from .oai_harvester_br import OAIHarvesterBR
from .database_manager_br import DatabaseManagerBR
from .resource_downloader_br import ResourceDownloaderBR
from .html_metadata_extractor_br import HTMLMetadataExtractorBR
from .keyword_searcher_br import KeywordSearcherBR

# Obtiene la ruta absoluta del directorio donde está scraper.py (es decir, BR/)
MODULE_BR_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

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
            'filename': 'logs/BR-SCRAPER.log', # Ruta relativa al módulo BR
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
    "base_url": "https://www.embrapa.br",
    # URLs base de los repositorios (no necesariamente OAI endpoints)
    "base_url_alice_repo": "https://www.alice.cnptia.embrapa.br/alice/",
    "base_url_infoteca_repo": "https://www.infoteca.cnptia.embrapa.br/infoteca/",
    "base_url_embrapa_search": "https://www.embrapa.br/busca-de-publicacoes",
    "country_code": "BR",
    "db_file": "db/scraper_br.db", # Modificada
    "log_config": DEFAULT_LOGGING_CONFIG, # Referencia al dict de logging
    "output_path": "output", # Modificada y renombrada de output_dir
    "repositories": {
        "alice": {
            "name": "Alice",
            "url_base": "https://www.alice.cnptia.embrapa.br/alice/",
            "oai_pmh_endpoint": "https://www.alice.cnptia.embrapa.br/alice-oai/request", # Endpoint corregido
            "preferred_metadata_prefix": "oai_dc",
            "set_spec": None # Sin set específico por ahora
        },
        "infoteca-e": {
            "name": "Infoteca-e",
            "url_base": "https://www.infoteca.cnptia.embrapa.br/infoteca/",
            "oai_pmh_endpoint": "https://www.infoteca.cnptia.embrapa.br/infoteca-oai/request", # Endpoint corregido
            "preferred_metadata_prefix": "oai_dc",
            "set_spec": None # Sin set específico por ahora
        }
    },
    "primary_harvest_repository": "all", # Procesar todos los repositorios configurados
    "download_delay_seconds": 1, # Reducido ligeramente
    "max_retries": 3,
    "user_agent": "EmbrapaScraper/1.0 (Compatible; +http://example.com/botinfo)",
    "selectors_file_br": "selectors.yaml", # Modificada y renombrada de selectors_file
    "download_timeout": 120,
    "request_timeout": 60, 
    "download_base_retry_delay": 5, 
    "delay": 0.1, 
    "oai_request_delay": 2, # Segundos de espera entre peticiones OAI
    "max_oai_records_alice": 20, # Límite para prueba OAI Alice
    "max_oai_records_infoteca": 20, # Límite para prueba OAI Infoteca
    "max_html_processing_items": 20,
    "max_pdf_link_extraction_items": 20, 
    "max_pdf_download_items": 20, 
    "download_batch_size": 5, # Aumentado ligeramente
    "state_file_br": "output/state_br.json", # Modificada y renombrada de output_state_file
    "test_results_file_br": "output/test_results_br.json", # Modificada y renombrada de output_test_results_file
    "docs_sample_pdfs_dir": "docs/sample_pdfs/", # Modificada
    "log_level": "INFO", 
    "timeout_seconds": 60,
    "keyword_search_keywords": ["maiz"], # Mantener o comentar según preferencia
    "keyword_max_pages": 3, 
    "keyword_search_items_per_page": 10, 
    "keyword_search_order_by": "relevancia-ordenacao",
    "keyword_search_direction": "asc",
    "concurrency_level": 5,
    "use_snapshots_for_html_processing": True, 
    "selenium_wait_timeout": 60, 
    "generate_state_json": True,
    "generate_test_results_json": True,
    "test_results_sample_size": 5, 
    "chromedriver_path": None,
    "max_items_to_process_total": 200, # Límite total de ítems a procesar en una ejecución (OAI + Keyword)
    "max_pdfs_to_download_total": 50, # Límite total de PDFs a descargar en una ejecución
}

class ScraperBR:
    def __init__(self, config):
        self.config = config
        # MODULE_BR_ROOT_DIR ya está definido globalmente en el módulo
        self.logger = self._setup_logging()
        self.selectors = self._load_selectors()

        db_file_relative = self.config['db_file']
        db_file_absolute = os.path.join(MODULE_BR_ROOT_DIR, db_file_relative)
        self.db_manager = DatabaseManagerBR(db_file_absolute, self.logger)
        self.db_manager.initialize_db()

        # Para ResourceDownloaderBR, también necesita la ruta base absoluta
        base_download_path_relative = self.config.get('output_path', 'output') # 'output_path' ahora es relativo
        base_download_path_absolute = os.path.join(MODULE_BR_ROOT_DIR, base_download_path_relative)
        
        # Creamos una config temporal o modificamos la que se le pasa a ResourceDownloader
        # para que use la ruta absoluta, o ResourceDownloader se adapta.
        # Por ahora, asumimos que ResourceDownloader también será modificado para tomar MODULE_BR_ROOT_DIR.
        # O, más simple, pasamos la ruta absoluta directamente si el constructor lo permite.
        # Vamos a modificar ResourceDownloaderBR para que también use MODULE_BR_ROOT_DIR.
        # Por ahora, aquí solo nos preocupamos de las rutas que ScraperBR maneja directamente.

        self.downloader = ResourceDownloaderBR(self.config, self.logger, self.db_manager, MODULE_BR_ROOT_DIR) # Pasamos MODULE_BR_ROOT_DIR
        
        self.oai_harvester = OAIHarvesterBR(self.config, self.logger, self.db_manager)
        self.html_extractor = HTMLMetadataExtractorBR(self.config, self.logger, self.selectors, self.downloader)
        self.keyword_searcher = KeywordSearcherBR(self.config, self.logger, self.db_manager, self.downloader, self.selectors) # KeywordSearcher no maneja rutas de archivos directamente

        self.logger.info("ScraperBR inicializado con todos los componentes principales.")
        self._ensure_output_dirs() # Este método también necesitará MODULE_BR_ROOT_DIR

    def _setup_logging(self):
        logger = logging.getLogger("EmbrapaScraper") # Usar self.config.get('app_name', "EmbrapaScraper")?
        # logger.setLevel(logging.INFO) # El nivel se define en la config del handler
        log_config_dict = self.config.get('log_config', DEFAULT_LOGGING_CONFIG)
        
        if 'file' in log_config_dict.get('handlers', {}):
            log_file_relative = log_config_dict['handlers']['file'].get('filename')
            if log_file_relative:
                log_file_absolute = os.path.join(MODULE_BR_ROOT_DIR, log_file_relative)
                log_dir_absolute = os.path.dirname(log_file_absolute)
                os.makedirs(log_dir_absolute, exist_ok=True)
                # Actualizar la config en el dict para que dictConfig use la ruta absoluta
                log_config_dict['handlers']['file']['filename'] = log_file_absolute
        
        logging.config.dictConfig(log_config_dict)
        # El logger se obtiene después de configurar con dictConfig
        final_logger = logging.getLogger(self.config.get('app_name', 'ScraperBR'))
        return final_logger

    def _load_selectors(self):
        try:
            selectors_file_relative = self.config['selectors_file_br']
            selectors_file_absolute = os.path.join(MODULE_BR_ROOT_DIR, selectors_file_relative)
            with open(selectors_file_absolute, 'r', encoding='utf-8') as f:
                selectors = yaml.safe_load(f)
            self.logger.info(f"Selectores cargados exitosamente desde {selectors_file_absolute}")
            return selectors
        except FileNotFoundError:
            # Reconstruir la ruta absoluta para el mensaje de error
            selectors_file_relative = self.config.get('selectors_file_br', 'selectors.yaml') # fallback
            selectors_file_absolute = os.path.join(MODULE_BR_ROOT_DIR, selectors_file_relative)
            self.logger.error(f"Archivo de selectores no encontrado en {selectors_file_absolute}")
            return {}
        except yaml.YAMLError as e:
            selectors_file_relative = self.config.get('selectors_file_br', 'selectors.yaml')
            selectors_file_absolute = os.path.join(MODULE_BR_ROOT_DIR, selectors_file_relative)
            self.logger.error(f"Error al parsear el archivo de selectores YAML: {selectors_file_absolute}: {e}")
            return {}

    def _run_oai_harvest(self):
        """Ejecuta la cosecha OAI para los repositorios configurados."""
        self.logger.info("Iniciando fase de cosecha OAI-PMH.")
        repos_config = self.config.get('repositories', {})
        target_repo_keys = []
        primary_config = self.config.get('primary_harvest_repository', 'all')

        if primary_config == 'all':
            target_repo_keys = list(repos_config.keys())
        elif primary_config in repos_config:
            target_repo_keys = [primary_config]
        else:
            self.logger.warning(f"Repositorio OAI primario '{primary_config}' no encontrado en la configuración.")

        if not target_repo_keys:
            self.logger.info("No hay repositorios OAI configurados o seleccionados para cosechar. Saltando fase OAI.")
            return

        all_repo_stats = []
        for repo_key in target_repo_keys:
            max_records = self.config.get(f'max_oai_records_{repo_key}', None) # Buscar límite específico
            if max_records == 0: # Si es 0, significa desactivado
                 self.logger.info(f"Cosecha OAI desactivada para '{repo_key}' (límite 0 en config). Saltando.")
                 continue
                 
            self.logger.info(f"--- Iniciando cosecha para repositorio: {repo_key} (Límite: {max_records or 'Todos'}) ---")
            try:
                repo_stats = self.oai_harvester.harvest_repository(repo_key, max_records_to_fetch=max_records)
                self.logger.info(f"--- Cosecha para {repo_key} finalizada. Estadísticas: {repo_stats} ---")
                all_repo_stats.append(repo_stats)
            except Exception as e:
                 self.logger.error(f"Error durante la cosecha OAI del repositorio '{repo_key}': {e}", exc_info=True)

        self.logger.info("Fase de cosecha OAI-PMH completada.")
        # Devolver un resumen de las estadísticas de todos los repositorios procesados
        total_fetched_all_repos = sum(stat.get('fetched', 0) for stat in all_repo_stats)
        total_processed_db_all_repos = sum(stat.get('processed_db', 0) for stat in all_repo_stats)
        total_new_db_all_repos = sum(stat.get('new_db', 0) for stat in all_repo_stats)
        total_updated_db_all_repos = sum(stat.get('updated_db', 0) for stat in all_repo_stats)
        total_failed_all_repos = sum(stat.get('failed', 0) for stat in all_repo_stats)
        
        return {
            "oai_total_fetched": total_fetched_all_repos,
            "oai_total_processed_db": total_processed_db_all_repos,
            "oai_total_new_items_db": total_new_db_all_repos,
            "oai_total_metadata_updated_db": total_updated_db_all_repos,
            "oai_total_harvest_failures": total_failed_all_repos,
            "oai_repositories_processed_count": len(all_repo_stats)
        }

    def _process_items_for_pdf_links(self, max_items_to_process=None):
        """Procesa ítems para extraer el enlace directo al PDF desde sus páginas HTML."""
        self.logger.info("Iniciando proceso de extracción de enlaces PDF de páginas de ítems...")
        # Estado que indica que los metadatos OAI fueron obtenidos y necesitamos el enlace al PDF
        items_pending_pdf_link = self.db_manager.get_items_to_process(statuses=['pending_download'], limit=max_items_to_process)

        if not items_pending_pdf_link:
            self.logger.info("No hay ítems pendientes de extracción de enlace PDF.")
            return

        self.logger.info(f"Se procesarán {len(items_pending_pdf_link)} ítems para extracción de enlace PDF.")
        processed_count = 0
        found_links_count = 0
        failed_extraction_count = 0

        for item_data in items_pending_pdf_link:
            item_id = item_data['item_id']
            item_page_url = item_data['item_page_url']
            oai_id = item_data.get('oai_identifier', 'N/A')
            log_prefix = f"[ItemDB {item_id} / OAI {oai_id}]"

            self.logger.info(f"{log_prefix} Preparando para obtener enlace PDF para {item_page_url}")
            self.db_manager.update_item_status(item_id, 'processing_pdf_link')

            pdf_direct_url = None
            # Verificar si pdf_direct_url ya vino de OAI
            if item_data.get('metadata_json'):
                try:
                    current_metadata = json.loads(item_data['metadata_json'])
                    if current_metadata.get('pdf_direct_url'):
                        pdf_direct_url = current_metadata['pdf_direct_url']
                        self.logger.info(f"{log_prefix} Enlace PDF encontrado directamente en metadatos OAI: {pdf_direct_url}")
                except json.JSONDecodeError:
                    self.logger.warning(f"{log_prefix} No se pudo parsear metadata_json para buscar pdf_direct_url preexistente.")

            if not pdf_direct_url: # Si no vino de OAI, intentar extraerlo del HTML
                self.logger.info(f"{log_prefix} No se encontró pdf_direct_url en metadatos OAI. Intentando extracción desde HTML: {item_page_url}")
                # Solo intentar extracción HTML si item_page_url no parece ser ya un PDF
                if item_page_url and not item_page_url.lower().endswith('.pdf'):
                    pdf_direct_url = self.html_extractor.extract_pdf_link(item_page_url, item_id_for_log=str(item_id))
                elif item_page_url and item_page_url.lower().endswith('.pdf'):
                    # Si el item_page_url es un PDF, pero no se capturó como pdf_direct_url antes (caso raro)
                    self.logger.info(f"{log_prefix} item_page_url ya es un enlace PDF: {item_page_url}. Usándolo directamente.")
                    pdf_direct_url = item_page_url
                else:
                    self.logger.warning(f"{log_prefix} item_page_url no es válido para extracción HTML: {item_page_url}")

            if pdf_direct_url:
                found_links_count += 1
                self.logger.info(f"{log_prefix} Enlace PDF final para descarga: {pdf_direct_url}")
                
                # Actualizar metadata_json del ítem con este nuevo enlace
                # Primero, obtener metadatos existentes si los hay
                existing_metadata = {}
                current_item_details = self.db_manager.get_item_details(item_id)
                if current_item_details and current_item_details.get('metadata_json'):
                    try:
                        existing_metadata = json.loads(current_item_details['metadata_json'])
                    except json.JSONDecodeError:
                        self.logger.warning(f"{log_prefix} No se pudo parsear metadata_json existente.")
                
                existing_metadata['pdf_direct_url'] = pdf_direct_url # Añadir o actualizar el enlace
                
                self.db_manager.log_item_metadata(item_id, existing_metadata)
                self.db_manager.update_item_status(item_id, 'awaiting_pdf_download')
            else:
                failed_extraction_count += 1
                self.logger.warning(f"{log_prefix} No se pudo extraer el enlace PDF de {item_page_url}")
                self.db_manager.update_item_status(item_id, 'error_pdf_link_extraction')
            
            processed_count += 1
            if self.config.get('delay', 0.1) > 0: # Aplicar un pequeño delay
                time.sleep(self.config.get('delay', 0.1) / 2) # Menor delay para esto que para OAI completo

        self.logger.info(f"Proceso de extracción de enlaces PDF finalizado. Total procesados: {processed_count}, Enlaces encontrados: {found_links_count}, Fallos de extracción: {failed_extraction_count}")
        return {"processed_count": processed_count, "found_links_count": found_links_count, "failed_extraction_count": failed_extraction_count}

    def _download_pdf_files(self, max_items_to_process=None):
        """Descarga los archivos PDF para ítems que tienen una URL directa de PDF."""
        self.logger.info("Iniciando proceso de descarga de archivos PDF...")
        items_awaiting_download = self.db_manager.get_items_to_process(statuses=['awaiting_pdf_download'], limit=max_items_to_process)

        if not items_awaiting_download:
            self.logger.info("No hay ítems esperando descarga de PDF.")
            return

        self.logger.info(f"Se intentará descargar PDF para {len(items_awaiting_download)} ítems.")
        processed_count = 0
        downloaded_count = 0
        failed_count = 0
        missing_url_count = 0

        for item_data in items_awaiting_download:
            item_id = item_data['item_id']
            oai_id = item_data.get('oai_identifier', 'N/A') 
            log_prefix = f"[ItemDB {item_id} / OAI {oai_id}]"
            self.db_manager.update_item_status(item_id, 'processing_pdf_download')

            pdf_direct_url = None
            raw_metadata_json_from_db = item_data.get('metadata_json') # DEBUG: Capturar el JSON crudo

            if raw_metadata_json_from_db:
                try:
                    metadata = json.loads(raw_metadata_json_from_db)
                    pdf_direct_url = metadata.get('pdf_direct_url')
                except json.JSONDecodeError:
                    self.logger.error(f"{log_prefix} No se pudo parsear metadata_json para obtener pdf_direct_url. JSON crudo: {raw_metadata_json_from_db}") # DEBUG LOG
            
            if not pdf_direct_url:
                self.logger.error(f"{log_prefix} No se encontró pdf_direct_url en metadatos para un ítem en 'awaiting_pdf_download'. Metadata JSON leído de BD: {raw_metadata_json_from_db}") # DEBUG LOG
                self.db_manager.update_item_status(item_id, 'error_missing_pdf_url')
                missing_url_count += 1
                processed_count += 1
                continue

            self.logger.info(f"{log_prefix} Intentando descargar PDF desde {pdf_direct_url}")
            download_status = self.downloader.download_resource(item_id, 'pdf', pdf_direct_url)

            if download_status == 'downloaded' or download_status == 'skipped_exists':
                self.logger.info(f"{log_prefix} PDF descargado/existente exitosamente: {pdf_direct_url}")
                self.db_manager.update_item_status(item_id, 'processed')
                downloaded_count += 1
            else:
                self.logger.error(f"{log_prefix} Falló la descarga del PDF desde {pdf_direct_url}. Estado downloader: {download_status}")
                self.db_manager.update_item_status(item_id, 'error_pdf_download')
                failed_count += 1
            
            processed_count += 1
            if self.config.get('download_delay_seconds', 1) > 0: 
                time.sleep(self.config.get('download_delay_seconds', 1))
        
        self.logger.info(f"Proceso de descarga de PDF finalizado. Total procesados: {processed_count}, Descargados OK: {downloaded_count}, Fallos: {failed_count}, URLs Faltantes: {missing_url_count}")
        return {"processed_count": processed_count, "downloaded_count": downloaded_count, "failed_count": failed_count, "missing_url_count": missing_url_count}

    def _generate_state_json(self):
        """Genera el archivo BR/output/state_br.json con el estado actual de los ítems."""
        self.logger.info("Generando archivo state_br.json...")
        state_data = []
        items_from_db = self.db_manager.get_all_items_for_report()

        if not items_from_db:
            self.logger.info("No hay ítems en la base de datos para generar state_br.json.")
        
        for item_row in items_from_db:
            # metadata_json ya viene parseado como dict desde get_all_items_for_report
            # y los pdfs también vienen como una lista de dicts.
            item_metadata_for_state = item_row.get('metadata_json', {})
            
            # Seleccionar solo los campos de metadatos que queremos en state.json
            # (title, authors, publication_date son comunes)
            # Los nombres de las claves en item_metadata_for_state dependen de lo que OAIHarvester guardó.
            # Asumimos que OAIHarvester guardó 'titles' (lista), 'authors' (lista), 'dates' (lista, tomar la primera?)
            # o 'publication_date' directamente.
            final_meta = {
                'title': item_metadata_for_state.get('titles', [None])[0] if item_metadata_for_state.get('titles') else None,
                'authors': item_metadata_for_state.get('authors', []), # Asumiendo que 'authors' es una lista de strings
                # Para la fecha, OAI puede tener múltiples. Tomamos la primera de 'dates' o una específica.
                'publication_date': item_metadata_for_state.get('dates', [None])[0] if item_metadata_for_state.get('dates') else item_metadata_for_state.get('publication_date')
            }
            # Limpiar metadatos nulos
            final_meta = {k: v for k, v in final_meta.items() if v is not None}

            pdfs_info_for_state = []
            for pdf_db_entry in item_row.get('pdfs', []):
                pdfs_info_for_state.append({
                    "url": pdf_db_entry.get('remote_url'),
                    "local_path": pdf_db_entry.get('local_path'),
                    "downloaded": pdf_db_entry.get('download_status') in ['downloaded', 'skipped_exists']
                })

            state_entry = {
                "url": item_row.get('item_page_url'),
                "metadata": final_meta,
                "html_path": item_row.get('html_local_path'),
                "pdfs": pdfs_info_for_state,
                "analyzed": item_row.get('processing_status') == 'processed'
            }
            state_data.append(state_entry)
        
        state_file_relative = self.config.get('state_file_br', 'output/state_br.json') # Debería ser 'output/state_br.json'
        output_file_path = os.path.join(MODULE_BR_ROOT_DIR, state_file_relative)

        try:
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            with open(output_file_path, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=4)
            self.logger.info(f"Archivo state_br.json generado exitosamente en: {output_file_path} con {len(state_data)} entradas.")
        except IOError as e:
            self.logger.error(f"Error de I/O escribiendo state_br.json en {output_file_path}: {e}")
        except Exception as e:
            self.logger.error(f"Error inesperado generando state_br.json: {e}", exc_info=True)

    def _generate_test_results_json(self, max_sample_pdfs=None):
        """Genera el archivo BR/output/test_results_br.json con una muestra de PDFs descargados."""
        self.logger.info(f"Generando archivo test_results_br.json (máximo {max_sample_pdfs or 'Todos'} PDFs de muestra)...")
        test_results_data = []
        # get_sample_downloaded_pdfs_for_report ya devuelve el metadata_json parseado como dict
        sample_pdfs_from_db = self.db_manager.get_sample_downloaded_pdfs_for_report(max_results=max_sample_pdfs)

        # Obtener rutas relativas de la configuración
        docs_sample_pdfs_dir_relative = self.config.get('docs_sample_pdfs_dir', 'docs/sample_pdfs/')
        test_results_file_relative = self.config.get('test_results_file_br', 'output/test_results_br.json')

        # Construir rutas absolutas
        sample_pdfs_dir_absolute = os.path.join(MODULE_BR_ROOT_DIR, docs_sample_pdfs_dir_relative)
        output_file_path_absolute = os.path.join(MODULE_BR_ROOT_DIR, test_results_file_relative)

        if not sample_pdfs_from_db:
            self.logger.info("No hay PDFs descargados de muestra en la base de datos para generar test_results_br.json y copiar a sample_pdfs.")
            # Asegurarse de que el directorio exista incluso si no hay PDFs para copiar
            os.makedirs(sample_pdfs_dir_absolute, exist_ok=True)
            # Vaciar el archivo JSON si no hay resultados
            try:
                with open(output_file_path_absolute, 'w', encoding='utf-8') as f: # Usar ruta absoluta
                    json.dump([], f, ensure_ascii=False, indent=4)
                self.logger.info(f"Archivo test_results_br.json generado vacío en: {output_file_path_absolute}")
            except IOError as e:
                self.logger.error(f"Error de I/O escribiendo test_results_br.json vacío: {e}")    
            return

        os.makedirs(sample_pdfs_dir_absolute, exist_ok=True)
        copied_pdf_count = 0

        for pdf_row in sample_pdfs_from_db:
            item_metadata_for_results = pdf_row.get('metadata_json', {})

            # Extraer metadatos del ítem para el reporte del PDF
            meta_for_pdf = {
                'title': item_metadata_for_results.get('titles', [None])[0] if item_metadata_for_results.get('titles') else None,
                'authors': item_metadata_for_results.get('authors', []),
                'publication_date': item_metadata_for_results.get('dates', [None])[0] if item_metadata_for_results.get('dates') else item_metadata_for_results.get('publication_date')
            }
            meta_for_pdf = {k: v for k, v in meta_for_pdf.items() if v is not None}

            pdf_entry = {
                "item_page_url": pdf_row.get('item_page_url'),
                "local_pdf_path": pdf_row.get('local_path'),
                "md5_hash": pdf_row.get('md5_hash'),
                "file_size_bytes": pdf_row.get('file_size_bytes'),
                "metadata": meta_for_pdf
            }
            test_results_data.append(pdf_entry)

            # Copiar el archivo PDF a la carpeta de muestra
            local_pdf_path_source = pdf_row.get('local_path')
            if local_pdf_path_source and os.path.exists(local_pdf_path_source):
                try:
                    # Por simplicidad, usamos el nombre base del archivo original.
                    pdf_filename_dest = os.path.basename(local_pdf_path_source)
                    destination_path = os.path.join(sample_pdfs_dir_absolute, pdf_filename_dest) # Usar ruta absoluta
                    shutil.copy2(local_pdf_path_source, destination_path)
                    self.logger.info(f"PDF de muestra copiado a: {destination_path}")
                    copied_pdf_count +=1
                except Exception as e_copy:
                    self.logger.error(f"Error copiando PDF de muestra {local_pdf_path_source} a {sample_pdfs_dir_absolute}: {e_copy}") # Usar ruta absoluta
            elif not local_pdf_path_source:
                self.logger.warning(f"No se encontró local_path para el PDF del ítem {pdf_row.get('item_id')}, no se puede copiar a muestras.")
            elif not os.path.exists(local_pdf_path_source):
                 self.logger.warning(f"El archivo PDF de muestra no existe en {local_pdf_path_source}, no se puede copiar.")

        try:
            os.makedirs(os.path.dirname(output_file_path_absolute), exist_ok=True) # Usar ruta absoluta
            with open(output_file_path_absolute, 'w', encoding='utf-8') as f: # Usar ruta absoluta
                json.dump(test_results_data, f, ensure_ascii=False, indent=4)
            self.logger.info(f"Archivo test_results_br.json generado exitosamente en: {output_file_path_absolute} con {len(test_results_data)} entradas.") # Usar ruta absoluta
            if copied_pdf_count > 0:
                self.logger.info(f"{copied_pdf_count} PDFs de muestra copiados a {sample_pdfs_dir_absolute}") # Usar ruta absoluta
            else:
                self.logger.warning(f"No se copiaron PDFs de muestra a {sample_pdfs_dir_absolute} (podría ser que no se encontraron archivos válidos o hubo errores).") # Usar ruta absoluta
        except IOError as e:
            self.logger.error(f"Error de I/O escribiendo test_results_br.json en {output_file_path_absolute}: {e}") # Usar ruta absoluta
        except Exception as e:
            self.logger.error(f"Error inesperado generando test_results_br.json: {e}", exc_info=True)

    def _run_keyword_search(self):
        """Ejecuta la búsqueda por palabras clave y registra los ítems encontrados."""
        keywords = self.config.get('keyword_search_keywords', [])
        if not keywords:
            self.logger.info("No se especificaron palabras clave en la configuración ('keyword_search_keywords'). Saltando fase de búsqueda web.")
            return

        max_pages = self.config.get('keyword_max_pages')
        # max_items_per_keyword = self.config.get('keyword_max_items_per_keyword') # Añadir si se quiere limitar por keyword

        if not hasattr(self, 'keyword_searcher') or self.keyword_searcher is None:
            try:
                self.keyword_searcher = KeywordSearcherBR(self.config, self.logger, self.db_manager, self.downloader, self.selectors)
                self.logger.info("KeywordSearcherBR instanciado.")
            except Exception as e:
                self.logger.error(f"Error al instanciar KeywordSearcherBR: {e}", exc_info=True)
                return
        
        self.logger.info(f"Starting keyword search process for: {', '.join(keywords)}")
        total_new_items_found = 0
        for keyword in keywords:
            try:
                count = self.keyword_searcher.search_and_register_keyword(
                    keyword,
                    max_pages=max_pages,
                    # max_items_per_keyword=max_items_per_keyword # Pasar si se implementa el límite
                )
                total_new_items_found += count
            except Exception as e:
                self.logger.error(f"Error durante la búsqueda de la keyword '{keyword}': {e}", exc_info=True)
        self.logger.info(f"Keyword search and item registration phase completed. Total new items registered: {total_new_items_found}")
        return {"keyword_new_items_found": total_new_items_found}

    def _process_items_for_html_metadata(self):
        """Procesa ítems pendientes de extracción de metadatos desde HTML."""
        limit = self.config.get('max_items_to_process_html')
        items = self.db_manager.get_items_to_process(statuses=['pending_html_processing'], limit=limit)
        self.logger.info(f"Iniciando procesamiento HTML para {len(items)} ítems (límite: {limit}). Estado buscado: pending_html_processing")

        if not items:
            self.logger.info("No hay ítems pendientes de procesamiento HTML ('pending_html_processing').")
            return 0

        processed_count = 0
        for item_data in items:
            item_id = item_data['item_id']
            item_page_url = item_data['item_page_url']
            self.logger.info(f"Procesando HTML para item ID {item_id}: {item_page_url}")
            
            # Marcar como en progreso
            self.db_manager.update_item_status(item_id, 'processing_html')
            
            # Llamar al nuevo método específico para HTML snapshots
            html_content, html_local_path = self.downloader.fetch_html_snapshot(item_page_url, item_id_for_path=item_id)
            # html_content, html_local_path = self.downloader.fetch_content(item_page_url, is_html_snapshot=True, item_id_for_path=item_id) # Llamada anterior incorrecta

            if html_content:
                self.logger.debug(f"Snapshot HTML obtenido para item ID {item_id}, guardado en: {html_local_path}")
                try:
                    metadata = self.html_extractor.extract_metadata(html_content, item_page_url)
                    self.logger.info(f"Metadatos extraídos para item ID {item_id}. Título: {metadata.get('title', 'N/A')}")
                    
                    # Combinar metadatos existentes (si los hay) con los nuevos
                    existing_metadata = {}
                    if item_data.get('metadata_json'):
                        try:
                            existing_metadata = json.loads(item_data['metadata_json'])
                        except json.JSONDecodeError:
                            self.logger.warning(f"Error decodificando metadatos JSON existentes para item ID {item_id}")
                    
                    # Dar prioridad a los nuevos metadatos extraídos del HTML
                    final_metadata = {**existing_metadata, **metadata}
                    
                    # Log metadatos y path HTML
                    self.db_manager.log_item_metadata(item_id, final_metadata, html_local_path)
                    
                    # Actualizar estado a pendiente de descarga (o completado si no hay PDF)
                    # (La lógica de encontrar PDF link debe estar aquí o en HTMLMetadataExtractor)
                    # Por ahora, asumimos que el extractor no busca PDF y pasamos a pendiente de descarga
                    self.db_manager.update_item_status(item_id, 'pending_download') 
                    processed_count += 1
                    
                except Exception as e_extract:
                    self.logger.error(f"Error extrayendo metadatos HTML para item ID {item_id}: {e_extract}", exc_info=True)
                    self.db_manager.update_item_status(item_id, 'failed_html_processing')
            else:
                self.logger.warning(f"No se pudo obtener contenido HTML para item ID {item_id} desde {item_page_url}. Marcando como fallo.")
                self.db_manager.update_item_status(item_id, 'failed_html_processing')

        self.logger.info(f"Procesamiento HTML completado. {processed_count} ítems actualizados.")
        return processed_count

    def _ensure_output_dirs(self):
        """Asegura que los directorios de salida principales existan."""
        output_path_relative = self.config.get('output_path', 'output')
        output_path_absolute = os.path.join(MODULE_BR_ROOT_DIR, output_path_relative)
        
        if not os.path.exists(output_path_absolute):
            try:
                os.makedirs(output_path_absolute)
                self.logger.info(f"Directorio de salida principal creado: {output_path_absolute}")
            except OSError as e:
                self.logger.error(f"Error creando directorio de salida {output_path_absolute}: {e}")
        # Los subdirectorios para PDFs y HTML snapshots son creados por ResourceDownloaderBR,
        # que ahora también usará MODULE_BR_ROOT_DIR y la ruta relativa de output_path.

    def run(self):
        """Orquesta el proceso completo del scraper."""
        self.logger.info("Iniciando ejecución del scraper de Embrapa.")
        start_time = time.time()

        overall_stats = {
            'oai_total_fetched': 0,
            'oai_total_processed_db': 0,
            'oai_total_new_items_db': 0,
            'oai_total_metadata_updated_db': 0,
            'oai_total_harvest_failures': 0,
            'oai_repositories_processed_count': 0,
            'keyword_new_items_found': 0,
            'html_items_processed': 0,
            'pdf_links_items_processed': 0,
            'pdf_links_found': 0,
            'pdf_links_failed_extraction': 0,
            'pdfs_download_items_processed': 0,
            'pdfs_downloaded_ok': 0,
            'pdfs_download_failed': 0,
            'pdfs_download_missing_url': 0
        }

        try:
            # --- Fase 1: Descubrimiento / Registro --- 
            # Ejecutar OAI PRIMERO si está activado
            run_oai = False
            for repo_key in self.config.get('repositories', {}):
                 if self.config.get(f'max_oai_records_{repo_key}', 0) > 0:
                      run_oai = True
                      break
            if run_oai:
                 oai_harvest_stats = self._run_oai_harvest() 
                 if oai_harvest_stats:
                    overall_stats['oai_total_fetched'] = oai_harvest_stats.get('oai_total_fetched', 0)
                    overall_stats['oai_total_processed_db'] = oai_harvest_stats.get('oai_total_processed_db', 0)
                    overall_stats['oai_total_new_items_db'] = oai_harvest_stats.get('oai_total_new_items_db', 0)
                    overall_stats['oai_total_metadata_updated_db'] = oai_harvest_stats.get('oai_total_metadata_updated_db', 0)
                    overall_stats['oai_total_harvest_failures'] = oai_harvest_stats.get('oai_total_harvest_failures', 0)
                    overall_stats['oai_repositories_processed_count'] = oai_harvest_stats.get('oai_repositories_processed_count', 0)
            else:
                 self.logger.info("Cosecha OAI desactivada en configuración. Saltando.")

            # Ejecutar Keyword Search DESPUÉS o si OAI está desactivado
            if self.config.get('keyword_search_keywords'):
                 keyword_stats = self._run_keyword_search()
                 if keyword_stats:
                    overall_stats['keyword_new_items_found'] = keyword_stats.get('keyword_new_items_found', 0)
            else:
                 self.logger.info("No hay keywords configuradas, saltando búsqueda por keyword.")
            
            # --- Fase 2: Procesamiento HTML y Metadatos --- 
            html_processed_count = self._process_items_for_html_metadata()
            overall_stats['html_items_processed'] = html_processed_count

            # --- Fase 3: Extracción Enlaces PDF --- 
            max_pdf_link_items = self.config.get('max_pdf_link_extraction_items')
            pdf_link_stats = self._process_items_for_pdf_links(max_items_to_process=max_pdf_link_items)
            if pdf_link_stats:
                overall_stats['pdf_links_items_processed'] = pdf_link_stats.get('processed_count',0)
                overall_stats['pdf_links_found'] = pdf_link_stats.get('found_links_count',0)
                overall_stats['pdf_links_failed_extraction'] = pdf_link_stats.get('failed_extraction_count',0)

            # --- Fase 4: Descarga de Archivos (PDFs) --- 
            max_download_items = self.config.get('max_pdf_download_items')
            pdf_download_stats = self._download_pdf_files(max_items_to_process=max_download_items)
            if pdf_download_stats:
                overall_stats['pdfs_download_items_processed'] = pdf_download_stats.get('processed_count',0)
                overall_stats['pdfs_downloaded_ok'] = pdf_download_stats.get('downloaded_count',0)
                overall_stats['pdfs_download_failed'] = pdf_download_stats.get('failed_count',0)
                overall_stats['pdfs_download_missing_url'] = pdf_download_stats.get('missing_url_count',0)
            
            # --- Fase 5: Generación de Reportes --- 
            if self.config.get('generate_state_json'):
                self._generate_state_json()
            if self.config.get('generate_test_results_json'):
                self._generate_test_results_json(max_sample_pdfs=self.config.get('test_results_sample_size', 5))
            
        except Exception as e:
            self.logger.critical(f"Error crítico durante la ejecución del scraper: {e}", exc_info=True)
        finally:
            end_time = time.time()
            duration = end_time - start_time
            self.logger.info(f"Ejecución del scraper de Embrapa finalizada en {duration:.2f} segundos. Estadísticas (parciales): {overall_stats}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scraper para el portal de publicaciones de Embrapa.")
    # Mantener argumentos por si se quieren usar en el futuro, pero simplificar la lógica de ejecución
    parser.add_argument("--keyword", type=str, help="Palabra clave para buscar (sobrescribe config).")
    parser.add_argument("--harvest-all", action="store_true", help="Activa la cosecha vía OAI-PMH (sobrescribe config).")
    parser.add_argument("--max-items", type=int, help="Número máximo de ítems a procesar (sobrescribe config).")

    args = parser.parse_args()

    # Crear directorios base si no existen
    os.makedirs("BR/db", exist_ok=True)
    os.makedirs("BR/logs", exist_ok=True)
    os.makedirs("BR/output/pdfs", exist_ok=True)
    os.makedirs("BR/output/html_snapshot", exist_ok=True)
    os.makedirs("BR/docs/sample_pdfs", exist_ok=True)

    config_to_use = DEFAULT_CONFIG_BR.copy()

    # Sobrescribir config con argumentos CLI si se proporcionan
    if args.keyword:
        config_to_use['keyword_search_keywords'] = [args.keyword] # Usar la clave correcta y ponerla en lista
    if args.harvest_all:
        # Activar OAI si se pide (ajustar límites si se implementa OAI)
        config_to_use['max_oai_records_alice'] = config_to_use.get('max_oai_records_alice', 100) if config_to_use.get('max_oai_records_alice', 0) == 0 else config_to_use.get('max_oai_records_alice') # Ejemplo de activación
        config_to_use['max_oai_records_infoteca'] = config_to_use.get('max_oai_records_infoteca', 100) if config_to_use.get('max_oai_records_infoteca', 0) == 0 else config_to_use.get('max_oai_records_infoteca')
    if args.max_items is not None:
        # Aplicar max_items a los límites relevantes
        config_to_use['max_items_to_process_html'] = min(config_to_use.get('max_items_to_process_html', 10), args.max_items)
        config_to_use['max_pdf_link_extraction_items'] = min(config_to_use.get('max_pdf_link_extraction_items', 10), args.max_items)
        config_to_use['max_pdf_download_items'] = min(config_to_use.get('max_pdf_download_items', 10), args.max_items)
        # Considerar si max_items debe limitar también OAI o keyword pages/items
        config_to_use['keyword_max_pages'] = min(config_to_use.get('keyword_max_pages', 3), (args.max_items // config_to_use.get('keyword_search_items_per_page', 10)) + 1) # Aproximación

    # Simplemente instanciar y ejecutar
    scraper = ScraperBR(config_to_use)
    scraper.run() 
