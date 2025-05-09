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

DEFAULT_CONFIG_BR = {
    "base_url": "https://www.embrapa.br",
    # URLs base de los repositorios (no necesariamente OAI endpoints)
    "base_url_alice_repo": "https://www.alice.cnptia.embrapa.br/alice/",
    "base_url_infoteca_repo": "https://www.infoteca.cnptia.embrapa.br/infoteca/",
    "base_url_embrapa_search": "https://www.embrapa.br/busca-de-publicacoes",
    "country_code": "BR",
    "db_file": "BR/db/scraper_br.db",
    "log_file": "BR/logs/BR-SCRAPER.log",
    "output_dir": "BR/output",
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
    "selectors_file": "BR/selectors.yaml",
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
    "output_state_file": "BR/output/state_br.json",
    "output_test_results_file": "BR/output/test_results_br.json",
    "docs_sample_pdfs_dir": "BR/docs/sample_pdfs/",
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
}

class ScraperBR:
    def __init__(self, config):
        self.config = config
        self.logger = self._setup_logging()
        self.selectors = self._load_selectors()

        self.db_manager = DatabaseManagerBR(self.config['db_file'], self.logger)
        self.db_manager.initialize_db() 

        # Pasar self.db_manager a ResourceDownloaderBR
        self.downloader = ResourceDownloaderBR(self.config, self.logger, self.db_manager)
        
        self.oai_harvester = OAIHarvesterBR(self.config, self.logger, self.db_manager)
        self.html_extractor = HTMLMetadataExtractorBR(self.config, self.logger, self.selectors, self.downloader)
        self.keyword_searcher = KeywordSearcherBR(self.config, self.logger, self.db_manager, self.downloader, self.selectors)

        self.logger.info("ScraperBR inicializado con todos los componentes principales.")

    def _setup_logging(self):
        logger = logging.getLogger("EmbrapaScraper")
        logger.setLevel(logging.INFO)
        # Crear directorio de logs si no existe
        os.makedirs(os.path.dirname(self.config['log_file']), exist_ok=True)
        # Handler para archivo
        fh = logging.FileHandler(self.config['log_file'], mode='a')
        fh.setLevel(logging.INFO)
        # Handler para consola
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # Formato
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # Añadir handlers
        if not logger.handlers:
            logger.addHandler(fh)
            logger.addHandler(ch)
        return logger

    def _load_selectors(self):
        try:
            with open(self.config['selectors_file'], 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.error(f"Archivo de selectores no encontrado en {self.config['selectors_file']}")
            return {}
        except yaml.YAMLError as e:
            self.logger.error(f"Error al parsear el archivo de selectores YAML: {e}")
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

    def _download_pdf_files(self, max_items_to_process=None):
        """Descarga los archivos PDF para los ítems marcados como 'awaiting_pdf_download'."""
        self.logger.info("Iniciando fase de descarga de PDFs...")
        items_to_download = self.db_manager.get_items_to_process(statuses=['awaiting_pdf_download'], limit=max_items_to_process)

        if not items_to_download:
            self.logger.info("No hay ítems esperando descarga de PDF.")
            return

        self.logger.info(f"Se intentará descargar PDFs para {len(items_to_download)} ítems.")
        successful_downloads = 0
        failed_downloads = 0

        for item_data in items_to_download:
            item_id = item_data['item_id']
            oai_id = item_data.get('oai_identifier', 'N/A')
            log_prefix = f"[ItemDB {item_id} / OAI {oai_id}]"
            
            pdf_url = None
            current_metadata = {}
            if item_data.get('metadata_json'):
                try:
                    current_metadata = json.loads(item_data['metadata_json'])
                    pdf_url = current_metadata.get('pdf_direct_url')
                except json.JSONDecodeError:
                    self.logger.error(f"{log_prefix} No se pudo parsear metadata_json para obtener pdf_direct_url para descarga.")
                    self.db_manager.update_item_status(item_id, 'error_pdf_download_metadata')
                    failed_downloads += 1
                    continue
            
            if not pdf_url:
                self.logger.error(f"{log_prefix} No se encontró pdf_direct_url en metadatos para descarga. Marcando como error.")
                self.db_manager.update_item_status(item_id, 'error_pdf_download_no_url')
                failed_downloads += 1
                continue

            self.logger.info(f"{log_prefix} Intentando descargar PDF desde: {pdf_url}")
            self.db_manager.update_item_status(item_id, 'processing_pdf_download')

            file_id, success, local_path, md5_hash, file_size = self.downloader.download_pdf(item_id, pdf_url)

            if success:
                successful_downloads += 1
                self.logger.info(f"{log_prefix} PDF descargado y registrado con file_id {file_id} en: {local_path}")
                self.db_manager.update_item_status(item_id, 'processed_pdf_downloaded')
                # Actualizar metadata_json con la info del archivo si es necesario (ya lo hace el downloader a través de log_file_result)
            else:
                failed_downloads += 1
                self.logger.error(f"{log_prefix} Falló la descarga del PDF desde {pdf_url}. Detalles en logs anteriores.")
                self.db_manager.update_item_status(item_id, 'error_pdf_download_failed')
            
            if self.config.get('delay', 0.1) > 0:
                time.sleep(self.config.get('download_delay_seconds', 1)) # Delay de descarga entre archivos
        
        self.logger.info(f"Fase de descarga de PDFs completada. Éxitos: {successful_downloads}, Fallos: {failed_downloads}.")

    def _generate_state_json(self):
        if not self.config.get('generate_state_json', True):
            self.logger.info("Generación de state.json desactivada.")
            return

        self.logger.info("Generando archivo de estado JSON...")
        output_file = self.config['output_state_file']
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        items_data = []
        try:
            # Consulta para obtener todos los ítems y sus archivos asociados
            # Esto podría ser muy grande para una base de datos con muchos ítems.
            # Considerar paginación o un resumen si es necesario.
            conn = sqlite3.connect(self.config['db_file'])
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM items ORDER BY item_id DESC") # Tomar los más recientes primero
            all_items = cursor.fetchall()
            
            for item_row in all_items:
                item_dict = dict(item_row)
                
                # Cargar metadata_json si existe y es un JSON válido
                if item_dict.get('metadata_json'):
                    try:
                        item_dict['metadata_json'] = json.loads(item_dict['metadata_json'])
                    except json.JSONDecodeError:
                        self.logger.warning(f"[ItemDB {item_dict['item_id']}] metadata_json no es un JSON válido. Se mantendrá como string.")
                
                # Obtener archivos asociados
                cursor.execute("SELECT * FROM files WHERE item_id = ?", (item_dict['item_id'],))
                files_for_item = cursor.fetchall()
                item_dict['files'] = [dict(f_row) for f_row in files_for_item]
                
                items_data.append(item_dict)
            
            conn.close()

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(items_data, f, indent=4, ensure_ascii=False)
            self.logger.info(f"Archivo de estado JSON generado en: {output_file} con {len(items_data)} ítems.")
        except sqlite3.Error as e:
            self.logger.error(f"Error de base de datos al generar state.json: {e}")
            if conn: conn.close()
        except Exception as e:
            self.logger.error(f"Error al generar state.json: {e}", exc_info=True)

    def _generate_test_results_json(self, max_sample_pdfs=5):
        if not self.config.get('generate_test_results_json', True):
            self.logger.info("Generación de test_results.json desactivada.")
            return

        self.logger.info("Generando archivo de resultados de prueba JSON...")
        output_file = self.config['output_test_results_file']
        sample_dir = self.config['docs_sample_pdfs_dir']
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        if sample_dir:
            os.makedirs(sample_dir, exist_ok=True)

        test_results = []
        try:
            conn = sqlite3.connect(self.config['db_file'])
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Seleccionar ítems que tienen un PDF descargado exitosamente
            query = """
            SELECT i.item_id, i.oai_identifier, i.item_page_url, i.metadata_json, f.local_path, f.md5_hash, f.file_size_bytes
            FROM items i
            JOIN files f ON i.item_id = f.item_id
            WHERE f.file_type = 'pdf' AND f.download_status = 'success'
            ORDER BY i.item_id DESC -- Tomar de los más recientes
            LIMIT ?
            """
            cursor.execute(query, (max_sample_pdfs,))
            items_with_pdfs = cursor.fetchall()

            for item_row in items_with_pdfs:
                item_dict = dict(item_row)
                metadata = {}
                if item_dict.get('metadata_json'):
                    try:
                        metadata = json.loads(item_dict['metadata_json'])
                    except json.JSONDecodeError:
                        self.logger.warning(f"[ItemDB {item_dict['item_id']}] metadata_json no es un JSON válido para test_results.")
                
                result_entry = {
                    "item_id_db": item_dict['item_id'],
                    "oai_identifier": item_dict.get('oai_identifier'),
                    "item_page_url": item_dict.get('item_page_url'),
                    "title": metadata.get('title', 'N/A'),
                    "authors": metadata.get('authors', []), # Asumiendo que authors es una lista en metadata_json
                    "publication_date": metadata.get('publication_date'),
                    "doi": metadata.get('doi'),
                    "pdf_direct_url": metadata.get('pdf_direct_url'),
                    "local_pdf_path": item_dict['local_path'],
                    "md5_hash": item_dict['md5_hash'],
                    "file_size_bytes": item_dict['file_size_bytes']
                }
                test_results.append(result_entry)

                # Copiar el PDF a la carpeta de muestras para el DOC
                if sample_dir and item_dict['local_path'] and os.path.exists(item_dict['local_path']):
                    try:
                        dest_filename = f"item_{item_dict['item_id']}_{os.path.basename(item_dict['local_path'])}"
                        shutil.copy2(item_dict['local_path'], os.path.join(sample_dir, dest_filename))
                        self.logger.info(f"PDF de muestra copiado a: {os.path.join(sample_dir, dest_filename)}")
                    except Exception as e_copy:
                        self.logger.error(f"Error al copiar PDF de muestra '{item_dict['local_path']}': {e_copy}")
            
            conn.close()
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(test_results, f, indent=4, ensure_ascii=False)
            self.logger.info(f"Archivo de resultados de prueba JSON generado en: {output_file} con {len(test_results)} ítems.")

        except sqlite3.Error as e:
            self.logger.error(f"Error de base de datos al generar test_results.json: {e}")
            if conn: conn.close()
        except Exception as e:
            self.logger.error(f"Error al generar test_results.json: {e}", exc_info=True)

    def _run_keyword_search(self):
        self.logger.info("Iniciando fase de búsqueda por palabra clave.")
        keywords = self.config.get('keyword_search_keywords', [])
        if not keywords:
            self.logger.info("No hay palabras clave configuradas. Saltando fase de búsqueda por palabra clave.")
            return

        total_items_found_keyword = 0
        try:
            for keyword in keywords:
                self.logger.info(f"Buscando palabra clave: '{keyword}'")
                items_found_this_keyword = self.keyword_searcher.search_keyword(keyword)
                total_items_found_keyword += items_found_this_keyword
                self.logger.info(f"Se encontraron {items_found_this_keyword} ítems para la palabra clave '{keyword}'. Acumulado total: {total_items_found_keyword}")
                if self.config.get('delay', 0.1) > 0: # Delay entre keywords
                    time.sleep(self.config.get('delay', 0.1) * 5) # Un delay mayor entre keywords
        except Exception as e:
            self.logger.error(f"Error durante la búsqueda por palabra clave: {e}", exc_info=True)
        finally:
            self.keyword_searcher.quit_driver() # Asegurarse de cerrar el driver de Selenium
        
        self.logger.info(f"Fase de búsqueda por palabra clave completada. Total de ítems encontrados/procesados: {total_items_found_keyword}")
        return {"keyword_search_total_items_processed": total_items_found_keyword} 

    def _process_items_for_html_metadata(self):
        """Procesa ítems que necesitan extracción de metadatos desde HTML."""
        self.logger.info("Iniciando proceso de extracción de metadatos HTML...")
        # Este estado es para ítems que vienen de Keyword Search y necesitan procesamiento de su página HTML
        items_to_process = self.db_manager.get_items_to_process(statuses=['pending_html_processing'], limit=self.config.get('max_html_processing_items'))

        if not items_to_process:
            self.logger.info("No hay ítems pendientes de procesamiento HTML.")
            return

        self.logger.info(f"Se procesarán {len(items_to_process)} ítems para extracción de metadatos HTML.")
        processed_count = 0
        successful_extraction_count = 0

        for item_data in items_to_process:
            item_id = item_data['item_id']
            item_page_url = item_data['item_page_url']
            log_prefix = f"[ItemDB {item_id}]"

            self.logger.info(f"{log_prefix} Procesando HTML para metadatos y enlace PDF: {item_page_url}")
            self.db_manager.update_item_status(item_id, 'processing_html')

            # La función extract_full_metadata_and_pdf_link ahora devuelve un dict de metadatos y el enlace PDF
            # El HTML es obtenido dentro de la función del extractor si no se provee.
            extracted_metadata, pdf_direct_url = self.html_extractor.extract_full_metadata_and_pdf_link(item_page_url, item_id_for_log=str(item_id))

            if extracted_metadata or pdf_direct_url: # Si se extrajo algo
                successful_extraction_count +=1
                # Combinar con metadatos existentes si los hay (ej. título de la pág de búsqueda)
                current_db_metadata = {}
                item_details = self.db_manager.get_item_details(item_id)
                if item_details and item_details.get('metadata_json'):
                    try: current_db_metadata = json.loads(item_details['metadata_json'])
                    except: pass
                
                # Priorizar metadatos recién extraídos de la página del ítem
                current_db_metadata.update(extracted_metadata) 
                if pdf_direct_url:
                    current_db_metadata['pdf_direct_url'] = pdf_direct_url
                
                self.db_manager.log_item_metadata(item_id, current_db_metadata)
                
                if pdf_direct_url:
                    self.logger.info(f"{log_prefix} Enlace PDF encontrado: {pdf_direct_url}. Ítem listo para descarga de PDF.")
                    self.db_manager.update_item_status(item_id, 'awaiting_pdf_download')
                else:
                    self.logger.warning(f"{log_prefix} No se encontró enlace PDF directo, pero se extrajeron metadatos. Ítem necesita revisión o ya está completo sin PDF.")
                    # Si no hay PDF, pero los metadatos son suficientes, se podría marcar como procesado sin PDF.
                    # O podría tener otro estado como 'processed_no_pdf_found'
                    self.db_manager.update_item_status(item_id, 'processed_metadata_only') # Nuevo estado sugerido
            else:
                self.logger.error(f"{log_prefix} Falló la extracción de metadatos y enlace PDF de {item_page_url}")
                self.db_manager.update_item_status(item_id, 'error_html_processing')
            
            processed_count += 1
            if self.config.get('delay', 0.1) > 0:
                time.sleep(self.config.get('delay', 0.1))

        self.logger.info(f"Proceso de extracción de metadatos HTML completado. Procesados: {processed_count}, Extracciones exitosas (parcial o total): {successful_extraction_count}")

    def run(self):
        self.logger.info(f"Iniciando ejecución del Scraper para Embrapa (BR) a las {time.strftime('%Y-%m-%d %H:%M:%S')}")
        start_time = time.time() # Esta línea causaba el NameError si 'time' no estaba importado

        # Fase 1: Cosecha OAI (si está habilitada)
        oai_stats = self._run_oai_harvest()
        if oai_stats:
            self.logger.info(f"Resumen de cosecha OAI: {oai_stats}")

        # Fase 2: Búsqueda por palabra clave (si está habilitada)
        keyword_stats = self._run_keyword_search()
        if keyword_stats:
             self.logger.info(f"Resumen de búsqueda por palabra clave: {keyword_stats}")

        # Fase 3: Procesar ítems para extracción de metadatos HTML y/o enlaces PDF
        # Esto es para ítems de keyword search (pending_html_processing) o ítems de OAI que no tenían pdf_direct_url (pending_download)
        self._process_items_for_html_metadata() # Para ítems de keyword search
        self._process_items_for_pdf_links(max_items_to_process=self.config.get('max_pdf_link_extraction_items')) # Para OAI items que no tenían el enlace directo

        # Fase 4: Descargar PDFs para ítems listos
        self._download_pdf_files(max_items_to_process=self.config.get('max_pdf_download_items'))

        # Fase 5: Generar archivos de estado y resultados de prueba
        self._generate_state_json()
        self._generate_test_results_json(max_sample_pdfs=self.config.get('test_results_sample_size', 5))

        end_time = time.time()
        self.logger.info(f"Ejecución del Scraper para Embrapa (BR) finalizada en {end_time - start_time:.2f} segundos.")
        # Aquí puedes añadir un resumen final de la base de datos si es necesario.
        final_item_stats = self.db_manager.get_item_stats()
        final_file_stats = self.db_manager.get_file_stats()
        self.logger.info(f"Estadísticas finales de ítems en BD: {final_item_stats}")
        self.logger.info(f"Estadísticas finales de archivos en BD: {final_file_stats}")

        self.db_manager.close_connection()
        self.logger.info("Conexión a la base de datos cerrada.")

def main(args):
    print(f"Argumentos recibidos por main: {args}") # Log de depuración
    config = DEFAULT_CONFIG_BR.copy()

    # Aquí podrías añadir lógica para cargar un archivo de configuración YAML si se proporciona
    # y sobrescribir DEFAULT_CONFIG_BR, y luego sobrescribir con args específicos.

    if args.keyword:
        config['keyword_search_keywords'] = [args.keyword]
        print(f"Búsqueda por palabra clave establecida a: {args.keyword}")
    
    if args.max_oai is not None: # Asegurarse de que el argumento no sea None
        # Asumiendo que quieres aplicar este límite a ambos repositorios OAI si se especifica
        config['max_oai_records_alice'] = args.max_oai
        config['max_oai_records_infoteca'] = args.max_oai
        print(f"Máximo de registros OAI por repositorio establecido a: {args.max_oai}")

    scraper = ScraperBR(config)
    scraper.run()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scraper para el portal de Embrapa (Brasil).")
    parser.add_argument("-k", "--keyword", type=str, help="Palabra clave específica para buscar.")
    parser.add_argument("--max-oai", type=int, help="Número máximo de registros a cosechar por cada repositorio OAI.")
    # Añadir más argumentos según sea necesario (ej. --config-file, --log-level, etc.)

    # Línea de depuración para ver qué argumentos se están parseando
    # import sys
    # print(f"Argumentos de línea de comando raw: {sys.argv}")

    args = parser.parse_args()
    
    # Línea de depuración para ver los argumentos parseados
    # print(f"Argumentos parseados: {args}")

    main(args)
