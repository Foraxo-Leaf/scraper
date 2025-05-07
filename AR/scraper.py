import requests
import xml.etree.ElementTree as ET
import yaml
import json
import os
import time
import hashlib
import datetime
from urllib.parse import urljoin, urlparse
from lxml import html
import logging # Usaremos logging estándar
import sqlite3
import shutil

# --- Configuración Global Inicial (se pasará a la clase Scraper) ---
# Estos valores podrían eventualmente cargarse desde un archivo YAML/JSON
DEFAULT_CONFIG = {
    "base_url": "https://repositorio.inta.gob.ar",
    "country_code": "AR",
    "oai_endpoint": "https://repositorio.inta.gob.ar/oai/request",
    "metadata_prefix": "oai_dc",
    "selectors_file": "selectors.yaml", # Relativo al script
    "output_dir": "output", # Relativo al script, se resolverá en __init__
    "pdf_dir": "docs/sample_pdfs/", # Relativo al script, se resolverá en __init__ (para empaquetado)
    "metadata_file": "output/metadata.jsonl", # Relativo al script, se resolverá en __init__
    "log_file": "logs/AR-SCRAPER.log", # Relativo al script, se resolverá en __init__
    "db_file": "db/scraper.db", # Relativo al script, se resolverá en __init__

    # --- Configuración específica para el modo de ejecución ---
    "mode": "keyword_search",  # Cambiado de 'oai' a 'keyword_search'
    "keywords": ["semilla"], # Cambiado para la prueba
    "max_search_pages": 1, # Limitado para la prueba
    "rpp": 10, # Resultados por página (DSpace default suele ser 10 o 20)

    # --- Configuración general del scraper ---
    "max_records": 50, # Límite para OAI (no aplica a search directamente, pero puede ser usado por get_items_to_process)
    "delay": 1, # Segundos entre requests
    "request_timeout": 45, # Timeout para requests generales
    "download_timeout": 120, # Timeout específico para descargas
    "download_max_retries": 3, # Nuevos: Número máximo de reintentos para descargas
    "download_base_retry_delay": 5, # Nuevos: Delay base en segundos para reintentos de descarga
    "process_statuses": [ # Estados que disparan el procesamiento completo del ítem
            'pending_download',
            'pending_metadata',
            'error',
            'error_extraction',
            'error_download'
        ]
}

# --- Funciones Auxiliares ---

def setup_logging(log_file):
    """Configura el logging para archivo y consola."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    log_level = logging.INFO

    # Crear directorio de logs si no existe
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print(f"Error creando directorio de logs {log_dir}: {e}")
            # Continuar sin logging a archivo si falla la creación del dir

    # Root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Limpiar handlers existentes para evitar duplicados en re-ejecuciones
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    # Handler para archivo (rotar archivo viejo)
    if log_dir and os.path.exists(log_dir): # Solo si el directorio existe
        log_file_old = f"{log_file}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.old"
        if os.path.exists(log_file):
            try:
                os.rename(log_file, log_file_old)
            except OSError as e:
                print(f"No se pudo renombrar log anterior {log_file}: {e}")
        try:
             file_handler = logging.FileHandler(log_file, encoding='utf-8')
             file_handler.setFormatter(log_formatter)
             logger.addHandler(file_handler)
        except Exception as e:
             print(f"Error al crear file_handler para {log_file}: {e}")

    else:
         print(f"Advertencia: No se pudo configurar el logging a archivo {log_file}")


    # Handler para consola
    try:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)
    except Exception as e:
        print(f"Error al crear console_handler: {e}")

def setup_directories(output_dir, pdf_dir):
    """Crea los directorios de salida necesarios."""
    try:
        if output_dir: # Asegurar que output_dir no sea None tampoco
            os.makedirs(output_dir, exist_ok=True)
        # Solo intentar crear pdf_dir si NO es None
        if pdf_dir:
            os.makedirs(pdf_dir, exist_ok=True)
    except OSError as e:
        logging.error(f"Error creando directorios de salida: {e}")
        raise

def calculate_md5(file_path):
    """Calcula el hash MD5 de un archivo."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        logging.error(f"Archivo no encontrado para calcular MD5: {file_path}")
        return None
    except Exception as e:
        logging.error(f"Error calculando MD5 para {file_path}: {e}")
        return None

# --- Clases ---

class RegistryManager:
    """Gestiona la lectura y escritura del archivo de registro JSON Lines."""
    def __init__(self, registry_file, country_code, base_url):
        self.registry_file = registry_file
        self.country_code = country_code
        self.base_url = base_url

    def load_processed_identifiers(self):
        """Lee el archivo registry.jsonl y devuelve un set de OAI IDs ya descargados o existentes."""
        processed = set()
        try:
            with open(self.registry_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        record = json.loads(line)
                        # Considerar éxito si tiene ruta local y estado 'downloaded' o 'skipped_exists'
                        if record.get('local_pdf_path') and record.get('download_status') in ['downloaded', 'skipped_exists']:
                            oai_id = record.get('oai_identifier')
                            if oai_id: # Asegurar que el identificador exista
                                 processed.add(oai_id)
                            else:
                                 logging.warning(f"Registro en línea {line_num} de {self.registry_file} sin oai_identifier.")
                    except json.JSONDecodeError:
                        logging.warning(f"Ignorando línea inválida {line_num} en {self.registry_file}: {line.strip()}")
        except FileNotFoundError:
            logging.info(f"Archivo de registro {self.registry_file} no encontrado. Se creará uno nuevo.")
        except Exception as e:
            logging.error(f"Error cargando identificadores procesados desde {self.registry_file}: {e}")
        logging.info(f"Cargados {len(processed)} identificadores ya procesados/existentes desde {self.registry_file}")
        return processed

    def log_to_registry(self, data):
        """Añade una entrada (diccionario) al archivo registry.jsonl."""
        # Asegurarse de que los campos esenciales estén presentes
        entry = {
            'oai_identifier': data.get('oai_identifier'),
            'item_page_url': data.get('item_page_url'),
            'pdf_url_found': data.get('pdf_url_found'),
            'download_status': data.get('download_status'),
            'local_pdf_path': data.get('local_pdf_path'),
            'md5_hash': data.get('md5_hash'),
            'file_size_bytes': data.get('file_size_bytes'),
            'title': data.get('title'), # Opcional
            'last_attempt_timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds') + 'Z',
            'country_code': self.country_code,
            'repo_base_url': self.base_url
        }
        try:
            with open(self.registry_file, 'a', encoding='utf-8') as f:
                json.dump(entry, f, ensure_ascii=False)
                f.write('\n')
        except Exception as e:
            logging.error(f"Error escribiendo en el registro {self.registry_file} para {entry.get('oai_identifier')}: {e}")

class OAIHarvester:
    """Obtiene identificadores de registros desde un endpoint OAI-PMH."""
    def __init__(self, endpoint, metadata_prefix, request_delay, timeout):
        self.endpoint = endpoint
        self.metadata_prefix = metadata_prefix
        self.delay = request_delay
        self.timeout = timeout

    def get_identifiers(self, max_records=None):
        """Obtiene la lista de identificadores OAI, manejando resumption tokens."""
        identifiers = []
        params = {"verb": "ListIdentifiers", "metadataPrefix": self.metadata_prefix}
        resumption_token = None
        records_fetched = 0
        request_attempts = 0
        max_attempts = 3 # Máximo de reintentos por request OAI

        while True:
            current_params = {"verb": "ListIdentifiers"}
            if resumption_token:
                current_params["resumptionToken"] = resumption_token
            else:
                current_params["metadataPrefix"] = self.metadata_prefix

            request_attempts = 0
            response = None # Initialize response to None
            while request_attempts < max_attempts:
                try:
                    logging.info(f"Obteniendo identificadores OAI... Token: {'Presente' if resumption_token else 'N/A'} (Intento {request_attempts + 1})")
                    response = requests.get(self.endpoint, params=current_params, timeout=self.timeout)
                    response.raise_for_status()
                    time.sleep(self.delay)
                    xml_content = response.content
                    break # Éxito, salir del bucle de reintentos
                except requests.exceptions.Timeout:
                    logging.warning(f"Timeout al obtener identificadores OAI (token: {resumption_token}). Reintentando en {self.delay * (2 ** (request_attempts+1))}s...")
                    request_attempts += 1
                    time.sleep(self.delay * (2 ** request_attempts)) # Exponential backoff
                    if request_attempts >= max_attempts:
                         logging.error("Máximos reintentos alcanzados por Timeout en OAI. Abortando obtención de IDs.")
                         return identifiers # Devolver lo obtenido hasta ahora
                except requests.exceptions.RequestException as e:
                    logging.error(f"Error de red al obtener identificadores OAI: {e}. Abortando obtención de IDs.")
                    return identifiers # Devolver lo obtenido hasta ahora
                except Exception as e:
                     logging.error(f"Error inesperado obteniendo identificadores OAI: {e}. Abortando obtención de IDs.")
                     return identifiers # Devolver lo obtenido hasta ahora

            # Si no obtuvimos respuesta (ej. error antes de asignar), no podemos continuar
            if response is None:
                 logging.error("No se obtuvo respuesta del servidor OAI después de reintentos.")
                 return identifiers

            # Si se agotaron los reintentos por Timeout, ya retornamos
            if request_attempts >= max_attempts:
                break

            try:
                # Parsear XML (con fallback de encoding)
                root = None
                detected_encoding = response.encoding if response.encoding else 'utf-8'
                try:
                    root = ET.fromstring(xml_content)
                except ET.ParseError:
                    logging.warning(f"Fallo parseo OAI con {detected_encoding}, intentando iso-8859-1...")
                    try:
                        root = ET.fromstring(xml_content.decode('iso-8859-1').encode('utf-8'))
                        logging.info("Parseo OAI exitoso con iso-8859-1.")
                    except Exception as pe_iso:
                        logging.error(f"Fallo parseo OAI con iso-8859-1 también. Error: {pe_iso}", exc_info=True)
                        logging.error(f"Contenido XML (inicio): {xml_content[:1000]}...")
                        break # No se pudo parsear, abortar

                if root is None: break # Si no se pudo parsear

                ns = {'oai': 'http://www.openarchives.org/OAI/2.0/'}
                oai_error = root.find('oai:error', ns)
                if oai_error is not None:
                    error_code = oai_error.get('code')
                    error_message = oai_error.text
                    logging.error(f"Error OAI recibido - Código: {error_code}, Mensaje: {error_message}")
                    if error_code == 'badResumptionToken':
                         logging.warning("Token de reanudación inválido. Deteniendo paginación.")
                    break # Detener en cualquier error OAI

                headers = root.findall('.//oai:header', ns)
                if not headers:
                    token_element_check = root.find('.//oai:resumptionToken', ns)
                    if token_element_check is None or not token_element_check.text:
                        logging.info("No se encontraron encabezados OAI y no hay token de reanudación. Posible fin de la lista.")
                        break
                    else:
                        logging.info("No se encontraron encabezados OAI pero hay token. Continuando...")
                        resumption_token = token_element_check.text # Asegurarse de actualizar el token
                        continue # Ir al siguiente ciclo while para usar el nuevo token

                found_new_in_batch = False
                for header in headers:
                    identifier_element = header.find('oai:identifier', ns)
                    if identifier_element is not None and identifier_element.text:
                        identifier = identifier_element.text
                        if header.get('status') != 'deleted':
                            identifiers.append(identifier)
                            records_fetched += 1
                            found_new_in_batch = True
                        else:
                            logging.info(f"Registro OAI marcado como eliminado: {identifier}")
                        if max_records and records_fetched >= max_records:
                            logging.info(f"Alcanzado límite máximo de {max_records} registros OAI.")
                            return identifiers

                # Si procesamos un lote y no encontramos nuevos registros válidos, podría ser un bucle
                if not found_new_in_batch and resumption_token:
                    logging.warning("Lote OAI procesado sin encontrar nuevos identificadores válidos. Verificando token...")

                # Buscar siguiente token
                token_element = root.find('.//oai:resumptionToken', ns)
                if token_element is not None and token_element.text:
                    new_token = token_element.text
                    # Anti-loop check: si el token es el mismo, algo va mal
                    if new_token == resumption_token:
                         logging.error("Token de reanudación OAI es idéntico al anterior. Deteniendo para evitar bucle.")
                         break
                    resumption_token = new_token
                    size = token_element.get('completeListSize', 'N/A')
                    cursor = token_element.get('cursor', 'N/A')
                    logging.info(f"Token de reanudación encontrado. Size: {size}, Cursor: {cursor}")
                else:
                    logging.info("No se encontró token de reanudación. Fin de la lista OAI.")
                    break # Fin de la paginación

            except ET.ParseError as e:
                logging.error(f"Error final al parsear XML de OAI: {e}", exc_info=True)
                break
            except Exception as e:
                logging.error(f"Error inesperado procesando respuesta OAI: {e}", exc_info=True)
                break

        logging.info(f"Total de identificadores OAI válidos obtenidos: {len(identifiers)}")
        return identifiers

class HTMLMetadataExtractor:
    """Obtiene y parsea HTML de la página de un ítem para extraer metadatos y URL de PDF."""
    def __init__(self, base_url, selectors, timeout, downloader_instance):
        self.base_url = base_url
        # Asegurar que selectors y item_page existen y son diccionarios
        self.selectors = selectors if isinstance(selectors, dict) else {}
        self.item_selectors = self.selectors.get('item_page', {})
        if not isinstance(self.item_selectors, dict): self.item_selectors = {}
        self.timeout = timeout
        self.downloader = downloader_instance # Guardar referencia al downloader

    def _get_item_page_url(self, identifier):
        """Construye la URL de la página del ítem a partir del OAI ID."""
        # Ejemplo OAI ID: oai:repositorio.inta.gob.ar:20.500.12123/10574
        # URL deseada: https://repositorio.inta.gob.ar/handle/20.500.12123/10574
        try:
            parts = identifier.split(':')
            if len(parts) < 3:
                 logging.warning(f"Formato OAI ID inesperado: {identifier}. No se puede construir URL handle.")
                 return None
            # El handle suele ser la última parte
            handle_part = parts[-1].strip('/')
            # Comprobar si la penúltima parte es el dominio esperado o 'localhost'
            # domain_part = parts[-2] # No siempre fiable
            # Construir URL directamente con la base y el handle
            # Asegurar que base_url no tenga / al final y handle no tenga / al inicio
            base = self.base_url.rstrip('/')
            handle = handle_part.lstrip('/')
            item_url = f"{base}/handle/{handle}"
            # Validar URL resultante mínimamente
            parsed = urlparse(item_url)
            if parsed.scheme and parsed.netloc and parsed.path.startswith('/handle/'):
                 return item_url
            else:
                 logging.error(f"URL construida inválida '{item_url}' desde OAI ID '{identifier}'")
                 return None
        except Exception as e:
            logging.error(f"Error al construir URL para OAI ID {identifier}: {e}")
            return None

    def fetch_and_extract(self, item_page_url, item_id, identifier=None, save_snapshot=False):
        """Método principal: obtiene HTML desde URL, extrae datos y opcionalmente guarda snapshot."""
        if not item_page_url:
             logging.error(f"[Item {item_id}] Se llamó a fetch_and_extract sin item_page_url.")
             return None, None # Devolver None para metadatos si no hay URL

        log_id = identifier if identifier else item_page_url.split('/')[-1]
        logging.info(f"[{log_id} / Item {item_id}] Obteniendo página HTML: {item_page_url}")
        html_content_str = None
        html_local_path = None # Inicializar
        metadata = None

        try:
            response = requests.get(item_page_url, timeout=self.timeout)
            response.raise_for_status()
            detected_encoding = response.encoding if response.encoding else 'utf-8'
            try:
                html_content_str = response.content.decode(detected_encoding, errors='replace')
            except UnicodeDecodeError:
                 logging.warning(f"Fallo decodificación HTML con {detected_encoding}, intentando iso-8859-1 para {item_page_url}")
                 try:
                     html_content_str = response.content.decode('iso-8859-1', errors='replace')
                 except Exception as decode_err:
                     logging.error(f"Error final decodificando HTML de {item_page_url}: {decode_err}")
                     return item_page_url, None # Fallo crítico en decodificación

            if html_content_str:
                # Guardar snapshot si está habilitado
                if save_snapshot and item_id is not None:
                    try:
                        # Usar el método del downloader para construir la ruta
                        snapshot_path = self.downloader._build_local_path(item_id, 'html_snapshot', item_page_url)
                        if snapshot_path:
                            with open(snapshot_path, 'w', encoding='utf-8') as f:
                                f.write(html_content_str)
                            html_local_path = snapshot_path # Guardar ruta para devolverla
                            logging.info(f"[Item {item_id}] Snapshot HTML guardado en: {snapshot_path}")
                        else:
                            logging.warning(f"[Item {item_id}] No se pudo generar la ruta para guardar el snapshot HTML.")
                    except Exception as save_err:
                        logging.error(f"[Item {item_id}] Error guardando snapshot HTML: {save_err}")
                        # Continuar con la extracción aunque falle el guardado del snapshot

                # Extraer metadatos
                metadata = self._extract_from_content(html_content_str, item_page_url)
                if html_local_path:
                     metadata['html_local_path'] = html_local_path # Añadir ruta al diccionario

                return item_page_url, metadata # Esta línea debe estar indentada
            else:
                logging.error(f"[{log_id} / Item {item_id}] Contenido HTML vacío o no decodificable para {item_page_url}")
                return item_page_url, None

        except requests.exceptions.Timeout:
            logging.error(f"[{log_id} / Item {item_id}] Timeout obteniendo página HTML {item_page_url}")
            return item_page_url, None
        except requests.exceptions.RequestException as e:
            status = e.response.status_code if e.response is not None else 'N/A'
            logging.error(f"[{log_id} / Item {item_id}] Error ({status}) obteniendo página HTML {item_page_url}: {e}")
            return item_page_url, None
        except Exception as e:
            logging.error(f"[{log_id} / Item {item_id}] Error inesperado procesando {item_page_url}: {e}", exc_info=True)
            return item_page_url, None

    def _extract_from_content(self, html_string, item_page_url):
        """Parsea el contenido HTML y extrae metadatos y URL PDF."""
        metadata = {}
        if not html_string: return metadata

        try:
            tree = html.fromstring(html_string)

            # Extraer metadatos DC/DCTERMS
            meta_tags = tree.xpath("//meta[starts-with(@name, 'DC.') or starts-with(@name, 'DCTERMS.')]")
            dc_metadata = {}
            for tag in meta_tags:
                name = tag.get('name', '').lower()
                content = tag.get('content', '').strip()
                if not name or not content: continue

                simple_name = name.split('.')[-1]

                if simple_name == 'identifier' and tag.get('scheme', '').upper() == 'DCTERMS.URI':
                     if 'handle_uri' not in dc_metadata: dc_metadata['handle_uri'] = content
                     continue
                if simple_name == 'identifier' and content == item_page_url: continue

                # Agrupar multivaluados
                multi_valued_fields = ['creator', 'contributor', 'subject', 'relation', 'language', 'type', 'format', 'publisher', 'rights', 'coverage', 'identifier']
                if simple_name in multi_valued_fields:
                    if simple_name not in dc_metadata: dc_metadata[simple_name] = []
                    if content not in dc_metadata[simple_name]: dc_metadata[simple_name].append(content)
                # Campos únicos (simplificado: toma el último encontrado)
                else:
                     dc_metadata[simple_name] = content

            # Mapeo a claves finales
            metadata['title'] = dc_metadata.get('title')
            metadata['authors'] = dc_metadata.get('creator', [])
            metadata['publication_date'] = dc_metadata.get('issued', dc_metadata.get('date'))
            metadata['abstract'] = dc_metadata.get('abstract', dc_metadata.get('description'))
            metadata['keywords'] = dc_metadata.get('subject', [])
            metadata['handle_uri'] = dc_metadata.get('handle_uri')
            # ... (se pueden añadir más campos mapeados si son necesarios)

            # Extraer URL del PDF
            pdf_url = None
            pdf_xpath_selector = self.selectors.get('pdf_link_xpath')
            if pdf_xpath_selector:
                try:
                    pdf_elements = tree.xpath(pdf_xpath_selector)
                    if pdf_elements:
                        href = pdf_elements[0].get('href')
                        if href:
                            pdf_url = urljoin(item_page_url, href)
                            logging.info(f"Encontrada URL PDF con selector XPath: {pdf_url}")
                except Exception as xpath_err:
                    logging.warning(f"Error aplicando selector XPath '{pdf_xpath_selector}': {xpath_err}")

            if not pdf_url:
                logging.info("Selector XPath de YAML no funcionó o no existe/encontró. Buscando enlaces PDF en el cuerpo...")
                body_links = tree.xpath("//a[contains(@href, '.pdf')] | //a[contains(@href, '/bitstream/')] | //a[contains(translate(text(), 'PDF', 'pdf'), 'pdf')]")
                for link in body_links:
                    href = link.get('href')
                    if href:
                        if '/bitstream/' in href or href.lower().endswith('.pdf'):
                            potential_url = urljoin(item_page_url, href)
                            parsed_url = urlparse(potential_url)
                            # Validar URL un poco más
                            if parsed_url.scheme in ['http', 'https'] and (parsed_url.path.lower().endswith('.pdf') or '/bitstream/' in parsed_url.path):
                                pdf_url = potential_url
                                logging.info(f"Encontrada URL PDF potencial en el cuerpo: {pdf_url}")
                                break # Tomar la primera

            metadata['pdf_url'] = pdf_url

        except Exception as e:
            logging.error(f"Error extrayendo metadatos del HTML ({item_page_url}): {e}", exc_info=True)

        return {k: v for k, v in metadata.items() if v} # Limpiar nulos/vacíos

class DatabaseManager:
    """Gestiona la base de datos SQLite para el scraper."""
    def __init__(self, db_file):
        self.db_file = db_file
        self._ensure_db_directory()

    def _ensure_db_directory(self):
        """Asegura que el directorio para el archivo de BD exista."""
        db_dir = os.path.dirname(self.db_file)
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir, exist_ok=True)
                logging.info(f"Directorio de base de datos creado: {db_dir}")
            except OSError as e:
                logging.error(f"Error creando directorio para la base de datos {db_dir}: {e}")
                raise

    def _connect(self):
        """Conecta a la base de datos SQLite."""
        conn = sqlite3.connect(self.db_file)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize_db(self):
        """Inicializa la base de datos y asegura que la tabla items tenga la columna metadata_json."""
        conn = self._connect()
        cursor = conn.cursor()
        try:
            # Crear tabla files si no existe
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id INTEGER,
                file_type TEXT,
                remote_url TEXT,
                local_path TEXT,
                download_status TEXT,
                md5_hash TEXT,
                file_size_bytes INTEGER,
                download_timestamp TEXT
            )
            """)
            
            # Crear tabla items si no existe
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS items (
                item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_page_url TEXT,
                oai_identifier TEXT,
                discovery_mode TEXT,
                search_keyword TEXT,
                download_status TEXT, 
                processing_status TEXT,
                metadata_json TEXT 
            )
            """)
            
            # Verificar y añadir la columna metadata_json a la tabla items si no existe
            cursor.execute("PRAGMA table_info(items)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'metadata_json' not in columns:
                logging.info("Añadiendo columna 'metadata_json' a la tabla 'items'...")
                cursor.execute("ALTER TABLE items ADD COLUMN metadata_json TEXT")
                logging.info("Columna 'metadata_json' añadida.")
            
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Error al inicializar la base de datos o al modificar la tabla items: {e}")
            conn.rollback()
        finally:
            conn.close()

    def check_file_status(self, item_id, remote_url):
        """
        Verifica si un archivo con esta URL remota ya existe para el item_id.
        Devuelve (download_status, local_path) si existe, o (None, None) si no.
        """
        conn = self._connect()
        cursor = conn.cursor()
        status = None
        local_path = None
        try:
            cursor.execute("""
                SELECT download_status, local_path
                FROM files
                WHERE item_id = ? AND remote_url = ?
            """, (item_id, remote_url))
            row = cursor.fetchone()
            if row:
                status = row['download_status']
                local_path = row['local_path']
                # Considerar 'downloaded' y 'skipped_exists' como éxito
                if status in ['downloaded', 'skipped_exists']:
                     logging.debug(f"Archivo existente encontrado en BD: item={item_id}, url={remote_url}, status={status}")
                # Podríamos decidir reintentar si status es 'failed_...'
        except sqlite3.Error as e:
            logging.error(f"Error consultando estado de archivo para item {item_id}, url {remote_url}: {e}")
        finally:
            conn.close()
        return status, local_path

    def log_file(self, item_id, file_type, remote_url, local_path, download_status, md5_hash=None, file_size_bytes=None):
        """
        Registra o actualiza una entrada en la tabla 'files'.
        Si ya existe una entrada para (item_id, remote_url), la actualiza.
        """
        if item_id is None:
             logging.error("Se intentó loggear archivo con item_id None.")
             return

        conn = self._connect()
        cursor = conn.cursor()
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds') + 'Z'
        try:
            # Verificar si ya existe para decidir entre INSERT u UPDATE
            cursor.execute("""
                SELECT file_id FROM files WHERE item_id = ? AND remote_url = ?
            """, (item_id, remote_url))
            row = cursor.fetchone()
            if row:
                # Update
                file_id = row['file_id']
                sql = """
                    UPDATE files SET
                        local_path = ?,
                        download_status = ?,
                        md5_hash = ?,
                        file_size_bytes = ?,
                        download_timestamp = ?
                    WHERE file_id = ?
                """
                cursor.execute(sql, (local_path, download_status, md5_hash, file_size_bytes, now, file_id))
                logging.info(f"Registro de archivo actualizado (ID={file_id}): item={item_id}, tipo={file_type}, status={download_status}")
            else:
                # Insert
                sql = """
                    INSERT INTO files (item_id, file_type, remote_url, local_path, download_status, md5_hash, file_size_bytes, download_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (item_id, file_type, remote_url, local_path, download_status, md5_hash, file_size_bytes, now))
                logging.info(f"Nuevo registro de archivo añadido: item={item_id}, tipo={file_type}, status={download_status}")
            conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Error registrando archivo en BD para item {item_id}, url {remote_url}: {e}")
            conn.rollback()
        finally:
            conn.close()

    def get_items_to_process(self, discovery_modes=None, statuses=None, limit=None):
        """
        Obtiene una lista de item_ids que coinciden con los modos de descubrimiento y estados.
        Permite obtener ítems 'pending' o 'error' para (re)procesamiento.
        """
        if statuses is None:
            statuses = ['pending', 'error'] # Por defecto, procesar pendientes y errores
        
        conn = self._connect()
        cursor = conn.cursor()
        item_ids = []
        
        # Construir placeholders para IN clauses de forma segura
        status_placeholders = ','.join('?' * len(statuses))
        query = f"SELECT item_id FROM items WHERE processing_status IN ({status_placeholders})"
        params = list(statuses)
        
        if discovery_modes:
            if isinstance(discovery_modes, str): discovery_modes = [discovery_modes]
            if discovery_modes: # Asegurar que la lista no esté vacía
                discovery_mode_placeholders = ','.join('?' * len(discovery_modes))
                query += f" AND discovery_mode IN ({discovery_mode_placeholders})"
                params.extend(discovery_modes)
            
        if limit is not None: # Chequear explícitamente por None, ya que 0 podría ser un límite válido (aunque raro)
            query += " LIMIT ?"
            params.append(limit)
            
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            item_ids = [row['item_id'] for row in rows]
            logging.info(f"Encontrados {len(item_ids)} ítems para procesar (status: {statuses}, mode: {discovery_modes or 'any'}).")
        except sqlite3.Error as e:
            logging.error(f"Error obteniendo ítems para procesar: {e}")
        finally:
            conn.close()
        return item_ids

    def get_item_details(self, item_id):
        """Obtiene todos los detalles de un ítem por su item_id."""
        if item_id is None: return None
        conn = self._connect()
        cursor = conn.cursor()
        item_data = None
        try:
            cursor.execute("SELECT * FROM items WHERE item_id = ?", (item_id,))
            row = cursor.fetchone()
            if row:
                item_data = dict(row) # Convertir sqlite3.Row a dict
        except sqlite3.Error as e:
            logging.error(f"Error obteniendo detalles para item_id {item_id}: {e}")
        finally:
            conn.close()
        return item_data

    def get_or_create_item_by_url(self, item_page_url, processing_status, discovery_mode, oai_identifier=None, search_keyword=None):
        """
        Busca un ítem por item_page_url. Si existe, devuelve su ID y estado.
        Si no existe, lo crea con los datos proporcionados y devuelve el nuevo ID y estado.
        """
        conn = self._connect()
        cursor = conn.cursor()
        item_id = None
        current_status = None
        try:
            cursor.execute("SELECT item_id, processing_status FROM items WHERE item_page_url = ?", (item_page_url,))
            row = cursor.fetchone()
            if row:
                item_id = row['item_id']
                current_status_in_db = row['processing_status']
                logging.debug(f"Ítem existente encontrado por URL: {item_page_url} (ID: {item_id}, Status en BD: {current_status_in_db})")
                # Si el estado deseado es diferente al de la BD, actualizarlo.
                # Esto permite 'resetear' un ítem a un estado pendiente si se vuelve a descubrir.
                if current_status_in_db != processing_status:
                    cursor.execute("UPDATE items SET processing_status = ? WHERE item_id = ?", (processing_status, item_id))
                    conn.commit()
                    logging.info(f"Ítem existente ID {item_id} actualizado a status: {processing_status} (era {current_status_in_db})")
                    current_status = processing_status # Reflejar el nuevo estado
                else:
                    current_status = current_status_in_db # Usar el estado de la BD si es el mismo
            else:
                # Insertar nuevo ítem
                sql = """
                    INSERT INTO items (item_page_url, oai_identifier, discovery_mode, search_keyword, processing_status)
                    VALUES (?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (item_page_url, oai_identifier, discovery_mode, search_keyword, processing_status))
                conn.commit()
                item_id = cursor.lastrowid
                current_status = processing_status
                logging.info(f"Nuevo ítem creado (ID: {item_id}): URL={item_page_url}, Status={current_status}, Mode={discovery_mode}")
        except sqlite3.Error as e:
            logging.error(f"Error en get_or_create_item_by_url para URL {item_page_url}: {e}")
            if conn: conn.rollback() # Asegurar rollback si la conexión está abierta
        finally:
            if conn: conn.close() # Asegurar cierre de conexión
        return item_id, current_status

    def update_item_status(self, item_id, new_status):
        """Actualiza el processing_status de un ítem en la tabla items."""
        if item_id is None:
            logging.error(f"Intento de actualizar estado para item_id None al estado {new_status}")
            return
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE items SET processing_status = ? WHERE item_id = ?", (new_status, item_id))
            conn.commit()
            if cursor.rowcount > 0:
                logging.info(f"Estado del ítem ID {item_id} actualizado a: {new_status}")
            else:
                logging.warning(f"No se encontró el ítem ID {item_id} para actualizar estado a {new_status} (¿ya estaba en ese estado o no existe?).")
        except sqlite3.Error as e:
            logging.error(f"Error actualizando estado para ítem ID {item_id} a {new_status}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def log_item_metadata(self, item_id, metadata_dict):
        """ Registra los metadatos extraídos para un ítem en la BD (columna metadata_json).
        """
        if item_id is None:
            logging.error("Intento de loggear metadatos para item_id None.")
            return

        if not metadata_dict:
            logging.warning(f"No se proporcionaron metadatos para loggear para item ID {item_id}.")
            # Aún así, podríamos querer actualizar alguna otra cosa o simplemente no hacer nada.
            # Por ahora, si no hay metadatos, no actualizamos la columna metadata_json.
            return

        conn = self._connect()
        cursor = conn.cursor()
        try:
            # Convertir el diccionario de metadatos a una cadena JSON
            metadata_str = json.dumps(metadata_dict)
            
            cursor.execute("UPDATE items SET metadata_json = ? WHERE item_id = ?", (metadata_str, item_id))
            conn.commit()
            
            if cursor.rowcount > 0:
                logging.info(f"Metadatos almacenados en BD para item ID {item_id}.")
            else:
                # Esto podría pasar si el item_id no existe, aunque el flujo normal debería crearlo primero.
                logging.warning(f"No se encontró el ítem ID {item_id} para almacenar metadatos.")
        except sqlite3.Error as e:
            logging.error(f"Error almacenando metadatos JSON para ítem ID {item_id}: {e}")
            if conn: conn.rollback()
        except TypeError as te: # Por si json.dumps falla con algún tipo de dato no serializable
            logging.error(f"Error de serialización JSON para metadatos del ítem ID {item_id}: {te}")
            # No hacemos rollback aquí porque la transacción podría no haberse iniciado si json.dumps falló antes.
        finally:
            if conn: conn.close()

class ResourceDownloader:
    """Descarga recursos (PDFs, imágenes, etc.), verifica existencia y calcula hashes/tamaño."""
    def __init__(self, base_output_dir, request_delay, timeout, db_manager, download_max_retries, download_base_retry_delay):
        self.base_output_dir = base_output_dir
        self.delay = request_delay
        self.timeout = timeout
        self.db_manager = db_manager
        self.download_max_retries = download_max_retries
        self.download_base_retry_delay = download_base_retry_delay

    def _build_local_path(self, item_id, file_type, remote_url):
        """Construye la ruta de archivo local basada en item_id y tipo."""
        try:
            url_path = urlparse(remote_url).path
            filename_base = os.path.basename(url_path).split('?')[0]
            safe_filename = "".join([c for c in filename_base if c.isalnum() or c in ('-', '_', '-')]).rstrip()
            if not safe_filename:
                safe_filename = f"file_{hashlib.md5(remote_url.encode()).hexdigest()[:8]}"
            if file_type == 'pdf' and not safe_filename.lower().endswith('.pdf'): safe_filename += ".pdf"
            elif file_type.startswith('image') and not any(safe_filename.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif']):
                 ext = os.path.splitext(url_path)[1].lower()
                 if ext in ['.jpg', '.jpeg', '.png', '.gif']: safe_filename += ext
                 else: safe_filename += ".jpg"
            elif file_type == 'html_snapshot' and not safe_filename.lower().endswith('.html'): safe_filename += ".html"

            safe_file_type_dir = "".join([c for c in file_type if c.isalnum() or c == '_'])
            target_dir = os.path.join(self.base_output_dir, safe_file_type_dir, str(item_id))
            os.makedirs(target_dir, exist_ok=True)
            return os.path.join(target_dir, safe_filename)
        except Exception as e:
            logging.error(f"Error creando ruta local para item {item_id}, tipo {file_type}, url {remote_url}: {e}")
            return None

    def _cleanup_partial_file(self, file_path):
        """Intenta eliminar un archivo parcial si existe después de un error."""
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Eliminado archivo parcial: {file_path}")
            except OSError as oe:
                logging.warning(f"Error eliminando archivo parcial {file_path}: {oe}")

    def download_resource(self, item_id, file_type, remote_url):
        """
        Descarga un recurso genérico (PDF, imagen, etc.).
        Verifica BD, descarga si es necesario, y registra en BD.
        Devuelve el estado final de la descarga ('downloaded', 'skipped_exists', 'failed_...', etc.)
        """
        final_status = 'pending' # Estado inicial
        local_path_final = None
        md5_final = None
        size_final = None

        if not remote_url:
            logging.warning(f"[Item {item_id}] No se proporcionó URL remota para tipo '{file_type}'.")
            final_status = 'no_remote_url'
            # No es necesario llamar a log_file aquí si no hay URL, ya que no hay identificador único de archivo
            return final_status

        if item_id is None:
             logging.error(f"[Item {item_id}] Intento de descarga para '{file_type}' con item_id None (URL: {remote_url}).")
             # Podríamos loguear un placeholder si es necesario, pero es mejor evitarlo si item_id es clave.
             return 'failed_error_item_id_none'

        # 1. Consultar BD si ya existe y está OK
        existing_status, existing_path = self.db_manager.check_file_status(item_id, remote_url)
        if existing_status in ['downloaded', 'skipped_exists']:
            logging.info(f"[Item {item_id}] Archivo '{file_type}' ({remote_url}) ya procesado ({existing_status}). Saltando descarga.")
            return existing_status

        # 2. Construir ruta local
        local_path_target = self._build_local_path(item_id, file_type, remote_url)
        if not local_path_target:
            final_status = 'failed_path_error'
            self.db_manager.log_file(item_id, file_type, remote_url, None, final_status)
            return final_status

        # 3. Intentar descarga (si no existe o falló antes)
        # Verificar si el archivo físico ya existe (caso raro si no está en BD como downloaded)
        if os.path.exists(local_path_target):
             logging.warning(f"[Item {item_id}] Archivo físico encontrado en {local_path_target} pero no marcado como 'downloaded' en BD. Verificando...")
             try:
                  md5_final = calculate_md5(local_path_target)
                  size_final = os.path.getsize(local_path_target)
                  final_status = 'skipped_exists' # Tratar como si ya existiera correctamente
                  local_path_final = local_path_target
                  logging.info(f"[Item {item_id}] Archivo físico verificado OK. Actualizando BD.")
                  # No retornamos aún, se loguea al final
             except Exception as e:
                  logging.error(f"[Item {item_id}] Error verificando archivo físico existente {local_path_target}: {e}. Se intentará re-descargar.")
                  final_status = 'pending_redownload' # Marcar para que el siguiente bloque intente la descarga

        # Bucle de reintentos para la descarga
        if final_status not in ['skipped_exists']: # Solo intentar descargar si no se saltó
            attempts = 0
            while attempts <= self.download_max_retries: # Permite un intento inicial (attempts=0) + N reintentos
                try:
                    if attempts > 0: # Si es un reintento
                        retry_delay = self.download_base_retry_delay * (2 ** (attempts -1)) # Backoff exponencial
                        logging.info(f"[Item {item_id}] Reintento {attempts}/{self.download_max_retries} para '{remote_url}' en {retry_delay}s...")
                        time.sleep(retry_delay)
                    
                    logging.info(f"[Item {item_id}] Descargando '{file_type}' desde: {remote_url} -> {local_path_target} (Intento {attempts + 1})")
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                    }
                    response = requests.get(remote_url, stream=True, timeout=self.timeout, headers=headers)
                    response.raise_for_status() # Levanta HTTPError para 4xx/5xx

                    with open(local_path_target, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)

                    logging.info(f"[Item {item_id}] '{file_type}' descargado en: {local_path_target}")
                    if self.delay > 0 and attempts == 0: # Aplicar delay solo en el primer intento exitoso
                        time.sleep(self.delay / 2)

                    md5_final = calculate_md5(local_path_target)
                    try:
                        size_final = os.path.getsize(local_path_target)
                    except OSError as e:
                        logging.warning(f"[Item {item_id}] Error obteniendo tamaño del archivo descargado {local_path_target}: {e}")
                    
                    final_status = "downloaded"
                    local_path_final = local_path_target
                    break # Salir del bucle de reintentos si la descarga fue exitosa

                except requests.exceptions.Timeout:
                    logging.warning(f"[Item {item_id}] Timeout descargando '{file_type}' {remote_url} (Intento {attempts + 1})")
                    self._cleanup_partial_file(local_path_target)
                    final_status = "failed_timeout" 
                except requests.exceptions.HTTPError as http_err:
                    status_code = http_err.response.status_code
                    logging.warning(f"[Item {item_id}] Error HTTP {status_code} descargando '{file_type}' {remote_url} (Intento {attempts + 1}): {http_err}")
                    self._cleanup_partial_file(local_path_target)
                    final_status = f"failed_http_{status_code}"
                    if 400 <= status_code < 500 and status_code != 429: # Errores de cliente (excepto 429) no suelen ser reintentables
                        break # Salir del bucle de reintentos para errores 4xx
                except requests.exceptions.RequestException as req_err: # Errores de conexión, etc.
                    logging.warning(f"[Item {item_id}] Error de red descargando '{file_type}' {remote_url} (Intento {attempts + 1}): {req_err}")
                    self._cleanup_partial_file(local_path_target)
                    final_status = "failed_network_error"
                except Exception as e:
                    logging.error(f"[Item {item_id}] Error inesperado descargando '{file_type}' {remote_url} (Intento {attempts + 1}): {e}", exc_info=True)
                    self._cleanup_partial_file(local_path_target)
                    final_status = "failed_exception"
                    break # Salir del bucle para errores inesperados graves

                attempts += 1
            
            if final_status.startswith('failed') and attempts > self.download_max_retries:
                 logging.error(f"[Item {item_id}] Máximos reintentos ({self.download_max_retries}) alcanzados para '{remote_url}'. Falló con: {final_status}")


        # 4. Registrar resultado final en BD (siempre, incluso si falló o se saltó)
        # Si final_status es 'pending' (porque nunca entró al bucle de descarga), se actualizará al estado real
        self.db_manager.log_file(item_id, file_type, remote_url, local_path_final, final_status, md5_final, size_final)

        return final_status

class KeywordSearcher:
    """Busca ítems por palabras clave en el repositorio y los registra en la BD."""
    def __init__(self, base_search_url, selectors_config, delay, timeout, db_manager):
        self.base_search_url = base_search_url # Ej: "https://repositorio.inta.gob.ar/discover"
        self.selectors = selectors_config.get('search_page', {}) if isinstance(selectors_config, dict) else {}
        self.delay = delay
        self.timeout = timeout
        self.db_manager = db_manager

        # Selectores esperados (ejemplos, deben estar en selectors.yaml)
        self.item_link_selector = self.selectors.get('item_link_selector', '//div[contains(@class,"artifact-description")]//h4/a')
        self.next_page_link_selector = self.selectors.get('next_page_link_selector', '//ul[contains(@class,"pagination")]//a[contains(text(),"Siguiente") or 모양새가 다음을 나타내는 것]') # Adaptado para DSpace y paginación general
        self.results_container_selector = self.selectors.get('results_container_selector', '#aspect_discovery_SimpleSearch_div_search-results') # Típico de DSpace

    def _make_request(self, url, params=None):
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            if self.delay > 0:
                time.sleep(self.delay)
            return response.content
        except requests.exceptions.Timeout:
            logging.error(f"Timeout buscando en {url} con params {params}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error de red/HTTP buscando en {url} con params {params}: {e}")
        return None

    def search_and_register(self, keywords: list, max_pages_per_keyword: int = 5):
        """
        Realiza la búsqueda para cada palabra clave y registra los ítems encontrados en la BD.
        """
        if not keywords:
            logging.warning("No se proporcionaron palabras clave para la búsqueda.")
            return

        logging.info(f"Iniciando búsqueda por palabras clave: {keywords}")
        items_found_total_run = 0 # Para este run específico

        for keyword in keywords:
            logging.info(f"Buscando palabra clave: '{keyword}'")
            items_found_for_this_keyword_run = 0
            # DSpace usa 'page' pero empieza en 1 para la segunda pág, la primera no tiene 'page' o es page=0
            # Para simplificar, construiremos la URL de forma un poco más manual para la paginación DSpace
            current_page_offset = 0 # El 'start' en DSpace es rpp * (page - 1), o rpp * page_num_zero_indexed
            results_per_page = self.config.get('rpp', 20) # Resultados por página, DSpace default suele ser 20 o 10

            page_count = 0
            while page_count < max_pages_per_keyword:
                params = {
                    'query': keyword,
                    'rpp': results_per_page
                }
                if current_page_offset > 0:
                    params['start'] = current_page_offset
                
                search_url = self.base_search_url # Ej: https://repositorio.inta.gob.ar/discover
                
                logging.info(f"Obteniendo página {page_count + 1} para '{keyword}' (offset {current_page_offset}) con params: {params}")
                html_content = self._make_request(search_url, params=params)

                if not html_content:
                    logging.warning(f"No se pudo obtener contenido para '{keyword}', página {page_count + 1}. Terminando búsqueda para esta keyword.")
                    break

                try:
                    tree = html.fromstring(html_content)
                    
                    # Usar cssselect si el selector parece CSS, sino xpath
                    if self.results_container_selector and self.results_container_selector.startswith(('.', '#')):
                        results_containers = tree.cssselect(self.results_container_selector)
                    elif self.results_container_selector: # Si no es CSS, asumir XPath
                        results_containers = tree.xpath(self.results_container_selector)
                    else: # Si no hay selector de contenedor, asumir que hay resultados
                        results_containers = [True] # Poner algo para que no rompa la lógica de abajo

                    if not results_containers or results_containers == [True] and not tree.xpath(self.item_link_selector): # Si usamos [True] o el contenedor está vacío y no hay items
                        logging.info(f"No se encontraron resultados (contenedor o ítems) para '{keyword}' en página {page_count + 1} (offset {current_page_offset}).")
                        break

                    # item_link_selector y next_page_link_selector se asumen XPath por defecto o desde YAML
                    item_elements = tree.xpath(self.item_link_selector)
                    if not item_elements:
                        logging.info(f"No se encontraron enlaces de ítems para '{keyword}' en página {page_count + 1}. Fin de resultados para esta keyword.")
                        break

                    page_items_registered_count = 0
                    for element in item_elements:
                        href = element.get('href')
                        if href:
                            # Asegurar URL absoluta. El href de DSpace suele ser /handle/...
                            item_page_url = urljoin(self.config.get('base_url', 'https://repositorio.inta.gob.ar/'), href)
                            
                            item_id, item_status_in_db = self.db_manager.get_or_create_item_by_url(
                                item_page_url=item_page_url,
                                processing_status="pending_download",
                                discovery_mode="keyword_search",
                                search_keyword=keyword
                            )
                            logging.info(f"KeywordSearcher: Ítem ID {item_id} (URL: {item_page_url}) tiene status en BD: {item_status_in_db} después de get_or_create. Se intentó establecer/verificar 'pending_download'.")
                            if item_id: 
                                items_found_for_this_keyword_run += 1
                                items_found_total_run +=1
                                page_items_registered_count += 1
                    
                    logging.info(f"Registrados/verificados {page_items_registered_count} ítems desde la página {page_count + 1} para '{keyword}'.")
                    if page_items_registered_count == 0 and page_count > 0: # Si una página > 0 no tiene nuevos, probablemente fin
                        logging.info(f"No se registraron ítems nuevos en la página {page_count + 1} para '{keyword}'. Asumiendo fin.")
                        break

                    # Paginación: DSpace a menudo usa un 'start' param = rpp * (page_num -1)
                    # O buscar un enlace 'next'
                    next_page_elements = tree.xpath(self.next_page_link_selector) # Actualizado selector
                    if next_page_elements and next_page_elements[0].get('href'):
                        # En DSpace, el enlace 'Siguiente' ya tiene la URL correcta con el 'start' actualizado
                        # No necesitamos calcularlo, solo seguirlo. Pero para controlar max_pages, incrementamos nuestro contador.
                        page_count += 1
                        # Actualizamos el offset para el log o si quisiéramos construir la URL manualmente
                        current_page_offset += results_per_page 
                    else:
                        logging.info(f"No se encontró enlace a la página siguiente para '{keyword}' después de la página {page_count + 1}.")
                        break 
                
                except Exception as e:
                    logging.error(f"Error parseando página de resultados para '{keyword}': {e}", exc_info=True)
                    break 
            
            logging.info(f"{items_found_for_this_keyword_run} ítems (nuevos/existentes) registrados/verificados para la palabra clave '{keyword}'.")

        logging.info(f"Búsqueda por palabras clave completada. Total de ítems (nuevos/existentes) procesados en esta ejecución: {items_found_total_run}")

class Scraper:
    """Orquesta el proceso completo de scraping."""
    def __init__(self, config):
        self.config = config # Guardar config completa
        self.selectors = {}

        # --- Construir rutas absolutas basadas en la ubicación del script ---
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.config['script_dir'] = script_dir # Guardar para referencia, ej. en _package_output

        # Lista de claves de configuración que representan rutas a resolver
        path_keys = ['selectors_file', 'output_dir', 'pdf_dir', 'metadata_file', 'log_file', 'db_file']
        for key in path_keys:
            if key in self.config:
                self.config[key] = os.path.join(script_dir, self.config[key])
        # --- Fin de construcción de rutas absolutas ---

        setup_logging(self.config['log_file'])

        try:
            setup_directories(self.config['output_dir'], self.config['pdf_dir'])

            self.db_manager = DatabaseManager(self.config['db_file'])
            self.db_manager.initialize_db()

            self.harvester = OAIHarvester(
                self.config['oai_endpoint'],
                self.config['metadata_prefix'],
                self.config['delay'],
                self.config['request_timeout']
            )
            self._load_selectors() # Carga en self.selectors
            
            # KeywordSearcher necesita config para base_url y rpp
            # Mover KeywordSearcher después de downloader y extractor
            
            # Instanciar ResourceDownloader PRIMERO
            self.downloader = ResourceDownloader(
                self.config['output_dir'], # Pasar dir base de salida
                self.config['delay'],
                self.config['download_timeout'],
                self.db_manager, # <--- Pasar instancia de DB Manager
                self.config.get('download_max_retries', 3), # Pasar max_retries
                self.config.get('download_base_retry_delay', 5) # Pasar base_retry_delay
            )

            # Pasar downloader a Extractor
            self.extractor = HTMLMetadataExtractor(
                self.config['base_url'],
                self.selectors,
                self.config['request_timeout'],
                self.downloader # <--- Ahora self.downloader existe
            )

            # Instanciar KeywordSearcher aquí
            self.keyword_searcher = KeywordSearcher(
                self.config.get('search_endpoint', urljoin(self.config['base_url'], "discover")), 
                self.selectors, # Pasa todos los selectores cargados
                self.config['delay'],
                self.config['request_timeout'],
                self.db_manager
            )
            # Pasar config completa a KeywordSearcher para que pueda acceder a base_url, rpp, etc.
            self.keyword_searcher.config = config 

        except Exception as e:
            logging.critical(f"Error durante la inicialización del Scraper: {e}", exc_info=True)
            raise

    def _load_selectors(self):
        """Carga los selectores desde el archivo YAML."""
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            selectors_file_name = self.config['selectors_file'] # Ahora solo el nombre del archivo, ej: "selectors.yaml"
            full_selectors_path = os.path.join(script_dir, selectors_file_name)

            logging.info(f"Directorio del script: {script_dir}")
            logging.info(f"Nombre del archivo de selectores configurado: {selectors_file_name}")
            logging.info(f"Intentando cargar selectores desde la ruta absoluta: {full_selectors_path}")
            logging.info(f"Verificando existencia (os.path.exists): {os.path.exists(full_selectors_path)}")
            logging.info(f"Verificando si es archivo (os.path.isfile): {os.path.isfile(full_selectors_path)}")

            with open(full_selectors_path, 'r', encoding='utf-8') as f:
                self.selectors = yaml.safe_load(f)
            if not self.selectors:
                 logging.warning(f"Archivo de selectores {self.config['selectors_file']} está vacío o no es válido.")
                 self.selectors = {} # Asegurar que sea un dict
        except FileNotFoundError:
            logging.error(f"Archivo de selectores no encontrado en {self.config['selectors_file']}")
            self.selectors = {} # Continuar sin selectores si no se encuentran
        except yaml.YAMLError as e:
            logging.error(f"Error al parsear el archivo YAML de selectores: {e}")
            self.selectors = {}
        except Exception as e:
            logging.error(f"Error inesperado cargando selectores: {e}", exc_info=True)
            self.selectors = {}

    def run(self):
        """Ejecuta el proceso completo de scraping, manejando modos OAI y Keyword."""
        
        # Leer configuración específica del run (podría venir de args o UI web en el futuro)
        mode = self.config.get('mode', 'oai') # 'oai' o 'keyword_search'
        keywords = self.config.get('keywords', [])
        max_oai_records = self.config.get('max_records') # Límite para OAI
        max_search_pages = self.config.get('max_search_pages', 5) # Límite páginas por keyword
        # Estados a procesar/reprocesar
        process_statuses = self.config.get('process_statuses', [
            'pending_download',
            'pending_metadata', # Para futura flexibilidad
            'error',            # Error genérico
            'error_extraction', # Si falla la extracción de metadatos
            'error_download'    # Si falla la descarga de un recurso crítico
        ]) 

        logging.info(f"Inicio del scraper {self.config['country_code']} (DB: {self.config['db_file']}, Modo: {mode})")

        # --- Fase de Descubrimiento ---
        items_discovered_this_run = 0
        if mode == 'oai':
            discovery_mode = 'oai'
            identifiers = self.harvester.get_identifiers(max_records=max_oai_records)
            if not identifiers:
                        logging.info("Modo OAI: No se obtuvieron identificadores OAI.")
            else:
                        logging.info(f"Modo OAI: {len(identifiers)} identificadores obtenidos. Registrando/verificando en BD...")
                        for oai_id in identifiers:
                            item_page_url = self.extractor._get_item_page_url(oai_id)
                            if item_page_url:
                                item_id, _ = self.db_manager.get_or_create_item_by_url(
                                    item_page_url=item_page_url,
                                    processing_status="pending_download",
                                    discovery_mode="oai",
                                    oai_identifier=oai_id
                                )
                                if item_id: items_discovered_this_run += 1 # Contar nuevos/existentes verificados
                        logging.info(f"Modo OAI: {items_discovered_this_run} ítems registrados/verificados en BD.")

        elif mode == 'keyword_search':
            discovery_mode = 'keyword_search'
            if not keywords:
                logging.error("Modo 'keyword_search' seleccionado pero no se especificaron 'keywords' en la config.")
                return # Salir si no hay keywords
            
            # Usar el KeywordSearcher instanciado en __init__
            self.keyword_searcher.search_and_register(keywords, max_search_pages)
            # search_and_register ya loggea el número de ítems registrados/verificados

        else:
            logging.error(f"Modo de descubrimiento '{mode}' no reconocido.")
            return

        # --- Fase de Procesamiento (Extracción/Descarga) ---
        # Obtener los ítems que necesitan procesamiento basado en el modo y estado
        # Si es OAI, procesamos los que acabamos de descubrir (o todos los pendientes si queremos reintentar)
        # Si es Keyword, procesamos los que tienen el discovery_mode correcto y estado pendiente/error
        
        # Podríamos filtrar por discovery_mode si quisiéramos procesar solo los de este run
        logging.info(f"Scraper.run: Se usarán los siguientes estados para buscar ítems a procesar: {process_statuses}")
        items_to_process_ids = self.db_manager.get_items_to_process(statuses=process_statuses) 

        if not items_to_process_ids:
            logging.info("No hay ítems pendientes o con error para procesar. Finalizando.")
            return

        logging.info(f"Iniciando procesamiento de {len(items_to_process_ids)} ítems...")
        total_items_to_process = len(items_to_process_ids)
        processed_count = 0
        error_count = 0
        # Skipped count se refiere a los saltados en el bucle anterior por ya estar 'processed'

        for i, item_id in enumerate(items_to_process_ids):
            if self.config['delay'] > 0 and i > 0: time.sleep(self.config['delay'] / 5)

            # Obtener detalles del ítem (URL, OAI ID si existe, estado actual)
            item_data = self.db_manager.get_item_details(item_id)
            if not item_data:
                logging.error(f"No se pudieron obtener detalles para item_id {item_id}. Saltando.")
                error_count += 1 # Contar como error si no podemos obtener detalles
                continue

            item_page_url = item_data['item_page_url']
            oai_identifier_from_db = item_data.get('oai_identifier')
            current_status = item_data.get('processing_status', 'unknown') 

            # El 'identifier' para logging/extractor puede ser OAI ID si existe, sino parte de la URL
            log_identifier = oai_identifier_from_db if oai_identifier_from_db else item_page_url.split('/')[-1]

            logging.info(f"--- Procesando Ítem DB ID {item_id} ({i+1}/{total_items_to_process}): {log_identifier} (Status actual: {current_status}) ---")

            # Marcar como 'processing'
            self.db_manager.update_item_status(item_id, 'processing')

            # --- Extracción y Descarga ---
            all_downloads_ok = True # Asumir éxito hasta que algo falle
            try:
                # --- Extracción ---
                # Llamar a fetch_and_extract pasando la URL obtenida de la BD
                # Pasamos oai_identifier_from_db por si el extractor lo usa internamente (ej. para logs)
                _, metadata = self.extractor.fetch_and_extract(
                    item_page_url=item_page_url, 
                    item_id=item_id,
                    identifier=oai_identifier_from_db,
                    save_snapshot=True
                )

                if metadata is None:
                    logging.error(f"[Item {item_id}] Error al extraer metadatos HTML para {log_identifier}")
                    self.db_manager.update_item_status(item_id, 'error')
                    error_count += 1
                    continue

                # Actualizar metadatos en BD (log_item_metadata ya maneja None)
                self.db_manager.log_item_metadata(item_id, metadata)

                # --- Descarga de Recursos ---
                resources_to_download = []
                if metadata.get('pdf_url'):
                     resources_to_download.append({'type': 'pdf', 'url': metadata['pdf_url']})
                if metadata.get('thumbnail_url'):
                     resources_to_download.append({'type': 'image_thumbnail', 'url': metadata['thumbnail_url']})
                # Añadir otras imágenes si se extraen

                if not resources_to_download:
                     logging.info(f"[Item {item_id}] No se encontraron recursos descargables (PDF/Thumb) en metadatos.")
                     # Si no había nada que descargar, el item se considera procesado OK
                     all_downloads_ok = True
                else:
                     for resource in resources_to_download:
                         logging.info(f"[Item {item_id}] Intentando descargar recurso tipo '{resource['type']}' desde {resource['url'][:80]}...")
                         download_status = self.downloader.download_resource(item_id, resource['type'], resource['url'])
                         
                         if download_status.startswith('failed'):
                              # Decidir si este fallo es crítico
                              is_critical = (resource['type'] == 'pdf') # Ejemplo: PDF es crítico, thumbnail no.
                              if is_critical:
                                   all_downloads_ok = False
                                   logging.error(f"[Item {item_id}] Falla CRÍTICA al descargar {resource['type']} ({download_status}). Ítem será marcado como error.")
                                   # Podríamos detener el procesamiento de este ítem aquí si falla algo crítico
                                   # break 
                              else:
                                   logging.warning(f"[Item {item_id}] Falla NO CRÍTICA al descargar {resource['type']} ({download_status}).")
                         # Si es 'downloaded' o 'skipped_exists', todo bien.

                # Marcar estado final del Ítem basado en éxito de descargas *críticas*
                if all_downloads_ok:
                     self.db_manager.update_item_status(item_id, 'processed')
                     processed_count += 1
                else:
                     self.db_manager.update_item_status(item_id, 'error')
                     error_count += 1

            except Exception as e:
                 logging.error(f"[Item {item_id}] Error inesperado procesando ítem: {e}", exc_info=True)
                 self.db_manager.update_item_status(item_id, 'error')
                 error_count += 1

        logging.info(f"Procesamiento de ítems finalizado. Procesados OK: {processed_count}, Errores: {error_count}")
        self._generate_state_json() # Generar state.json al final
        self._generate_test_results_json() # Generar test_results.json al final
        self._package_output() # Empaquetar al final
        logging.info(f"Fin del scraper {self.config['country_code']}.")

    def _generate_state_json(self):
        """Genera el archivo AR/state.json con el estado actual de los ítems desde la BD."""
        logging.info("Generando archivo state.json...")
        state_data = []
        conn = self.db_manager._connect() # Usar el método de conexión de db_manager
        cursor = conn.cursor()

        try:
            # Obtener todos los ítems
            cursor.execute("SELECT item_id, item_page_url, processing_status, metadata_json FROM items ORDER BY item_id")
            items_rows = cursor.fetchall()

            for item_row in items_rows:
                item_id = item_row['item_id']
                item_page_url = item_row['item_page_url']
                processing_status = item_row['processing_status']
                metadata_json_str = item_row['metadata_json']
                
                item_metadata = {}
                html_local_path = None
                if metadata_json_str:
                    try:
                        loaded_meta = json.loads(metadata_json_str)
                        # Extraer campos específicos para state.json
                        item_metadata['title'] = loaded_meta.get('title')
                        item_metadata['authors'] = loaded_meta.get('authors') # Asumiendo que es una lista
                        item_metadata['publication_date'] = loaded_meta.get('publication_date')
                        # ... (se pueden añadir más si son necesarios y están en el JSON)
                        html_local_path = loaded_meta.get('html_local_path')
                    except json.JSONDecodeError:
                        logging.warning(f"Error decodificando metadata_json para item_id {item_id}")

                # Obtener PDFs asociados al item_id
                cursor.execute("""
                    SELECT remote_url, local_path, download_status 
                    FROM files 
                    WHERE item_id = ? AND file_type = ?
                """, (item_id, 'pdf'))
                pdf_files_rows = cursor.fetchall()
                
                pdfs_info = []
                for pdf_row in pdf_files_rows:
                    pdfs_info.append({
                        "url": pdf_row['remote_url'],
                        "local_path": pdf_row['local_path'],
                        "downloaded": pdf_row['download_status'] == 'downloaded' or pdf_row['download_status'] == 'skipped_exists'
                    })

                state_entry = {
                    "url": item_page_url,
                    "metadata": {k: v for k, v in item_metadata.items() if v is not None}, # Limpiar nulos
                    "html_path": html_local_path,
                    "pdfs": pdfs_info,
                    "analyzed": processing_status == 'processed'
                }
                state_data.append(state_entry)
            
            # Escribir a AR/state.json
            # Asegurar que el directorio de output exista (aunque setup_directories ya lo hace)
            output_dir = self.config.get('output_dir', 'AR/output')
            if not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            state_file_path = os.path.join(output_dir, "state.json") # Guardar en el directorio de output
            with open(state_file_path, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=4)
            logging.info(f"Archivo state.json generado en: {state_file_path}")

        except sqlite3.Error as e:
            logging.error(f"Error de base de datos generando state.json: {e}")
        except Exception as e:
            logging.error(f"Error inesperado generando state.json: {e}", exc_info=True)
        finally:
            if conn: conn.close()

    def _generate_test_results_json(self, max_results=5):
        """Genera el archivo AR/output/test_results.json con una muestra de PDFs descargados y verificados."""
        logging.info(f"Generando archivo test_results.json (máximo {max_results} resultados)..." )
        test_results = []
        conn = self.db_manager._connect()
        cursor = conn.cursor()

        try:
            # Obtener una muestra de PDFs descargados exitosamente
            sql_query = """ 
                SELECT f.item_id, f.local_path, f.md5_hash, f.file_size_bytes, i.item_page_url, i.metadata_json
                FROM files f
                JOIN items i ON f.item_id = i.item_id
                WHERE f.file_type = ? AND (f.download_status = ? OR f.download_status = ?)
                ORDER BY f.download_timestamp DESC -- o item_id, o aleatorio si se prefiere
                LIMIT ?
            """
            cursor.execute(sql_query, ('pdf', 'downloaded', 'skipped_exists', max_results))
            pdf_rows = cursor.fetchall()

            if not pdf_rows:
                logging.info("No se encontraron PDFs descargados para generar test_results.json.")
                # Crear un archivo vacío o con un mensaje si se prefiere
                output_dir = self.config.get('output_dir', 'AR/output')
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir, exist_ok=True)
                results_file_path = os.path.join(output_dir, "test_results.json")
                with open(results_file_path, 'w', encoding='utf-8') as f:
                    json.dump([], f, ensure_ascii=False, indent=4) # Escribir lista vacía
                logging.info(f"Archivo test_results.json vacío generado en: {results_file_path}")
                return

            for row in pdf_rows:
                item_metadata_min = {}
                if row['metadata_json']:
                    try:
                        full_meta = json.loads(row['metadata_json'])
                        item_metadata_min['title'] = full_meta.get('title')
                        item_metadata_min['authors'] = full_meta.get('authors')
                        item_metadata_min['publication_date'] = full_meta.get('publication_date')
                    except json.JSONDecodeError:
                        logging.warning(f"Error decodificando metadata_json para item_id {row['item_id']} al generar test_results.json")
                
                test_entry = {
                    "item_page_url": row['item_page_url'],
                    "local_pdf_path": row['local_path'],
                    "md5_hash": row['md5_hash'],
                    "file_size_bytes": row['file_size_bytes'],
                    "metadata": {k: v for k,v in item_metadata_min.items() if v is not None} # Limpiar nulos
                }
                test_results.append(test_entry)
            
            output_dir = self.config.get('output_dir', 'AR/output')
            if not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            results_file_path = os.path.join(output_dir, "test_results.json")
            with open(results_file_path, 'w', encoding='utf-8') as f:
                json.dump(test_results, f, ensure_ascii=False, indent=4)
            logging.info(f"Archivo test_results.json generado en: {results_file_path} con {len(test_results)} entradas.")

        except sqlite3.Error as e:
            logging.error(f"Error de base de datos generando test_results.json: {e}")
        except Exception as e:
            logging.error(f"Error inesperado generando test_results.json: {e}", exc_info=True)
        finally:
            if conn: conn.close()

    def _package_output(self, max_sample_pdfs=5):
        """Crea el paquete de salida en AR/output_package/."""
        
        script_dir = self.config.get('script_dir', os.path.dirname(os.path.abspath(__file__)))
        # base_package_dir ahora se construye relativo al script_dir
        base_package_dir = os.path.join(script_dir, "output_package")

        logging.info(f"Generando paquete de salida en {base_package_dir}...")
        
        docs_dir = os.path.join(base_package_dir, "docs")
        sample_pdfs_dir = os.path.join(docs_dir, "sample_pdfs")

        try:
            # 1. Crear directorios del paquete
            os.makedirs(sample_pdfs_dir, exist_ok=True)
            logging.info(f"Directorio del paquete creado/verificado: {base_package_dir}")

            # 2. Definir archivos a copiar y sus destinos
            # output_dir_config ya es una ruta absoluta desde __init__
            output_dir_config = self.config.get('output_dir') 

            files_to_copy = [
                (os.path.join(script_dir, "scraper.py"), os.path.join(base_package_dir, "scraper.py")),
                (os.path.join(script_dir, "selectors.yaml"), os.path.join(base_package_dir, "selectors.yaml")),
                (os.path.join(script_dir, "README.md"), os.path.join(base_package_dir, "README.md")),
                # test_results.json está en el output_dir_config que ya es absoluto
                (os.path.join(output_dir_config, "test_results.json"), os.path.join(base_package_dir, "test_results.json"))
            ]

            for src, dest in files_to_copy:
                if os.path.exists(src):
                    shutil.copy2(src, dest) # copy2 preserva metadatos
                    logging.info(f"Copiado: {src} -> {dest}")
                else:
                    logging.warning(f"Archivo fuente no encontrado para empaquetar: {src}")

            # 3. Copiar PDFs de muestra
            conn = self.db_manager._connect()
            cursor = conn.cursor()
            sql_query = """ 
                SELECT local_path 
                FROM files 
                WHERE file_type = ? AND (download_status = ? OR download_status = ?)
                ORDER BY download_timestamp DESC 
                LIMIT ? 
            """
            cursor.execute(sql_query, ('pdf', 'downloaded', 'skipped_exists', max_sample_pdfs))
            pdf_file_paths = [row['local_path'] for row in cursor.fetchall()]
            conn.close()

            copied_pdf_count = 0
            for pdf_path in pdf_file_paths:
                if pdf_path and os.path.exists(pdf_path):
                    try:
                        # Usar solo el nombre del archivo para el destino para evitar rutas anidadas innecesarias
                        dest_pdf_name = os.path.basename(pdf_path)
                        dest_path = os.path.join(sample_pdfs_dir, dest_pdf_name)
                        shutil.copy2(pdf_path, dest_path)
                        logging.info(f"PDF de muestra copiado: {pdf_path} -> {dest_path}")
                        copied_pdf_count += 1
                    except Exception as e_pdf:
                        logging.error(f"Error copiando PDF de muestra {pdf_path}: {e_pdf}")
                elif pdf_path: # Si pdf_path tiene valor pero no existe el archivo
                    logging.warning(f"PDF de muestra no encontrado en la ruta (podría ser None o la ruta no existe): {pdf_path}")
                # Si pdf_path es None, no logueamos nada extra, ya que get_items_to_process podría no devolverlo.
            
            logging.info(f"{copied_pdf_count} PDFs de muestra copiados a {sample_pdfs_dir}")
            logging.info(f"Paquete de salida generado exitosamente en {base_package_dir}")

        except Exception as e:
            logging.error(f"Error generando el paquete de salida: {e}", exc_info=True)
        finally:
            if 'conn' in locals() and conn: # Asegurar que conn exista y no esté cerrada
                 try:
                     # Simplemente intentar cerrar la conexión
                     conn.close()
                 except Exception: # Capturar cualquier error al cerrar
                      pass # Fallback silencioso

# --- Punto de Entrada ---

if __name__ == "__main__":
    # Usar configuración por defecto definida arriba
    # En un futuro, podríamos cargarla desde un archivo o argumentos
    config = DEFAULT_CONFIG

    try:
        scraper = Scraper(config)
        scraper.run()
    except Exception as e:
         # Capturar errores críticos durante la inicialización o ejecución
         logging.critical(f"Error fatal en la ejecución del scraper: {e}", exc_info=True)
         exit(1) # Salir con código de error 
