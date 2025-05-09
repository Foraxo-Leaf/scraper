import os
import requests
import time
import hashlib
import logging
from urllib.parse import urlparse, unquote
import re

class ResourceDownloaderBR:
    """
    Clase responsable de descargar recursos (PDFs, snapshots HTML) para el scraper de Brasil (BR).
    Gestiona la creación de directorios, nombres de archivo, descargas con reintentos,
    cálculo de hash MD5 y registro en la base de datos.
    """

    def __init__(self, config, logger, db_manager):
        """
        Inicializa el ResourceDownloaderBR.

        Args:
            config (dict): Diccionario de configuración del scraper.
            logger (logging.Logger): Instancia del logger.
            db_manager (DatabaseManagerBR): Instancia del gestor de base de datos.
        """
        self.config = config
        self.logger = logger if logger else logging.getLogger(__name__)
        self.db_manager = db_manager

        self.base_download_path = self.config.get('base_download_path', 'BR/output')
        self.pdf_path_segment = self.config.get('pdf_path_segment', 'pdfs')
        self.html_snapshot_path_segment = self.config.get('html_snapshot_path_segment', 'html_snapshot')
        
        self.download_timeout = self.config.get('download_timeout', 60) # Segundos para PDFs
        self.max_retries = self.config.get('max_download_retries', 3)
        self.retry_delay = self.config.get('download_retry_delay', 10) # Segundos
        self.user_agent = self.config.get('user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        self.stream_chunk_size = self.config.get('stream_chunk_size', 8192) # 8KB

        self._ensure_base_dirs()

    def _ensure_dir_exists(self, dir_path):
        """Asegura que un directorio exista, creándolo si es necesario."""
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
                self.logger.info(f"Directorio creado: {dir_path}")
            except OSError as e:
                self.logger.error(f"Error creando directorio {dir_path}: {e}")
                raise
        return dir_path

    def _ensure_base_dirs(self):
        """Asegura que los directorios base para PDFs y HTML snapshots existan."""
        self._ensure_dir_exists(os.path.join(self.base_download_path, self.pdf_path_segment))
        self._ensure_dir_exists(os.path.join(self.base_download_path, self.html_snapshot_path_segment))

    def _sanitize_filename(self, filename):
        """
        Limpia un nombre de archivo eliminando caracteres no válidos y limitando la longitud.
        """
        filename = unquote(filename) # Decodificar %20, etc.
        filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename) # Reemplazar caracteres no válidos
        filename = re.sub(r'\s+', '_', filename) # Reemplazar espacios múltiples con un solo guion bajo
        # Limitar longitud del nombre del archivo (sin extensión)
        name, ext = os.path.splitext(filename)
        max_len = 100 # Longitud máxima para el nombre base
        if len(name) > max_len:
            name = name[:max_len]
        return name + ext

    def get_local_path_for_item(self, item_id, resource_url, resource_type='pdf'):
        """
        Construye la ruta local de guardado para un recurso de un ítem.
        Crea un subdirectorio usando el item_id para organizar los archivos.

        Args:
            item_id (str or int): Identificador único del ítem.
            resource_url (str): URL del recurso (para extraer el nombre del archivo).
            resource_type (str): 'pdf' o 'html'.

        Returns:
            str: Ruta local completa para guardar el archivo.
        """
        path_segment = self.pdf_path_segment if resource_type == 'pdf' else self.html_snapshot_path_segment
        item_dir_path = self._ensure_dir_exists(os.path.join(self.base_download_path, path_segment, str(item_id)))
        
        parsed_url = urlparse(resource_url)
        filename_from_url = os.path.basename(parsed_url.path)
        
        if not filename_from_url or (resource_type == 'html' and not filename_from_url.endswith( ('.html', '.htm')) ):
            filename_from_url = f"{item_id}_{resource_type}.html" if resource_type == 'html' else f"{item_id}_file.pdf"
            
        sanitized_filename = self._sanitize_filename(filename_from_url)
        
        # Asegurar extensión .pdf si es un PDF y no la tiene (o tiene una extraña)
        if resource_type == 'pdf':
            name, ext = os.path.splitext(sanitized_filename)
            if ext.lower() != '.pdf':
                # Si es una extensión común de documento, mantenerla si es la única
                common_doc_exts = ['.doc', '.docx', '.odt', '.txt', '.rtf']
                if ext.lower() in common_doc_exts and name:
                     # Podríamos decidir mantener estas extensiones, pero para PDFs, forzamos .pdf
                     sanitized_filename = name + '.pdf' 
                elif not name and ext: # Solo extensión rara, sin nombre
                    sanitized_filename = f"{item_id}_downloaded.pdf"
                elif not ext: # Sin extensión
                    sanitized_filename = sanitized_filename + '.pdf'
                # Si ya tiene .pdf, no hace nada. Si tiene otra extensión, la cambia a .pdf.

        return os.path.join(item_dir_path, sanitized_filename)

    def calculate_md5(self, file_path):
        """Calcula el hash MD5 de un archivo."""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except IOError as e:
            self.logger.error(f"Error de I/O calculando MD5 para {file_path}: {e}")
        except Exception as e:
            self.logger.error(f"Error inesperado calculando MD5 para {file_path}: {e}")
        return None

    def download_resource(self, item_id, resource_url, resource_type='pdf', file_id_in_db=None):
        """
        Descarga un recurso (PDF o HTML snapshot) desde una URL.

        Args:
            item_id (str or int): ID del ítem al que pertenece el recurso.
            resource_url (str): URL del recurso a descargar.
            resource_type (str): 'pdf' o 'html'.
            file_id_in_db (int, optional): ID del archivo en la tabla 'files' si ya existe.

        Returns:
            tuple: (local_path, md5_hash, file_size_bytes, download_status_message)
                   Retorna (None, None, 0, "error_message") en caso de fallo.
        """
        local_path = self.get_local_path_for_item(item_id, resource_url, resource_type)
        md5_hash = None
        file_size_bytes = 0
        status_message = f"pending_{resource_type}_download"

        if os.path.exists(local_path):
            # Si el archivo ya existe, verificamos si está registrado y es válido
            # Esto previene re-descargas innecesarias si el proceso fue interrumpido.
            existing_file_info = self.db_manager.get_file_by_path(local_path)
            if existing_file_info and existing_file_info['download_status'] in ['downloaded', 'verified'] and existing_file_info['md5_hash']:
                self.logger.info(f"El archivo {local_path} ya existe y está verificado en la BD. Saltando descarga.")
                return (
                    local_path,
                    existing_file_info['md5_hash'],
                    existing_file_info['file_size_bytes'],
                    existing_file_info['download_status']
                )
            else:
                self.logger.info(f"El archivo {local_path} ya existe localmente pero no está (bien) registrado en la BD o no tiene hash. Se intentará verificar/re-descargar.")
                # Podríamos calcular MD5 aquí si no está en BD, pero por simplicidad, si no está bien, se re-descarga o se marca.
                # Por ahora, si no está bien registrado, lo trataremos como si necesitara ser (re)descargado.
                # Una lógica más avanzada podría intentar verificar el archivo existente antes de sobreescribir.
                pass # Continuar con la descarga para asegurar la integridad y el registro correcto

        headers = {
            'User-Agent': self.user_agent,
             # Para PDFs, es común aceptar application/pdf directamente
            'Accept': 'application/pdf, text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8' 
        }
        if resource_type == 'html':
            headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'

        self.logger.info(f"Iniciando descarga de {resource_type} desde {resource_url} para item {item_id}")
        self.logger.info(f"Se guardará en: {local_path}")

        for attempt in range(self.max_retries):
            try:
                with requests.get(resource_url, headers=headers, stream=True, timeout=self.download_timeout, allow_redirects=True) as r:
                    r.raise_for_status()
                    
                    # Verificar Content-Type para PDFs antes de descargar
                    content_type = r.headers.get('Content-Type', '').lower()
                    if resource_type == 'pdf' and 'application/pdf' not in content_type:
                        # A veces el Content-Type puede ser octet-stream pero el archivo es un PDF válido
                        # O puede ser text/html si es una página de error o un visualizador HTML.
                        # Si no es application/pdf, logueamos una advertencia pero continuamos si la URL sugiere PDF.
                        if not resource_url.lower().endswith('.pdf') and 'octet-stream' not in content_type:
                            self.logger.warning(f"La URL {resource_url} no parece ser un PDF directo. Content-Type: {content_type}. Se intentará descargar de todas formas.")
                        elif 'text/html' in content_type:
                             self.logger.error(f"La URL {resource_url} devolvió HTML en lugar de PDF. Content-Type: {content_type}. Abortando descarga para este recurso.")
                             status_message = "download_failed_not_a_pdf"
                             # Registrar el fallo en la base de datos
                             if file_id_in_db:
                                self.db_manager.log_file_download_attempt(file_id_in_db, local_path, 0, None, status_message, resource_url, resource_type)
                             else: # Si no hay file_id, podría ser un nuevo intento no registrado previamente
                                self.db_manager.log_download_attempt_for_item(item_id, resource_url, resource_type, local_path, 0, None, status_message)
                             return None, None, 0, status_message

                    with open(local_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=self.stream_chunk_size):
                            if chunk: # filter out keep-alive new chunks
                                f.write(chunk)
                                file_size_bytes += len(chunk)
                    
                    self.logger.info(f"{resource_type.capitalize()} descargado exitosamente: {local_path} ({file_size_bytes} bytes)")
                    md5_hash = self.calculate_md5(local_path)
                    if md5_hash:
                        self.logger.info(f"MD5 para {local_path}: {md5_hash}")
                        status_message = 'downloaded' # O 'verified' si la verificación es solo el MD5
                    else:
                        self.logger.warning(f"No se pudo calcular el MD5 para {local_path}. El archivo podría estar corrupto.")
                        status_message = 'downloaded_md5_failed'

                    # Registrar en la base de datos
                    if file_id_in_db:
                        self.db_manager.log_file_download_attempt(file_id_in_db, local_path, file_size_bytes, md5_hash, status_message, resource_url, resource_type)
                    else:
                        self.db_manager.log_download_attempt_for_item(item_id, resource_url, resource_type, local_path, file_size_bytes, md5_hash, status_message)
                    
                    return local_path, md5_hash, file_size_bytes, status_message

            except requests.exceptions.HTTPError as e:
                self.logger.error(f"Error HTTP {e.response.status_code} descargando {resource_url} (intento {attempt + 1}/{self.max_retries}): {e}")
                status_message = f"download_failed_http_{e.response.status_code}"
                if e.response.status_code == 404:
                    break # No reintentar si es 404
            except requests.exceptions.Timeout as e:
                self.logger.error(f"Timeout descargando {resource_url} (intento {attempt + 1}/{self.max_retries}): {e}")
                status_message = "download_failed_timeout"
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error de red descargando {resource_url} (intento {attempt + 1}/{self.max_retries}): {e}")
                status_message = "download_failed_network_error"
            except IOError as e:
                self.logger.error(f"Error de I/O guardando {resource_url} en {local_path} (intento {attempt + 1}/{self.max_retries}): {e}")
                status_message = "download_failed_io_error"
                break # Probablemente no se solucione con reintento
            except Exception as e:
                self.logger.error(f"Error inesperado descargando {resource_url} (intento {attempt+1}/{self.max_retries}): {e}")
                status_message = "download_failed_unexpected_error"
                break

            if attempt < self.max_retries - 1:
                self.logger.info(f"Reintentando descarga de {resource_url} en {self.retry_delay} segundos...")
                time.sleep(self.retry_delay)
            else:
                self.logger.error(f"Fallaron todos los {self.max_retries} intentos para descargar {resource_url}")
                # Registrar el fallo final en la base de datos
                if file_id_in_db:
                    self.db_manager.log_file_download_attempt(file_id_in_db, local_path, 0, None, status_message, resource_url, resource_type)
                else:
                     self.db_manager.log_download_attempt_for_item(item_id, resource_url, resource_type, local_path, 0, None, status_message)

        return None, None, 0, status_message

# Ejemplo de uso (requiere configuración, logger y db_manager mocks/reales)
if __name__ == '__main__':
    # --- Mockups para prueba --- 
    class MockLogger:
        def info(self, msg): print(f"INFO: {msg}")
        def error(self, msg): print(f"ERROR: {msg}")
        def warning(self, msg): print(f"WARNING: {msg}")
        def debug(self, msg): print(f"DEBUG: {msg}")

    class MockDBManager:
        def log_file_download_attempt(self, file_id, local_path, size, md5, status, url, res_type):
            print(f"DB: Logged file_id {file_id}, path {local_path}, status {status}")
        def log_download_attempt_for_item(self, item_id, url, res_type, local_path, size, md5, status):
            print(f"DB: Logged item_id {item_id}, path {local_path}, status {status}")
        def get_file_by_path(self, local_path):
            print(f"DB: Checked path {local_path}")
            # Simular que el archivo no existe en BD para forzar descarga en prueba
            return None 

    mock_logger = MockLogger()
    mock_db_manager = MockDBManager()
    mock_config = {
        'base_download_path': 'BR_test_output',
        'pdf_path_segment': 'pdfs',
        'html_snapshot_path_segment': 'html_snapshots',
        'download_timeout': 10, # Timeout corto para pruebas
        'max_download_retries': 2,
        'download_retry_delay': 1,
        'user_agent': 'TestDownloader/1.0'
    }
    # --- Fin Mockups ---

    downloader = ResourceDownloaderBR(mock_config, mock_logger, mock_db_manager)
    
    # Crear directorios de prueba si no existen
    if not os.path.exists(mock_config['base_download_path']):
        os.makedirs(mock_config['base_download_path'])

    # --- Pruebas --- 
    mock_logger.info("--- Iniciando pruebas del ResourceDownloaderBR ---")

    test_item_id = "test_item_001"
    # URL de un PDF pequeño para prueba (reemplazar con una URL real y válida)
    # Ejemplo: un PDF de Creative Commons
    test_pdf_url = "https://creativecommons.org/licenses/by/4.0/legalcode.pdf" 
    # test_pdf_url = "URL_INVALIDA_PARA_PROBAR_FALLO.pdf"
    # test_pdf_url = "URL_QUE_NO_ES_PDF_PARA_PROBAR_CONTENT_TYPE.html" #ej: https://www.google.com

    mock_logger.info(f"\n1. Probando descarga de PDF (item: {test_item_id}, url: {test_pdf_url})")
    local_p, md5_h, size_b, status_msg = downloader.download_resource(test_item_id, test_pdf_url, resource_type='pdf')
    if local_p:
        mock_logger.info(f"Resultado descarga PDF: Path={local_p}, MD5={md5_h}, Size={size_b} bytes, Status={status_msg}")
        # Intentar descargar de nuevo para probar si salta la descarga
        mock_logger.info("Intentando descargar el mismo PDF de nuevo (debería saltar si ya está en BD y verificado)...")
        # Para que salte, necesitaríamos simular que get_file_by_path devuelve la info correcta.
        # Por ahora, la prueba simplemente re-descargará o fallará si la URL es mala.
        # downloader.download_resource(test_item_id, test_pdf_url, resource_type='pdf') 
    else:
        mock_logger.error(f"Fallo la descarga del PDF. Status: {status_msg}")

    test_html_item_id = "test_html_item_002"
    test_html_url = "https://www.embrapa.br/busca-de-publicacoes/-/publicacao/1155607/panorama-da-pecuaria-de-corte-no-estado-de-roraima" # Página HTML real
    mock_logger.info(f"\n2. Probando descarga de HTML snapshot (item: {test_html_item_id}, url: {test_html_url})")
    local_h_p, _, size_h_b, status_h_msg = downloader.download_resource(test_html_item_id, test_html_url, resource_type='html')
    if local_h_p:
        mock_logger.info(f"Resultado descarga HTML: Path={local_h_p}, Size={size_h_b} bytes, Status={status_h_msg}")
    else:
        mock_logger.error(f"Fallo la descarga del HTML. Status: {status_h_msg}")
    
    mock_logger.info(f"\n3. Probando sanitización de nombres de archivo:")
    tricky_url = "https://example.com/some path/with spaces/and%20encoded chars/file:name*with?special<>|.pdf"
    sanitized = downloader._sanitize_filename(os.path.basename(urlparse(tricky_url).path))
    mock_logger.info(f"URL original: {os.path.basename(urlparse(tricky_url).path)}")
    mock_logger.info(f"Nombre sanitizado: {sanitized}")
    assert sanitized == "file_name_with_special__.pdf" # Ajustar según la lógica exacta de _sanitize_filename

    mock_logger.info(f"\n4. Probando get_local_path_for_item:")
    path1 = downloader.get_local_path_for_item("item123", "http://example.com/docs/document.pdf", "pdf")
    mock_logger.info(f"Ruta para PDF: {path1}")
    assert "BR_test_output/pdfs/item123/document.pdf" in path1.replace('\\', '/')

    path2 = downloader.get_local_path_for_item("item456", "http://example.com/page.html", "html")
    mock_logger.info(f"Ruta para HTML: {path2}")
    assert "BR_test_output/html_snapshots/item456/page.html" in path2.replace('\\', '/')
    
    path3 = downloader.get_local_path_for_item("item789", "http://example.com/data/some_strange_file.xyz", "pdf")
    mock_logger.info(f"Ruta para PDF con extensión extraña: {path3}")
    assert "BR_test_output/pdfs/item789/some_strange_file.pdf" in path3.replace('\\', '/') # Debería forzar .pdf

    mock_logger.info("\n--- Pruebas del ResourceDownloaderBR finalizadas ---")
    mock_logger.info(f"Verifica los archivos descargados en el directorio: {os.path.abspath(mock_config['base_download_path'])}")
