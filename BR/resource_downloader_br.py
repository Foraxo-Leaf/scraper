# BR/resource_downloader_br.py
import requests
import os
import time
import hashlib
from urllib.parse import urlparse, unquote
import logging

def calculate_md5_util(file_path, logger_instance):
    """Calcula el hash MD5 de un archivo."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        logger_instance.error(f"Archivo no encontrado para calcular MD5: {file_path}")
        return None
    except Exception as e:
        logger_instance.error(f"Error calculando MD5 para {file_path}: {e}")
        return None

class ResourceDownloaderBR:
    def __init__(self, config, logger_instance=None, db_manager_instance=None, module_br_root_dir=None):
        self.config = config
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger("ResourceDownloaderBR")
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                self.logger.setLevel(logging.INFO)
        
        self.db_manager = db_manager_instance
        if not self.db_manager:
            self.logger.critical("ResourceDownloaderBR inicializado sin una instancia de DatabaseManager. Las operaciones de BD fallarán.")

        if not module_br_root_dir:
            # Fallback si no se pasa, aunque ScraperBR debería pasarlo.
            # Esto haría que las rutas sean relativas al CWD si no se pasa, lo cual queremos evitar.
            self.logger.warning("module_br_root_dir no fue proporcionado a ResourceDownloaderBR. Las rutas podrían no ser relativas al módulo BR.")
            self.module_br_root_dir = os.getcwd() # Comportamiento anterior como fallback
        else:
            self.module_br_root_dir = module_br_root_dir

        # Usar output_path de la config, que ahora es relativo al módulo BR
        base_output_dir_relative = self.config.get('output_path', 'output') 
        self.base_output_dir_absolute = os.path.join(self.module_br_root_dir, base_output_dir_relative)

        self.delay_seconds = self.config.get('download_delay_seconds', 1)
        self.max_retries = self.config.get('max_retries', 3)
        # Usar un timeout de descarga más largo que el de requests generales
        self.download_timeout = self.config.get('download_timeout', self.config.get('request_timeout', 60) * 2) 
        self.user_agent = self.config.get('user_agent', 'GenericScraper/1.0')
        self.retry_base_delay = self.config.get('download_base_retry_delay', 5) # Para backoff

    def _ensure_dir_exists(self, file_path):
        """Asegura que el directorio para un archivo exista."""
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            try:
                os.makedirs(directory, exist_ok=True)
                self.logger.debug(f"Directorio creado: {directory}")
            except OSError as e:
                self.logger.error(f"Error creando directorio {directory}: {e}")
                raise # Propagar el error si no se puede crear el directorio

    def _build_local_path(self, item_id, file_type_group, remote_url_for_filename, desired_extension=None):
        """Construye la ruta de archivo local basada en item_id, tipo y URL remota."""
        if item_id is None:
            self.logger.error("_build_local_path llamado con item_id None.")
            return None
        try:
            # Simplificado: crear un nombre de archivo basado en el item_id y un hash de la URL si es necesario
            # y el tipo de archivo.
            # El file_type_group sería 'pdfs', 'html_snapshot', etc.
            url_path = urlparse(remote_url_for_filename).path
            filename_base = os.path.basename(url_path).split('?')[0] if url_path else "file"
            safe_filename = "".join([c for c in filename_base if c.isalnum() or c in ('-', '_', '.')]).rstrip('. ')
            
            if not safe_filename or len(safe_filename) > 100: # Si es vacío o muy largo
                # Usar un hash de la URL para asegurar unicidad y longitud razonable
                safe_filename = hashlib.md5(remote_url_for_filename.encode()).hexdigest()[:16]

            # Determinar extensión
            file_extension = desired_extension
            if not file_extension:
                # Intentar obtenerla del nombre de archivo si existe y es razonable
                _, ext_from_name = os.path.splitext(safe_filename)
                if ext_from_name and len(ext_from_name) <= 5 and ext_from_name.startswith('.'):
                    file_extension = ext_from_name
                else: # Fallback a .dat o algo genérico si no se puede determinar
                    file_extension = '.dat' 
            
            if not safe_filename.endswith(file_extension):
                # Quitar cualquier extensión previa si vamos a añadir una nueva
                name_part_only, _ = os.path.splitext(safe_filename)
                safe_filename = name_part_only + file_extension
            
            # Asegurar que el nombre no sea solo la extensión (ej. ".pdf")
            if safe_filename == file_extension:
                safe_filename = hashlib.md5(remote_url_for_filename.encode()).hexdigest()[:16] + file_extension

            # Usar self.base_output_dir_absolute
            target_dir = os.path.join(self.base_output_dir_absolute, file_type_group, str(item_id))
            os.makedirs(target_dir, exist_ok=True)
            final_path = os.path.join(target_dir, safe_filename)
            
            # Truncar si la ruta completa es demasiado larga (raro, pero posible en algunos OS)
            max_path_len = 255 # Límite común
            if len(final_path) > max_path_len:
                name_part, ext_part = os.path.splitext(safe_filename)
                allowed_name_len = max_path_len - len(os.path.join(target_dir, '')) - len(ext_part) -1 # -1 por si acaso
                if allowed_name_len < 5: # No tiene sentido si el nombre es muy corto
                    self.logger.error(f"La ruta base es demasiado larga para crear un archivo válido en {target_dir}")
                    return None
                truncated_name = name_part[:allowed_name_len]
                final_path = os.path.join(target_dir, truncated_name + ext_part)
                self.logger.warning(f"La ruta del archivo ha sido truncada a: {final_path}")

            return final_path
        except Exception as e:
            self.logger.error(f"Error creando ruta local para item {item_id}, tipo {file_type_group}, url {remote_url_for_filename}: {e}", exc_info=True)
            return None

    def _cleanup_partial_file(self, file_path):
        """Intenta eliminar un archivo parcial si existe después de un error de descarga."""
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                self.logger.info(f"Eliminado archivo parcial/fallido: {file_path}")
            except OSError as oe:
                self.logger.warning(f"Error eliminando archivo parcial {file_path}: {oe}")

    def download_resource(self, item_id, file_type, remote_url, is_snapshot=False):
        result = {
            'status': 'pending',
            'local_path': None,
            'md5': None,
            'size': None,
            'remote_url': remote_url,
            'file_type': file_type
        }

        if not remote_url:
            self.logger.warning(f"[Item {item_id}] No se proporcionó URL remota para descargar tipo '{file_type}'.")
            result['status'] = 'failed_no_remote_url'
            return result['status']
        
        if item_id is None: 
            self.logger.error(f"[ResourceDownloader] item_id es None para URL '{remote_url}'. No se puede descargar.")
            result['status'] = 'failed_item_id_none'
            return result['status']
        
        # Determinar la extensión deseada para _build_local_path
        desired_ext = None
        if file_type == 'pdf':
            desired_ext = '.pdf'
        elif file_type.startswith('image'): # Ej: 'image_thumbnail', 'image_cover'
            # Intentar obtener la extensión de la URL si es una imagen común, sino default a .jpg
            _, ext_from_url = os.path.splitext(urlparse(remote_url).path)
            if ext_from_url.lower() in ['.jpg', '.jpeg', '.png', '.gif']:
                desired_ext = ext_from_url.lower()
            else:
                desired_ext = '.jpg' # Default para imágenes desconocidas
        # Para html_snapshot, _build_local_path ya añade .html si desired_extension es None y el nombre no lo tiene
        # Pero es mejor ser explícito si save_html_snapshot lo llama directamente. Este método es más genérico.

        local_path_target = self._build_local_path(item_id, file_type, remote_url, desired_extension=desired_ext)
        if not local_path_target:
            result['status'] = 'error_path_creation'
            self.db_manager.log_file_result(item_id, file_type, remote_url, result['status'], None, None, None)
            return result['status']
        
        result['local_path'] = local_path_target

        # Verificar si el archivo ya existe y está OK (esta lógica estaba en el Scraper AR)
        existing_db_status, existing_db_path = self.db_manager.get_file_status(item_id, remote_url)
        if existing_db_status in ['downloaded', 'skipped_exists']:
            if existing_db_path and os.path.exists(existing_db_path):
                 self.logger.info(f"[Item {item_id}] Archivo '{file_type}' ({remote_url}) ya procesado y existe en BD/disco ({existing_db_status} en {existing_db_path}). Saltando descarga.")
                 return existing_db_status
            else:
                 self.logger.warning(f"[Item {item_id}] Archivo '{file_type}' ({remote_url}) marcado como '{existing_db_status}' en BD pero no encontrado en disco en '{existing_db_path}'. Se intentará descargar de nuevo.")

        attempts = 0
        download_successful = False
        while attempts <= self.max_retries:
            try:
                if attempts > 0:
                    retry_delay = self.retry_base_delay * (2 ** (attempts - 1))
                    self.logger.info(f"[Item {item_id}] Reintento {attempts}/{self.max_retries} para '{remote_url}' en {retry_delay}s...")
                    time.sleep(retry_delay)
                
                self.logger.info(f"[Item {item_id}] Descargando {file_type} desde: {remote_url} -> {local_path_target} (Intento {attempts + 1})")
                headers = {'User-Agent': self.user_agent}
                
                response = requests.get(remote_url, stream=True, timeout=self.download_timeout, headers=headers)
                response.raise_for_status()

                with open(local_path_target, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                self.logger.info(f"[Item {item_id}] {file_type} descargado en: {local_path_target}")
                if self.delay_seconds > 0 and attempts == 0:
                    time.sleep(self.delay_seconds / 2)

                result['size'] = os.path.getsize(local_path_target)
                result['md5'] = calculate_md5_util(local_path_target, self.logger)
                result['status'] = "downloaded"
                download_successful = True
                break 
            except requests.exceptions.Timeout:
                self.logger.warning(f"[Item {item_id}] Timeout descargando {file_type} '{remote_url}' (Intento {attempts + 1})")
                self._cleanup_partial_file(local_path_target)
                result['status'] = "error_timeout"
            except requests.exceptions.HTTPError as http_err:
                status_code = http_err.response.status_code if http_err.response is not None else 'N/A'
                self.logger.warning(f"[Item {item_id}] Error HTTP {status_code} descargando {file_type} '{remote_url}' (Intento {attempts + 1}).")
                self._cleanup_partial_file(local_path_target)
                result['status'] = f"error_http_{status_code}"
                if 400 <= (status_code if isinstance(status_code, int) else 0) < 500 and status_code not in [408, 429]:
                    break 
            except requests.exceptions.RequestException as req_err:
                self.logger.warning(f"[Item {item_id}] Error de red descargando {file_type} '{remote_url}' (Intento {attempts + 1}): {req_err}")
                self._cleanup_partial_file(local_path_target)
                result['status'] = "error_network"
            except Exception as e:
                self.logger.error(f"[Item {item_id}] Error inesperado descargando {file_type} '{remote_url}' (Intento {attempts + 1}): {e}", exc_info=True)
                self._cleanup_partial_file(local_path_target)
                result['status'] = "error_exception"
                break 
            attempts += 1
        
        if not download_successful and attempts > self.max_retries:
            self.logger.error(f"[Item {item_id}] Máximos reintentos ({self.max_retries}) alcanzados para {file_type} '{remote_url}'. Falló con: {result['status']}")
        
        # Registrar el resultado final en la BD
        self.db_manager.log_file_result(
            item_id,
            result['file_type'],
            result['remote_url'],
            result['status'],
            result['local_path'] if download_successful else local_path_target,
            result['md5'],
            result['size']
        )
        return result['status']

    def save_html_snapshot(self, item_id, page_url, html_content_str):
        """Guarda el contenido HTML como un archivo local y lo registra en la BD."""
        if item_id is None:
            self.logger.error("Se intentó guardar snapshot HTML con item_id None.")
            return None

        file_type_group = 'html_snapshot'
        local_path = self._build_local_path(item_id, file_type_group, page_url, desired_extension='.html') 

        if not local_path:
            self.logger.error(f"[Item {item_id}] No se pudo construir la ruta local para el snapshot HTML de {page_url}")
            self.db_manager.log_file_result(
                item_id=item_id, 
                file_type=file_type_group, 
                remote_url=page_url, 
                local_path=None, 
                download_status='error_path_creation', 
                md5_hash=None, 
                file_size_bytes=None
            )
            return None
        
        try:
            with open(local_path, 'w', encoding='utf-8') as f:
                f.write(html_content_str)
            
            file_size = os.path.getsize(local_path)
            md5_hash = hashlib.md5(html_content_str.encode('utf-8')).hexdigest()
            
            self.logger.info(f"[Item {item_id}] Snapshot HTML guardado en: {local_path} ({file_size} bytes)")
            
            self.db_manager.log_file_result(
                item_id=item_id,
                file_type=file_type_group,
                remote_url=page_url, 
                local_path=local_path,
                download_status='downloaded',
                md5_hash=md5_hash,
                file_size_bytes=file_size
            )
            return local_path
        except IOError as e:
            self.logger.error(f"[Item {item_id}] Error de I/O guardando snapshot HTML en {local_path}: {e}")
        except Exception as e:
            self.logger.error(f"[Item {item_id}] Error inesperado guardando snapshot HTML en {local_path}: {e}", exc_info=True)
        
        self.db_manager.log_file_result(
            item_id=item_id, 
            file_type=file_type_group, 
            remote_url=page_url, 
            local_path=local_path, 
            download_status='error_saving_snapshot', 
            md5_hash=None, 
            file_size_bytes=None
        )
        return None

    # Nuevo método para obtener snapshots HTML
    def fetch_html_snapshot(self, url, item_id_for_path):
        """Obtiene el contenido HTML de una URL y lo guarda como snapshot."""
        self.logger.debug(f"Intentando obtener snapshot HTML para item {item_id_for_path} desde {url}")
        snapshot_content = None
        snapshot_path = None
        
        # Construir la ruta de guardado para el snapshot
        snapshot_dir = os.path.join(self.config.get('output_dir', 'BR/output'), 'html_snapshot', str(item_id_for_path))
        snapshot_filename = f"item_{item_id_for_path}_snapshot.html"
        snapshot_path = os.path.join(snapshot_dir, snapshot_filename)

        try:
            headers = {'User-Agent': self.user_agent}
            response = requests.get(url, headers=headers, timeout=self.download_timeout, allow_redirects=True)
            response.raise_for_status() # Lanza excepción para códigos 4xx/5xx
            snapshot_content = response.text # Usar .text para HTML
            self.logger.debug(f"HTML obtenido exitosamente para {url} (item {item_id_for_path}). Tamaño: {len(snapshot_content)} bytes.")

            # Guardar el snapshot
            os.makedirs(snapshot_dir, exist_ok=True)
            with open(snapshot_path, 'w', encoding='utf-8') as f:
                f.write(snapshot_content)
            self.logger.info(f"Snapshot HTML guardado para item {item_id_for_path} en: {snapshot_path}")
            
        except requests.exceptions.Timeout:
            self.logger.warning(f"Timeout obteniendo HTML snapshot para {url} (item {item_id_for_path})")
            # Podríamos querer registrar este fallo en la tabla items?
        except requests.exceptions.HTTPError as e:
             self.logger.warning(f"Error HTTP {e.response.status_code} obteniendo HTML snapshot para {url} (item {item_id_for_path})")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error de red obteniendo HTML snapshot para {url} (item {item_id_for_path}): {e}")
        except IOError as e:
             self.logger.error(f"Error de I/O guardando HTML snapshot en {snapshot_path} (item {item_id_for_path}): {e}")
        except Exception as e:
            self.logger.error(f"Error inesperado obteniendo/guardando HTML snapshot para {url} (item {item_id_for_path}): {e}", exc_info=True)
            snapshot_content = None # Asegurar que no se devuelva contenido parcial en error inesperado
            snapshot_path = None # No devolver path si hubo error grave

        return snapshot_content, snapshot_path

# Ejemplo de uso básico (para pruebas)
if __name__ == '__main__':
    print("Ejecutando pruebas básicas de ResourceDownloaderBR...")
    test_logger = logging.getLogger("ResourceDownloaderTest")
    test_logger.setLevel(logging.DEBUG)
    test_handler = logging.StreamHandler()
    test_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    test_handler.setFormatter(test_formatter)
    if not test_logger.handlers: test_logger.addHandler(test_handler)

    # Configuración de prueba
    test_config = {
        "output_dir": "BR/output_test_downloader", # Directorio de prueba
        "download_delay_seconds": 0.1,
        "max_retries": 1,
        "download_timeout": 10,
        "user_agent": "TestDownloader/1.0",
        "download_base_retry_delay": 1
    }
    # Asegurar que el directorio de prueba exista y esté limpio
    if os.path.exists(test_config["output_dir"]):
        import shutil
        shutil.rmtree(test_config["output_dir"])
    os.makedirs(test_config["output_dir"], exist_ok=True)

    downloader = ResourceDownloaderBR(test_config, logger_instance=test_logger)

    test_logger.info("--- Probando descarga de PDF (ejemplo real) ---")
    # Se necesita un item_id y una URL de PDF real y pequeña para probar
    # Usaremos el ejemplo de BR/analysis.json, pero solo si es accesible y pequeño
    # Por ahora, un placeholder, ya que no quiero descargas reales en esta prueba unitaria.
    # pdf_item_id = "test_pdf_item_001"
    # pdf_url_real = "https://www.infoteca.cnptia.embrapa.br/infoteca/bitstream/doc/1175312/1/BA-04-2025.pdf" # Ejemplo
    # pdf_result = downloader.download_pdf(pdf_item_id, pdf_url_real)
    # test_logger.info(f"Resultado descarga PDF: {pdf_result}")
    # if pdf_result['status'] == 'downloaded':
    #     assert os.path.exists(pdf_result['local_path'])
    #     assert pdf_result['md5'] is not None
    #     assert pdf_result['size'] > 0
    #     test_logger.info(f"PDF descargado en: {pdf_result['local_path']}")
    test_logger.info("Prueba de PDF real comentada para evitar descargas no deseadas en tests básicos.")

    test_logger.info("--- Probando descarga de HTML Snapshot (ejemplo) ---")
    html_item_id = "test_html_item_001"
    html_page_url = "https://www.embrapa.br/"
    html_result = downloader.download_html_snapshot(html_item_id, html_page_url)
    test_logger.info(f"Resultado descarga HTML: {html_result}")
    if html_result['status'] == 'downloaded':
        assert os.path.exists(html_result['local_path'])
        assert html_result['size'] > 0
        # MD5 no se calcula para snapshots HTML por defecto
        test_logger.info(f"HTML Snapshot descargado en: {html_result['local_path']}")
    
    test_logger.info("--- Probando URL inválida/no existente ---")
    invalid_url_item_id = "test_invalid_001"
    invalid_url = "http://thissitedoesnotexist.invalid/file.pdf"
    invalid_result = downloader.download_pdf(invalid_url_item_id, invalid_url)
    test_logger.info(f"Resultado URL inválida: {invalid_result}")
    assert invalid_result['status'].startswith("failed")

    test_logger.info("--- Probando _build_local_path con varios nombres ---")
    path1 = downloader._build_local_path("item01", "pdf", "http://example.com/some/path/documento%20final.pdf?query=1")
    test_logger.info(f"Path generado 1: {path1}") # Esperado: .../pdfs/item01/documentofinal.pdf
    assert "documentofinal.pdf" in path1

    path2 = downloader._build_local_path("item02", "html_snapshot", "http://example.com/page.html")
    test_logger.info(f"Path generado 2: {path2}") # Esperado: .../html_snapshot/item02/item02_snapshot.html
    assert "item02_snapshot.html" in path2

    path3 = downloader._build_local_path("item03", "pdf", "http://example.com/very_long_name_with_special_ chars!@#$.pdf")
    test_logger.info(f"Path generado 3: {path3}") 
    assert "very_long_name_with_special_chars.pdf" in path3.lower() # La limpieza puede afectar mayúsculas

    path4 = downloader._build_local_path("item04", "pdf", "http://example.com/") # Sin nombre de archivo
    test_logger.info(f"Path generado 4: {path4}") 
    assert path4.endswith(".pdf") # Debería generar un nombre con extensión pdf

    test_logger.info("Pruebas de ResourceDownloaderBR finalizadas.") 
