import sqlite3
import os
import logging
import json
import datetime

class DatabaseManagerBR:
    def __init__(self, db_file, logger_instance=None):
        self.db_file = db_file
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger("DatabaseManagerBR")
            # Configurar un logger básico si no se proporciona uno
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                self.logger.setLevel(logging.INFO)
        
        self._ensure_db_directory()
        # La inicialización de la BD se llamará explícitamente desde el scraper principal
        # para asegurar que el logger del scraper esté completamente configurado.

    def _ensure_db_directory(self):
        db_dir = os.path.dirname(self.db_file)
        # Añadir log para ver la ruta absoluta
        self.logger.info(f"Ruta absoluta del archivo de BD configurado: {os.path.abspath(self.db_file)}")
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir, exist_ok=True)
                self.logger.info(f"Directorio de base de datos creado: {db_dir}")
            except OSError as e:
                self.logger.error(f"Error creando directorio para la base de datos {db_dir}: {e}")
                raise

    def _connect(self):
        """Conecta a la base de datos SQLite y devuelve un objeto de conexión."""
        try:
            conn = sqlite3.connect(self.db_file, timeout=10) # Aumentar timeout si hay concurrencia
            conn.row_factory = sqlite3.Row
            # Habilitar WAL mode para mejor concurrencia (si es apropiado para el uso)
            # conn.execute("PRAGMA journal_mode=WAL") 
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"Error al conectar con la base de datos {self.db_file}: {e}")
            raise # Re-lanzar para que el llamador maneje el fallo de conexión

    def initialize_db(self):
        """Inicializa la base de datos creando las tablas si no existen."""
        conn = None # Asegurar que conn esté definida
        try:
            conn = self._connect()
            cursor = conn.cursor()
            self.logger.info(f"Inicializando/verificando esquema de BD en {self.db_file}...")

            # Tabla items:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS items (
                item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_page_url TEXT UNIQUE NOT NULL, -- URL canónica de la página del ítem
                oai_identifier TEXT,              -- Identificador OAI, puede ser NULO si no es de OAI
                repository_source TEXT,           -- Ej: 'alice', 'infoteca-e', 'web_search'
                discovery_mode TEXT,              -- Ej: 'oai', 'keyword_search'
                search_keyword TEXT,              -- Palabra clave si discovery_mode es 'keyword_search'
                processing_status TEXT DEFAULT 'pending_metadata', -- Estado actual del procesamiento del ítem
                metadata_json TEXT,               -- JSON con los metadatos extraídos
                html_local_path TEXT,             -- Ruta al snapshot HTML de la página del ítem
                last_processed_timestamp TEXT,    -- Cuándo se procesó/actualizó por última vez
                created_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """)
            self.logger.debug("Tabla 'items' verificada/creada.")

            # Índices para la tabla items
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_oai_repo ON items (oai_identifier, repository_source) WHERE oai_identifier IS NOT NULL AND repository_source IS NOT NULL")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_items_page_url ON items (item_page_url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_items_processing_status ON items (processing_status)")
            self.logger.debug("Índices para 'items' verificados/creados.")

            # Tabla files:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id INTEGER NOT NULL,
                file_type TEXT NOT NULL,          -- Ej: 'pdf', 'thumbnail', 'supplementary'
                remote_url TEXT UNIQUE NOT NULL,  -- URL original del archivo
                local_path TEXT,                  -- Ruta local si se descargó
                download_status TEXT DEFAULT 'pending', -- Ej: 'pending', 'downloaded', 'failed_download', 'skipped_exists'
                md5_hash TEXT,
                file_size_bytes INTEGER,
                download_timestamp TEXT,
                last_attempt_timestamp TEXT,
                FOREIGN KEY (item_id) REFERENCES items (item_id) ON DELETE CASCADE
            )
            """)
            self.logger.debug("Tabla 'files' verificada/creada.")

            # Índices para la tabla files
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_item_id ON files (item_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_remote_url ON files (remote_url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_status_type ON files (download_status, file_type)")
            self.logger.debug("Índices para 'files' verificados/creados.")

            conn.commit()
            self.logger.info(f"Base de datos inicializada/verificada exitosamente en {self.db_file}")
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite al inicializar la base de datos: {e}")
            if conn: conn.rollback()
        except Exception as e:
            self.logger.error(f"Error inesperado al inicializar la base de datos: {e}", exc_info=True)
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    # --- Métodos para Items --- 
    def get_or_create_item(self, item_page_url, repository_source=None, oai_identifier=None, discovery_mode=None, search_keyword=None, initial_status='pending_metadata'):
        """Obtiene un ítem por item_page_url. Si no existe, lo crea.
           Devuelve el item_id y su processing_status actual.
        """
        if not item_page_url: 
            self.logger.error("get_or_create_item llamado sin item_page_url")
            return None, None

        conn = self._connect()
        cursor = conn.cursor()
        item_id = None
        current_status = None
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')

        try:
            cursor.execute("SELECT item_id, processing_status FROM items WHERE item_page_url = ?", (item_page_url,))
            row = cursor.fetchone()
            if row:
                item_id = row['item_id']
                current_status = row['processing_status']
                # Actualizar last_processed_timestamp si ya existe, podría ser opcional
                # cursor.execute("UPDATE items SET last_processed_timestamp = ? WHERE item_id = ?", (now, item_id))
                self.logger.debug(f"Ítem existente encontrado ID {item_id} para URL: {item_page_url}")
            else:
                sql = """INSERT INTO items 
                           (item_page_url, oai_identifier, repository_source, discovery_mode, search_keyword, processing_status, last_processed_timestamp, created_timestamp)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
                cursor.execute(sql, (item_page_url, oai_identifier, repository_source, discovery_mode, search_keyword, initial_status, now, now))
                item_id = cursor.lastrowid
                current_status = initial_status
                self.logger.info(f"Nuevo ítem creado ID {item_id} para URL: {item_page_url}, Repo: {repository_source}")
            conn.commit()
        except sqlite3.IntegrityError as ie:
             # Esto podría pasar si hay una condición de carrera o un UNIQUE constraint falla inesperadamente
             self.logger.error(f"Error de integridad SQLite en get_or_create_item para URL {item_page_url}: {ie}")
             if conn: conn.rollback()
             # Intentar leerlo de nuevo por si se creó en otro hilo/proceso justo ahora
             cursor.execute("SELECT item_id, processing_status FROM items WHERE item_page_url = ?", (item_page_url,))
             row = cursor.fetchone()
             if row: item_id, current_status = row['item_id'], row['processing_status']
             else: raise # Si sigue sin encontrarlo, relanzar el error
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite en get_or_create_item para URL {item_page_url}: {e}")
            if conn: conn.rollback()
            raise
        finally:
            if conn: conn.close()
        return item_id, current_status

    def update_item_status(self, item_id, new_status):
        """Actualiza el processing_status y last_processed_timestamp de un ítem."""
        if item_id is None:
            self.logger.error("update_item_status llamado con item_id None.")
            return False
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
        conn = self._connect()
        cursor = conn.cursor()
        updated = False
        try:
            cursor.execute("UPDATE items SET processing_status = ?, last_processed_timestamp = ? WHERE item_id = ?", 
                           (new_status, now, item_id))
            conn.commit()
            if cursor.rowcount > 0:
                self.logger.info(f"Estado del ítem ID {item_id} actualizado a: {new_status}")
                updated = True
            else:
                self.logger.warning(f"No se encontró el ítem ID {item_id} para actualizar estado a {new_status}.")
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite actualizando estado para ítem ID {item_id}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()
        return updated

    def log_item_metadata(self, item_id, metadata_dict, html_path=None):
        """Almacena/actualiza los metadatos (como JSON) y la ruta del snapshot HTML de un ítem."""
        if item_id is None:
            self.logger.error("log_item_metadata llamado con item_id None.")
            return False
        
        updates = []
        params = []
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')

        if metadata_dict:
            try:
                self.logger.info(f"[DB_DEBUG] Item ID {item_id}: log_item_metadata - metadata_dict ANTES de dumps: {metadata_dict}") # DEBUG LOG
                metadata_str = json.dumps(metadata_dict)
                self.logger.info(f"[DB_DEBUG] Item ID {item_id}: log_item_metadata - metadata_str DESPUÉS de dumps: {metadata_str[:500]}...") # DEBUG LOG (truncado)
                updates.append("metadata_json = ?")
                params.append(metadata_str)
            except TypeError as te:
                self.logger.error(f"Error de serialización JSON para metadatos del ítem ID {item_id}: {te}")
        
        if html_path:
            updates.append("html_local_path = ?")
            params.append(html_path)

        if not updates:
            self.logger.debug(f"No hay metadatos ni ruta HTML para actualizar para item ID {item_id}.")
            return False

        updates.append("last_processed_timestamp = ?")
        params.append(now)
        params.append(item_id) 

        sql = f"UPDATE items SET {', '.join(updates)} WHERE item_id = ?"
        
        conn = self._connect()
        cursor = conn.cursor()
        updated = False
        try:
            self.logger.debug(f"[DB_DEBUG] Item ID {item_id}: Ejecutando SQL para log_item_metadata: {sql} con params (último es item_id): {params}") # DEBUG LOG
            cursor.execute(sql, tuple(params))
            conn.commit()
            if cursor.rowcount > 0:
                self.logger.info(f"Metadatos/HTML path actualizados para ítem ID {item_id}.")
                updated = True
            else:
                self.logger.warning(f"No se encontró el ítem ID {item_id} para actualizar metadatos/HTML path (rowcount 0).")
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite almacenando metadatos/HTML path para ítem ID {item_id}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()
        return updated

    def get_item_details(self, item_id):
        """Obtiene todos los detalles (columnas) de un ítem por su item_id."""
        if item_id is None: return None
        conn = self._connect()
        cursor = conn.cursor()
        item_data = None
        try:
            cursor.execute("SELECT * FROM items WHERE item_id = ?", (item_id,))
            row = cursor.fetchone()
            if row:
                item_data = dict(row)
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo detalles para item_id {item_id}: {e}")
        finally:
            if conn: conn.close()
        return item_data

    def get_items_to_process(self, statuses=None, discovery_modes=None, limit=None):
        """Obtiene ítems que necesitan procesamiento, filtrados por estado y/o modo de descubrimiento."""
        
        conditions = []
        params = []

        # Asegurar que seleccionamos todas las columnas para tener metadata_json
        sql = "SELECT * FROM items" 

        if statuses:
            # Asegurar que statuses sea una lista, incluso si se pasa un solo string
            if isinstance(statuses, str): 
                statuses = [statuses]
            if statuses: # Solo añadir condición si la lista no está vacía
                placeholders = ', '.join('?' * len(statuses))
                conditions.append(f"processing_status IN ({placeholders})")
                params.extend(statuses)
        
        if discovery_modes:
            if isinstance(discovery_modes, str): 
                discovery_modes = [discovery_modes]
            if discovery_modes: # Solo añadir condición si la lista no está vacía
                placeholders = ', '.join('?' * len(discovery_modes))
                conditions.append(f"discovery_mode IN ({placeholders})")
                params.extend(discovery_modes)
        
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        
        # Procesar más antiguos o menos recientes primero, para darles oportunidad si fallaron antes
        sql += " ORDER BY last_processed_timestamp ASC, created_timestamp ASC" 
        
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)

        conn = self._connect()
        cursor = conn.cursor()
        items_to_process = []
        try:
            # Loguear la query y los parámetros ANTES de ejecutarla
            self.logger.debug(f"Ejecutando get_items_to_process con query: {sql} y params: {params}")
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
            items_to_process = [dict(row) for row in rows]
            # Loguear el número de ítems encontrados DESPUÉS de la consulta
            self.logger.info(f"Encontrados {len(items_to_process)} ítems para procesar (status: {statuses if statuses else 'any'}, modes: {discovery_modes if discovery_modes else 'any'}, SQL limit: {limit if limit is not None else 'None'}).")
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo ítems para procesar: {e} (Query: {sql}, Params: {params})")
        except Exception as e:
            self.logger.error(f"Error inesperado obteniendo ítems para procesar: {e} (Query: {sql}, Params: {params})", exc_info=True)
        finally:
            if conn: conn.close()
        return items_to_process

    # --- Métodos para Files --- 
    def log_file_attempt(self, item_id, file_type, remote_url):
        """Registra un intento de descarga, actualizando el last_attempt_timestamp."""
        if item_id is None or not remote_url:
            self.logger.error("log_file_attempt llamado con item_id o remote_url None.")
            return
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
        conn = self._connect()
        cursor = conn.cursor()
        try:
            # Intentar actualizar. Si no existe, se insertará en el siguiente log_file_result.
            cursor.execute("UPDATE files SET last_attempt_timestamp = ? WHERE item_id = ? AND remote_url = ?",
                           (now, item_id, remote_url))
            # Si no se actualizó ninguna fila, es porque aún no existe. Esto está bien.
            conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite en log_file_attempt para item {item_id}, url {remote_url}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def log_file_result(self, item_id, file_type, remote_url, download_status, 
                          local_path=None, md5_hash=None, file_size_bytes=None):
        """Registra o actualiza el resultado de una descarga en la tabla 'files'."""
        if item_id is None or not remote_url:
            self.logger.error("log_file_result llamado con item_id o remote_url None.")
            return False

        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
        conn = self._connect()
        cursor = conn.cursor()
        success = False
        try:
            cursor.execute("SELECT file_id, download_timestamp, download_status FROM files WHERE item_id = ? AND remote_url = ?", (item_id, remote_url))
            row = cursor.fetchone()
            
            final_download_timestamp = now # Por defecto, si es un nuevo registro exitoso o un fallo
            
            if row: # Si el registro de archivo ya existe
                current_db_download_status = row['download_status']
                current_db_download_timestamp = row['download_timestamp']
                
                # Mantener el timestamp original si ya era un éxito y sigue siéndolo, o si es un fallo.
                if download_status in ['downloaded', 'skipped_exists']:
                    if current_db_download_status in ['downloaded', 'skipped_exists']:
                        final_download_timestamp = current_db_download_timestamp # Ya era éxito, no cambiar timestamp
                    # else: es un nuevo éxito, se usa 'now' (ya asignado a final_download_timestamp)
                else: # Es un fallo o estado intermedio
                    final_download_timestamp = current_db_download_timestamp # Mantener timestamp si ya había uno, sino será None para INSERT
                    if not final_download_timestamp: # Si era None, pero ahora es un fallo, no poner ts de descarga.
                         final_download_timestamp = None # Asegurar que no se guarde un ts de descarga para fallos si no había antes

                file_id = row['file_id']
                sql = """UPDATE files SET 
                           file_type = ?, local_path = ?, download_status = ?, md5_hash = ?, file_size_bytes = ?, 
                           download_timestamp = ?, last_attempt_timestamp = ?
                           WHERE file_id = ?"""
                cursor.execute(sql, (file_type, local_path, download_status, md5_hash, file_size_bytes, 
                                    final_download_timestamp, now, file_id))
                self.logger.info(f"Registro de archivo actualizado (ID={file_id}): item={item_id}, tipo={file_type}, status={download_status}")
            else: # Nuevo registro de archivo
                # Para nuevos registros, el timestamp de descarga solo se pone si es un éxito.
                ts_for_new_record = now if download_status in ['downloaded', 'skipped_exists'] else None
                sql = """INSERT INTO files 
                           (item_id, file_type, remote_url, local_path, download_status, md5_hash, file_size_bytes, download_timestamp, last_attempt_timestamp)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                cursor.execute(sql, (item_id, file_type, remote_url, local_path, download_status, md5_hash, file_size_bytes, 
                                    ts_for_new_record, 
                                    now))
                self.logger.info(f"Nuevo registro de archivo añadido: item={item_id}, tipo={file_type}, status={download_status}")
            conn.commit()
            success = True
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite en log_file_result para item {item_id}, url {remote_url}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()
        return success

    def get_file_status(self, item_id, remote_url):
        """Obtiene el estado de descarga y la ruta local de un archivo específico."""
        if item_id is None or not remote_url: return None, None
        conn = self._connect()
        cursor = conn.cursor()
        status, local_path = None, None
        try:
            cursor.execute("SELECT download_status, local_path FROM files WHERE item_id = ? AND remote_url = ?", (item_id, remote_url))
            row = cursor.fetchone()
            if row:
                status = row['download_status']
                local_path = row['local_path']
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo estado de archivo para item {item_id}, url {remote_url}: {e}")
        finally:
            if conn: conn.close()
        return status, local_path

    def get_downloaded_files_for_item(self, item_id, file_type='pdf'):
        """Obtiene una lista de archivos descargados (o existentes) para un ítem y tipo específicos."""
        if item_id is None: return []
        conn = self._connect()
        cursor = conn.cursor()
        downloaded_files = []
        try:
            sql = "SELECT local_path, remote_url, md5_hash, file_size_bytes FROM files WHERE item_id = ? AND file_type = ? AND (download_status = 'downloaded' OR download_status = 'skipped_exists')"
            cursor.execute(sql, (item_id, file_type))
            downloaded_files = [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo archivos descargados para item {item_id}, tipo {file_type}: {e}")
        finally:
            if conn: conn.close()
        return downloaded_files

    # --- Métodos para Generación de Reportes (Adaptados de Scraper AR) ---
    def get_all_items_for_report(self):
        """Obtiene todos los ítems con sus metadatos para reportes como state.json."""
        conn = self._connect()
        cursor = conn.cursor()
        items_data = []
        try:
            cursor.execute("SELECT item_id, item_page_url, processing_status, metadata_json, html_local_path FROM items ORDER BY item_id")
            raw_items = cursor.fetchall()
            for item_row in raw_items:
                item_dict = dict(item_row)
                if item_dict.get('metadata_json'):
                    try:
                        item_dict['metadata_json'] = json.loads(item_dict['metadata_json'])
                    except json.JSONDecodeError:
                        self.logger.warning(f"Error decodificando metadata_json para item_id {item_dict['item_id']} en reporte.")
                        item_dict['metadata_json'] = {}
                else:
                    item_dict['metadata_json'] = {}
                
                # Obtener PDFs asociados
                cursor.execute("SELECT remote_url, local_path, download_status FROM files WHERE item_id = ? AND file_type = 'pdf'", (item_dict['item_id'],))
                item_dict['pdfs'] = [dict(pdf_row) for pdf_row in cursor.fetchall()]
                items_data.append(item_dict)
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo ítems para reporte: {e}")
        finally:
            if conn: conn.close()
        return items_data

    def get_sample_downloaded_pdfs_for_report(self, max_results=5):
        """Obtiene una muestra de PDFs descargados para reportes como test_results.json."""
        conn = self._connect()
        cursor = conn.cursor()
        pdf_data = []
        try:
            sql_query = """ 
                SELECT f.item_id, f.local_path, f.md5_hash, f.file_size_bytes, i.item_page_url, i.metadata_json
                FROM files f
                JOIN items i ON f.item_id = i.item_id
                WHERE f.file_type = 'pdf' AND (f.download_status = 'downloaded' OR f.download_status = 'skipped_exists')
                ORDER BY f.download_timestamp DESC
                LIMIT ?
            """
            raw_pdfs = cursor.execute(sql_query, (max_results,)).fetchall()
            for pdf_row in raw_pdfs:
                pdf_dict = dict(pdf_row)
                if pdf_dict.get('metadata_json'):
                    try:
                        pdf_dict['metadata_json'] = json.loads(pdf_dict['metadata_json'])
                    except json.JSONDecodeError:
                        self.logger.warning(f"Error decodificando metadata_json para item_id {pdf_dict['item_id']} en reporte PDF.")
                        pdf_dict['metadata_json'] = {}
                else:
                    pdf_dict['metadata_json'] = {}
                pdf_data.append(pdf_dict)
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo PDFs para reporte: {e}")
        finally:
            if conn: conn.close()
        return pdf_data

    def check_item_exists(self, oai_identifier):
        """Verifica si un ítem ya existe en la base de datos usando su OAI identifier."""
        query = "SELECT 1 FROM items WHERE oai_identifier = ? LIMIT 1"
        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (oai_identifier,))
                result = cursor.fetchone()
                return result is not None
        except sqlite3.Error as e:
            self.logger.error(f"Error al verificar existencia del ítem {oai_identifier}: {e}")
            return False # Asumir que no existe en caso de error para intentar procesarlo

    def register_item(self, item_page_url, repository_source=None, oai_identifier=None, discovery_mode=None, search_keyword=None, initial_status='pending_metadata'):
        """Registra un nuevo ítem si no existe por item_page_url. Devuelve el ID del ítem (nuevo o existente)."""
        
        # Reutilizar la lógica de get_or_create_item para evitar duplicación y manejar concurrencia
        item_id, _ = self.get_or_create_item(
            item_page_url=item_page_url,
            repository_source=repository_source,
            oai_identifier=oai_identifier,
            discovery_mode=discovery_mode,
            search_keyword=search_keyword,
            initial_status=initial_status # Asegurarse que el status deseado se pasa aquí
        )
        return item_id

# Ejemplo de uso básico (para pruebas si se ejecuta este archivo directamente)
if __name__ == '__main__':
    print("Ejecutando pruebas básicas de DatabaseManagerBR...")
    # Configurar un logger de prueba
    test_logger = logging.getLogger("DBManagerTest")
    test_logger.setLevel(logging.DEBUG)
    test_handler = logging.StreamHandler()
    test_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    test_handler.setFormatter(test_formatter)
    if not test_logger.handlers: test_logger.addHandler(test_handler)

    # Crear una BD de prueba en memoria o un archivo temporal
    test_db_file = "BR/db/test_scraper_br.db"
    if os.path.exists(test_db_file): 
        os.remove(test_db_file) # Limpiar para cada prueba
    
    db_manager = DatabaseManagerBR(test_db_file, logger_instance=test_logger)
    db_manager.initialize_db() # Inicializarla aquí para prueba

    test_logger.info("--- Probando get_or_create_item ---")
    item_id1, status1 = db_manager.get_or_create_item("http://example.com/item/1", repository_source="alice", oai_identifier="oai:alice:1")
    test_logger.info(f"Item 1: ID={item_id1}, Status={status1}")
    item_id1_retry, status1_retry = db_manager.get_or_create_item("http://example.com/item/1") # Intentar obtenerlo de nuevo
    test_logger.info(f"Item 1 Retry: ID={item_id1_retry}, Status={status1_retry}")
    assert item_id1 == item_id1_retry

    item_id2, status2 = db_manager.get_or_create_item("http://example.com/item/2", repository_source="infoteca-e", discovery_mode="oai")
    test_logger.info(f"Item 2: ID={item_id2}, Status={status2}")

    test_logger.info("--- Probando update_item_status ---")
    db_manager.update_item_status(item_id1, "processing")
    details1 = db_manager.get_item_details(item_id1)
    test_logger.info(f"Item 1 Details after status update: {details1}")
    assert details1['processing_status'] == "processing"

    test_logger.info("--- Probando log_item_metadata ---")
    meta1 = {"title": "Título del Item 1", "authors": ["Autor A", "Autor B"], "year": 2023}
    db_manager.log_item_metadata(item_id1, meta1, html_path="BR/output/html_snapshot/1/item1.html")
    details1_meta = db_manager.get_item_details(item_id1)
    test_logger.info(f"Item 1 Details after metadata: {details1_meta}")
    assert json.loads(details1_meta['metadata_json'])['title'] == "Título del Item 1"
    assert details1_meta['html_local_path'] == "BR/output/html_snapshot/1/item1.html"

    test_logger.info("--- Probando log_file_result (nuevo archivo) ---")
    pdf_url1 = "http://example.com/item/1/file.pdf"
    db_manager.log_file_result(item_id1, "pdf", pdf_url1, "downloaded", "BR/output/pdfs/1/file.pdf", "md5hash123", 1024)
    status_pdf1, path_pdf1 = db_manager.get_file_status(item_id1, pdf_url1)
    test_logger.info(f"PDF 1 para Item 1: Status={status_pdf1}, Path={path_pdf1}")
    assert status_pdf1 == "downloaded"

    test_logger.info("--- Probando log_file_result (actualizar archivo existente) ---")
    db_manager.log_file_result(item_id1, "pdf", pdf_url1, "failed_download") # Simular fallo
    status_pdf1_failed, _ = db_manager.get_file_status(item_id1, pdf_url1)
    test_logger.info(f"PDF 1 para Item 1 (después de fallo simulado): Status={status_pdf1_failed}")
    assert status_pdf1_failed == "failed_download"
    # Ahora simular que se descargó bien de nuevo
    db_manager.log_file_result(item_id1, "pdf", pdf_url1, "downloaded", "BR/output/pdfs/1/file.pdf", "md5hash456", 2048)
    updated_file_info = db_manager.get_downloaded_files_for_item(item_id1)
    test_logger.info(f"PDF 1 para Item 1 (después de re-descarga): {updated_file_info}")
    assert updated_file_info[0]['md5_hash'] == "md5hash456"

    test_logger.info("--- Probando get_items_to_process ---")
    db_manager.update_item_status(item_id2, "pending_download")
    items_pending = db_manager.get_items_to_process(statuses=['pending_download', 'pending_metadata'])
    test_logger.info(f"Items pendientes: {items_pending}")
    assert len(items_pending) >= 1 # Puede ser 1 o 2 si item1 volvió a pendiente

    test_logger.info("--- Probando reportes (sin validación de contenido, solo ejecución) ---")
    db_manager.get_all_items_for_report()
    db_manager.get_sample_downloaded_pdfs_for_report()
    test_logger.info("Funciones de reporte ejecutadas.")

    test_logger.info("Pruebas de DatabaseManagerBR finalizadas.")
    # os.remove(test_db_file) # Comentar para inspeccionar la BD de prueba 
