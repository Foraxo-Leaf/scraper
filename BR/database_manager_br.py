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
                metadata_json_str = json.dumps(metadata_dict, ensure_ascii=False)
                updates.append("metadata_json = ?")
                params.append(metadata_json_str)
            except TypeError as te:
                self.logger.error(f"Error serializando metadatos a JSON para item ID {item_id}: {te}")
                # No añadir metadata_json si falla la serialización
        
        if html_path:
            updates.append("html_local_path = ?")
            params.append(html_path)
        
        if not updates: # No hay nada que actualizar
            self.logger.debug(f"log_item_metadata llamado para item ID {item_id} pero sin metadatos ni html_path para actualizar.")
            return False

        updates.append("last_processed_timestamp = ?")
        params.append(now)
        params.append(item_id) # Para el WHERE clause

        conn = self._connect()
        cursor = conn.cursor()
        updated_meta = False
        try:
            sql = f"UPDATE items SET { ', '.join(updates) } WHERE item_id = ?"
            cursor.execute(sql, tuple(params))
            conn.commit()
            if cursor.rowcount > 0:
                self.logger.info(f"Metadatos y/o HTML path para ítem ID {item_id} actualizados/guardados.")
                updated_meta = True
            else:
                self.logger.warning(f"No se encontró el ítem ID {item_id} para actualizar metadatos/HTML path.")
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite actualizando metadatos/HTML path para ítem ID {item_id}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()
        return updated_meta

    def get_item_details(self, item_id):
        """Obtiene todos los detalles de un ítem por su ID."""
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM items WHERE item_id = ?", (item_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
            else:
                self.logger.warning(f"No se encontró ítem con ID {item_id}")
                return None
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo detalles del ítem ID {item_id}: {e}")
            return None
        finally:
            if conn: conn.close()

    def get_items_to_process(self, statuses=None, discovery_modes=None, limit=None):
        """Obtiene ítems que necesitan procesamiento, opcionalmente filtrados por estado(s) y modo(s) de descubrimiento."""
        conn = self._connect()
        cursor = conn.cursor()
        items = []
        
        base_sql = "SELECT item_id, item_page_url, oai_identifier, repository_source, discovery_mode, metadata_json, processing_status FROM items"
        conditions = []
        params = []

        if statuses:
            if isinstance(statuses, str):
                statuses = [statuses]
            placeholders = ', '.join('?' * len(statuses))
            conditions.append(f"processing_status IN ({placeholders})")
            params.extend(statuses)
        
        if discovery_modes:
            if isinstance(discovery_modes, str):
                discovery_modes = [discovery_modes]
            placeholders = ', '.join('?' * len(discovery_modes))
            conditions.append(f"discovery_mode IN ({placeholders})")
            params.extend(discovery_modes)

        if conditions:
            base_sql += " WHERE " + " AND ".join(conditions)
        
        base_sql += " ORDER BY last_processed_timestamp ASC, created_timestamp ASC" # Priorizar más antiguos / menos procesados

        if limit is not None and isinstance(limit, int) and limit > 0:
            base_sql += " LIMIT ?"
            params.append(limit)

        try:
            cursor.execute(base_sql, tuple(params))
            for row in cursor.fetchall():
                item_dict = dict(row)
                # Parsear metadata_json si existe
                # if item_dict.get('metadata_json'):
                #     try:
                #         item_dict['metadata_json'] = json.loads(item_dict['metadata_json'])
                #     except json.JSONDecodeError:
                #         self.logger.warning(f"Error decodificando metadata_json para item_id {item_dict['item_id']}. Se devolverá como string.")
                items.append(item_dict)
            self.logger.info(f"Se encontraron {len(items)} ítems para procesar. Filtros: statuses={statuses}, modes={discovery_modes}, limit={limit}")
            return items
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo ítems para procesar: {e}. SQL: {base_sql}, Params: {params}")
            return []
        finally:
            if conn: conn.close()

    # --- Métodos para Files ---
    def log_file_attempt(self, item_id, file_type, remote_url):
        """Registra un intento de descarga de archivo. Crea o actualiza la entrada del archivo."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT file_id FROM files WHERE item_id = ? AND remote_url = ?", (item_id, remote_url))
            row = cursor.fetchone()
            if row: # Actualizar intento
                # cursor.execute("UPDATE files SET last_attempt_timestamp = ? WHERE file_id = ?", (now, row['file_id']))
                # La lógica de actualizar el intento está implícita en log_file_result, no es necesario aquí.
                pass
            else: # Crear nueva entrada de archivo
                cursor.execute("""INSERT INTO files (item_id, file_type, remote_url, download_status, last_attempt_timestamp)
                                  VALUES (?, ?, ?, ?, ?)""", 
                               (item_id, file_type, remote_url, 'pending', now))
            conn.commit()
            # self.logger.debug(f"Intento de descarga para item {item_id}, URL {remote_url} registrado/actualizado.")
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite registrando intento de descarga para item {item_id}, URL {remote_url}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def log_file_result(self, item_id, file_type, remote_url, download_status, 
                          local_path=None, md5_hash=None, file_size_bytes=None):
        """Registra el resultado de una descarga de archivo (éxito o fallo)."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
        conn = self._connect()
        cursor = conn.cursor()
        logged = False
        try:
            # Verificar si el archivo ya existe para este item_id y remote_url
            cursor.execute("SELECT file_id FROM files WHERE item_id = ? AND remote_url = ?", (item_id, remote_url))
            row = cursor.fetchone()

            if row: # Actualizar el registro existente
                file_id = row['file_id']
                updates = []
                params_update = []
                if local_path is not None: updates.append("local_path = ?"); params_update.append(local_path)
                if md5_hash is not None: updates.append("md5_hash = ?"); params_update.append(md5_hash)
                if file_size_bytes is not None: updates.append("file_size_bytes = ?"); params_update.append(file_size_bytes)
                updates.append("download_status = ?"); params_update.append(download_status)
                updates.append("last_attempt_timestamp = ?"); params_update.append(now)
                if download_status == 'downloaded':
                     updates.append("download_timestamp = ?"); params_update.append(now)
                
                params_update.append(file_id) # Para el WHERE
                
                sql_update = f"UPDATE files SET { ', '.join(updates) } WHERE file_id = ?"
                cursor.execute(sql_update, tuple(params_update))
                self.logger.info(f"Resultado de descarga para archivo existente ID {file_id} (Item {item_id}, URL {remote_url}) actualizado a: {download_status}")
            else: # Crear un nuevo registro de archivo
                dt = now if download_status == 'downloaded' else None # Download timestamp solo si fue exitoso
                cursor.execute("""INSERT INTO files 
                                  (item_id, file_type, remote_url, local_path, download_status, md5_hash, file_size_bytes, download_timestamp, last_attempt_timestamp)
                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                               (item_id, file_type, remote_url, local_path, download_status, md5_hash, file_size_bytes, dt, now))
                self.logger.info(f"Nuevo archivo registrado para item {item_id} (URL {remote_url}). Estado: {download_status}, Path: {local_path}")
            
            conn.commit()
            logged = True
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite registrando resultado de descarga para item {item_id}, URL {remote_url}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()
        return logged

    def get_file_status(self, item_id, remote_url):
        """Obtiene el download_status y local_path de un archivo por item_id y remote_url."""
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT download_status, local_path FROM files WHERE item_id = ? AND remote_url = ?", (item_id, remote_url))
            row = cursor.fetchone()
            if row:
                return row['download_status'], row['local_path']
            return None, None # No encontrado
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo estado de archivo para item {item_id}, URL {remote_url}: {e}")
            return None, None
        finally:
            if conn: conn.close()

    def get_downloaded_files_for_item(self, item_id, file_type='pdf'):
        """Obtiene una lista de archivos descargados (con su ruta local) para un ítem específico y tipo de archivo."""
        conn = self._connect()
        cursor = conn.cursor()
        files = []
        try:
            cursor.execute("SELECT local_path, remote_url, md5_hash, file_size_bytes FROM files WHERE item_id = ? AND file_type = ? AND download_status IN ('downloaded', 'skipped_exists')", 
                           (item_id, file_type))
            for row in cursor.fetchall():
                files.append(dict(row))
            return files
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo archivos descargados para ítem {item_id}: {e}")
            return []
        finally:
            if conn: conn.close()

    def get_all_items_for_report(self):
        """Prepara datos para reportes como state.json. Devuelve una lista de dicts con info de ítems y sus PDFs."""
        conn = self._connect()
        cursor = conn.cursor()
        items_report_data = []
        try:
            # Obtener todos los ítems primero
            cursor.execute("SELECT item_id, item_page_url, oai_identifier, repository_source, processing_status, metadata_json, html_local_path FROM items ORDER BY item_id")
            all_items_rows = cursor.fetchall()

            for item_row_raw in all_items_rows:
                item_data = dict(item_row_raw)
                item_id = item_data['item_id']
                
                # Parsear metadata_json si existe
                if item_data.get('metadata_json'):
                    try:
                        item_data['metadata_json'] = json.loads(item_data['metadata_json'])
                    except json.JSONDecodeError:
                        self.logger.warning(f"Error decodificando metadata_json para item_id {item_id} en get_all_items_for_report. Se dejará como string.")
                else:
                    item_data['metadata_json'] = {} # Asegurar que sea un dict si es None

                # Obtener archivos PDF para este ítem
                cursor.execute("SELECT remote_url, local_path, download_status FROM files WHERE item_id = ? AND file_type = 'pdf'", (item_id,))
                pdf_files_for_item = [dict(pdf_row) for pdf_row in cursor.fetchall()]
                item_data['pdfs'] = pdf_files_for_item
                
                items_report_data.append(item_data)
                
            self.logger.info(f"Se prepararon datos de reporte para {len(items_report_data)} ítems.")
            return items_report_data
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite preparando datos de reporte para todos los ítems: {e}")
            return []
        finally:
            if conn: conn.close()

    def get_sample_downloaded_pdfs_for_report(self, max_results=5):
        """Obtiene una muestra de PDFs descargados con sus metadatos de ítem para el test_results.json."""
        conn = self._connect()
        cursor = conn.cursor()
        pdf_report_data = []
        try:
            # Seleccionar archivos PDF descargados y unir con la tabla de ítems para obtener metadatos del ítem
            # Usamos una subconsulta para seleccionar file_ids aleatorios o los primeros N para la muestra.
            # SQLite puede ser peculiar con RAND(), así que LIMIT puede ser más predecible.
            sql = """
            SELECT 
                f.remote_url, f.local_path, f.md5_hash, f.file_size_bytes,
                i.item_id, i.item_page_url, i.metadata_json 
            FROM files f
            JOIN items i ON f.item_id = i.item_id
            WHERE f.file_type = 'pdf' AND f.download_status IN ('downloaded', 'skipped_exists')
            ORDER BY f.file_id -- Opcional: o RANDOM()
            LIMIT ?
            """
            cursor.execute(sql, (max_results,))
            for row_raw in cursor.fetchall():
                pdf_entry = dict(row_raw)
                # Parsear metadata_json del ítem asociado
                if pdf_entry.get('metadata_json'):
                    try:
                        pdf_entry['metadata_json'] = json.loads(pdf_entry['metadata_json'])
                    except json.JSONDecodeError:
                        self.logger.warning(f"Error decodificando metadata_json del ítem {pdf_entry['item_id']} para el reporte de PDF muestreado. Se dejará como string.")
                else:
                    pdf_entry['metadata_json'] = {} # Asegurar dict si es None
                pdf_report_data.append(pdf_entry)
            
            self.logger.info(f"Se obtuvo una muestra de {len(pdf_report_data)} PDFs descargados para reporte.")
            return pdf_report_data
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo muestra de PDFs descargados: {e}")
            return []
        finally:
            if conn: conn.close()

    # --- Funciones de utilidad / chequeo (ejemplos, pueden no ser todas necesarias) ---
    def check_item_exists(self, oai_identifier):
        """Verifica si un ítem existe basado en su identificador OAI."""
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT item_id FROM items WHERE oai_identifier = ?", (oai_identifier,))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            self.logger.error(f"Error verificando existencia del ítem OAI {oai_identifier}: {e}")
            return False # Asumir que no existe si hay error
        finally:
            if conn: conn.close()

    # Este es un método de conveniencia que combina get_or_create e update_status
    # Puede ser útil en OAIHarvester
    def register_item(self, item_page_url, repository_source=None, oai_identifier=None, discovery_mode=None, search_keyword=None, initial_status='pending_metadata'):
        """Simplifica el registro o actualización de un ítem, devolviendo su ID y si fue recién creado."""
        item_id, current_status = self.get_or_create_item(
            item_page_url=item_page_url, 
            repository_source=repository_source, 
            oai_identifier=oai_identifier,
            discovery_mode=discovery_mode,
            search_keyword=search_keyword,
            initial_status=initial_status
        )
        created = (current_status == initial_status) # Asumimos que si el estado es el inicial, fue creado ahora
                                                 # Esto es una simplificación, get_or_create_item podría ser más preciso
        return item_id, created

# No incluir el bloque if __name__ == '__main__' aquí, ya que este archivo es un módulo
# Las pruebas unitarias deben estar en un archivo de prueba separado (ej. test_database_manager_br.py)
# o ejecutarse condicionalmente si este archivo se ejecuta como script principal (pero es menos ideal para módulos).
