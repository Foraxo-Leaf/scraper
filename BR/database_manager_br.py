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
            metadata_json_str = json.dumps(metadata_dict, ensure_ascii=False)
            updates.append("metadata_json = ?")
            params.append(metadata_json_str)

        if html_path:
            updates.append("html_local_path = ?")
            params.append(html_path)
        
        if not updates: # No hay nada que actualizar
            self.logger.debug(f"No hay metadatos ni ruta HTML para loguear para el ítem ID {item_id}")
            return False

        updates.append("last_processed_timestamp = ?")
        params.append(now)
        params.append(item_id) # Para la cláusula WHERE

        sql = f"UPDATE items SET { ', '.join(updates) } WHERE item_id = ?"
        
        conn = self._connect()
        cursor = conn.cursor()
        updated_row = False
        try:
            cursor.execute(sql, tuple(params))
            conn.commit()
            if cursor.rowcount > 0:
                self.logger.info(f"Metadatos y/o HTML path logueados para ítem ID {item_id}.")
                updated_row = True
            else:
                self.logger.warning(f"No se encontró el ítem ID {item_id} para loguear metadatos/HTML path.")
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite logueando metadatos/HTML path para ítem ID {item_id}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()
        return updated_row

    def get_item_details(self, item_id):
        """Obtiene todos los detalles de un ítem específico de la tabla items."""
        if item_id is None:
            self.logger.error("get_item_details llamado con item_id None.")
            return None
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM items WHERE item_id = ?", (item_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo detalles para ítem ID {item_id}: {e}")
            return None
        finally:
            if conn: conn.close()

    def get_items_to_process(self, statuses=None, discovery_modes=None, limit=None):
        """Obtiene ítems basados en su estado de procesamiento y/o modo de descubrimiento.
           Devuelve una lista de diccionarios, cada uno representando un ítem.
        """
        conn = self._connect()
        cursor = conn.cursor()
        items = []
        try:
            base_sql = "SELECT * FROM items"
            conditions = []
            params = []

            if statuses and isinstance(statuses, list):
                # Crear placeholders para la cláusula IN
                status_placeholders = ', '.join('?' for _ in statuses)
                conditions.append(f"processing_status IN ({status_placeholders})")
                params.extend(statuses)
            elif statuses and isinstance(statuses, str):
                conditions.append("processing_status = ?")
                params.append(statuses)
            
            if discovery_modes and isinstance(discovery_modes, list):
                discovery_placeholders = ', '.join('?' for _ in discovery_modes)
                conditions.append(f"discovery_mode IN ({discovery_placeholders})")
                params.extend(discovery_modes)
            elif discovery_modes and isinstance(discovery_modes, str):
                conditions.append("discovery_mode = ?")
                params.append(discovery_modes)

            if conditions:
                base_sql += " WHERE " + " AND ".join(conditions)
            
            base_sql += " ORDER BY last_processed_timestamp ASC, created_timestamp ASC" # Priorizar más antiguos o menos procesados

            if limit is not None and isinstance(limit, int) and limit > 0:
                base_sql += " LIMIT ?"
                params.append(limit)
            
            cursor.execute(base_sql, tuple(params))
            rows = cursor.fetchall()
            for row in rows:
                items.append(dict(row))
            
            self.logger.info(f"Encontrados {len(items)} ítems para procesar. Estados: {statuses}, Modos: {discovery_modes}, Límite: {limit}")
            return items
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo ítems para procesar: {e}")
            return []
        finally:
            if conn: conn.close()

    # --- Métodos para Files ---
    def log_file_attempt(self, item_id, file_type, remote_url):
        """Registra un intento de descarga de archivo. Crea la entrada si no existe."""
        if item_id is None or not remote_url:
            self.logger.error("log_file_attempt llamado con item_id o remote_url None.")
            return None # O False

        conn = self._connect()
        cursor = conn.cursor()
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
        file_id = None
        try:
            # Verificar si ya existe este archivo para este ítem
            cursor.execute("SELECT file_id FROM files WHERE item_id = ? AND remote_url = ?", (item_id, remote_url))
            row = cursor.fetchone()
            if row:
                file_id = row['file_id']
                # Actualizar solo el timestamp del último intento
                cursor.execute("UPDATE files SET last_attempt_timestamp = ? WHERE file_id = ?", (now, file_id))
                self.logger.debug(f"Actualizado timestamp de intento para archivo existente ID {file_id}, URL: {remote_url}")
            else:
                # Crear nueva entrada de archivo
                sql_insert = """INSERT INTO files 
                                  (item_id, file_type, remote_url, download_status, last_attempt_timestamp, download_timestamp)
                                  VALUES (?, ?, ?, 'pending', ?, NULL)""" # download_timestamp es NULL hasta descarga exitosa
                cursor.execute(sql_insert, (item_id, file_type, remote_url, now))
                file_id = cursor.lastrowid
                self.logger.info(f"Nueva entrada de archivo creada ID {file_id} para ítem {item_id}, URL: {remote_url}")
            conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite en log_file_attempt para ítem {item_id}, URL {remote_url}: {e}")
            if conn: conn.rollback()
            file_id = None # Asegurar que no se devuelva un file_id inválido
        finally:
            if conn: conn.close()
        return file_id

    def log_file_result(self, item_id, file_type, remote_url, download_status, 
                          local_path=None, md5_hash=None, file_size_bytes=None):
        """Registra el resultado de una descarga de archivo. 
           Crea la entrada si no existe, o la actualiza si existe.
        """
        if item_id is None or not remote_url:
            self.logger.error("log_file_result llamado con item_id o remote_url None.")
            return False
        
        conn = self._connect()
        cursor = conn.cursor()
        now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')
        updated_rows = 0
        try:
            # Buscar si el archivo ya existe
            cursor.execute("SELECT file_id FROM files WHERE item_id = ? AND remote_url = ?", (item_id, remote_url))
            row = cursor.fetchone()
            file_id = row['file_id'] if row else None

            if file_id: # Actualizar existente
                sql_update = """UPDATE files SET 
                                   file_type = COALESCE(?, file_type), 
                                   local_path = ?, 
                                   download_status = ?, 
                                   md5_hash = ?, 
                                   file_size_bytes = ?, 
                                   last_attempt_timestamp = ?, 
                                   download_timestamp = CASE WHEN ? = 'downloaded' THEN ? ELSE download_timestamp END
                               WHERE file_id = ?"""
                params_update = (file_type, local_path, download_status, md5_hash, file_size_bytes, now, 
                                 download_status, now, # Para actualizar download_timestamp solo si es 'downloaded'
                                 file_id)
                cursor.execute(sql_update, params_update)
                updated_rows = cursor.rowcount
                if updated_rows > 0:
                    self.logger.info(f"Resultado de archivo ID {file_id} actualizado. Estado: {download_status}, Path: {local_path}")
                else:
                    self.logger.warning(f"No se actualizó el archivo ID {file_id} (quizás no hubo cambios o no se encontró). URL: {remote_url}")
            else: # Crear nuevo
                sql_insert = """INSERT INTO files 
                                  (item_id, file_type, remote_url, local_path, download_status, 
                                   md5_hash, file_size_bytes, last_attempt_timestamp, download_timestamp)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                download_ts_val = now if download_status == 'downloaded' else None
                params_insert = (item_id, file_type, remote_url, local_path, download_status, 
                                 md5_hash, file_size_bytes, now, download_ts_val)
                cursor.execute(sql_insert, params_insert)
                new_file_id = cursor.lastrowid
                updated_rows = 1 # Se insertó una fila
                self.logger.info(f"Nuevo resultado de archivo ID {new_file_id} registrado. Ítem: {item_id}, URL: {remote_url}, Estado: {download_status}")
            
            conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite en log_file_result para ítem {item_id}, URL {remote_url}: {e}")
            if conn: conn.rollback()
            updated_rows = 0 # Indicar fallo
        finally:
            if conn: conn.close()
        return updated_rows > 0

    def get_file_status(self, item_id, remote_url):
        """Obtiene el estado de descarga y la ruta local de un archivo específico."""
        if item_id is None or not remote_url:
            self.logger.warning("get_file_status llamado con item_id o remote_url None.")
            return None, None
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT download_status, local_path FROM files WHERE item_id = ? AND remote_url = ?", 
                           (item_id, remote_url))
            row = cursor.fetchone()
            if row:
                return row['download_status'], row['local_path']
            else:
                return None, None # No encontrado
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo estado de archivo para ítem {item_id}, URL {remote_url}: {e}")
            return None, None
        finally:
            if conn: conn.close()

    def get_downloaded_files_for_item(self, item_id, file_type='pdf'):
        """Obtiene una lista de archivos descargados (con su ruta local) para un ítem y tipo de archivo."""
        if item_id is None:
            self.logger.warning("get_downloaded_files_for_item llamado con item_id None.")
            return []
        conn = self._connect()
        cursor = conn.cursor()
        files = []
        try:
            cursor.execute("SELECT local_path, remote_url, md5_hash, file_size_bytes FROM files "
                           "WHERE item_id = ? AND file_type = ? AND download_status IN ('downloaded', 'skipped_exists')", 
                           (item_id, file_type))
            rows = cursor.fetchall()
            for row in rows:
                files.append(dict(row))
            return files
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo archivos descargados para ítem {item_id}, tipo {file_type}: {e}")
            return []
        finally:
            if conn: conn.close()

    def get_all_items_for_report(self):
        """Obtiene todos los ítems con sus metadatos y archivos asociados para un reporte.
           Devuelve una lista de diccionarios, uno por ítem.
        """
        conn = self._connect()
        cursor = conn.cursor()
        items_report = []
        try:
            # Obtener todos los ítems primero
            cursor.execute("SELECT * FROM items ORDER BY item_id")
            item_rows = cursor.fetchall()

            for item_row_raw in item_rows:
                item_dict = dict(item_row_raw)
                item_id = item_dict['item_id']
                
                # Parsear metadata_json si existe
                if item_dict.get('metadata_json'):
                    try:
                        item_dict['metadata_json'] = json.loads(item_dict['metadata_json'])
                    except json.JSONDecodeError:
                        self.logger.warning(f"No se pudo parsear metadata_json para item ID {item_id}. Se devolverá como string.")
                else:
                    item_dict['metadata_json'] = {} # Default a dict vacío

                # Obtener archivos asociados al ítem
                cursor.execute("SELECT * FROM files WHERE item_id = ? ORDER BY file_id", (item_id,))
                file_rows = cursor.fetchall()
                item_dict['pdfs'] = [dict(f_row) for f_row in file_rows if f_row['file_type'] == 'pdf']
                item_dict['other_files'] = [dict(f_row) for f_row in file_rows if f_row['file_type'] != 'pdf']
                
                items_report.append(item_dict)
            
            self.logger.info(f"Reporte generado para {len(items_report)} ítems.")
            return items_report
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite generando reporte de todos los ítems: {e}")
            return []
        finally:
            if conn: conn.close()
            
    def get_sample_downloaded_pdfs_for_report(self, max_results=5):
        """Obtiene una muestra de PDFs que han sido descargados exitosamente, 
           junto con los metadatos del ítem al que pertenecen.
           Útil para generar test_results.json y copiar muestras.
        """
        conn = self._connect()
        cursor = conn.cursor()
        sample_data = []
        try:
            # Seleccionar PDFs descargados y unir con la tabla de ítems para obtener metadatos
            sql = """
            SELECT 
                i.item_id, i.item_page_url, i.oai_identifier, i.metadata_json,
                f.file_id, f.remote_url, f.local_path, f.md5_hash, f.file_size_bytes
            FROM files f
            JOIN items i ON f.item_id = i.item_id
            WHERE f.file_type = 'pdf' AND f.download_status IN ('downloaded', 'skipped_exists')
            ORDER BY RANDOM() -- Para obtener una muestra aleatoria
            LIMIT ?
            """
            cursor.execute(sql, (max_results,))
            rows = cursor.fetchall()
            for row_raw in rows:
                row_dict = dict(row_raw)
                # Parsear metadata_json
                if row_dict.get('metadata_json'):
                    try:
                        row_dict['metadata_json'] = json.loads(row_dict['metadata_json'])
                    except json.JSONDecodeError:
                        self.logger.warning(f"No se pudo parsear metadata_json para item ID {row_dict['item_id']} en muestra de PDFs. Se usará dict vacío.")
                        row_dict['metadata_json'] = {}
                else:
                    row_dict['metadata_json'] = {}
                sample_data.append(row_dict)
            
            self.logger.info(f"Muestra de {len(sample_data)} PDFs descargados obtenida para reporte.")
            return sample_data
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite obteniendo muestra de PDFs descargados: {e}")
            return []
        finally:
            if conn: conn.close()

    def check_item_exists(self, oai_identifier):
        """Verifica si un ítem con un identificador OAI específico ya existe."""
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT item_id FROM items WHERE oai_identifier = ?", (oai_identifier,))
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite verificando si existe ítem OAI {oai_identifier}: {e}")
            return False # Asumir que no existe en caso de error
        finally:
            if conn: conn.close()

    def register_item(self, item_page_url, repository_source=None, oai_identifier=None, discovery_mode=None, search_keyword=None, initial_status='pending_metadata'):
        """Función de conveniencia para simplificar el registro de un nuevo ítem si no existe."""
        # Esta función es esencialmente un alias más simple para get_or_create_item si solo se quiere registrar
        # y no necesariamente preocuparse por si fue creado o ya existía, solo obtener su ID.
        item_id, _ = self.get_or_create_item(
            item_page_url=item_page_url,
            repository_source=repository_source,
            oai_identifier=oai_identifier,
            discovery_mode=discovery_mode,
            search_keyword=search_keyword,
            initial_status=initial_status
        )
        return item_id
    
    def update_item_metadata_if_newer(self, item_id, new_metadata_dict, source_timestamp_str=None):
        """Actualiza los metadatos de un ítem solo si los nuevos datos son más recientes
           o si no hay metadatos previos. Usa 'last_processed_timestamp' del ítem para comparar.
        """
        # Esta función necesitaría una lógica más robusta para comparar timestamps
        # y decidir si actualizar. Por ahora, es un placeholder conceptual.
        # Si source_timestamp_str es provisto (ej. de OAI datestamp), se podría usar eso.
        # O, simplemente, si los metadatos nuevos son diferentes, se actualizan.
        
        # Simplificación: por ahora, simplemente llama a log_item_metadata que sobrescribe.
        # Una implementación real compararía timestamps o el contenido del JSON.
        self.logger.debug(f"Actualización condicional de metadatos (actualmente simple) para item ID {item_id}")
        return self.log_item_metadata(item_id, new_metadata_dict)

    def count_items_by_status(self, status):
        """Cuenta cuántos ítems tienen un estado de procesamiento específico."""
        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(item_id) FROM items WHERE processing_status = ?", (status,))
            count = cursor.fetchone()[0]
            return count
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite contando ítems por estado {status}: {e}")
            return 0
        finally:
            if conn: conn.close()
            
    def delete_item_and_associated_data(self, item_id):
        """Elimina un ítem y todos sus datos asociados (metadatos, archivos) de la BD."""
        # Esta es una operación destructiva. Usar con cuidado.
        # ON DELETE CASCADE debería manejar la eliminación de registros en files y metadata.
        conn = self._connect()
        cursor = conn.cursor()
        updated_rows = None # Para rastrear si la eliminación fue exitosa
        try:
            # Opcional: primero eliminar archivos físicos asociados si es necesario.
            # Esta función solo se encarga de los registros de la BD.
            cursor.execute("DELETE FROM items WHERE item_id = ?", (item_id,))
            conn.commit()
            updated_rows = cursor.rowcount
            if updated_rows > 0:
                self.logger.info(f"Ítem ID {item_id} y sus datos asociados eliminados de la base de datos.")
            else:
                self.logger.warning(f"No se encontró el ítem ID {item_id} para eliminar.")
        except sqlite3.Error as e:
            self.logger.error(f"Error SQLite eliminando ítem ID {item_id}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()
        return updated_rows > 0 if updated_rows is not None else False
