import sqlite3
import logging
import os
import hashlib
from datetime import datetime

# Constants for database schema and status values
DB_SCHEMA_VERSION = 2  # Increment if schema changes

# Item processing statuses
STATUS_PENDING_OAI_HARVEST = "pending_oai_harvest" # Initial state if discovered via placeholder before OAI
STATUS_PENDING_PDF_LINK = "pending_pdf_link" # After OAI harvest, needs PDF link from item page
STATUS_PENDING_HTML_PROCESSING = "pending_html_processing" # After keyword search, needs HTML processing
STATUS_PENDING_PDF_DOWNLOAD = "pending_pdf_download" # PDF link found, ready for download
STATUS_PROCESSED = "processed" # PDF downloaded and verified
STATUS_PROCESSING_ERROR = "processing_error" # Error during HTML processing or metadata extraction
STATUS_DOWNLOAD_ERROR = "download_error" # Error during PDF download
STATUS_DUPLICATE_OAI = "duplicate_oai" # Item is a duplicate based on OAI identifier
STATUS_MANUAL_REVIEW_NEEDED = "manual_review_needed"

# File types
FILE_TYPE_PDF = "pdf"
FILE_TYPE_HTML_SNAPSHOT = "html_snapshot"

# File download statuses
FILE_DOWNLOAD_PENDING = "pending"
FILE_DOWNLOAD_SUCCESS = "success"
FILE_DOWNLOAD_FAILED = "failed"
FILE_DOWNLOAD_SKIPPED_EXIST = "skipped_exist"

class DatabaseManagerBR:
    """
    Manages the SQLite database for the Brazil scraper.
    Handles schema creation, item and file logging, and status updates.
    """

    def __init__(self, db_file_path, logger=None):
        """
        Initializes the DatabaseManagerBR.

        Args:
            db_file_path (str): The path to the SQLite database file.
            logger (logging.Logger, optional): Logger instance. Defaults to None.
        """
        self.db_file_path = db_file_path
        self.logger = logger or logging.getLogger(__name__)
        self._ensure_db_directory_exists()
        self.conn = None
        self._connect()
        self._initialize_db()

    def _ensure_db_directory_exists(self):
        """Ensures that the directory for the database file exists."""
        db_dir = os.path.dirname(self.db_file_path)
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir)
                self.logger.info(f"Created database directory: {db_dir}")
            except OSError as e:
                self.logger.error(f"Error creating database directory {db_dir}: {e}")
                raise

    def _connect(self):
        """Establishes a connection to the SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_file_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            self.conn.row_factory = sqlite3.Row # Access columns by name
            self.logger.info(f"Successfully connected to database: {self.db_file_path}")
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to database {self.db_file_path}: {e}")
            raise

    def _initialize_db(self):
        """
        Initializes the database schema if it doesn't exist or updates it.
        Creates tables and indices.
        """
        if not self.conn:
            self.logger.error("Database connection not established. Cannot initialize DB.")
            return

        try:
            cursor = self.conn.cursor()

            # Check and manage schema version (optional, but good practice for evolution)
            cursor.execute("PRAGMA user_version")
            current_version = cursor.fetchone()[0]
            if current_version < DB_SCHEMA_VERSION:
                self.logger.info(f"Database schema version {current_version} is older than expected {DB_SCHEMA_VERSION}. Applying migrations if any...")
                # Here you would put schema migration logic if needed.
                # For now, we just set the new version.
                cursor.execute(f"PRAGMA user_version = {DB_SCHEMA_VERSION}")
                self.conn.commit()
                self.logger.info(f"Database schema version updated to {DB_SCHEMA_VERSION}.")


            # Items table: Stores metadata about items (publications, articles, etc.)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS items (
                item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                repository_source TEXT NOT NULL, -- e.g., 'alice', 'infoteca-e', 'embrapa_search'
                oai_identifier TEXT UNIQUE,      -- OAI-PMH unique identifier (if applicable)
                item_page_url TEXT UNIQUE,       -- URL to the item's HTML page (can be primary key if OAI ID not present)
                processing_status TEXT NOT NULL DEFAULT 'pending_oai_harvest',
                title TEXT,
                publication_date TEXT,           -- Consider storing as ISO date string or Unix timestamp
                abstract TEXT,
                doi TEXT,
                pdf_url_harvested TEXT,          -- PDF URL found directly from OAI or initial metadata parsing
                last_processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            # Files table: Stores information about files associated with items (PDFs, HTML snapshots)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id INTEGER NOT NULL,
                file_type TEXT NOT NULL,        -- e.g., 'pdf', 'html_snapshot'
                file_url TEXT,                  -- Original URL of the file
                local_path TEXT UNIQUE,         -- Local path where the file is stored
                download_status TEXT NOT NULL DEFAULT 'pending',
                md5_hash TEXT,
                file_size_bytes INTEGER,
                download_timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (item_id) REFERENCES items (item_id)
            )
            """)

            # Authors table (many-to-many relationship with items)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS authors (
                author_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS item_authors (
                item_id INTEGER NOT NULL,
                author_id INTEGER NOT NULL,
                PRIMARY KEY (item_id, author_id),
                FOREIGN KEY (item_id) REFERENCES items (item_id),
                FOREIGN KEY (author_id) REFERENCES authors (author_id)
            )
            """)

            # Keywords/Subjects table (many-to-many relationship with items)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS keywords (
                keyword_id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword_text TEXT NOT NULL UNIQUE
            )
            """)

            cursor.execute("""
            CREATE TABLE IF NOT EXISTS item_keywords (
                item_id INTEGER NOT NULL,
                keyword_id INTEGER NOT NULL,
                PRIMARY KEY (item_id, keyword_id),
                FOREIGN KEY (item_id) REFERENCES items (item_id),
                FOREIGN KEY (keyword_id) REFERENCES keywords (keyword_id)
            )
            """)

            # Create indices for faster queries
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_items_status ON items (processing_status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_items_repo_source ON items (repository_source)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_item_id ON files (item_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_status ON files (download_status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_item_authors_item_id ON item_authors (item_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_item_authors_author_id ON item_authors (author_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_item_keywords_item_id ON item_keywords (item_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_item_keywords_keyword_id ON item_keywords (keyword_id)")

            # Triggers to update 'updated_at' timestamps
            cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS update_items_updated_at
            AFTER UPDATE ON items
            FOR EACH ROW
            BEGIN
                UPDATE items SET updated_at = CURRENT_TIMESTAMP WHERE item_id = OLD.item_id;
            END;
            """)
            cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS update_files_updated_at
            AFTER UPDATE ON files
            FOR EACH ROW
            BEGIN
                UPDATE files SET updated_at = CURRENT_TIMESTAMP WHERE file_id = OLD.file_id;
            END;
            """)

            self.conn.commit()
            self.logger.info("Database schema initialized/verified successfully.")
        except sqlite3.Error as e:
            self.logger.error(f"Error initializing database schema: {e}")
            if self.conn: self.conn.rollback()
            raise
        finally:
            if cursor: cursor.close()

    def get_or_create_item(self, repository_source, oai_identifier=None, item_page_url=None, initial_status=STATUS_PENDING_OAI_HARVEST):
        """
        Retrieves an existing item or creates a new one.
        Prioritizes oai_identifier if provided, then item_page_url for uniqueness.

        Args:
            repository_source (str): The source repository (e.g., 'alice').
            oai_identifier (str, optional): OAI identifier.
            item_page_url (str, optional): URL of the item page.
            initial_status (str, optional): The initial status for a new item.

        Returns:
            tuple: (item_id, is_new_item) or (None, False) if error.
        """
        if not oai_identifier and not item_page_url:
            self.logger.error("Either oai_identifier or item_page_url must be provided to get_or_create_item.")
            return None, False

        cursor = None
        try:
            cursor = self.conn.cursor()
            item_id = None
            is_new_item = False

            if oai_identifier:
                cursor.execute("SELECT item_id FROM items WHERE oai_identifier = ?", (oai_identifier,))
                row = cursor.fetchone()
                if row:
                    item_id = row['item_id']
            
            if not item_id and item_page_url: # If not found by OAI ID, try by page URL
                cursor.execute("SELECT item_id FROM items WHERE item_page_url = ?", (item_page_url,))
                row = cursor.fetchone()
                if row:
                    item_id = row['item_id']

            if item_id:
                self.logger.debug(f"Retrieved existing item_id: {item_id} for oai_id: {oai_identifier}, page_url: {item_page_url}")
                # Potentially update repository_source if it was missing or different
                # cursor.execute("UPDATE items SET repository_source = ? WHERE item_id = ? AND repository_source IS NULL", 
                #                (repository_source, item_id))
                # self.conn.commit()
            else:
                # Create new item
                sql = """
                INSERT INTO items (repository_source, oai_identifier, item_page_url, processing_status)
                VALUES (?, ?, ?, ?)
                """
                cursor.execute(sql, (repository_source, oai_identifier, item_page_url, initial_status))
                self.conn.commit()
                item_id = cursor.lastrowid
                is_new_item = True
                self.logger.info(f"Created new item_id: {item_id} for oai_id: {oai_identifier}, page_url: {item_page_url}, source: {repository_source}")
            
            return item_id, is_new_item

        except sqlite3.IntegrityError as e:
            # This can happen if another process inserted the same unique key concurrently
            self.logger.warning(f"IntegrityError in get_or_create_item (oai: {oai_identifier}, url: {item_page_url}): {e}. Attempting to re-fetch.")
            self.conn.rollback() # Rollback the failed insert
            # Try to fetch again, assuming it was just created by another thread/process
            if oai_identifier:
                cursor.execute("SELECT item_id FROM items WHERE oai_identifier = ?", (oai_identifier,))
                row = cursor.fetchone()
                if row: return row['item_id'], False
            if item_page_url:
                cursor.execute("SELECT item_id FROM items WHERE item_page_url = ?", (item_page_url,))
                row = cursor.fetchone()
                if row: return row['item_id'], False
            self.logger.error(f"Failed to re-fetch after IntegrityError for oai: {oai_identifier}, url: {item_page_url}")
            return None, False # Truly failed
        except sqlite3.Error as e:
            self.logger.error(f"Database error in get_or_create_item (oai: {oai_identifier}, url: {item_page_url}): {e}")
            if self.conn: self.conn.rollback()
            return None, False
        finally:
            if cursor: cursor.close()

    def update_item_status(self, item_id, status):
        """
        Updates the processing status of an item.

        Args:
            item_id (int): The ID of the item to update.
            status (str): The new processing status.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("UPDATE items SET processing_status = ?, last_processed_timestamp = CURRENT_TIMESTAMP WHERE item_id = ?", (status, item_id))
            self.conn.commit()
            if cursor.rowcount > 0:
                self.logger.info(f"Updated item_id: {item_id} status to: {status}")
            else:
                self.logger.warning(f"Attempted to update status for non-existent item_id: {item_id} to {status}")
        except sqlite3.Error as e:
            self.logger.error(f"Database error updating item_id {item_id} status to {status}: {e}")
            if self.conn: self.conn.rollback()

    def log_item_metadata(self, item_id, title=None, publication_date=None, abstract=None, doi=None, authors=None, keywords=None, pdf_url_harvested=None):
        """
        Logs or updates metadata for an item.

        Args:
            item_id (int): The ID of the item.
            title (str, optional): Item title.
            publication_date (str, optional): Publication date.
            abstract (str, optional): Item abstract.
            doi (str, optional): Item DOI.
            authors (list of str, optional): List of author names.
            keywords (list of str, optional): List of keywords.
            pdf_url_harvested (str, optional): PDF URL found from initial source (OAI/search result page).
        """
        fields_to_update = {}
        if title is not None: fields_to_update['title'] = title
        if publication_date is not None: fields_to_update['publication_date'] = publication_date
        if abstract is not None: fields_to_update['abstract'] = abstract
        if doi is not None: fields_to_update['doi'] = doi
        if pdf_url_harvested is not None: fields_to_update['pdf_url_harvested'] = pdf_url_harvested

        if not fields_to_update and not authors and not keywords:
            self.logger.debug(f"No new metadata provided for item_id: {item_id}. Skipping update.")
            return
        
        cursor = None
        try:
            cursor = self.conn.cursor()
            if fields_to_update:
                set_clause = ", ".join([f"{field} = :{field}" for field in fields_to_update.keys()])
                sql = f"UPDATE items SET {set_clause}, last_processed_timestamp = CURRENT_TIMESTAMP WHERE item_id = :item_id"
                fields_to_update['item_id'] = item_id
                cursor.execute(sql, fields_to_update)
                self.logger.info(f"Updated metadata for item_id: {item_id}. Fields: {list(fields_to_update.keys())}")

            # Handle authors (many-to-many)
            if authors:
                for author_name in authors:
                    author_name = author_name.strip()
                    if not author_name: continue
                    # Get or create author
                    cursor.execute("SELECT author_id FROM authors WHERE name = ?", (author_name,))
                    author_row = cursor.fetchone()
                    if author_row:
                        author_id = author_row['author_id']
                    else:
                        cursor.execute("INSERT INTO authors (name) VALUES (?) ON CONFLICT(name) DO NOTHING", (author_name,))
                        author_id = cursor.lastrowid
                        # If conflict (another process inserted), fetch the existing one
                        if not author_id:
                             cursor.execute("SELECT author_id FROM authors WHERE name = ?", (author_name,))
                             author_row = cursor.fetchone()
                             if author_row: author_id = author_row['author_id']
                    
                    if author_id:
                        # Link item to author, ignore if already exists
                        cursor.execute("INSERT OR IGNORE INTO item_authors (item_id, author_id) VALUES (?, ?)", (item_id, author_id))
                self.logger.info(f"Processed {len(authors)} authors for item_id: {item_id}")

            # Handle keywords (many-to-many)
            if keywords:
                for keyword_text in keywords:
                    keyword_text = keyword_text.strip()
                    if not keyword_text: continue
                    # Get or create keyword
                    cursor.execute("SELECT keyword_id FROM keywords WHERE keyword_text = ?", (keyword_text,))
                    keyword_row = cursor.fetchone()
                    if keyword_row:
                        keyword_id = keyword_row['keyword_id']
                    else:
                        cursor.execute("INSERT INTO keywords (keyword_text) VALUES (?) ON CONFLICT(keyword_text) DO NOTHING", (keyword_text,))
                        keyword_id = cursor.lastrowid
                        if not keyword_id:
                            cursor.execute("SELECT keyword_id FROM keywords WHERE keyword_text = ?", (keyword_text,))
                            keyword_row = cursor.fetchone()
                            if keyword_row: keyword_id = keyword_row['keyword_id']

                    if keyword_id:
                        # Link item to keyword, ignore if already exists
                        cursor.execute("INSERT OR IGNORE INTO item_keywords (item_id, keyword_id) VALUES (?, ?)", (item_id, keyword_id))
                self.logger.info(f"Processed {len(keywords)} keywords for item_id: {item_id}")

            self.conn.commit()
        except sqlite3.Error as e:
            self.logger.error(f"Database error logging metadata for item_id {item_id}: {e}")
            if self.conn: self.conn.rollback()
        finally:
            if cursor: cursor.close()

    def get_item_by_id(self, item_id):
        """Retrieves an item by its ID."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM items WHERE item_id = ?", (item_id,))
            return cursor.fetchone() # Returns a Row object or None
        except sqlite3.Error as e:
            self.logger.error(f"Database error retrieving item_id {item_id}: {e}")
            return None
        finally:
            if cursor: cursor.close()

    def get_items_by_status(self, status, limit=None):
        """
        Retrieves items with a specific processing status.

        Args:
            status (str or list): The processing status or a list of statuses.
            limit (int, optional): Maximum number of items to retrieve.

        Returns:
            list: A list of Row objects representing items.
        """
        cursor = None
        try:
            cursor = self.conn.cursor()
            if isinstance(status, list):
                placeholders = ",".join(["?"] * len(status))
                sql = f"SELECT * FROM items WHERE processing_status IN ({placeholders}) ORDER BY last_processed_timestamp ASC"
                params = tuple(status)
            else:
                sql = "SELECT * FROM items WHERE processing_status = ? ORDER BY last_processed_timestamp ASC"
                params = (status,)
            
            if limit is not None:
                sql += " LIMIT ?"
                params += (limit,)
                
            cursor.execute(sql, params)
            return cursor.fetchall()
        except sqlite3.Error as e:
            self.logger.error(f"Database error retrieving items with status {status}: {e}")
            return []
        finally:
            if cursor: cursor.close()

    def log_file_download(self, item_id, file_type, file_url, local_path, download_status, md5_hash=None, file_size_bytes=None):
        """
        Logs a file download attempt.
        Creates or updates a file record.

        Args:
            item_id (int): The ID of the associated item.
            file_type (str): Type of file (e.g., 'pdf', 'html_snapshot').
            file_url (str): Original URL of the file.
            local_path (str): Local path where the file is stored or attempted to be stored.
            download_status (str): Download status (e.g., 'success', 'failed', 'skipped_exist').
            md5_hash (str, optional): MD5 hash of the downloaded file.
            file_size_bytes (int, optional): Size of the downloaded file in bytes.

        Returns:
            int: The file_id of the logged/updated file record, or None if error.
        """
        cursor = None
        try:
            cursor = self.conn.cursor()
            # Check if a file record already exists for this item_id and local_path (or file_url if local_path is not yet set for this attempt)
            # Prioritize local_path for uniqueness if available, then file_url for a given item and type.
            # This logic might need refinement based on how re-downloads are handled.
            # For now, we assume a new attempt might mean a new URL or if it previously failed.

            # Try to find an existing file by local_path (if it's unique) or by item_id, file_type, and file_url
            # This simplified version will update if found by local_path, otherwise insert.
            # More robust would be to check item_id + file_type + file_url first for an existing record.

            cursor.execute("SELECT file_id FROM files WHERE local_path = ? AND item_id = ? AND file_type = ?", 
                           (local_path, item_id, file_type))
            existing_file = cursor.fetchone()

            current_ts = datetime.utcnow()

            if existing_file:
                file_id = existing_file['file_id']
                sql = """UPDATE files SET 
                           file_url = ?, download_status = ?, md5_hash = ?, 
                           file_size_bytes = ?, download_timestamp = ?
                         WHERE file_id = ?"""
                params = (file_url, download_status, md5_hash, file_size_bytes, current_ts if download_status == FILE_DOWNLOAD_SUCCESS else None, file_id)
                self.logger.info(f"Updating existing file record (file_id: {file_id}) for item_id: {item_id}, path: {local_path}, status: {download_status}")
            else:
                # If not found by local_path, try by item_id, file_type, and file_url to avoid duplicates if local_path changed
                cursor.execute("SELECT file_id FROM files WHERE item_id = ? AND file_type = ? AND file_url = ?", 
                               (item_id, file_type, file_url))
                existing_file_by_url = cursor.fetchone()
                if existing_file_by_url and download_status != FILE_DOWNLOAD_SKIPPED_EXIST : # if skipped, we might be confirming an existing entry
                    file_id = existing_file_by_url['file_id']
                    sql = """UPDATE files SET 
                               local_path = ?, download_status = ?, md5_hash = ?, 
                               file_size_bytes = ?, download_timestamp = ?
                             WHERE file_id = ?"""
                    params = (local_path, download_status, md5_hash, file_size_bytes, current_ts if download_status == FILE_DOWNLOAD_SUCCESS else None, file_id)
                    self.logger.info(f"Updating existing file record (file_id: {file_id}) based on URL match for item_id: {item_id}, path: {local_path}, status: {download_status}")
                else:
                    sql = """INSERT INTO files (item_id, file_type, file_url, local_path, download_status, md5_hash, file_size_bytes, download_timestamp)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
                    params = (item_id, file_type, file_url, local_path, download_status, md5_hash, file_size_bytes, current_ts if download_status == FILE_DOWNLOAD_SUCCESS else None)
                    self.logger.info(f"Creating new file record for item_id: {item_id}, path: {local_path}, status: {download_status}")
            
            cursor.execute(sql, params)
            self.conn.commit()
            return cursor.lastrowid if not existing_file and not (existing_file_by_url and download_status != FILE_DOWNLOAD_SKIPPED_EXIST) else (existing_file['file_id' ] if existing_file else (existing_file_by_url['file_id'] if existing_file_by_url else None) ) # Return the ID
        
        except sqlite3.Error as e:
            self.logger.error(f"Database error logging file download for item_id {item_id}, path {local_path}: {e}")
            if self.conn: self.conn.rollback()
            return None
        finally:
            if cursor: cursor.close()

    def get_file_by_local_path(self, local_path):
        """Retrieves a file record by its local path."""
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM files WHERE local_path = ?", (local_path,))
            return cursor.fetchone()
        except sqlite3.Error as e:
            self.logger.error(f"Database error retrieving file by local_path {local_path}: {e}")
            return None
        finally:
            if cursor: cursor.close()

    def get_files_for_item(self, item_id, file_type=None):
        """Retrieves all files or files of a specific type for a given item."""
        cursor = None
        try:
            cursor = self.conn.cursor()
            sql = "SELECT * FROM files WHERE item_id = ?"
            params = [item_id]
            if file_type:
                sql += " AND file_type = ?"
                params.append(file_type)
            cursor.execute(sql, tuple(params))
            return cursor.fetchall()
        except sqlite3.Error as e:
            self.logger.error(f"Database error retrieving files for item_id {item_id}: {e}")
            return []
        finally:
            if cursor: cursor.close()

    def get_item_stats(self):
        """
        Retrieves statistics about item processing statuses.
        Returns:
            dict: A dictionary with status counts.
        """
        stats = {}
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT processing_status, COUNT(*) as count FROM items GROUP BY processing_status")
            rows = cursor.fetchall()
            for row in rows:
                stats[row['processing_status']] = row['count']
            return stats
        except sqlite3.Error as e:
            self.logger.error(f"Database error retrieving item stats: {e}")
            return {}
        finally:
            if cursor: cursor.close()

    def get_file_stats(self):
        """
        Retrieves statistics about file download statuses.
        Returns:
            dict: A dictionary with status counts, possibly broken down by file_type.
        """
        stats = {}
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT file_type, download_status, COUNT(*) as count FROM files GROUP BY file_type, download_status")
            rows = cursor.fetchall()
            for row in rows:
                if row['file_type'] not in stats:
                    stats[row['file_type']] = {}
                stats[row['file_type']][row['download_status']] = row['count']
            return stats
        except sqlite3.Error as e:
            self.logger.error(f"Database error retrieving file stats: {e}")
            return {}
        finally:
            if cursor: cursor.close()

    def close_connection(self):
        """Closes the database connection."""
        if self.conn:
            try:
                self.conn.close()
                self.logger.info("Database connection closed.")
            except sqlite3.Error as e:
                self.logger.error(f"Error closing database connection: {e}")

# Example usage (typically not run directly from here but from the main scraper script)
if __name__ == '__main__':
    # This is a basic example and test block
    # Configure a temporary logger for this example
    temp_logger = logging.getLogger("db_manager_test")
    temp_logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    temp_logger.addHandler(handler)
    temp_logger.propagate = False

    # Use a temporary in-memory database for testing or a file
    # db_path = ":memory:"
    db_path = "test_scraper_br.db"
    if os.path.exists(db_path):
        os.remove(db_path) # Clean start for test

    db_manager = None
    try:
        db_manager = DatabaseManagerBR(db_path, logger=temp_logger)
        temp_logger.info("DatabaseManagerBR initialized for testing.")

        # Test 1: Get or create item
        item_id1, new1 = db_manager.get_or_create_item("alice", "oai:alice:123", "http://example.com/item/123")
        assert item_id1 is not None and new1 is True, "Test 1 Failed: Create new item"
        temp_logger.info(f"Test 1 Passed. Item 1 ID: {item_id1}, Is New: {new1}")

        item_id1_refetch, new1_refetch = db_manager.get_or_create_item("alice", "oai:alice:123", "http://example.com/item/123")
        assert item_id1_refetch == item_id1 and new1_refetch is False, "Test 1 Failed: Refetch existing item"
        temp_logger.info(f"Test 1 Passed. Item 1 Refetched ID: {item_id1_refetch}, Is New: {new1_refetch}")

        # Test 2: Update item status
        db_manager.update_item_status(item_id1, STATUS_PROCESSED)
        item_data = db_manager.get_item_by_id(item_id1)
        assert item_data['processing_status'] == STATUS_PROCESSED, "Test 2 Failed: Update status"
        temp_logger.info(f"Test 2 Passed. Item 1 status: {item_data['processing_status']}")

        # Test 3: Log metadata
        authors_list = ["Doe, John", "Smith, Jane "]
        keywords_list = [" agriculture ", "research", "Brazil"]
        db_manager.log_item_metadata(item_id1, title="Test Title", publication_date="2023-01-01", 
                                     abstract="This is a test abstract.", doi="10.1234/test.doi",
                                     authors=authors_list, keywords=keywords_list,
                                     pdf_url_harvested="http://example.com/item/123/test.pdf")
        item_data = db_manager.get_item_by_id(item_id1)
        assert item_data['title'] == "Test Title", "Test 3 Failed: Log title"
        assert item_data['pdf_url_harvested'] == "http://example.com/item/123/test.pdf", "Test 3 Failed: Log PDF URL"
        temp_logger.info(f"Test 3 Passed. Item 1 title: {item_data['title']}")

        # Test 4: Log file download
        file_id1 = db_manager.log_file_download(item_id1, FILE_TYPE_PDF, "http://example.com/item/123/test.pdf", 
                                             "output/pdfs/1/test.pdf", FILE_DOWNLOAD_SUCCESS, 
                                             "abcdef123456", 102400)
        assert file_id1 is not None, "Test 4 Failed: Log file download"
        temp_logger.info(f"Test 4 Passed. File 1 ID: {file_id1}")
        
        # Test 4.1: Attempt to log the same file again, should update or be handled gracefully
        file_id1_retry = db_manager.log_file_download(item_id1, FILE_TYPE_PDF, "http://example.com/item/123/test.pdf", 
                                             "output/pdfs/1/test.pdf", FILE_DOWNLOAD_SUCCESS, 
                                             "abcdef123456_new", 102401) # new hash and size
        assert file_id1_retry == file_id1, "Test 4.1 Failed: Re-logging same file path should update"
        updated_file_data = db_manager.get_file_by_local_path("output/pdfs/1/test.pdf")
        assert updated_file_data['md5_hash'] == "abcdef123456_new", "Test 4.1 Failed: MD5 not updated on re-log"
        temp_logger.info(f"Test 4.1 Passed. File 1 re-logged/updated ID: {file_id1_retry}")


        # Test 5: Get items by status
        pending_items = db_manager.get_items_by_status(STATUS_PENDING_OAI_HARVEST)
        assert isinstance(pending_items, list), "Test 5 Failed: Get items by status"
        temp_logger.info(f"Test 5 Passed. Found {len(pending_items)} items with status PENDING_OAI_HARVEST.")

        # Test 6: Item and File Stats
        item_stats = db_manager.get_item_stats()
        assert STATUS_PROCESSED in item_stats and item_stats[STATUS_PROCESSED] == 1, "Test 6 Failed: Item Stats"
        temp_logger.info(f"Test 6 Passed. Item stats: {item_stats}")
        file_stats = db_manager.get_file_stats()
        assert FILE_TYPE_PDF in file_stats and FILE_DOWNLOAD_SUCCESS in file_stats[FILE_TYPE_PDF] and file_stats[FILE_TYPE_PDF][FILE_DOWNLOAD_SUCCESS] == 1, "Test 6 Failed: File Stats"
        temp_logger.info(f"Test 6 Passed. File stats: {file_stats}")
        
        # Test 7: Check authors and keywords linking
        cursor = db_manager.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM item_authors WHERE item_id = ?", (item_id1,))
        assert cursor.fetchone()[0] == 2, "Test 7 Failed: Author linking count"
        cursor.execute("SELECT COUNT(*) FROM item_keywords WHERE item_id = ?", (item_id1,))
        assert cursor.fetchone()[0] == 3, "Test 7 Failed: Keyword linking count"
        cursor.close()
        temp_logger.info(f"Test 7 Passed. Author and keyword links verified.")


        temp_logger.info("All basic tests passed successfully!")

    except AssertionError as e:
        temp_logger.error(f"An assertion failed during testing: {e}")
    except Exception as e:
        temp_logger.error(f"An error occurred during testing: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close_connection()
        if os.path.exists(db_path) and db_path != ":memory:": # Clean up test db file
            # os.remove(db_path) # Keep it for inspection if needed
            pass
        temp_logger.info(f"Test database remains at {db_path} for inspection (if not in-memory).")

    pass # End of example block
