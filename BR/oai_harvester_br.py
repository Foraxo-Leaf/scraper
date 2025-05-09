import requests
import time
from lxml import etree # Usar etree para parsear XML
import logging
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

class OAIHarvesterBR:
    """
    Clase para cosechar metadatos de repositorios OAI-PMH, adaptada para Brasil (BR).
    Extrae metadatos en formato Dublin Core (oai_dc).
    """

    def __init__(self, config, logger, db_manager):
        """
        Inicializa el OAIHarvesterBR.

        Args:
            config (dict): Configuración del scraper.
            logger (logging.Logger): Instancia del logger.
            db_manager (DatabaseManagerBR): Instancia del gestor de base de datos.
        """
        self.config = config
        self.logger = logger if logger else logging.getLogger(__name__)
        self.db_manager = db_manager
        
        self.oai_base_url = None # Se establecerá para cada repositorio
        self.metadata_prefix = self.config.get('oai_metadata_prefix', 'oai_dc')
        self.request_timeout = self.config.get('oai_request_timeout', 60) # segundos
        self.max_retries = self.config.get('oai_max_retries', 3)
        self.retry_delay = self.config.get('oai_retry_delay', 10) # segundos
        self.user_agent = self.config.get('user_agent', 'OAIHarvesterBR/1.0 (compatible; Python Requests)')
        self.max_records_to_fetch_total = self.config.get('oai_max_records_total_per_repo', None) # Límite global por repo
        self.from_date = self.config.get('oai_from_date', None) # Formato YYYY-MM-DD
        self.until_date = self.config.get('oai_until_date', None) # Formato YYYY-MM-DD
        self.set_spec = self.config.get('oai_set_spec', None) # Especificación del conjunto OAI

        # Definición de namespaces para el parseo XML, crucial para OAI-PMH
        self.oai_namespaces = {
            'oai': 'http://www.openarchives.org/OAI/2.0/',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcterms': 'http://purl.org/dc/terms/',
            # Añadir otros namespaces si son necesarios (ej. xoai, etc.)
        }

    def _make_oai_request(self, params):
        """
        Realiza una petición al endpoint OAI-PMH con los parámetros dados.

        Args:
            params (dict): Diccionario de parámetros para la petición OAI.

        Returns:
            lxml.etree._Element: El elemento raíz del XML de respuesta, o None si falla.
        """
        if not self.oai_base_url:
            self.logger.error("La URL base OAI no está configurada.")
            return None

        # Limpiar params de valores None antes de codificar
        cleaned_params = {k: v for k, v in params.items() if v is not None}
        query_string = urlencode(cleaned_params)
        request_url = f"{self.oai_base_url}?{query_string}"
        
        self.logger.info(f"Realizando petición OAI a: {request_url}")
        headers = {'User-Agent': self.user_agent, 'Accept': 'application/xml'}

        for attempt in range(self.max_retries):
            try:
                response = requests.get(request_url, headers=headers, timeout=self.request_timeout)
                response.raise_for_status() # Lanza HTTPError para respuestas 4xx/5xx

                # Parsear XML con lxml.etree
                # Es importante usar response.content para asegurar el manejo correcto de la codificación
                # que lxml puede auto-detectar o que se puede especificar.
                xml_tree = etree.fromstring(response.content)
                self.logger.debug(f"Respuesta OAI XML recibida y parseada exitosamente desde {request_url}")
                return xml_tree
            except requests.exceptions.HTTPError as e:
                self.logger.error(f"Error HTTP {e.response.status_code} en petición OAI a {request_url}: {e.response.text[:500]}...")
                if e.response.status_code in [400, 403, 404, 422]: # Errores que no suelen resolverse con reintento
                    # Comprobar si hay un error OAI específico en el XML
                    try:
                        error_tree = etree.fromstring(e.response.content)
                        oai_error = error_tree.find('.//oai:error', namespaces=self.oai_namespaces)
                        if oai_error is not None:
                            error_code = oai_error.get('code', 'N/A')
                            error_message = oai_error.text.strip() if oai_error.text else 'No message'
                            self.logger.error(f"Error OAI específico: Code='{error_code}', Message='{error_message}'")
                    except Exception as xml_parse_error:
                        self.logger.warning(f"No se pudo parsear el cuerpo del error OAI como XML: {xml_parse_error}")
                    break # Salir del bucle de reintentos
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error de red en petición OAI a {request_url} (intento {attempt + 1}/{self.max_retries}): {e}")
            except etree.XMLSyntaxError as e:
                self.logger.error(f"Error de sintaxis XML parseando respuesta de {request_url} (intento {attempt + 1}/{self.max_retries}): {e}")
                self.logger.debug(f"Contenido XML problemático (primeros 500 chars): {response.content[:500]}")
            
            if attempt < self.max_retries - 1:
                self.logger.info(f"Reintentando en {self.retry_delay} segundos...")
                time.sleep(self.retry_delay)
            else:
                self.logger.error(f"Fallaron todos los intentos para la petición OAI a {request_url}")
        return None

    def _parse_oai_response(self, xml_tree):
        """
        Parsea la respuesta XML de OAI-PMH para extraer registros y el resumptionToken.

        Args:
            xml_tree (lxml.etree._Element): El elemento raíz del XML de respuesta.

        Returns:
            tuple: (list_of_records, resumption_token)
                   list_of_records es una lista de diccionarios, cada uno representando un ítem.
                   resumption_token es un string, o None si no hay más registros.
        """
        if xml_tree is None:
            return [], None

        # Primero, buscar errores OAI explícitos en la respuesta
        oai_error_element = xml_tree.find('.//oai:error', namespaces=self.oai_namespaces)
        if oai_error_element is not None:
            error_code = oai_error_element.get('code', 'N/A')
            error_message = oai_error_element.text.strip() if oai_error_element.text else 'No error message provided.'
            self.logger.error(f"Error OAI recibido del servidor: Code='{error_code}', Message='{error_message}'")
            if error_code == 'noRecordsMatch':
                self.logger.info("El servidor OAI reportó 'noRecordsMatch' para la consulta actual.")
            # Otros errores como 'badArgument', 'badResumptionToken', etc., también se manejarán aquí.
            return [], None # No hay registros o token si hay un error

        records = []
        # Encontrar todos los elementos <record>
        for record_element in xml_tree.findall('.//oai:record', namespaces=self.oai_namespaces):
            header = record_element.find('oai:header', namespaces=self.oai_namespaces)
            if header is None:
                self.logger.warning("Registro OAI encontrado sin elemento <header>. Saltando.")
                continue
            
            oai_identifier = header.find('oai:identifier', namespaces=self.oai_namespaces)
            if oai_identifier is None or not oai_identifier.text:
                self.logger.warning("Registro OAI encontrado sin oai:identifier. Saltando.")
                continue
            oai_id_text = oai_identifier.text.strip()

            # Comprobar si el registro está marcado como eliminado
            if header.get('status') == 'deleted':
                self.logger.info(f"Registro OAI {oai_id_text} está marcado como 'deleted'. Se registrará como eliminado.")
                self.db_manager.mark_oai_item_as_deleted(oai_id_text, self.oai_base_url)
                continue # No procesar metadatos para registros eliminados

            metadata_element = record_element.find('oai:metadata', namespaces=self.oai_namespaces)
            if metadata_element is None:
                self.logger.warning(f"Registro OAI {oai_id_text} sin elemento <metadata>. Saltando.")
                continue
            
            # Asumimos oai_dc, pero podría ser parametrizable para otros formatos
            dc_element = metadata_element.find(f'oai:{self.metadata_prefix}', namespaces=self.oai_namespaces)
            if dc_element is None:
                # Intenta buscar con el namespace dc directamente, algunos servidores lo ponen así
                dc_element = metadata_element.find(f'dc:{self.metadata_prefix.split(':')[-1]}', namespaces=self.oai_namespaces)
                if dc_element is None:
                    self.logger.warning(f"Registro OAI {oai_id_text} sin elemento <{self.metadata_prefix}>. Metadata Prefix configurado: {self.metadata_prefix}")
                    # Loguear los hijos de metadata_element para depuración
                    # children_tags = [child.tag for child in metadata_element]
                    # self.logger.debug(f"Hijos de <metadata> para {oai_id_text}: {children_tags}")
                    continue

            item_metadata = {
                'oai_identifier': oai_id_text,
                'repository_source_type': 'oai',
                'oai_repository_url': self.oai_base_url # Guardar de qué repo OAI vino
            }
            
            # Extraer metadatos Dublin Core (o el prefijo configurado)
            for dc_field in dc_element:
                # El tag viene con el namespace, ej: {http://purl.org/dc/elements/1.1/}title
                field_name = dc_field.tag.split('}')[-1] # Obtener 'title' de '{namespace}title'
                field_value = dc_field.text.strip() if dc_field.text else ''
                
                if field_value: # Solo agregar si hay valor
                    if field_name in item_metadata: # Si el campo ya existe (ej. dc:subject múltiple)
                        if not isinstance(item_metadata[field_name], list):
                            item_metadata[field_name] = [item_metadata[field_name]] # Convertir a lista
                        item_metadata[field_name].append(field_value)
                    else:
                        item_metadata[field_name] = field_value
            
            # Intentar obtener un enlace directo a la página del ítem si está en dc:identifier
            # (a menudo hay varios dc:identifier, uno puede ser la URL de la página del ítem)
            item_page_url_from_dc = None
            if 'identifier' in item_metadata:
                identifiers = item_metadata['identifier']
                if not isinstance(identifiers, list):
                    identifiers = [identifiers]
                for ident in identifiers:
                    if ident.startswith('http') and oai_id_text not in ident: # No tomar el propio OAI ID si es una URL
                        # Heurística: tomar la primera URL HTTP/S que no sea el propio OAI ID como item_page_url
                        # Esto puede necesitar refinamiento según el repositorio
                        item_page_url_from_dc = ident
                        break
            item_metadata['item_page_url_from_oai'] = item_page_url_from_dc

            records.append(item_metadata)
            self.logger.debug(f"Registro OAI procesado: {oai_id_text}, Título: {item_metadata.get('title', 'N/A')[:50]}...")

        # Encontrar el resumptionToken
        resumption_token_element = xml_tree.find('.//oai:resumptionToken', namespaces=self.oai_namespaces)
        resumption_token = None
        if resumption_token_element is not None and resumption_token_element.text:
            resumption_token = resumption_token_element.text.strip()
            if not resumption_token: # Si el token está vacío, es como si no hubiera token
                resumption_token = None
                self.logger.info("ResumptionToken encontrado pero está vacío. Se considera fin de la lista.")
            else:
                self.logger.info(f"ResumptionToken encontrado para la siguiente página: {resumption_token[:30]}...")
        else:
            self.logger.info("No se encontró ResumptionToken. Se asume fin de la lista de registros.")
            
        return records, resumption_token

    def harvest_repository(self, oai_repo_config):
        """
        Cosecha todos los registros de un repositorio OAI configurado.

        Args:
            oai_repo_config (dict): Configuración específica para este repositorio OAI,
                                    debe contener 'url' y opcionalmente 'name', 'set_spec', etc.
        """
        self.oai_base_url = oai_repo_config.get('url')
        repo_name = oai_repo_config.get('name', self.oai_base_url)
        repo_set_spec = oai_repo_config.get('set_spec', self.set_spec) # Priorizar config de repo
        max_records_this_repo = oai_repo_config.get('max_records', self.max_records_to_fetch_total)

        if not self.oai_base_url:
            self.logger.error(f"No se proporcionó URL para el repositorio OAI: {repo_name}")
            return 0, 0, 0 # fetched, processed, failed

        self.logger.info(f"--- Iniciando cosecha OAI para el repositorio: {repo_name} ({self.oai_base_url}) ---")
        if max_records_this_repo is not None:
            self.logger.info(f"Límite máximo de registros a obtener para este repositorio: {max_records_this_repo}")
        if repo_set_spec:
            self.logger.info(f"Usando OAI setSpec: {repo_set_spec}")
        if self.from_date or self.until_date:
             self.logger.info(f"Filtrando por fecha: from={self.from_date or 'N/A'}, until={self.until_date or 'N/A'}")

        params = {
            'verb': 'ListRecords',
            'metadataPrefix': self.metadata_prefix
        }
        if repo_set_spec: params['set'] = repo_set_spec
        if self.from_date: params['from'] = self.from_date
        if self.until_date: params['until'] = self.until_date
        
        total_records_fetched_this_repo = 0
        total_records_processed_successfully = 0
        total_records_failed_to_process = 0
        page_num = 1

        while True:
            self.logger.info(f"Cosechando página {page_num} de {repo_name}...")
            if max_records_this_repo is not None and total_records_fetched_this_repo >= max_records_this_repo:
                self.logger.info(f"Se alcanzó el límite de {max_records_this_repo} registros para {repo_name}. Deteniendo cosecha.")
                break

            xml_response_tree = self._make_oai_request(params)
            if xml_response_tree is None:
                self.logger.error(f"Fallo al obtener o parsear la página {page_num} de {repo_name}. Abortando cosecha para este repositorio.")
                break # Salir del bucle si la petición falla catastróficamente

            records, resumption_token = self._parse_oai_response(xml_response_tree)
            
            if not records and not resumption_token:
                 # Podría ser un error OAI como noRecordsMatch o un error de parseo que ya fue logueado
                oai_error_element = xml_response_tree.find('.//oai:error', namespaces=self.oai_namespaces)
                if oai_error_element is not None and oai_error_element.get('code') == 'noRecordsMatch':
                    self.logger.info(f"No se encontraron más registros que coincidan con los criterios en {repo_name} (noRecordsMatch).")
                elif not records:
                    self.logger.info(f"No se encontraron registros en la página {page_num} de {repo_name} y no hay resumption token. Asumiendo fin de la lista.")
                break # Salir si no hay registros ni token
            
            num_fetched_this_page = len(records)
            total_records_fetched_this_repo += num_fetched_this_page
            self.logger.info(f"Se obtuvieron {num_fetched_this_page} registros de la página {page_num} de {repo_name}. Total para este repo: {total_records_fetched_this_repo}")

            for item_data in records:
                if max_records_this_repo is not None and total_records_processed_successfully >= max_records_this_repo:
                    # Este chequeo es para el caso en que el límite se alcance a mitad de página
                    self.logger.info(f"Se alcanzó el límite de {max_records_this_repo} registros procesados para {repo_name} (a mitad de página). Deteniendo.")
                    resumption_token = None # Forzar salida del bucle while
                    break 

                try:
                    # self.logger.debug(f"Procesando OAI item: {item_data.get('oai_identifier')}") # Muy verboso
                    # El item_id de la BD se genera o se obtiene dentro de get_or_create_item
                    item_id, created = self.db_manager.get_or_create_item(
                        oai_identifier=item_data['oai_identifier'],
                        repository_source=repo_name, # Usar el nombre del repo OAI como fuente
                        item_page_url=item_data.get('item_page_url_from_oai') # Puede ser None
                    )
                    if created:
                        self.logger.info(f"Nuevo ítem OAI creado en BD con ID {item_id} para OAI ID: {item_data['oai_identifier']}")
                    else:
                        self.logger.info(f"Ítem OAI existente encontrado en BD con ID {item_id} para OAI ID: {item_data['oai_identifier']}")
                    
                    # Guardar todos los metadatos OAI en la tabla de metadatos
                    # y actualizar el estado del ítem
                    self.db_manager.log_item_metadata(item_id, item_data, source_type='oai')
                    self.db_manager.update_item_status(item_id, 'pending_pdf_link') # Estado inicial después de OAI
                    total_records_processed_successfully += 1
                except Exception as e:
                    self.logger.error(f"Error procesando/guardando en BD el ítem OAI {item_data.get('oai_identifier', 'ID_DESCONOCIDO')}: {e}")
                    total_records_failed_to_process += 1
            
            if resumption_token:
                # Para la siguiente petición, solo se necesita el resumptionToken
                params = {'verb': 'ListRecords', 'resumptionToken': resumption_token}
                page_num += 1
            else:
                self.logger.info(f"No hay más ResumptionToken para {repo_name}. Fin de la cosecha OAI para este repositorio.")
                break # Salir del bucle while
        
        self.logger.info(f"--- Cosecha OAI finalizada para: {repo_name} ---")
        self.logger.info(f"Total registros obtenidos de {repo_name}: {total_records_fetched_this_repo}")
        self.logger.info(f"Total registros procesados y guardados exitosamente de {repo_name}: {total_records_processed_successfully}")
        self.logger.info(f"Total registros que fallaron al procesar/guardar de {repo_name}: {total_records_failed_to_process}")
        
        return total_records_fetched_this_repo, total_records_processed_successfully, total_records_failed_to_process


# Ejemplo de uso (requiere mocks o una BD y config reales)
if __name__ == '__main__':
    # --- Mockups para prueba --- 
    class MockLogger:
        def info(self, msg): print(f"INFO: {msg}")
        def error(self, msg): print(f"ERROR: {msg}")
        def warning(self, msg): print(f"WARNING: {msg}")
        def debug(self, msg): print(f"DEBUG: {msg}")

    class MockDBManager:
        def get_or_create_item(self, oai_identifier, repository_source, item_page_url=None):
            print(f"DB: Get/Create item OAI ID {oai_identifier} from {repository_source}")
            return f"fake_item_id_for_{oai_identifier.replace(':','_')}", True # Simula creación
        def log_item_metadata(self, item_id, metadata_dict, source_type):
            print(f"DB: Logged metadata for item {item_id} (source: {source_type}), Title: {metadata_dict.get('title', 'N/A')[:30]}")
        def update_item_status(self, item_id, status):
            print(f"DB: Updated status for item {item_id} to {status}")
        def mark_oai_item_as_deleted(self, oai_identifier, repo_url):
            print(f"DB: Marked OAI item {oai_identifier} from {repo_url} as deleted.")

    mock_logger = MockLogger()
    mock_db_manager = MockDBManager()
    mock_config = {
        'oai_metadata_prefix': 'oai_dc',
        'oai_request_timeout': 10,
        'oai_max_retries': 1,
        'oai_retry_delay': 1,
        'user_agent': 'TestHarvester/1.0',
        # 'oai_max_records_total_per_repo': 5 # Descomentar para probar límite
    }
    # --- Fin Mockups ---

    harvester = OAIHarvesterBR(mock_config, mock_logger, mock_db_manager)

    # --- Prueba con un repositorio OAI público (ej. de prueba de OCLC o similar) ---
    # Reemplazar con URLs OAI reales y válidas para probar
    # Ejemplo: Repositorio de prueba de OCLC (puede tener pocos registros o cambiar)
    # oai_test_repo_config = {
    #     'name': 'OCLC Test OAI Repo',
    #     'url': 'http://purl.oclc.org/OAI/2.0/test' # Este es un ejemplo, puede no funcionar siempre
    #     # 'set_spec': 'testSet' # Ejemplo de set
    # }

    # Ejemplo con un repositorio que podría tener el error "noRecordsMatch" si no hay nada reciente
    # oai_test_repo_config_bci = {
    #     'name': 'Biblioteca Digital BCI Chile (Ejemplo)',
    #     'url': 'https://bibliotecadigital.bci.cl/oai/request' # Endpoint real, pero la cosecha dependerá de filtros
    # }

    # IMPORTANTE: Para una prueba real, necesitas un endpoint OAI que sepas que funciona y tiene datos.
    # El siguiente es un endpoint de prueba conocido pero puede no tener siempre registros visibles sin 'set' o con 'oai_dc'
    oai_dspace_test_repo = {
        'name': 'DSpace Demo OAI',
        'url': 'https://demo.dspace.org/oai/request' # Un demo de DSpace
        # 'max_records': 10 # Para limitar la prueba
    }

    mock_logger.info("--- Iniciando prueba del OAIHarvesterBR ---")
    # fetched, processed, failed = harvester.harvest_repository(oai_test_repo_config_bci)
    fetched, processed, failed = harvester.harvest_repository(oai_dspace_test_repo)
    
    mock_logger.info(f"Resultado de la cosecha de prueba: Obtenidos={fetched}, Procesados={processed}, Fallidos={failed}")
    mock_logger.info("--- Prueba del OAIHarvesterBR finalizada ---")
    mock_logger.info("Si la URL de prueba es válida y tiene registros, deberías ver actividad de 'DB: ...' arriba.")
    mock_logger.info("Recuerda que los metadatos reales dependen del contenido del repositorio OAI.")
