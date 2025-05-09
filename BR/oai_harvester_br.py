import requests
import time
import logging
from lxml import etree # Usaremos lxml para un parseo XML más robusto y manejo de namespaces
from urllib.parse import urlparse, urlencode

# Definición de namespaces comunes de OAI-PMH
OAI_NAMESPACES = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'dcterms': 'http://purl.org/dc/terms/',
    # Añadir otros namespaces comunes si se identifican, ej. 'xsi'
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance' 
}

class OAIHarvesterBR:
    def __init__(self, config, logger_instance=None, db_manager_instance=None):
        self.config = config
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger("OAIHarvesterBR")
            if not self.logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                self.logger.addHandler(handler)
                self.logger.setLevel(logging.INFO)
        
        self.db_manager = db_manager_instance # Necesario para registrar ítems y metadatos

        self.request_delay = self.config.get('delay', self.config.get('download_delay_seconds', 1))
        self.request_timeout = self.config.get('request_timeout', 60)
        self.user_agent = self.config.get('user_agent', 'GenericScraper/1.0')
        self.max_request_retries = self.config.get('max_retries', 3)
        self.retry_base_delay = self.config.get('download_base_retry_delay', 5)
        self.ns = {'oai': OAI_NAMESPACES['oai'], 'dc': OAI_NAMESPACES['dc']} # Namespaces para XPath

    def _make_oai_request(self, base_url, params):
        """Realiza una petición GET al endpoint OAI y devuelve el contenido o None."""
        full_url = f"{base_url}?{urlencode(params)}"
        self.logger.debug(f"Realizando petición OAI a: {full_url}")
        try:
            headers = {'User-Agent': self.user_agent}
            response = requests.get(full_url, timeout=self.request_timeout, headers=headers)
            response.raise_for_status() # Lanza excepción para 4xx/5xx

            # --- Inicio: Código para guardar XML de depuración ---
            if "alice" in base_url and not hasattr(self, '_alice_xml_saved'):
                debug_filename = "debug_alice_oai_response.xml"
                with open(debug_filename, 'wb') as f:
                    f.write(response.content)
                self.logger.info(f"Respuesta OAI de Alice guardada en {debug_filename} para análisis.")
                self._alice_xml_saved = True 
            elif "infoteca" in base_url and not hasattr(self, '_infoteca_xml_saved'):
                debug_filename = "debug_infoteca_oai_response.xml"
                with open(debug_filename, 'wb') as f:
                    f.write(response.content)
                self.logger.info(f"Respuesta OAI de Infoteca-e guardada en {debug_filename} para análisis.")
                self._infoteca_xml_saved = True
            # --- Fin: Código para guardar XML de depuración ---

            return response.content
        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout durante la petición OAI a {base_url} con params {params}")
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"Error HTTP {e.response.status_code} en petición OAI a {base_url} con params {params}. Respuesta: {e.response.text[:500]}...")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error de red en petición OAI a {base_url} con params {params}: {e}")
        except Exception as e:
            self.logger.error(f"Error inesperado en petición OAI a {base_url} con params {params}: {e}", exc_info=True)
        return None

    def _parse_oai_response(self, xml_content):
        """Parsea la respuesta XML de OAI y extrae registros y resumptionToken."""
        records = []
        resumption_token = None
        error_message = None
        
        if not xml_content:
            return records, resumption_token, "Contenido XML vacío."

        try:
            # Usar fromstring para parsear desde el contenido binario/texto
            tree = etree.fromstring(xml_content)
            
            # Verificar errores OAI explícitos
            error_node = tree.find('.//oai:error', namespaces=self.ns)
            if error_node is not None:
                error_code = error_node.get('code', 'unknown')
                error_text = error_node.text or 'No description'
                error_message = f"Error OAI recibido: code='{error_code}', message='{error_text.strip()}'"
                self.logger.error(error_message)
                return records, resumption_token, error_message

            # Extraer registros
            for record_element in tree.xpath('//oai:record', namespaces=self.ns):
                header = record_element.find('.//oai:header', namespaces=self.ns)
                metadata = record_element.find('.//oai:metadata', namespaces=self.ns)
                
                if header is not None and metadata is not None:
                    identifier_node = header.find('.//oai:identifier', namespaces=self.ns)
                    status_attr = header.get('status')
                    
                    if status_attr == 'deleted':
                        self.logger.info(f"Registro OAI marcado como eliminado: {identifier_node.text if identifier_node is not None else 'Unknown ID'}. Saltando.")
                        continue

                    if identifier_node is not None:
                        oai_id = identifier_node.text
                        # Parsear metadatos DC (o el prefijo configurado)
                        dc_metadata = self._parse_dc_metadata(metadata)
                        if dc_metadata:
                            records.append({'oai_identifier': oai_id, 'metadata': dc_metadata})
                        else:
                            self.logger.warning(f"No se encontraron metadatos DC para el registro OAI: {oai_id}")
                    else:
                        self.logger.warning("Se encontró un elemento <record> sin <identifier> en la cabecera.")
                else:
                    # Podría ser solo una respuesta ListIdentifiers
                    pass 

            # Extraer resumptionToken
            token_node = tree.find('.//oai:resumptionToken', namespaces=self.ns)
            if token_node is not None and token_node.text:
                resumption_token = token_node.text
                self.logger.debug(f"Encontrado resumptionToken: {resumption_token[:30]}...")
            
        except etree.XMLSyntaxError as e:
            error_message = f"Error de sintaxis XML al parsear respuesta OAI: {e}. Contenido inicial: {xml_content[:500]}..."
            self.logger.error(error_message)
        except Exception as e:
            error_message = f"Error inesperado parseando respuesta OAI: {e}."
            self.logger.error(error_message, exc_info=True)
            
        return records, resumption_token, error_message

    def _parse_dc_metadata(self, metadata_element):
        """Extrae metadatos Dublin Core (dc:) del elemento <metadata>."""
        dc_data = {'titles': [], 'authors': [], 'subjects': [], 'descriptions': [], 'publishers': [], 'dates': [], 'types': [], 'formats': [], 'identifiers': [], 'languages': [], 'relations': [], 'rights': []}
        
        dc_container = metadata_element.find('.//{http://www.openarchives.org/OAI/2.0/oai_dc/}dc', namespaces=self.ns)
        if dc_container is None:
            # self.logger.debug("Contenedor <oai_dc:dc> no encontrado, intentando parsear directamente <metadata>") # DEBUG Log
            dc_container = metadata_element 
        
        # self.logger.debug(f"Contenedor DC encontrado: {etree.tostring(dc_container, pretty_print=True).decode()[:300]}...") # DEBUG Log
        for element in dc_container:
            tag_name = etree.QName(element.tag).localname
            tag_text = element.text.strip() if element.text else None
            # self.logger.debug(f"Procesando metadato DC: Tag='{tag_name}', Text='{tag_text}'") # DEBUG Log

            if tag_text: 
                if tag_name == 'title': dc_data['titles'].append(tag_text)
                elif tag_name == 'creator' or tag_name == 'contributor': dc_data['authors'].append(tag_text)
                elif tag_name == 'subject': dc_data['subjects'].append(tag_text)
                elif tag_name == 'description': dc_data['descriptions'].append(tag_text)
                elif tag_name == 'publisher': dc_data['publishers'].append(tag_text)
                elif tag_name == 'date': dc_data['dates'].append(tag_text)
                elif tag_name == 'type': dc_data['types'].append(tag_text)
                elif tag_name == 'format': dc_data['formats'].append(tag_text)
                elif tag_name == 'identifier': dc_data['identifiers'].append(tag_text)
                elif tag_name == 'language': dc_data['languages'].append(tag_text)
                elif tag_name == 'relation': dc_data['relations'].append(tag_text)
                elif tag_name == 'rights': dc_data['rights'].append(tag_text)
        
        if not any(dc_data.values()):
            self.logger.warning("No se extrajo ningún metadato DC del elemento <metadata>. Verificar estructura XML y logs de depuración de tags.")
            # self.logger.debug(f"Contenido del metadata_element que no produjo metadatos DC: {etree.tostring(metadata_element, pretty_print=True).decode()}") # DEBUG Log
            return None
             
        return dc_data

    def harvest_repository(self, repo_key, max_records_to_fetch=None):
        """Realiza la cosecha OAI para un repositorio específico."""
        repo_config = self.config.get('repositories', {}).get(repo_key)
        if not repo_config:
            self.logger.error(f"Configuración no encontrada para el repositorio OAI: {repo_key}")
            return {'status': 'error', 'message': f'Configuración no encontrada para {repo_key}'}

        base_url = repo_config.get('oai_pmh_endpoint')
        metadata_prefix = repo_config.get('preferred_metadata_prefix', 'oai_dc')
        set_spec = repo_config.get('set_spec')

        if not base_url:
            self.logger.error(f"No se definió 'oai_pmh_endpoint' para el repositorio: {repo_key}")
            return {'status': 'error', 'message': f'Falta oai_pmh_endpoint para {repo_key}'}

        self.logger.info(f"Iniciando cosecha OAI para '{repo_key}' desde {base_url} (prefix: {metadata_prefix}, set: {set_spec or 'N/A'})" )
        
        params = {'verb': 'ListRecords', 'metadataPrefix': metadata_prefix}
        if set_spec:
            params['set'] = set_spec

        total_fetched = 0
        total_processed_db = 0
        new_items_db = 0
        updated_metadata_db = 0
        failed_processing = 0
        resumption_token = None

        while True:
            if resumption_token:
                current_params = {'verb': 'ListRecords', 'resumptionToken': resumption_token}
            else:
                current_params = params.copy() # Usar copia para no modificar params original

            xml_content = self._make_oai_request(base_url, current_params)
            if not xml_content:
                self.logger.error(f"Fallo al obtener datos OAI para {repo_key}. Abortando cosecha.")
                return {'status': 'error', 'message': f'Fallo en petición OAI para {repo_key}', 'fetched': total_fetched, 'processed_db': total_processed_db, 'new_db': new_items_db, 'updated_db': updated_metadata_db, 'failed': failed_processing}

            records, next_token, error_msg = self._parse_oai_response(xml_content)
            
            if error_msg and not records: # Si hubo error OAI y no se parsearon registros
                self.logger.error(f"Error OAI impidió continuar la cosecha para {repo_key}: {error_msg}")
                return {'status': 'error', 'message': f'Error OAI: {error_msg}', 'fetched': total_fetched, 'processed_db': total_processed_db, 'new_db': new_items_db, 'updated_db': updated_metadata_db, 'failed': failed_processing}
            
            if not records and not next_token and total_fetched == 0:
                self.logger.warning(f"No se encontraron registros OAI para {repo_key} con los parámetros dados.")
                break # No hay nada que hacer

            for record in records:
                total_fetched += 1
                oai_id = record['oai_identifier']
                metadata = record['metadata']
                item_page_url = None
                pdf_url_from_oai = None
                
                # (DEBUG Log comentado)
                for identifier in metadata.get('identifiers', []):
                    is_pdf_link = identifier.lower().endswith('.pdf')
                    is_handle_or_doc_link = '/handle/' in identifier or '/doc/' in identifier

                    if identifier.startswith('http'):
                        if is_pdf_link and not pdf_url_from_oai:
                            pdf_url_from_oai = identifier
                            # (DEBUG Log comentado)
                        
                        if is_handle_or_doc_link and not item_page_url: # Priorizar handle/doc como item_page_url
                            item_page_url = identifier
                            # (DEBUG Log comentado)
                        elif not item_page_url and not is_pdf_link: # Fallback para item_page_url si no es PDF y no se encontró handle/doc aún
                            item_page_url = identifier
                
                if not item_page_url and pdf_url_from_oai: # Si solo encontramos un PDF pero ninguna página HTML de aterrizaje
                    item_page_url = pdf_url_from_oai # Usar el PDF como URL principal del ítem en este caso
                    self.logger.info(f"OAI ID {oai_id}: Se usará la URL del PDF ({pdf_url_from_oai}) como item_page_url principal al no encontrar un /handle/ o /doc/.")

                if pdf_url_from_oai:
                    metadata['pdf_direct_url'] = pdf_url_from_oai # Asegurar que se guarda para uso posterior

                if not item_page_url: # Si después de todo, no hay URL principal para el ítem
                    self.logger.warning(f"No se encontró una URL de página de ítem adecuada (patrón /handle/, /doc/ o HTTP) en los metadatos OAI para {oai_id}. Todos los dc:identifier: {metadata.get('identifiers', [])}. Saltando registro.")
                    failed_processing += 1
                    continue

                try:
                    # Registrar o encontrar el ítem en la BD
                    # El estado inicial podría ser 'pending_metadata_validation' o 'pending_pdf_link'
                    # Si confiamos en OAI, quizás 'pending_pdf_link' sea mejor.
                    item_id, current_status = self.db_manager.get_or_create_item(
                        item_page_url=item_page_url,
                        repository_source=repo_key,
                        oai_identifier=oai_id,
                        discovery_mode='oai',
                        initial_status='pending_pdf_link' # Asumir que OAI da metadatos OK, buscar PDF después
                    )
                    total_processed_db += 1
                    if current_status == 'pending_pdf_link': # Si realmente fue creado ahora
                        new_items_db += 1

                    # Siempre actualizar/loguear los metadatos obtenidos de OAI
                    if item_id:
                        update_success = self.db_manager.log_item_metadata(item_id, metadata)
                        if update_success and current_status != 'pending_pdf_link': # Si actualizó metadatos de uno existente
                            updated_metadata_db += 1
                    else:
                        self.logger.error(f"No se pudo obtener/crear item_id en DB para OAI ID {oai_id}, URL {item_page_url}")
                        failed_processing += 1

                except Exception as e_db:
                    self.logger.error(f"Error de base de datos procesando OAI ID {oai_id}: {e_db}", exc_info=True)
                    failed_processing += 1

                if max_records_to_fetch is not None and total_fetched >= max_records_to_fetch:
                    self.logger.info(f"Alcanzado límite de max_records_to_fetch ({max_records_to_fetch}) para {repo_key}.")
                    next_token = None # Forzar salida del bucle
                    break # Salir del bucle for records

            resumption_token = next_token # Actualizar token para la siguiente iteración
            
            if not resumption_token:
                self.logger.info(f"No hay más resumptionToken. Cosecha OAI para {repo_key} completada.")
                break # Salir del bucle while

            # Aplicar delay entre peticiones OAI
            if self.request_delay > 0:
                self.logger.debug(f"Esperando {self.request_delay}s antes de la siguiente petición OAI...")
                time.sleep(self.request_delay)
                
            # Romper si se alcanzó el límite (doble check)
            if max_records_to_fetch is not None and total_fetched >= max_records_to_fetch:
                break

        self.logger.info(f"Cosecha OAI finalizada para {repo_key}. Total fetched: {total_fetched}, Processed in DB: {total_processed_db}, New in DB: {new_items_db}, Updated metadata: {updated_metadata_db}, Failed processing: {failed_processing}")
        return {
            'status': 'completed', 
            'repository': repo_key,
            'fetched': total_fetched, 
            'processed_db': total_processed_db, 
            'new_db': new_items_db, 
            'updated_db': updated_metadata_db, 
            'failed': failed_processing
        }

# Ejemplo de uso básico (para pruebas)
if __name__ == '__main__':
    print("Ejecutando pruebas básicas de OAIHarvesterBR...")
    test_logger = logging.getLogger("OAIHarvesterTest")
    test_logger.setLevel(logging.DEBUG)
    test_handler = logging.StreamHandler()
    test_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    test_handler.setFormatter(test_formatter)
    if not test_logger.handlers: test_logger.addHandler(test_handler)

    # Configuración de prueba (simulada)
    # Para pruebas reales, se necesitaría un DatabaseManager funcional
    class MockDBManager:
        def get_or_create_item(self, *args, **kwargs): return (1, 'pending')
        def log_item_metadata(self, *args, **kwargs): pass

    test_config = {
        "delay": 0.1,
        "request_timeout": 10,
        "max_retries": 1,
        "download_base_retry_delay": 1, # Reusado para OAI por simplicidad aquí
        "user_agent": "TestOAIHarvester/1.0",
        "repositories": {
            "alice_test": { # Usar una configuración de prueba
                "name": "Alice (Test)",
                "oai_pmh_endpoint": "https://www.alice.cnptia.embrapa.br/alice-oai/request", # Endpoint real
                "preferred_metadata_prefix": "oai_dc"
            }
        }
    }

    harvester = OAIHarvesterBR(test_config, logger_instance=test_logger, db_manager_instance=MockDBManager())

    test_logger.info("--- Probando cosecha OAI (Alice, max 5 records) ---")
    results = harvester.harvest_repository('alice_test', max_records_to_fetch=5)
    test_logger.info(f"Resultado de la cosecha de prueba: {results}")

    # Añadir más pruebas si es necesario (ej. error handling, sets, etc.)
