import requests
import logging
import time
from lxml import etree # For parsing XML
from urllib.parse import urlencode

# OAI-PMH specific constants
OAI_VERB_LIST_RECORDS = "ListRecords"
OAI_METADATA_PREFIX_DC = "oai_dc" # Dublin Core

# Mock classes for testing if real DB manager is not available
class MockDBManager:
    def get_or_create_item(self, repository_source, oai_identifier=None, item_page_url=None, initial_status='pending_pdf_link'):
        print(f"DB: Get/Create item for source={repository_source}, oai_id={oai_identifier}")
        return (int(time.time()*1000)%100000, True) # Simulate item_id, is_new
    def update_item_status(self, item_id, status):
        print(f"DB: Update item_id={item_id} to status={status}")
    def log_item_metadata(self, item_id, title=None, publication_date=None, abstract=None, doi=None, authors=None, keywords=None, pdf_url_harvested=None):
        print(f"DB: Log metadata for item_id={item_id}, title={title}")

class OAIHarvesterBR:
    """
    Handles harvesting metadata from OAI-PMH compliant repositories for Brazil.
    Extracts Dublin Core metadata and manages pagination with resumption tokens.
    """

    def __init__(self, config, logger=None, db_manager=None, selectors_dict=None):
        """
        Initializes the OAIHarvesterBR.

        Args:
            config (dict): Configuration dictionary.
            logger (logging.Logger, optional): Logger instance.
            db_manager (DatabaseManagerBR, optional): Database manager instance.
            selectors_dict (dict, optional): Dictionary of selectors (mainly for OAI XML parsing keys).
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.db_manager = db_manager
        
        self.oai_repositories = config.get('oai_repositories', [])
        self.request_timeout = config.get('oai_request_timeout', 60) # Seconds
        self.request_delay = config.get('oai_request_delay_seconds', 3) # Seconds between paginated requests
        self.max_records_per_repo = config.get('max_records_oai_per_repo', float('inf'))
        self.user_agent = config.get('user_agent', 'Mozilla/5.0 (compatible; OAIHarvester/1.0; +http://example.com/bot)')

        if selectors_dict and 'oai_selectors' in selectors_dict:
            self.oai_selectors = selectors_dict['oai_selectors']
        else:
            self.logger.warning("OAI selectors not found in selectors_dict. Using default keys for parsing.")
            # Define default selectors (keys for lxml.etree find) if not provided
            # These are common paths for OAI-PMH XML using Dublin Core
            self.oai_selectors = {
                'record': ".//{http://www.openarchives.org/OAI/2.0/}record",
                'record_identifier': ".//{http://www.openarchives.org/OAI/2.0/}header/{http://www.openarchives.org/OAI/2.0/}identifier",
                'datestamp': ".//{http://www.openarchives.org/OAI/2.0/}header/{http://www.openarchives.org/OAI/2.0/}datestamp",
                'dc_title': ".//{http://purl.org/dc/elements/1.1/}title",
                'dc_creator': ".//{http://purl.org/dc/elements/1.1/}creator",
                'dc_subject': ".//{http://purl.org/dc/elements/1.1/}subject",
                'dc_description': ".//{http://purl.org/dc/elements/1.1/}description",
                'dc_publisher': ".//{http://purl.org/dc/elements/1.1/}publisher",
                'dc_contributor': ".//{http://purl.org/dc/elements/1.1/}contributor",
                'dc_date': ".//{http://purl.org/dc/elements/1.1/}date",
                'dc_type': ".//{http://purl.org/dc/elements/1.1/}type",
                'dc_format': ".//{http://purl.org/dc/elements/1.1/}format",
                'dc_identifier': ".//{http://purl.org/dc/elements/1.1/}identifier", # Can be URL, DOI
                'dc_source': ".//{http://purl.org/dc/elements/1.1/}source",
                'dc_language': ".//{http://purl.org/dc/elements/1.1/}language",
                'dc_relation': ".//{http://purl.org/dc/elements/1.1/}relation",
                'dc_coverage': ".//{http://purl.org/dc/elements/1.1/}coverage",
                'dc_rights': ".//{http://purl.org/dc/elements/1.1/}rights",
                'resumption_token': ".//{http://www.openarchives.org/OAI/2.0/}resumptionToken",
                'oai_error': ".//{http://www.openarchives.org/OAI/2.0/}error"
            }
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})

    def _make_oai_request(self, base_url, params):
        """
        Makes a request to the OAI-PMH endpoint.

        Args:
            base_url (str): The base URL of the OAI repository.
            params (dict): Dictionary of OAI parameters (verb, metadataPrefix, etc.).

        Returns:
            lxml.etree._Element: The parsed XML tree root, or None on error.
        """
        full_url = f"{base_url}?{urlencode(params)}"
        self.logger.debug(f"Making OAI request to: {full_url}")
        try:
            response = self.session.get(full_url, timeout=self.request_timeout)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            
            # It's good practice to check content type, though OAI should be XML
            content_type = response.headers.get('Content-Type', '')
            if 'xml' not in content_type.lower():
                self.logger.warning(f"Unexpected content type '{content_type}' for OAI response from {full_url}. Expected XML.")
                # Still try to parse, it might be mislabeled
            
            xml_tree = etree.fromstring(response.content)
            # Check for OAI-level errors
            error_elements = xml_tree.xpath(self.oai_selectors.get('oai_error', './/error')) # Fallback xpath
            if error_elements:
                for error_element in error_elements:
                    error_code = error_element.get('code', 'UnknownCode')
                    error_message = error_element.text or "No error message provided."
                    self.logger.error(f"OAI-PMH error from {base_url}: Code '{error_code}' - {error_message.strip()}")
                # Specific error handling for 'noRecordsMatch' which isn't a failure but an empty set
                if any(err.get('code') == 'noRecordsMatch' for err in error_elements):
                    self.logger.info(f"OAI Info: No records match the request criteria for {base_url} with params {params}.")
                    return xml_tree # Return the tree so resumption token (if any) can be checked as empty
                return None # For other OAI errors
            return xml_tree
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP request failed for OAI endpoint {full_url}: {e}")
            return None
        except etree.XMLSyntaxError as e:
            self.logger.error(f"Failed to parse XML response from {full_url}: {e}")
            self.logger.debug(f"Response content snippet (first 500 chars): {response.content[:500]}")
            return None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during OAI request to {full_url}: {e}")
            return None

    def _parse_record(self, record_element):
        """
        Parses a single OAI <record> element to extract metadata.

        Args:
            record_element (lxml.etree._Element): The OAI <record> XML element.

        Returns:
            dict: A dictionary of extracted metadata, or None if essential info is missing.
        """
        metadata = {}
        try:
            header = record_element.find(".//{http://www.openarchives.org/OAI/2.0/}header")
            if header is None:
                self.logger.warning("Record has no header. Skipping.")
                return None
            
            oai_id_element = header.find(self.oai_selectors['record_identifier'].replace('.//', '')) # Relative to header
            if oai_id_element is not None and oai_id_element.text:
                metadata['oai_identifier'] = oai_id_element.text.strip()
            else:
                self.logger.warning("Record has no OAI identifier. Skipping.")
                return None # OAI identifier is crucial

            # Extract Dublin Core fields (or other configured metadata fields)
            meta_section = record_element.find(".//{http://www.openarchives.org/OAI/2.0/}metadata")
            if meta_section is None:
                self.logger.warning(f"Record {metadata['oai_identifier']} has no metadata section. Skipping.")
                return metadata # Return OAI ID at least

            # Example for DC fields, adapt if using other metadataPrefix
            dc_fields_map = {
                'title': self.oai_selectors.get('dc_title'),
                'authors': self.oai_selectors.get('dc_creator'), # Can be multiple
                'keywords': self.oai_selectors.get('dc_subject'), # Can be multiple
                'abstract': self.oai_selectors.get('dc_description'),
                'publication_date': self.oai_selectors.get('dc_date'),
                'publisher': self.oai_selectors.get('dc_publisher'),
                'type': self.oai_selectors.get('dc_type'),
                'format': self.oai_selectors.get('dc_format'),
                'dc_identifier_list': self.oai_selectors.get('dc_identifier'), # List of all identifiers (URLs, DOIs, etc.)
                'language': self.oai_selectors.get('dc_language')
            }

            for key, selector in dc_fields_map.items():
                if not selector: continue
                # Use .// to search anywhere within the meta_section for these specific DC tags
                elements = meta_section.xpath(selector)
                if elements:
                    # For fields that can have multiple values (authors, keywords, dc_identifier_list)
                    if key in ['authors', 'keywords', 'dc_identifier_list']:
                        metadata[key] = [el.text.strip() for el in elements if el.text and el.text.strip()]
                    else: # For single value fields, take the first non-empty one
                        for el in elements:
                            if el.text and el.text.strip():
                                metadata[key] = el.text.strip()
                                break
            
            # Try to find a direct item page URL or DOI from dc_identifier_list
            # This is a common pattern for OAI records.
            item_page_url_found = None
            doi_found = None
            if 'dc_identifier_list' in metadata:
                for ident in metadata['dc_identifier_list']:
                    if ident.startswith('http://') or ident.startswith('https://'):
                        if not item_page_url_found and '/handle/' in ident: # DSpace handle URLs are good candidates
                            item_page_url_found = ident
                        elif not item_page_url_found and '/jspui/' in ident: # Another DSpace pattern
                            item_page_url_found = ident
                        elif not item_page_url_found and '.html' in ident.lower(): # Generic html page
                            item_page_url_found = ident
                        # Prioritize specific patterns before generic http, if multiple http links exist
                    if ident.lower().startswith('doi:') or (ident.startswith('10.') and '/' in ident):
                        doi_found = ident.lower().replace('doi:','').strip()
                
                if item_page_url_found: metadata['item_page_url_from_oai'] = item_page_url_found
                if doi_found: metadata['doi_from_oai'] = doi_found
                # pdf_url_harvested might also come from dc_identifier if it ends with .pdf
                for ident in metadata['dc_identifier_list']:
                    if ident.lower().endswith('.pdf'):
                        metadata['pdf_url_harvested'] = ident
                        break # Take the first one

            # Also check dc:format for PDF indication
            if metadata.get('format', '').lower() == 'application/pdf' and 'pdf_url_harvested' not in metadata and item_page_url_found:
                # This is tricky. OAI usually doesn't give direct PDF links if format is PDF.
                # It implies the main identifier *is* the PDF, or one of the identifiers is.
                # We primarily rely on HTML scraping for PDF links unless dc_identifier gives a direct one.
                self.logger.debug(f"Record {metadata['oai_identifier']} format is PDF, but no direct PDF URL in identifiers. Will need HTML processing.")

            return metadata
        except Exception as e:
            oai_id = metadata.get('oai_identifier', 'UNKNOWN_OAI_ID')
            self.logger.error(f"Error parsing record {oai_id}: {e}", exc_info=False)
            # Log the problematic record's XML for debugging
            try:
                problematic_xml = etree.tostring(record_element, pretty_print=True).decode('utf-8')[:500]
                self.logger.debug(f"Problematic record XML snippet: {problematic_xml}...")
            except Exception as log_e:
                self.logger.error(f"Could not serialize problematic record element: {log_e}")
            return None # Return None if parsing fails for a record

    def harvest_repository(self, repo_config):
        """
        Harvests all records from a single OAI-PMH repository based on config.

        Args:
            repo_config (dict): Configuration for the repository (name, base_url, set_spec, etc.).

        Returns:
            int: Count of new items found and added/updated in the DB from this repository.
        """
        repo_name = repo_config.get('name', 'UnknownRepo')
        base_url = repo_config.get('base_url')
        set_spec = repo_config.get('set_spec') # Optional
        metadata_prefix = repo_config.get('metadata_prefix', OAI_METADATA_PREFIX_DC)
        
        if not base_url or not self.db_manager:
            self.logger.error(f"Cannot harvest for {repo_name}: base_url or db_manager is missing.")
            return 0

        self.logger.info(f"Starting OAI harvest for repository: {repo_name} ({base_url})")
        total_records_fetched_for_repo = 0
        new_items_added_for_repo = 0
        resumption_token = None
        first_request = True

        while True:
            params = {
                'verb': OAI_VERB_LIST_RECORDS
            }
            if resumption_token:
                params['resumptionToken'] = resumption_token
                first_request = False # Not the first request anymore
            else: # Only add metadataPrefix and set on the initial request
                params['metadataPrefix'] = metadata_prefix
                if set_spec:
                    params['set'] = set_spec
            
            xml_tree = self._make_oai_request(base_url, params)
            if xml_tree is None:
                self.logger.error(f"Failed to fetch or critical OAI error for {repo_name} (params: {params}). Aborting harvest for this repository.")
                break # Abort for this repo if a request fails critically

            records_in_batch = xml_tree.xpath(self.oai_selectors.get('record', './/record')) # Default XPath if not in selectors
            self.logger.info(f"Retrieved {len(records_in_batch)} records in this batch from {repo_name}.")

            if not records_in_batch and first_request:
                # Check again for noRecordsMatch specifically, _make_oai_request might have logged it
                error_elements = xml_tree.xpath(self.oai_selectors.get('oai_error', './/error'))
                if any(err.get('code') == 'noRecordsMatch' for err in error_elements):
                    self.logger.info(f"No records match the initial criteria for {repo_name}. Harvest for this repo ends.")
                else:
                    self.logger.warning(f"No records found in the first batch from {repo_name}, but no explicit 'noRecordsMatch' error. Check OAI endpoint or config.")
                break # No records to process

            for record_element in records_in_batch:
                if total_records_fetched_for_repo >= self.max_records_per_repo:
                    self.logger.info(f"Reached max_records_oai_per_repo ({self.max_records_per_repo}) for {repo_name}. Stopping harvest for this repo.")
                    break # Break from processing records in batch
                
                parsed_data = self._parse_record(record_element)
                if parsed_data and 'oai_identifier' in parsed_data:
                    total_records_fetched_for_repo += 1
                    item_id, is_new = self.db_manager.get_or_create_item(
                        repository_source=repo_name, # Use the configured repo name as source
                        oai_identifier=parsed_data['oai_identifier'],
                        item_page_url=parsed_data.get('item_page_url_from_oai'), # May be None
                        initial_status='pending_pdf_link' # Items from OAI need PDF link extraction from item page
                    )
                    if item_id:
                        if is_new:
                            new_items_added_for_repo += 1
                            self.logger.info(f"New OAI item: ID {item_id}, OAI_ID: {parsed_data['oai_identifier']} from {repo_name}")
                        else:
                            self.logger.info(f"Existing OAI item: ID {item_id}, OAI_ID: {parsed_data['oai_identifier']} from {repo_name}. Will check/update.")
                        
                        # Log all extracted metadata
                        self.db_manager.log_item_metadata(
                            item_id=item_id,
                            title=parsed_data.get('title'),
                            publication_date=parsed_data.get('publication_date'),
                            abstract=parsed_data.get('abstract'),
                            doi=parsed_data.get('doi_from_oai'),
                            authors=parsed_data.get('authors'),
                            keywords=parsed_data.get('keywords'),
                            pdf_url_harvested=parsed_data.get('pdf_url_harvested') # If found directly in OAI identifiers
                        )
                        # If a direct PDF URL was found in OAI, update status
                        if parsed_data.get('pdf_url_harvested'):
                            self.db_manager.update_item_status(item_id, 'pending_pdf_download')
                else:
                    self.logger.warning(f"Could not parse a record or missing OAI ID from {repo_name}. Record skipped.")
            
            if total_records_fetched_for_repo >= self.max_records_per_repo:
                break # Break from while loop (pagination)

            # Check for resumptionToken
            token_elements = xml_tree.xpath(self.oai_selectors.get('resumption_token', './/resumptionToken'))
            if token_elements and token_elements[0].text and token_elements[0].text.strip():
                resumption_token = token_elements[0].text.strip()
                self.logger.info(f"Got resumptionToken: '{resumption_token[:20]}...' for {repo_name}")
                # Check if the token is empty or signals end (some OAI servers send empty token with attributes)
                # Example: <resumptionToken completeListSize="123" cursor="0"></resumptionToken> (empty text means end)
                if not resumption_token: # Explicitly empty text means end
                    self.logger.info(f"Empty resumptionToken text received. Assuming end of list for {repo_name}.")
                    break
                time.sleep(self.request_delay) # Be polite to the server
            else:
                self.logger.info(f"No resumptionToken found. End of harvest for {repo_name}.")
                break # No token, end of list
        
        self.logger.info(f"Finished OAI harvest for {repo_name}. Fetched {total_records_fetched_for_repo} records in total. Added {new_items_added_for_repo} new items to DB.")
        return new_items_added_for_repo

    def run_harvest(self):
        """
        Runs the OAI harvesting process for all configured repositories.
        """
        if not self.oai_repositories:
            self.logger.info("No OAI repositories configured. Skipping OAI harvesting.")
            return 0
        
        if not self.db_manager:
            self.logger.error("Database manager not available. OAI harvesting cannot proceed.")
            return 0

        self.logger.info(f"Starting OAI harvesting for {len(self.oai_repositories)} repositories.")
        total_new_items_across_all_repos = 0
        for repo_conf in self.oai_repositories:
            total_new_items_across_all_repos += self.harvest_repository(repo_conf)
        
        self.logger.info(f"OAI harvesting phase completed. Total new items added from all OAI sources: {total_new_items_across_all_repos}")
        return total_new_items_across_all_repos

# Example Usage (Illustrative)
if __name__ == '__main__':
    # Setup basic logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    example_logger = logging.getLogger("OAIHarvesterBRExample")

    # Mock configuration
    mock_config_data = {
        'oai_repositories': [
            {
                'name': 'alice_test',
                'base_url': 'https://www.alice.cnptia.embrapa.br/oai/request',
                # 'set_spec': 'com_123456789_1', # Example set, if needed
                'metadata_prefix': 'oai_dc'
            }
            # Add more repositories if needed, e.g., Infoteca-e if it has OAI
            # {
            #     'name': 'infoteca_test',
            #     'base_url': 'https://www.infoteca.cnptia.embrapa.br/infoteca/oai', # Hypothetical URL
            #     'metadata_prefix': 'oai_dc'
            # }
        ],
        'oai_request_timeout': 30,
        'oai_request_delay_seconds': 2,
        'max_records_oai_per_repo': 15, # Limit for example run
        'user_agent': 'TestHarvester/1.0'
    }

    # Mock selectors (can be loaded from a YAML file in a real scenario)
    mock_selectors_data = {
        'oai_selectors': { # These are often standard, but can be customized if needed
            'record': ".//{http://www.openarchives.org/OAI/2.0/}record",
            'record_identifier': ".//{http://www.openarchives.org/OAI/2.0/}header/{http://www.openarchives.org/OAI/2.0/}identifier",
            'dc_title': ".//{http://purl.org/dc/elements/1.1/}title",
            'dc_creator': ".//{http://purl.org/dc/elements/1.1/}creator",
            'dc_identifier': ".//{http://purl.org/dc/elements/1.1/}identifier",
            'resumption_token': ".//{http://www.openarchives.org/OAI/2.0/}resumptionToken"
            # Add other DC fields as needed following the pattern
        }
    }

    example_logger.info("Initializing OAIHarvesterBR for example...")
    harvester = OAIHarvesterBR(
        config=mock_config_data, 
        logger=example_logger, 
        db_manager=MockDBManager(), # Using mock DB for this example
        selectors_dict=mock_selectors_data
    )

    example_logger.info("Starting example OAI harvest run...")
    # new_items = harvester.run_harvest()
    # example_logger.info(f"Example OAI harvest finished. Found {new_items} new items.")
    example_logger.info("Example harvest call is commented out to prevent actual web requests during non-interactive tests.")
    example_logger.info("To run a real test against Alice, uncomment the run_harvest() call.")
    example_logger.info("Make sure the base_url for Alice is correct and it's accessible.")
    pass
