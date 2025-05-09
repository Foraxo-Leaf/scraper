# Scraper para Embrapa (Brasil)

Este scraper está diseñado para recolectar información sobre publicaciones del portal de Embrapa, la Empresa Brasileña de Investigación Agropecuaria.

## Características Principales

- **Estrategia de Recolección Dual:** Utiliza tanto OAI-PMH (para los repositorios Alice e Infoteca-e) como búsqueda por palabras clave en el portal principal de Embrapa para descubrir publicaciones.
- **Extracción de Metadatos:** Extrae metadatos detallados de los registros OAI y, si es necesario, de las páginas HTML de los ítems.
- **Descarga de PDFs:** Gestiona la descarga de los archivos PDF asociados a las publicaciones.
- **Gestión de Estado con SQLite:** Utiliza una base de datos SQLite para llevar un registro de los ítems procesados, su estado, los metadatos y los archivos descargados, asegurando la idempotencia y permitiendo la reanudación del proceso.
- **Configuración Flexible:** Permite configurar diversos parámetros de operación, como los repositorios a consultar, límites de registros, y comportamiento de la descarga.
- **Logging Detallado:** Genera un log detallado de todas las operaciones, errores y decisiones tomadas.
- **Reportes:** Puede generar reportes en formato JSON sobre el estado del procesamiento y los resultados de las pruebas de descarga.

## Estructura del Proyecto (Directorio `BR/`)

- `scraper.py`: Script principal que orquesta todo el proceso de scraping.
- `database_manager_br.py`: Gestiona la interacción con la base de datos SQLite.
- `html_metadata_extractor_br.py`: Extrae metadatos de páginas HTML (si es necesario como complemento a OAI).
- `keyword_searcher_br.py`: Implementa la lógica de búsqueda por palabras clave en el portal web.
- `oai_harvester_br.py`: Implementa la lógica para cosechar metadatos desde endpoints OAI-PMH.
- `resource_downloader_br.py`: Maneja la descarga de recursos (PDFs, snapshots HTML).
- `selectors.yaml`: Contiene los selectores XPath/CSS para extraer información de las páginas HTML.
- `requirements.txt`: Lista las dependencias de Python necesarias.
- `README.md`: Este archivo.
- `db/`: Directorio donde se almacena la base de datos SQLite (ej. `scraper_br.db`).
- `logs/`: Directorio para los archivos de log (ej. `BR-SCRAPER.log`).
- `output/`: Directorio para los archivos generados por el scraper:
    - `pdfs/`: Almacena los PDFs descargados, organizados por ID de ítem.
    - `html_snapshot/`: Almacena snapshots HTML de las páginas de ítems (si se configura).
    - `state_br.json`: Archivo JSON con el estado general del scraping.
    - `test_results_br.json`: Resultados de las pruebas de descarga y verificación.
- `output_package/`: (Si se ejecuta la tarea de empaquetado final) Contiene una versión empaquetada del scraper y sus resultados.

## Instalación y Configuración

1.  **Clonar el Repositorio:**
    ```bash
    git clone <URL_DEL_REPOSITORIO>
    cd <NOMBRE_DEL_REPOSITORIO>
    ```
2.  **Crear y Activar un Entorno Virtual (Recomendado):**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```
3.  **Instalar Dependencias:**
    Asegúrate de que todas las dependencias listadas en `BR/requirements.txt` estén instaladas. Puedes instalarlas con:
    ```bash
    pip install -r BR/requirements.txt
    ```
    Las dependencias típicas incluyen: `requests`, `lxml`, `PyYAML`, `cssselect`, `python-dateutil`, `selenium`.

4.  **Configuración Inicial:**
    El script `BR/scraper.py` contiene una configuración por defecto (`DEFAULT_CONFIG_BR`). Puedes modificarla directamente en el script o, idealmente, adaptarla para que cargue configuraciones desde un archivo externo si se requiere mayor flexibilidad para diferentes ejecuciones.
    Los parámetros clave a revisar son:
    - URLs de los repositorios OAI (`oai_repositories`).
    - URL base para búsquedas por palabra clave (`search_config`).
    - Límites de registros a procesar (`max_records_oai`, `max_items_keyword_search`).
    - Rutas de archivos y directorios.

## Ejecución del Scraper

Para ejecutar el scraper desde la raíz del proyecto:

```bash
python3 -m BR.scraper
```

Esto iniciará el proceso de scraping según la configuración definida. El scraper primero intentará cosechar vía OAI-PMH, y luego (si está configurado) realizará búsquedas por palabras clave.

### Opciones de Ejecución (Ejemplos a implementar/considerar)

Podrías extender el scraper para aceptar argumentos de línea de comando, por ejemplo:

- Para ejecutar solo la cosecha OAI:
  `python3 -m BR.scraper --source oai`
- Para buscar una palabra clave específica:
  `python3 -m BR.scraper --keyword "inteligencia artificial" --source keyword`
- Para limitar el número de registros:
  `python3 -m BR.scraper --max-records 100`

## Flujo del Proceso

1.  **Inicialización:** Carga la configuración, inicializa el logger y el gestor de base de datos.
2.  **Cosecha OAI (si está habilitada):**
    - Itera sobre los repositorios OAI configurados (Alice, Infoteca-e).
    - Realiza peticiones OAI-PMH (`ListRecords`) para obtener metadatos.
    - Parsea las respuestas XML y extrae los metadatos Dublin Core.
    - Registra cada ítem en la base de datos con estado `pending_pdf_link`.
3.  **Búsqueda por Palabras Clave (si está habilitada):**
    - Construye URLs de búsqueda para el portal de Embrapa.
    - Utiliza Selenium (o una librería HTTP) para obtener las páginas de resultados.
    - Parsea los resultados para identificar ítems individuales.
    - Para cada ítem encontrado, intenta extraer un enlace a su página de detalles.
    - Registra cada ítem en la base de datos con estado `pending_html_processing` (o directamente `pending_pdf_link` si se puede obtener el PDF de inmediato).
4.  **Procesamiento de HTML y Extracción de Enlaces PDF (para ítems pendientes):**
    - Itera sobre los ítems en la base de datos cuyo estado requiera procesamiento HTML (ej. `pending_html_processing` o `pending_pdf_link` si el enlace PDF no se obtuvo directamente de OAI).
    - Descarga la página HTML del ítem.
    - Utiliza `HTMLMetadataExtractorBR` y los selectores de `selectors.yaml` para extraer el enlace directo al PDF.
    - Actualiza el ítem en la base de datos con el enlace al PDF y cambia su estado a `pending_pdf_download`.
5.  **Descarga de PDFs:**
    - Itera sobre los ítems en estado `pending_pdf_download`.
    - Utiliza `ResourceDownloaderBR` para descargar el archivo PDF.
    - Calcula el hash MD5 del archivo descargado.
    - Actualiza el ítem en la base de datos con la ruta local, el hash MD5, y cambia su estado a `processed` o `download_failed`.
6.  **Generación de Reportes:** Al finalizar, puede generar archivos JSON (`state_br.json`, `test_results_br.json`) con un resumen del proceso y los resultados.

## Desafíos y Consideraciones

- **Mantenimiento de Selectores:** Los selectores HTML (XPath/CSS) pueden romperse si la estructura del sitio web de Embrapa cambia. Necesitarán ser revisados y actualizados periódicamente.
- **Bloqueos y Medidas Anti-Scraping:** Aunque OAI-PMH es robusto, el scraping de páginas HTML puede encontrar medidas anti-scraping. El uso de Selenium y la simulación de comportamiento humano (retrasos, user-agents) pueden ayudar a mitigar esto.
- **Variedad de Formatos y Estructuras:** Las diferentes secciones y repositorios de Embrapa podrían tener estructuras de metadatos o formatos de página ligeramente diferentes.
- **Manejo de Grandes Volúmenes de Datos:** Para una recolección exhaustiva, se deben considerar optimizaciones en el manejo de memoria y las operaciones de base de datos.

Este `README.md` proporciona una visión general. El código fuente de cada módulo contiene la lógica detallada de implementación.
