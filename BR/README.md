# Scraper para Publicaciones de Embrapa (Brasil)

Este scraper está diseñado para recolectar información sobre publicaciones de Embrapa, el organismo de investigación agrícola de Brasil. Utiliza una combinación de cosecha vía OAI-PMH de los repositorios institucionales Alice e Infoteca-e, y búsqueda por palabras clave en el portal principal de Embrapa.

## Características Principales

*   **Doble Estrategia de Cosecha:**
    *   **OAI-PMH:** Interactúa con los endpoints OAI-PMH de los repositorios Alice (`https://www.alice.cnptia.embrapa.br/alice-oai/request`) e Infoteca-e (`https://www.infoteca.cnptia.embrapa.br/infoteca-oai/request`) para obtener listados de registros y metadatos estructurados (Dublin Core). Esta es la vía preferida para obtener información robusta y completa.
    *   **Búsqueda por Palabras Clave:** Realiza búsquedas en `https://www.embrapa.br/busca-de-publicacoes` para encontrar publicaciones relevantes a las palabras clave configuradas. Esta vía utiliza Selenium para interactuar con el formulario de búsqueda y paginar los resultados.
*   **Extracción de Metadatos:**
    *   Para ítems OAI, parsea los metadatos Dublin Core (título, autores, fechas, resumen, identificadores, etc.).
    *   Para ítems de búsqueda por palabra clave, descarga el HTML de la página del ítem y extrae metadatos utilizando selectores XPath definidos en `BR/selectors.yaml`.
*   **Descarga de PDFs:**
    *   Identifica enlaces directos a PDFs a partir de los metadatos OAI (`dc:identifier` que son URLs de PDF) o los extrae de las páginas HTML de los ítems.
    *   Descarga los PDFs y los organiza en directorios locales.
*   **Gestión de Estado con SQLite:**
    *   Utiliza una base de datos SQLite (`BR/db/scraper_br.db`) para llevar un registro de todos los ítems procesados, su estado (ej. `pending_html_processing`, `pending_pdf_link`, `awaiting_pdf_download`, `processed`, `error_...`), metadatos, y la ubicación de los archivos descargados (HTML snapshots, PDFs).
    *   Esto permite la reanudación del scraper y evita el reprocesamiento de ítems ya completados.
*   **Configuración Flexible:**
    *   La configuración principal se encuentra dentro de `BR/scraper.py` en el diccionario `DEFAULT_CONFIG_BR`.
    *   Los selectores XPath para el parseo HTML se definen en `BR/selectors.yaml`.
*   **Generación de Reportes:**
    *   `BR/output/state_br.json`: Un archivo JSON que contiene un resumen del estado de todos los ítems procesados, incluyendo sus metadatos y la información de los PDFs asociados.
    *   `BR/output/test_results_br.json`: Un archivo JSON con detalles de una muestra de PDFs descargados, incluyendo su hash MD5 y metadatos básicos.
*   **Logging Detallado:**
    *   Registra el progreso, las decisiones clave, y los errores en `BR/logs/BR-SCRAPER.log`.

## Estructura del Proyecto

```
BR/
├── db/
│   └── scraper_br.db         # Base de datos SQLite
├── docs/
│   └── sample_pdfs/          # PDFs de muestra para verificación (se copian aquí)
├── logs/
│   └── BR-SCRAPER.log        # Archivo de log principal
├── output/
│   ├── html_snapshot/        # Snapshots HTML guardados (para ítems de búsqueda por palabra clave)
│   │   └── <item_id>/...html
│   ├── pdfs/                 # PDFs descargados
│   │   └── <item_id>/...pdf
│   ├── state_br.json         # Reporte de estado de todos los ítems
│   └── test_results_br.json  # Reporte de verificación de PDFs de muestra
├── output_package/           # Directorio para el paquete final (manual)
├── __init__.py
├── oai_harvester_br.py       # Lógica para la cosecha OAI-PMH
├── database_manager_br.py    # Gestión de la base de datos SQLite
├── resource_downloader_br.py # Descarga de recursos (PDFs, HTML)
├── html_metadata_extractor_br.py # Extracción de metadatos desde HTML
├── keyword_searcher_br.py    # Lógica para búsqueda por palabra clave y Selenium
├── scraper.py                # Script principal del scraper y orquestación
├── selectors.yaml            # Selectores XPath para parseo HTML
└── requirements.txt          # Dependencias de Python
```

## Requisitos

*   Python 3.7+
*   Google Chrome (o Chromium) instalado (para la búsqueda por palabra clave con Selenium)
*   ChromeDriver compatible con la versión de Chrome/Chromium instalado y accesible en el PATH del sistema, o su ruta especificada en la configuración (`chromedriver_path`).

## Instalación

1.  **Clonar el Repositorio (si aplica):**
    ```bash
    # git clone ...
    # cd BR-scraper-directory 
    ```

2.  **Crear un Entorno Virtual (Recomendado):**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate 
    ```

3.  **Instalar Dependencias:**
    ```bash
    pip install -r BR/requirements.txt
    ```

4.  **Configurar ChromeDriver (si es necesario):**
    *   Asegúrate de que ChromeDriver esté en tu PATH.
    *   Alternativamente, puedes editar `BR/scraper.py` y establecer la variable `chromedriver_path` dentro de `DEFAULT_CONFIG_BR` a la ruta absoluta de tu ejecutable ChromeDriver. Por defecto es `None`, lo que implica que Selenium intentará encontrarlo en el PATH.

## Configuración

El scraper se configura principalmente a través del diccionario `DEFAULT_CONFIG_BR` en el archivo `BR/scraper.py`. Algunas configuraciones clave incluyen:

*   **Límites de Cosecha OAI:**
    *   `max_oai_records_alice`: Número máximo de registros a cosechar de Alice (ej. `20`). Poner en `None` para ilimitado.
    *   `max_oai_records_infoteca`: Número máximo de registros a cosechar de Infoteca-e (ej. `20`). Poner en `None` para ilimitado.
*   **Búsqueda por Palabras Clave:**
    *   `keyword_search_keywords`: Lista de palabras clave a buscar (ej. `["maiz", "soja"]`). Dejar vacío `[]` para omitir.
    *   `keyword_max_pages`: Número máximo de páginas de resultados a procesar por palabra clave.
*   **Límites de Procesamiento de Ítems:**
    *   `max_html_processing_items`: Máximo de ítems a procesar para extracción de metadatos HTML por ejecución.
    *   `max_pdf_link_extraction_items`: Máximo de ítems a procesar para extracción de enlaces PDF por ejecución.
    *   `max_pdf_download_items`: Máximo de PDFs a descargar por ejecución.
*   **Delays:**
    *   `oai_request_delay`: Segundos de espera entre peticiones OAI.
    *   `download_delay_seconds`: Segundos de espera entre descargas de archivos.
*   **Rutas de Archivos y Directorios:** Definidas para logs, base de datos, salidas, etc.

Los selectores XPath para la extracción de metadatos de páginas HTML (usados por `KeywordSearcherBR` y `HTMLMetadataExtractorBR`) se encuentran en `BR/selectors.yaml`.

## Ejecución del Scraper

1.  **Asegúrate de que el entorno virtual esté activado** (si usaste uno).
2.  **Desde el directorio raíz del proyecto (donde está el directorio `BR/`), ejecuta el scraper usando el módulo:**
    ```bash
    python3 -m BR.scraper
    ```
3.  **Para una prueba limpia, especialmente después de cambios significativos o para probar la cosecha OAI desde cero, se recomienda borrar la base de datos:**
    ```bash
    # rm BR/db/scraper_br.db  # (Opcional, pero recomendado para pruebas limpias)
    # python3 -m BR.scraper
    ```

El scraper registrará su progreso en la consola y en el archivo `BR/logs/BR-SCRAPER.log`.

## Flujo del Scraper

1.  **Inicialización:** Carga configuración, inicializa logging y componentes (DB manager, OAI harvester, etc.).
2.  **Cosecha OAI-PMH (si está configurada):**
    *   Para cada repositorio OAI activado (Alice, Infoteca-e):
        *   Realiza peticiones OAI para obtener registros.
        *   Parsea los metadatos Dublin Core.
        *   Identifica la URL de la página del ítem y, si está disponible, una URL directa al PDF.
        *   Registra los ítems y sus metadatos en la base de datos con estado inicial `pending_pdf_link`.
3.  **Búsqueda por Palabras Clave (si está configurada):**
    *   Para cada palabra clave:
        *   Utiliza Selenium y ChromeDriver para navegar a `https://www.embrapa.br/busca-de-publicacoes`, ingresar la palabra clave y enviar el formulario.
        *   Itera sobre las páginas de resultados.
        *   Extrae las URLs de las páginas de los ítems individuales.
        *   Registra los nuevos ítems en la base de datos con estado inicial `pending_html_processing`.
4.  **Procesamiento de HTML y Metadatos:**
    *   Busca en la base de datos ítems con estado `pending_html_processing`.
    *   Para cada uno:
        *   Descarga (o usa snapshot si ya existe) el contenido HTML de la página del ítem.
        *   Utiliza `HTMLMetadataExtractorBR` y `BR/selectors.yaml` para extraer metadatos (título, autores, etc.).
        *   Guarda los metadatos y actualiza el estado del ítem a `pending_download`.
5.  **Extracción de Enlaces PDF:**
    *   Busca ítems con estado `pending_download`.
    *   Para cada uno:
        *   Si los metadatos ya contienen un `pdf_direct_url` (proveniente de OAI), se usa ese.
        *   De lo contrario (típicamente para ítems de búsqueda por palabra clave), intenta extraer un enlace PDF desde el HTML de la página del ítem (si no se hizo ya o si el `item_page_url` no es un PDF en sí mismo).
        *   Si se encuentra un enlace PDF, se actualizan los metadatos del ítem y su estado cambia a `awaiting_pdf_download`.
6.  **Descarga de PDFs:**
    *   Busca ítems con estado `awaiting_pdf_download`.
    *   Para cada uno, descarga el archivo PDF referenciado por `pdf_direct_url`.
    *   Calcula el hash MD5 y el tamaño del archivo.
    *   Actualiza la base de datos con la ruta local, el hash, el tamaño y cambia el estado a `processed`.
7.  **Generación de Reportes Finales:**
    *   Crea/actualiza `BR/output/state_br.json` con el resumen de todos los ítems.
    *   Crea/actualiza `BR/output/test_results_br.json` con una muestra de los PDFs descargados.

## Desafíos Superados

*   **Integración de dos fuentes de datos:** Se implementó tanto la cosecha OAI-PMH (preferida) como la búsqueda por palabras clave con Selenium para maximizar la cobertura.
*   **Parseo de Respuestas OAI:** Se desarrolló lógica para manejar las respuestas XML de los endpoints OAI, incluyendo la extracción de metadatos Dublin Core y la identificación de URLs de ítems y PDFs directos. Inicialmente hubo problemas para identificar correctamente los `item_page_url` y parsear los metadatos de todos los repositorios, lo que se solucionó refinando los XPaths y la lógica de extracción de identificadores.
*   **Manejo de Búsqueda Web con Selenium:** Se implementó el uso de Selenium para la interacción con el formulario de búsqueda de Embrapa, incluyendo esperas explícitas para asegurar la carga de contenido dinámico.
*   **Flujo de Estados de Ítems:** Se definió un flujo de estados claro para cada ítem, gestionado a través de la base de datos, para permitir el procesamiento por etapas y la reanudabilidad.
*   **Identificación de Enlaces PDF:** Se refinó la lógica para obtener enlaces PDF tanto de los metadatos OAI (donde a veces el `dc:identifier` es el PDF mismo) como de la extracción de contenido HTML.

## Salidas del Scraper

*   **Archivos PDF:** Descargados en `BR/output/pdfs/<item_id>/<nombre_archivo.pdf>`
*   **Snapshots HTML:** (Para ítems de búsqueda por palabra clave) Guardados en `BR/output/html_snapshot/<item_id>/<item_id>_snapshot.html`
*   **Logs:** `BR/logs/BR-SCRAPER.log`
*   **Base de Datos:** `BR/db/scraper_br.db`
*   **Reporte de Estado:** `BR/output/state_br.json`
    ```json
    [
        {
            "url": "http://www.alice.cnptia.embrapa.br/alice/handle/doc/105403",
            "metadata": {
                "title": "Título del Documento Ejemplo",
                "authors": ["Autor Uno", "Autor Dos"],
                "publication_date": "2023-01-15"
            },
            "html_path": null, // o "BR/output/html_snapshot/123/123_snapshot.html"
            "pdfs": [
                {
                    "url": "http://www.alice.cnptia.embrapa.br/alice/bitstream/doc/105403/1/Pab8901.pdf",
                    "local_path": "BR/output/pdfs/41/Pab8901.pdf",
                    "downloaded": true
                }
            ],
            "analyzed": true // true si el estado es "processed"
        },
        // ... más ítems
    ]
    ```
*   **Resultados de Pruebas de PDF:** `BR/output/test_results_br.json`
    ```json
    [
        {
            "item_page_url": "http://www.alice.cnptia.embrapa.br/alice/handle/doc/105403",
            "local_pdf_path": "BR/output/pdfs/41/Pab8901.pdf",
            "md5_hash": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
            "file_size_bytes": 1234567,
            "metadata": {
                "title": "Título del Documento Ejemplo",
                "authors": ["Autor Uno", "Autor Dos"],
                "publication_date": "2023-01-15"
            }
        },
        // ... más resultados de prueba
    ]
    ```

Este README debería proporcionar una buena base. 
