# Scraper para Repositorio Digital (Proyecto AR)

Este documento describe el scraper desarrollado para el Proyecto AR, diseñado para buscar, extraer metadatos y descargar documentos (principalmente PDFs) desde un repositorio digital, actualmente configurado para `https://repositorio.inta.gob.ar`.

## 1. Características Principales

*   **Descubrimiento Flexible**:
    *   **Búsqueda por Palabras Clave**: El modo principal de operación. Busca ítems en el repositorio basados en una lista de palabras clave configurables.
    *   **OAI-PMH Harvesting**: Capacidad de obtener identificadores de registros a través del protocolo OAI-PMH (configurable).
*   **Gestión de Estado Robusta**: Utiliza una base de datos SQLite (`AR/db/scraper.db`) para:
    *   Rastrear todos los ítems descubiertos y su estado de procesamiento (`pending_download`, `processing`, `processed`, `error`).
    *   Almacenar metadatos detallados extraídos de cada ítem (título, autores, fecha, resumen, URL de la página del ítem, ruta local del snapshot HTML, etc.) en formato JSON.
    *   Registrar información de cada archivo descargado (PDFs, snapshots HTML), incluyendo URL remota, ruta local, estado de la descarga, hash MD5 y tamaño.
*   **Extracción de Metadatos y PDFs**:
    *   Obtiene la página HTML de cada ítem.
    *   Guarda un snapshot del HTML en `AR/output/html_snapshot/`.
    *   Extrae metadatos estructurados (priorizando Dublin Core) y la URL del PDF principal.
    *   Maneja selectores XPath configurables (vía `AR/selectors.yaml`) y tiene mecanismos de fallback para encontrar PDFs.
*   **Descargas Resilientes**:
    *   Implementa una lógica de reintentos con backoff exponencial para las descargas de archivos, mejorando la robustez frente a problemas de red o timeouts temporales.
*   **Generación de Reportes**:
    *   `AR/output/state.json`: Un archivo JSON que resume el estado de todos los ítems procesados, sus metadatos principales, y los PDFs asociados.
    *   `AR/output/test_results.json`: Un archivo JSON con detalles de una muestra de hasta 5 PDFs descargados exitosamente, incluyendo su MD5, tamaño y metadatos clave.
*   **Configuración Flexible**: Parámetros clave como URLs, palabras clave, límites, timeouts y selectores se gestionan a través de `DEFAULT_CONFIG` en `AR/scraper.py` y el archivo `AR/selectors.yaml`.
*   **Logging Detallado**: Registra el progreso y los errores en `AR/logs/AR-SCRAPER.log`.

## 2. Cómo Ejecutar el Scraper

Sigue estos pasos desde la raíz del proyecto (`web-scraping/`):

1.  **Asegurar Entorno Virtual y Dependencias**:
    Se asume que ya tienes un entorno virtual (ej. `.venv`) y las dependencias instaladas. Si no, puedes crearlo e instalarlas:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate  # En Linux/macOS
    # o .venv\\Scripts\\activate   # En Windows

    # Instalar dependencias (asegúrate que AR/requirements.txt está actualizado)
    pip install requests lxml PyYAML
    ```
    *(Nota: El archivo `AR/requirements.txt` debe contener `requests`, `lxml`, `PyYAML`)*

2.  **Configurar el Scraper (Opcional)**:
    Puedes modificar los parámetros de ejecución directamente en el diccionario `DEFAULT_CONFIG` al inicio del archivo `AR/scraper.py`. Los más relevantes para una prueba son:
    *   `"mode"`: Puede ser `"keyword_search"` (por defecto) o `"oai"`.
    *   `"keywords"`: Lista de palabras clave a buscar si `mode` es `"keyword_search"`. Ejemplo: `["trigo", "maiz"]`.
    *   `"max_search_pages"`: Límite de páginas de resultados a procesar por palabra clave.
    *   `"max_records"`: Límite de registros a obtener si `mode` es `"oai"`.
    *   `"selectors_file"`: Ruta al archivo YAML con los selectores XPath (por defecto `AR/selectors.yaml`).

3.  **Ejecutar el Scraper**:
    ```bash
    source .venv/bin/activate  # Si no está activo
    python AR/scraper.py
    ```

## 3. Estrategia Empleada y Dificultades Superadas

*   **Estrategia General**:
    1.  **Inicialización**: Configura logging, directorios y la base de datos SQLite (creando tablas y añadiendo columnas si es necesario).
    2.  **Descubrimiento**:
        *   Si modo `keyword_search`: Utiliza `KeywordSearcher` para iterar sobre las palabras clave, navegar por las páginas de resultados del repositorio, extraer URLs de ítems y registrarlos/actualizarlos en la BD con estado `pending_download`.
        *   Si modo `oai`: Utiliza `OAIHarvester` para obtener identificadores OAI, construir las URLs de las páginas de los ítems y registrarlos/actualizarlos en la BD.
    3.  **Procesamiento de Ítems**:
        *   Obtiene de la BD los ítems marcados como `pending_download` o con ciertos estados de error.
        *   Para cada ítem:
            *   Marca el ítem como `processing`.
            *   Utiliza `HTMLMetadataExtractor` para descargar el HTML de la página del ítem y guardar un snapshot.
            *   Extrae metadatos (DC, etc.) y la URL del PDF.
            *   Almacena los metadatos extraídos en la columna `metadata_json` de la tabla `items` en la BD.
            *   Utiliza `ResourceDownloader` para descargar el PDF (y otros recursos si se configuran). Este módulo incluye reintentos, cálculo de MD5 y tamaño.
            *   Actualiza el estado del ítem (`processed` o `error`) y de los archivos en la BD.
    4.  **Generación de Reportes**: Al finalizar, genera `AR/output/state.json` y `AR/output/test_results.json` a partir de los datos en la BD.

*   **Dificultades Superadas**:
    *   **Gestión de Estado Persistente**: La migración de un registro simple a una base de datos SQLite ha permitido una gestión de estado mucho más granular y robusta, facilitando la reanudación y el reprocesamiento.
    *   **Evolución del Esquema de BD**: Se implementó lógica para añadir columnas (`metadata_json`) a tablas existentes mediante `ALTER TABLE` para no perder datos entre ejecuciones al modificar la estructura.
    *   **Timeouts en Descargas**: La implementación de una lógica de reintentos con backoff exponencial en `ResourceDownloader` ha mejorado la resiliencia frente a descargas lentas o fallos temporales de red.
    *   **Errores de Codificación y Parseo**: Se han añadido manejadores para diferentes encodings en respuestas OAI y HTML.
    *   **Extracción de PDFs**: Se utilizan selectores XPath configurables con un fallback a búsqueda genérica de enlaces PDF en el HTML.

## 4. Estructura de Carpetas y Archivos Relevantes

El scraper opera principalmente dentro del directorio `AR/`. Durante su ejecución, genera archivos en `AR/db/`, `AR/logs/` y `AR/output/`. Al finalizar, crea un paquete consolidado en `AR/output_package/`.

**Directorios de Trabajo (dentro de `AR/`):**
```
AR/
├── db/
│   └── scraper.db             # Base de datos SQLite con el estado y metadatos.
├── logs/
│   └── AR-SCRAPER.log         # Log detallado de la ejecución.
├── output/                     # Directorio de trabajo durante la ejecución
│   ├── html_snapshot/         # Snapshots HTML de las páginas de ítems.
│   │   └── <item_id>/
│   │       └── <nombre_snapshot>.html
│   ├── pdf/                   # PDFs descargados.
│   │   └── <item_id>/
│   │       └── <nombre_pdf>.pdf
│   ├── state.json             # Resumen del estado de todos los ítems procesados.
│   └── test_results.json      # Resultados de verificación para una muestra de PDFs.
├── README.md                  # Este archivo.
├── requirements.txt           # Dependencias Python.
├── scraper.py                 # El script principal del scraper.
└── selectors.yaml             # Selectores XPath para la extracción de datos HTML.
```

**Paquete de Salida Final (generado en `AR/output_package/`):**
```
AR/output_package/
├── scraper.py                 # El script ejecutable.
├── selectors.yaml             # Archivo de selectores.
├── README.md                  # Una copia de este README.
├── test_results.json          # Resultados de verificación para una muestra de PDFs.
└── docs/
    └── sample_pdfs/
        └── (hasta 5 PDFs de muestra de los descargados)
```

### Ejemplos de Archivos de Salida

*   **`AR/output/state.json` (extracto de una entrada):**
    ```json
    {
        "url": "https://repositorio.inta.gob.ar/handle/20.500.12123/xxxx",
        "metadata": {
            "title": "Título del Documento Ejemplo",
            "authors": ["Autor Uno", "Autor Dos"],
            "publication_date": "2023-01-15"
        },
        "html_path": "AR/output/html_snapshot/yy/snapshot_name.html",
        "pdfs": [
            {
                "url": "https://repositorio.inta.gob.ar/bitstream/handle/xxx/file.pdf",
                "local_path": "AR/output/pdf/yy/file.pdf",
                "downloaded": true
            }
        ],
        "analyzed": true
    }
    ```

*   **`AR/output/test_results.json` (extracto de una entrada):**
    ```json
    {
        "item_page_url": "https://repositorio.inta.gob.ar/handle/20.500.12123/xxxx",
        "local_pdf_path": "AR/output/pdf/yy/file.pdf",
        "md5_hash": "abcdef1234567890abcdef1234567890",
        "file_size_bytes": 1234567,
        "metadata": {
            "title": "Título del Documento Ejemplo",
            "authors": ["Autor Uno", "Autor Dos"],
            "publication_date": "2023-01-15"
        }
    }
    ```

## 5. Próximos Pasos y Mejoras Potenciales

*   Crear/actualizar `AR/requirements.txt` de forma más robusta (ej. `pip freeze > AR/requirements.txt`).
*   Mejorar la configuración para que sea más externa (ej. un solo archivo de configuración principal JSON o YAML que englobe `DEFAULT_CONFIG` y `selectors.yaml`).
*   Refinar selectores en `selectors.yaml` para mayor precisión si se encuentran repositorios con estructuras HTML diferentes.
*   Añadir más opciones de filtrado o procesamiento para `get_items_to_process`.
*   Considerar la gestión de diferentes `User-Agent` si se detectan bloqueos.
