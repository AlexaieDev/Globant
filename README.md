## Abrimos ventana del terminal para activar venv y uvicorn
------------------------------------------------------------
C:\proyecto\Test-API
venv\Scripts\activate
uvicorn src.main:app --reload
--------------------------------------------------------
--En una segunda ventana del terminal
## Endpoints
C:\proyecto\Test-API
venv\Scripts\activate
curl.exe -X POST -F "file=@upload-csv\hired_employees.csv" http://localhost:8000/upload-csv/
curl.exe -X GET "http://localhost:8000/hired-by-quarter/"
curl.exe -X GET "http://localhost:8000/above-average-hired/"
--------------------------------------------------------
## TEST Automatico
--En la segunda ventana del terminal
pytest test\test_api.py -v 
--------------------------------------------------------
## Swagger http://localhost:8000/docs#/
--View build details: docker-desktop://dashboard/build/desktop-linux/desktop-linux/tht496kyz0caqtwrrce1v3vao
## Endpoints Principales
-------------------------------------------------------------------------
Método	Endpoint	            Descripción
POST	/upload-csv/	        Carga archivos CSV a la base de datos
GET	    /hired-by-quarter/	    Métricas de contratación por trimestre
GET	    /above-average-hired/	Departamentos con contratación superior
-------------------------------------------------------------------------

 Estructura del Proyecto
Copy
test-api/
├── src/
│   ├── main.py                     # Punto de entrada de la API
│   ├── database.py                 # Configuración de PostgreSQL
├── test/
│   └── test_api.py                 # Pruebas automatizadas
├── upload-csv/
│   └── hired_employees.csv         # Archivo de subida (1999 registros)
├── data/                           # Archivos CSV de ejemplo
├── Dockerfile
├── render.yaml
└── requirements.txt