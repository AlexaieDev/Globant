from fastapi import FastAPI, UploadFile, File, HTTPException
from src.database import get_db_connection, init_db
import logging
import csv
import io

app = FastAPI()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar la base de datos al iniciar
init_db()

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    conn = None
    try:
        # Leer el contenido del archivo y manejar BOM
        contents = await file.read()
        content_str = contents.decode("utf-8-sig").strip()  # <--- Usar utf-8-sig para eliminar BOM
        
        # Verificar caracteres especiales
        logger.info(f"Primeros 50 caracteres del CSV: {content_str[:50]}")  # <--- Para debug
        
        # Parsear el CSV
        content_io = io.StringIO(content_str)
        reader = csv.reader(
            content_io,
            delimiter=",",
            skipinitialspace=True,  # <--- Ignorar espacios después del delimitador
            quoting=csv.QUOTE_MINIMAL
        )
        
        data = []
        for row in reader:
            logger.info(f"Fila cruda: {row}")  # <--- Para debug
            if len(row) != 5:
                raise HTTPException(
                    status_code=400,
                    detail=f"Fila inválida: {row}. Debe tener 5 columnas.",
                )
            
            try:
                # Convertir campos numéricos (manejar cadenas vacías)
                id = int(row[0])
                name = row[1].strip() or None
                datetime = row[2].strip() or None
                department_id = int(row[3]) if row[3].strip() else None
                job_id = int(row[4]) if row[4].strip() else None
            except ValueError as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Error en fila {row}: {str(e)}",
                )
            
            data.append((id, name, datetime, department_id, job_id))

        logger.info(f"Filas procesadas: {len(data)}")

        conn = get_db_connection()
        cursor = conn.cursor()

        # Insertar datos
        cursor.executemany(
            """
            INSERT INTO hired_employees 
                (id, name, datetime, department_id, job_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            data,
        )
        conn.commit()

        return {"message": f"{len(data)} filas insertadas"}
    except Exception as e:
        logger.error(f"Error al cargar el archivo CSV: {str(e)}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")
    finally:
        if conn:
            conn.close()

@app.get("/hired-by-quarter/")
def hired_by_quarter():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT d.department, j.job,
                   SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime::timestamp) = 1 THEN 1 ELSE 0 END) as Q1,
                   SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime::timestamp) = 2 THEN 1 ELSE 0 END) as Q2,
                   SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime::timestamp) = 3 THEN 1 ELSE 0 END) as Q3,
                   SUM(CASE WHEN EXTRACT(QUARTER FROM e.datetime::timestamp) = 4 THEN 1 ELSE 0 END) as Q4
            FROM hired_employees e
            JOIN departments d ON e.department_id = d.id
            JOIN jobs j ON e.job_id = j.id
            WHERE EXTRACT(YEAR FROM e.datetime::timestamp) = 2021
            GROUP BY d.department, j.job
            ORDER BY d.department, j.job
        ''')
        results = cursor.fetchall()
        return [{"department": row[0], "job": row[1], "Q1": row[2], "Q2": row[3], "Q3": row[4], "Q4": row[5]} for row in results]

@app.get("/above-average-hired/")
def above_average_hired():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            WITH avg_hired AS (
                SELECT AVG(count) as avg_count
                FROM (
                    SELECT COUNT(*) as count
                    FROM hired_employees
                    WHERE EXTRACT(YEAR FROM datetime::timestamp) = 2021
                    GROUP BY department_id
                )
            )
            SELECT d.id, d.department, COUNT(*) as hired
            FROM hired_employees e
            JOIN departments d ON e.department_id = d.id
            WHERE EXTRACT(YEAR FROM e.datetime::timestamp) = 2021
            GROUP BY d.id, d.department
            HAVING COUNT(*) > (SELECT avg_count FROM avg_hired)
            ORDER BY hired DESC
        ''')
        results = cursor.fetchall()
        return [{"id": row[0], "department": row[1], "hired": row[2]} for row in results]
