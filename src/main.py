from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
from src.database import get_db_connection, init_db
import os

app = FastAPI()

# Inicializar la base de datos al iniciar
init_db()

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    conn = None
    try:
        df = pd.read_csv(file.file)
        if len(df) > 3000:  # Cambiado de 1000 a 3000
            raise HTTPException(status_code=400, detail="Límite de 3000 filas excedido")

        # Verificar que las columnas requeridas estén presentes
        required_columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
        if not all(column in df.columns for column in required_columns):
            raise HTTPException(status_code=400, detail="El archivo CSV no tiene las columnas requeridas")

        conn = get_db_connection()
        cursor = conn.cursor()

        # Convertir valores vacíos a NULL en columnas que lo permiten
        df['department_id'] = df['department_id'].replace('', None)
        df['job_id'] = df['job_id'].replace('', None)

        # Convertir a lista de tuplas
        data = df[['id', 'name', 'datetime', 'department_id', 'job_id']].values.tolist()

        # Insertar datos
        cursor.executemany(
            '''INSERT INTO hired_employees 
               (id, name, datetime, department_id, job_id)
               VALUES (%s, %s, %s, %s, %s)''',
            data
        )
        conn.commit()
        return {"message": f"{len(df)} filas insertadas"}
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    finally:
        if conn:
            conn.close()

@app.get("/hired-by-quarter/")
def hired_by_quarter():
    conn = get_db_connection()
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
    conn.close()
    return [{"department": row[0], "job": row[1], "Q1": row[2], "Q2": row[3], "Q3": row[4], "Q4": row[5]} for row in results]

@app.get("/above-average-hired/")
def above_average_hired():
    conn = get_db_connection()
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
    conn.close()
    return [{"id": row[0], "department": row[1], "hired": row[2]} for row in results]
