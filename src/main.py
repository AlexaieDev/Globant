from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
from src.database import get_db_connection, init_db
import os

# Asegúrate de que esta variable se llame "app"
app = FastAPI()

# Solo inicializar la base de datos en desarrollo
if os.getenv("ENV") != "prod":
    init_db()

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    try:
        df = pd.read_csv(file.file)
        if len(df) > 1000:
            raise HTTPException(400, "Límite de 1000 filas excedido")

        conn = get_db_connection()
        cursor = conn.cursor()
        data = df[['id', 'name', 'datetime', 'department_id', 'job_id']].values.tolist()
        cursor.executemany(
            '''INSERT INTO hired_employees 
               (id, name, datetime, department_id, job_id)
               VALUES (%s, %s, %s, %s, %s)''',
            data
        )
        conn.commit()
        return {"message": f"{len(df)} filas insertadas"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        conn.close()
