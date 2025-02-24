import os
import psycopg2
from fastapi import HTTPException

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME", "bd_pruebas"),
            user=os.getenv("DB_USER", "users"),
            password=os.getenv("DB_PASSWORD", "JpjUg1Nm3xmCJ0LkeCl7VeUe5p1hEXsE"),
            host=os.getenv("DB_HOST", "dpg-cuuaf01opnds739vcvi0-a.oregon-postgres.render.com"),
            port=os.getenv("DB_PORT", "5432")
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {str(e)}")
def init_db():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        # Crear tablas si no existen
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS departments (
                id SERIAL PRIMARY KEY,
                department TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                job TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS hired_employees (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                datetime TEXT NOT NULL,
                department_id INTEGER REFERENCES departments(id),
                job_id INTEGER REFERENCES jobs(id)
            )
        ''')
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Error inicializando la base de datos: {str(e)}")
    finally:
        conn.close()