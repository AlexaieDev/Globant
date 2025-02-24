import os
import psycopg2
from fastapi import HTTPException

def get_db_connection():
    try:
        # Usar la URL externa de Render
        conn = psycopg2.connect(
            dbname="bd_pruebas",
            user="users",
            password="JpjUg1Nm3xmCJ0LkeCl7VeUe5p1hEXsE",
            host="dpg-cuuaf01opnds739vcvi0-a.oregon-postgres.render.com",  # URL externa
            port="5432"
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión a la base de datos: {str(e)}")