import os
import psycopg

def connect():
    return psycopg.connect(
        host=os.getenv("ODS_CP_HOST", "localhost"),
        port=int(os.getenv("ODS_CP_PORT", "5440")),
        dbname=os.getenv("ODS_CP_DB", "ods_cp"),
        user=os.getenv("ODS_CP_USER", "ods"),
        password=os.getenv("ODS_CP_PASSWORD", "ods"),
    )
