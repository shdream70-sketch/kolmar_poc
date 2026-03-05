# db_helper.py
import os
import struct
import logging
import pyodbc
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)

# Fabric SQL Endpoint 전용 상수
SQL_COPT_SS_ACCESS_TOKEN = 1256
FABRIC_TOKEN_SCOPE = "https://database.windows.net/.default"


def _get_token_bytes() -> bytes:
    """
    Managed Identity(Azure 배포) 또는 DefaultAzureCredential(로컬 az login)으로
    Fabric 접근 토큰을 가져와 pyodbc용 바이트 구조체로 변환
    """
    credential = DefaultAzureCredential()
    token = credential.get_token(FABRIC_TOKEN_SCOPE).token

    # UTF-16-LE 인코딩 후 pyodbc attrs_before 형식으로 패킹
    token_bytes = token.encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token_bytes)}s",
                               len(token_bytes), token_bytes)
    return token_struct


def get_connection() -> pyodbc.Connection:
    """
    Gold Layer Warehouse에 연결된 pyodbc Connection 반환
    환경변수: FABRIC_SQL_ENDPOINT, FABRIC_DATABASE
    """
    endpoint = os.environ["FABRIC_SQL_ENDPOINT"]
    database = os.environ["FABRIC_DB_NAME"]         
    
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={endpoint},1433;"
        f"Database={database};"
        "Encrypt=Yes;"
        "TrustServerCertificate=No;"
        "Connection Timeout=30;"
    )

    conn = pyodbc.connect(
        conn_str,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: _get_token_bytes()}
    )
    conn.autocommit = True
    return conn


def execute_query(sql: str, params: list = None) -> list[dict]:
    """
    SELECT 쿼리 실행 후 dict 리스트 반환
    pyodbc Connection은 with 컨텍스트 매니저 미지원 → try/finally 처리
    """
    conn = None
    try:                                               # BUG FIX: with → try/finally
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params or [])
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except pyodbc.Error as e:
        logger.error("execute_query DB 오류: %s | SQL: %.200s", str(e), sql)
        raise
    finally:
        if conn:
            conn.close()


def execute_scalar(sql: str, params: list = None):
    """
    단일 값(COUNT 등) 반환
    pyodbc Connection은 with 컨텍스트 매니저 미지원 → try/finally 처리
    """
    conn = None
    try:                                               # BUG FIX: with → try/finally
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params or [])
        row = cursor.fetchone()
        return row[0] if row else 0
    except pyodbc.Error as e:
        logger.error("execute_scalar DB 오류: %s | SQL: %.200s", str(e), sql)
        raise
    finally:
        if conn:
            conn.close()