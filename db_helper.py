# db_helper.py
import os
import struct
import logging
import time
import pyodbc
from itertools import chain, repeat
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)

# ── Connection Pooling: ODBC Driver Manager 풀링 명시 활성화 ──
# Premium EP1 상시 인스턴스에서 커넥션 풀이 유지되어 재연결 비용 절감
pyodbc.pooling = True

# Fabric SQL Endpoint 전용 상수
SQL_COPT_SS_ACCESS_TOKEN = 1256
FABRIC_TOKEN_SCOPE = "https://database.windows.net/.default"

# ── MI 토큰 캐싱: 모듈 레벨 credential 싱글턴 ──
# azure-identity 내장 in-memory 토큰 캐싱 활용 (매 호출 시 재생성 방지)
_credential = DefaultAzureCredential()

def _prewarm_token():
    """Premium 인스턴스 시작 시 MI 토큰을 미리 캐싱 (IMDS/az login 워밍)"""
    try:
        token = _credential.get_token(FABRIC_TOKEN_SCOPE)
        logger.info("MI 토큰 프리웜 완료 (만료: %s)", token.expires_on)
    except Exception as e:
        logger.warning("MI 토큰 프리웜 실패 (무시, 런타임에 재시도): %s", e)

_prewarm_token()

# 모듈 레벨 커넥션 캐시 (Premium EP1은 인스턴스 상시 유지 → 캐시 수명 연장)
_cached_conn = None


def _get_token_bytes() -> bytes:
    """
    Managed Identity(Azure 배포) 또는 DefaultAzureCredential(로컬 az login)으로
    Fabric 접근 토큰을 가져와 pyodbc용 바이트 구조체로 변환

    Azure SQL / Fabric Warehouse 공식 인증 방식:
      1. 토큰을 UTF-8로 인코딩
      2. 각 바이트 사이에 null(0x00) 삽입 (Windows UTF-16-LE 에뮬레이션)
      3. struct.pack으로 길이 헤더 + 바이트 패킹
    Ref: https://learn.microsoft.com/en-us/sql/connect/odbc/using-azure-active-directory
    """
    token = _credential.get_token(FABRIC_TOKEN_SCOPE).token

    # UTF-8 인코딩 후 각 바이트 사이에 null 삽입
    token_bytes   = token.encode("UTF-8")
    encoded_bytes = bytes(chain.from_iterable(zip(token_bytes, repeat(0))))
    token_struct  = struct.pack(f"<i{len(encoded_bytes)}s",
                                len(encoded_bytes), encoded_bytes)
    return token_struct


def _create_new_connection() -> pyodbc.Connection:
    """
    Gold Layer Warehouse에 새 pyodbc Connection 생성
    환경변수: FABRIC_SQL_ENDPOINT, FABRIC_DB_NAME

    연결 방식:
      Azure 환경: Authentication=ActiveDirectoryMsi (ODBC 드라이버 내장 MI 인증)
      로컬 환경: SQL_COPT_SS_ACCESS_TOKEN (DefaultAzureCredential → az login 토큰)
    """
    endpoint = os.environ["FABRIC_SQL_ENDPOINT"]
    database = os.environ["FABRIC_DB_NAME"]

    # Azure 환경 (Managed Identity): ActiveDirectoryMsi
    conn_str_msi = (
        "Driver={ODBC Driver 17 for SQL Server};"
        f"Server={endpoint},1433;"
        f"Database={database};"
        "Encrypt=Yes;"
        "TrustServerCertificate=No;"
        "Authentication=ActiveDirectoryMsi;"
        "Connection Timeout=30;"
    )
    try:
        conn = pyodbc.connect(conn_str_msi)
        conn.autocommit = True
        return conn
    except pyodbc.Error as e:
        logger.warning("ActiveDirectoryMsi 연결 실패, 토큰 방식으로 재시도: %s", e)

    # 로컬/대체 환경: 수동 토큰 (DefaultAzureCredential)
    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
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


def get_connection() -> pyodbc.Connection:
    """
    캐시된 커넥션 반환. 끊어졌으면 재생성.
    Premium EP1 상시 인스턴스에서 커넥션이 장기 유지됨.
    사전 SELECT 1 검증 제거 — 오류 시 execute_query/execute_scalar의
    except 블록에서 _invalidate_connection() 후 재시도로 처리.
    """
    global _cached_conn
    if _cached_conn is not None:
        return _cached_conn

    t0 = time.time()
    conn = _create_new_connection()
    elapsed = time.time() - t0
    logger.info("DB 신규 연결 소요시간: %.2f초", elapsed)
    _cached_conn = conn
    return conn


def execute_query(sql: str, params: list = None) -> list[dict]:
    """
    SELECT 쿼리 실행 후 dict 리스트 반환
    캐시된 커넥션 사용 (close하지 않음)
    연결 끊김 시 1회 재시도 (SELECT 1 사전검증 제거에 따른 보완)
    """
    for attempt in range(2):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, params or [])
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except pyodbc.Error as e:
            _invalidate_connection()
            if attempt == 0:
                logger.warning("execute_query 연결 오류, 재시도: %s", e)
                continue
            logger.error("execute_query 재시도 실패: %s | SQL: %.200s", str(e), sql)
            raise


def execute_scalar(sql: str, params: list = None):
    """
    단일 값(COUNT 등) 반환
    캐시된 커넥션 사용 (close하지 않음)
    연결 끊김 시 1회 재시도 (SELECT 1 사전검증 제거에 따른 보완)
    """
    for attempt in range(2):
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, params or [])
            row = cursor.fetchone()
            return row[0] if row else 0
        except pyodbc.Error as e:
            _invalidate_connection()
            if attempt == 0:
                logger.warning("execute_scalar 연결 오류, 재시도: %s", e)
                continue
            logger.error("execute_scalar 재시도 실패: %s | SQL: %.200s", str(e), sql)
            raise


def _invalidate_connection():
    """오류 발생 시 캐시된 커넥션을 무효화"""
    global _cached_conn
    if _cached_conn is not None:
        try:
            _cached_conn.close()
        except Exception:
            pass
        _cached_conn = None