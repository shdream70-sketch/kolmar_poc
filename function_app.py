import sys as _sys
import os as _os
import json
import logging
from collections import defaultdict

import azure.functions as func

_pkg_path = _os.path.join(_os.path.dirname(__file__), '.python_packages', 'lib', 'site-packages')
if _pkg_path not in _sys.path:
    _sys.path.insert(0, _pkg_path)

from db_helper import execute_query, execute_scalar

app = func.FunctionApp()
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────
# 공통 유틸
# ──────────────────────────────────────────────────────

def _json_response(data, status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        body=json.dumps(data, ensure_ascii=False, default=str),
        status_code=status_code,
        mimetype="application/json"
    )

def _error_response(message: str, status_code: int = 400) -> func.HttpResponse:
    return _json_response({"error": message}, status_code)

def _safe_int(value, default: int, min_val: int = 1, max_val: int = None) -> int:
    try:
        v = int(value)
        v = max(min_val, v)
        if max_val:
            v = min(max_val, v)
        return v
    except (TypeError, ValueError):
        return default

def _get_json_body(req: func.HttpRequest) -> dict:
    try:
        return req.get_json() if req.get_body() else {}
    except ValueError:
        return None


# ──────────────────────────────────────────────────────
# 공통 SQL 빌더: 조건 만족 paper_id 서브쿼리
# ──────────────────────────────────────────────────────

def _build_paper_filter(
    mainkeyword: str = "",
    keywords: list = None,
    year_start=None,
    year_end=None,
    journal: str = None,
    publication_type: str = None
) -> tuple:
    """
    조건을 만족하는 paper_id 서브쿼리 + params 반환

    keywords[] AND 조건:
      각 키워드별 paper_id를 INTERSECT로 교집합 처리

    Returns: (subquery_sql, params_list)
    """
    conditions = []
    params = []

    if mainkeyword:
        conditions.append("pf.main_keyword = ?")
        params.append(mainkeyword)
    if year_start:
        conditions.append("pf.published_year >= ?")
        params.append(int(year_start))
    if year_end:
        conditions.append("pf.published_year <= ?")
        params.append(int(year_end))
    if journal:
        conditions.append("pf.journal_name = ?")
        params.append(journal)
    if publication_type:
        conditions.append("pf.publication_type = ?")
        params.append(publication_type)

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    base_sql = f"SELECT pf.paper_id FROM gold.paper_fact pf {where_clause}"

    if not keywords:
        return base_sql, params

    # keywords[] AND: INTERSECT
    parts = [base_sql]
    for kw in keywords:
        parts.append("""
            SELECT pkb.paper_id
            FROM gold.paper_keyword_bridge pkb
            INNER JOIN gold.keyword_dim kd ON pkb.keyword_id = kd.keyword_id
            WHERE kd.keyword_text = ?
        """)
        params.append(kw)

    return " INTERSECT ".join(parts), params


# ──────────────────────────────────────────────────────
# 1. GET /api/v1/keywords/search
# ──────────────────────────────────────────────────────

@app.route(route="v1/keywords/search", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def keywords_search(req: func.HttpRequest) -> func.HttpResponse:
    """
    키워드 Like 검색

    Query Params:
      text     (string, 필수): 검색 문자열 → like '%text%'
      pageno   (int,    선택): 기본 1
      pagesize (int,    선택): 기본 20

    Response:
      {
        "pageno": 1, "pagesize": 3, "totalcount": 5, "totalpages": 2,
        "items": [
          { "keyword": "haas extract", "categories": ["active_ingredient"] },
          ...
        ]
      }
    """
    text     = req.params.get("text", "").strip()
    pageno   = _safe_int(req.params.get("pageno"),   1,  min_val=1)
    pagesize = _safe_int(req.params.get("pagesize"), 20, min_val=1, max_val=100)
    offset   = (pageno - 1) * pagesize

    if not text:
        return _error_response("text 파라미터는 필수입니다.", 400)

    pattern = f"%{text}%"

    try:
        totalcount = execute_scalar("""
            SELECT COUNT(DISTINCT kd.keyword_text)
            FROM gold.keyword_dim kd
            WHERE kd.keyword_text LIKE ? OR kd.normalized_text LIKE ?
        """, [pattern, pattern])

        totalpages = (totalcount + pagesize - 1) // pagesize if totalcount > 0 else 0

        # [BUG FIX] N+1 쿼리 제거: keyword + category를 한 번의 JOIN 쿼리로 조회
        # 기존: 키워드마다 루프로 categories 별도 조회 (페이지당 최대 100회 DB 호출)
        # 수정: keyword_dim + paper_keyword_bridge JOIN으로 한 번에 조회 후 Python groupby
        kw_cat_rows = execute_query("""
            SELECT DISTINCT kd.keyword_text, pkb.keyword_type
            FROM gold.keyword_dim kd
            LEFT JOIN gold.paper_keyword_bridge pkb ON kd.keyword_id = pkb.keyword_id
            WHERE kd.keyword_text LIKE ? OR kd.normalized_text LIKE ?
            ORDER BY kd.keyword_text, pkb.keyword_type
        """, [pattern, pattern])

        # Python에서 keyword 기준으로 groupby 후 페이징 처리
        kw_map = defaultdict(list)
        for row in kw_cat_rows:
            kw = row["keyword_text"]
            kt = row["keyword_type"]
            if kt and kt not in kw_map[kw]:
                kw_map[kw].append(kt)

        # 정렬 후 페이징
        sorted_keywords = sorted(kw_map.keys())
        paged_keywords  = sorted_keywords[offset: offset + pagesize]

        items = [
            {"keyword": kw, "categories": kw_map[kw]}
            for kw in paged_keywords
        ]

        return _json_response({
            "pageno": pageno, "pagesize": pagesize,
            "totalcount": totalcount, "totalpages": totalpages,
            "items": items
        })

    except Exception as e:
        logger.exception("keywords_search error")
        return _error_response(str(e), 500)


# ──────────────────────────────────────────────────────
# 2. POST /api/v1/keywords/mainkeywords
# ──────────────────────────────────────────────────────

@app.route(route="v1/keywords/mainkeywords", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def keywords_mainkeywords(req: func.HttpRequest) -> func.HttpResponse:
    """
    main_keyword 목록 조회

    Request Body:
      {
        "keywords": ["nanoparticle", "antioxidant"],  // 필수, AND 조건
        "pageno": 1,
        "pagesize": 5
      }

    Response:
      {
        "pageno": 1, "pagesize": 5, "totalcount": 18, "totalpages": 4,
        "mainkeywords": ["zein", "chitosan", "silk fibroin", ...]
      }
      totalcount: 조건 만족 논문 수
    """
    body = _get_json_body(req)
    if body is None:
        return _error_response("Invalid JSON body")

    keywords = body.get("keywords", [])
    pageno   = _safe_int(body.get("pageno"),   1,  min_val=1)
    pagesize = _safe_int(body.get("pagesize"), 20, min_val=1, max_val=100)
    offset   = (pageno - 1) * pagesize

    if not keywords or not isinstance(keywords, list):
        return _error_response("keywords 배열은 필수이며 1개 이상이어야 합니다.", 400)

    try:
        subquery, params = _build_paper_filter(keywords=keywords)

        # totalcount: 조건 만족 논문 수
        # [BUG FIX] params를 COUNT 쿼리와 SELECT 쿼리에 각각 독립적으로 전달
        totalcount = execute_scalar(
            f"SELECT COUNT(*) FROM ({subquery}) AS t", list(params)
        )
        totalpages = (totalcount + pagesize - 1) // pagesize if totalcount > 0 else 0

        # 해당 논문들의 main_keyword DISTINCT 목록
        mk_rows = execute_query(f"""
            SELECT DISTINCT pf.main_keyword
            FROM gold.paper_fact pf
            WHERE pf.paper_id IN ({subquery})
              AND pf.main_keyword IS NOT NULL AND pf.main_keyword <> ''
            ORDER BY pf.main_keyword
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, list(params) + [offset, pagesize])

        return _json_response({
            "pageno": pageno, "pagesize": pagesize,
            "totalcount": totalcount, "totalpages": totalpages,
            "mainkeywords": [r["main_keyword"] for r in mk_rows]
        })

    except Exception as e:
        logger.exception("keywords_mainkeywords error")
        return _error_response(str(e), 500)


# ──────────────────────────────────────────────────────
# 3. POST /api/v1/keywords/count
# ──────────────────────────────────────────────────────

@app.route(route="v1/keywords/count", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def keywords_count(req: func.HttpRequest) -> func.HttpResponse:
    """
    논문 키워드 집계 수 조회

    Request Body:
      {
        "mainkeyword": "zein",              // 필수
        "keywords": ["nanoparticle"],       // 선택, AND 조건
        "year_start": 2020,                 // 선택
        "year_end": 2024,                   // 선택
        "journal": "Nature",                // 선택, exact
        "publication_type": "Article",      // 선택, exact
        "result_category": ["formulation"]  // 선택, [] = 전체
      }

    Response:
      { "count": 10 }
    """
    body = _get_json_body(req)
    if body is None:
        return _error_response("Invalid JSON body")

    mainkeyword      = body.get("mainkeyword", "").strip()
    keywords         = body.get("keywords", [])
    year_start       = body.get("year_start")
    year_end         = body.get("year_end")
    journal          = body.get("journal", "").strip() or None
    publication_type = body.get("publication_type", "").strip() or None
    result_category  = body.get("result_category", [])

    if not mainkeyword:
        return _error_response("mainkeyword는 필수입니다.", 400)

    try:
        subquery, params = _build_paper_filter(
            mainkeyword=mainkeyword, keywords=keywords,
            year_start=year_start, year_end=year_end,
            journal=journal, publication_type=publication_type
        )

        # [BUG FIX] params를 list()로 복사해서 원본 변형 방지
        query_params = list(params)

        cat_condition = ""
        if result_category:
            placeholders  = ", ".join(["?" for _ in result_category])
            cat_condition = f"AND pkb.keyword_type IN ({placeholders})"
            query_params  = query_params + list(result_category)

        count = execute_scalar(f"""
            SELECT COUNT(DISTINCT kd.keyword_text)
            FROM gold.paper_keyword_bridge pkb
            INNER JOIN gold.keyword_dim kd ON pkb.keyword_id = kd.keyword_id
            WHERE pkb.paper_id IN ({subquery})
              AND kd.keyword_text IS NOT NULL
              AND kd.keyword_text <> ''
            {cat_condition}
        """, query_params)

        return _json_response({"count": count})

    except Exception as e:
        logger.exception("keywords_count error")
        return _error_response(str(e), 500)


# ──────────────────────────────────────────────────────
# 4. POST /api/v1/keywords/list
# ──────────────────────────────────────────────────────

@app.route(route="v1/keywords/list", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def keywords_list(req: func.HttpRequest) -> func.HttpResponse:
    """
    논문 키워드 집계 목록 조회

    Request Body:
      {
        "mainkeyword": "zein",              // 필수
        "keywords": ["nanoparticle"],       // 선택, AND 조건
        "pageno": 1, "pagesize": 20,
        "year_start": 2020, "year_end": 2024,
        "journal": "Nature",
        "publication_type": "Article",
        "result_category": ["formulation"]  // [] = 전체
      }

    Response:
      {
        "pageno": 1, "pagesize": 2, "totalcount": 5, "totalpages": 3,
        "keywords": [
          { "keyword": "nanoparticle", "categories": ["formulation"], "papers": 5 },
          ...
        ]
      }
      totalcount: 매칭 논문 수
    """
    body = _get_json_body(req)
    if body is None:
        return _error_response("Invalid JSON body")

    mainkeyword      = body.get("mainkeyword", "").strip()
    keywords         = body.get("keywords", [])
    pageno           = _safe_int(body.get("pageno"),   1,  min_val=1)
    pagesize         = _safe_int(body.get("pagesize"), 20, min_val=1, max_val=100)
    year_start       = body.get("year_start")
    year_end         = body.get("year_end")
    journal          = body.get("journal", "").strip() or None
    publication_type = body.get("publication_type", "").strip() or None
    result_category  = body.get("result_category", [])
    offset           = (pageno - 1) * pagesize

    if not mainkeyword:
        return _error_response("mainkeyword는 필수입니다.", 400)

    try:
        subquery, base_params = _build_paper_filter(
            mainkeyword=mainkeyword, keywords=keywords,
            year_start=year_start, year_end=year_end,
            journal=journal, publication_type=publication_type
        )

        cat_condition = ""
        cat_params    = []
        if result_category:
            placeholders  = ", ".join(["?" for _ in result_category])
            cat_condition = f"AND pkb.keyword_type IN ({placeholders})"
            cat_params    = list(result_category)

        # totalcount: DISTINCT 키워드 수 (keywords/count와 동일한 기준)
        totalcount = execute_scalar(f"""
            SELECT COUNT(DISTINCT kd.keyword_text)
            FROM gold.paper_keyword_bridge pkb
            INNER JOIN gold.keyword_dim kd ON pkb.keyword_id = kd.keyword_id
            WHERE pkb.paper_id IN ({subquery})
              AND kd.keyword_text IS NOT NULL
              AND kd.keyword_text <> ''
            {cat_condition}
        """, list(base_params) + cat_params)
        totalpages = (totalcount + pagesize - 1) // pagesize if totalcount > 0 else 0

        # 키워드별 논문 수 집계
        kw_rows = execute_query(f"""
            SELECT
                kd.keyword_text,
                COUNT(DISTINCT pkb.paper_id) AS paper_count
            FROM gold.paper_keyword_bridge pkb
            INNER JOIN gold.keyword_dim kd ON pkb.keyword_id = kd.keyword_id
            WHERE pkb.paper_id IN ({subquery})
              AND kd.keyword_text IS NOT NULL
              AND kd.keyword_text <> ''
            {cat_condition}
            GROUP BY kd.keyword_text
            ORDER BY paper_count DESC, kd.keyword_text
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, list(base_params) + cat_params + [offset, pagesize])

        # [BUG FIX] N+1 쿼리 제거: 키워드 목록의 categories를 한 번의 쿼리로 조회
        # 기존: 키워드마다 루프로 categories 별도 조회
        # 수정: 키워드 목록을 IN 절로 한 번에 조회 후 Python groupby
        result_keywords = []
        if kw_rows:
            kw_texts     = [row["keyword_text"] for row in kw_rows]
            paper_counts = {row["keyword_text"]: row["paper_count"] for row in kw_rows}

            placeholders = ", ".join(["?" for _ in kw_texts])
            cat_rows = execute_query(f"""
                SELECT DISTINCT kd.keyword_text, pkb.keyword_type
                FROM gold.paper_keyword_bridge pkb
                INNER JOIN gold.keyword_dim kd ON pkb.keyword_id = kd.keyword_id
                WHERE kd.keyword_text IN ({placeholders})
                  AND pkb.paper_id IN ({subquery})
                ORDER BY kd.keyword_text, pkb.keyword_type
            """, kw_texts + list(base_params))

            cat_map = defaultdict(list)
            for row in cat_rows:
                kw = row["keyword_text"]
                kt = row["keyword_type"]
                if kt and kt not in cat_map[kw]:
                    cat_map[kw].append(kt)

            # kw_rows 순서 유지 (paper_count DESC 정렬)
            result_keywords = [
                {
                    "keyword":    kw,
                    "categories": cat_map.get(kw, []),
                    "papers":     paper_counts[kw]
                }
                for kw in kw_texts
            ]

        return _json_response({
            "pageno": pageno, "pagesize": pagesize,
            "totalcount": totalcount, "totalpages": totalpages,
            "keywords": result_keywords
        })

    except Exception as e:
        logger.exception("keywords_list error")
        return _error_response(str(e), 500)


# ──────────────────────────────────────────────────────
# 5. POST /api/v1/papers/search
# ──────────────────────────────────────────────────────

@app.route(route="v1/papers/search", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def papers_search(req: func.HttpRequest) -> func.HttpResponse:
    """
    논문 목록 조회

    Request Body:
      {
        "mainkeyword": "zein",                   // 필수
        "keywords": ["nanoparticle"],            // 선택, AND 조건
        "sort_title":   true,                    // 선택 (true=ASC, false=DESC)
        "sort_journal": true,                    // 선택
        "sort_year":    false,                   // 선택 (기본 미지정시 최신순)
        "pageno": 1, "pagesize": 20
      }

    Response:
      {
        "pageno": 1, "pagesize": 2, "totalcount": 7, "totalpages": 4,
        "papers": [
          { "paperid": "doi:10.xxx", "title": "...", "journal": "...", "year": 2024 },
          ...
        ]
      }
    """
    body = _get_json_body(req)
    if body is None:
        return _error_response("Invalid JSON body")

    mainkeyword  = body.get("mainkeyword", "").strip()
    keywords     = body.get("keywords", [])
    sort_title   = body.get("sort_title")
    sort_journal = body.get("sort_journal")
    sort_year    = body.get("sort_year")
    pageno       = _safe_int(body.get("pageno"),   1,  min_val=1)
    pagesize     = _safe_int(body.get("pagesize"), 20, min_val=1, max_val=100)
    offset       = (pageno - 1) * pagesize

    if not mainkeyword:
        return _error_response("mainkeyword는 필수입니다.", 400)

    try:
        subquery, params = _build_paper_filter(
            mainkeyword=mainkeyword, keywords=keywords
        )

        # [BUG FIX] params를 list()로 복사해서 COUNT/SELECT 쿼리에 독립 전달
        # 기존: params + [offset, pagesize] 시 subquery 안의 params가 2번 전달되는 버그
        totalcount = execute_scalar(
            f"SELECT COUNT(*) FROM ({subquery}) AS t", list(params)
        )
        totalpages = (totalcount + pagesize - 1) // pagesize if totalcount > 0 else 0

        # 정렬 조건
        order_parts = []
        if sort_title   is not None: order_parts.append(f"pf.title {'ASC' if sort_title else 'DESC'}")
        if sort_journal is not None: order_parts.append(f"pf.journal_name {'ASC' if sort_journal else 'DESC'}")
        if sort_year    is not None: order_parts.append(f"pf.published_year {'ASC' if sort_year else 'DESC'}")
        if not order_parts:          order_parts.append("pf.published_year DESC")
        order_clause = "ORDER BY " + ", ".join(order_parts)

        papers = execute_query(f"""
            SELECT
                pf.paper_id        AS paperid,
                pf.title,
                pf.journal_name    AS journal,
                pf.published_year  AS year
            FROM gold.paper_fact pf
            WHERE pf.paper_id IN ({subquery})
            {order_clause}
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
        """, list(params) + [offset, pagesize])

        return _json_response({
            "pageno": pageno, "pagesize": pagesize,
            "totalcount": totalcount, "totalpages": totalpages,
            "papers": papers
        })

    except Exception as e:
        logger.exception("papers_search error")
        return _error_response(str(e), 500)


# ──────────────────────────────────────────────────────
# 6. POST /api/v1/papers/detail
# ──────────────────────────────────────────────────────

@app.route(route="v1/papers/detail", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def papers_detail(req: func.HttpRequest) -> func.HttpResponse:
    """
    논문 상세 조회 (메타 + 카테고리별 키워드)

    Request Body:
      { "paperid": "doi:10.xxx" }   // paperid 또는 title 중 1개 이상 필수
      { "title": "Zein nanoparticles..." }

    Response:
      {
        "paperid": "...", "title": "...", "doi": "...", "abstract": "...",
        "year": 2024, "journal": "...",
        "authors": ["John Smith", "Emily Johnson"],
        "main_keyword": "zein",
        "main_material": ["zein"],
        "active_ingredient": ["THC"],
        "formulation": ["nanoparticle"],
        "application": ["cosmetic"],
        "fabrication_method": ["co-assembly"],
        "analysis_method": ["cell model"],
        "efficacy": ["anti-photoaging"],
        "binding_component": ["HA"],
        "cell_type": [],
        "stimulus": [],
        "key_molecule": [],
        "signaling_pathway": [],
        "etcs": ["UVB", "GRAS"]
      }
    """
    body = _get_json_body(req)
    if body is None:
        return _error_response("Invalid JSON body")

    paperid = body.get("paperid", "").strip()
    title   = body.get("title",   "").strip()

    if not paperid and not title:
        return _error_response("paperid 또는 title 중 하나는 필수입니다.", 400)

    try:
        # 논문 기본 정보
        if paperid:
            papers = execute_query("""
                SELECT paper_id, doi, title, abstract, published_year,
                       journal_name, main_keyword, publication_type, main_material
                FROM gold.paper_fact WHERE paper_id = ?
            """, [paperid])
        else:
            papers = execute_query("""
                SELECT paper_id, doi, title, abstract, published_year,
                       journal_name, main_keyword, publication_type, main_material
                FROM gold.paper_fact WHERE title = ?
            """, [title])

        if not papers:
            return _error_response("논문을 찾을 수 없습니다.", 404)

        paper = papers[0]
        pid   = paper["paper_id"]

        # 저자 목록
        author_rows = execute_query("""
            SELECT pad.author_name
            FROM gold.paper_author_bridge pab
            INNER JOIN gold.paper_author_dim pad ON pab.author_key = pad.author_key
            WHERE pab.paper_id = ?
            ORDER BY pab.author_seq
        """, [pid])
        authors = [r["author_name"] for r in author_rows]

        # 카테고리별 키워드 (한 번의 쿼리로 전체 조회)
        keyword_rows = execute_query("""
            SELECT pkb.keyword_type, kd.keyword_text
            FROM gold.paper_keyword_bridge pkb
            INNER JOIN gold.keyword_dim kd ON pkb.keyword_id = kd.keyword_id
            WHERE pkb.paper_id = ?
            ORDER BY pkb.keyword_type, pkb.weight DESC
        """, [pid])

        # 카테고리별 그룹화 (설계서 정의 순서)
        CATEGORIES = [
            "main_material", "active_ingredient", "formulation",
            "application", "fabrication_method", "analysis_method",
            "efficacy", "binding_component", "cell_type",
            "stimulus", "key_molecule", "signaling_pathway", "etcs"
        ]
        cat_map = {cat: [] for cat in CATEGORIES}
        for row in keyword_rows:
            kt = row["keyword_type"]
            kw = row["keyword_text"]
            if kt in cat_map and kw not in cat_map[kt]:
                cat_map[kt].append(kw)

        result = {
            "paperid":      pid,
            "title":        paper["title"],
            "doi":          paper["doi"],
            "abstract":     paper["abstract"],
            "year":         paper["published_year"],
            "journal":      paper["journal_name"],
            "authors":      authors,
            "main_keyword": paper["main_keyword"],
        }
        for cat in CATEGORIES:
            result[cat] = cat_map[cat]

        return _json_response(result)

    except Exception as e:
        logger.exception("papers_detail error")
        return _error_response(str(e), 500)