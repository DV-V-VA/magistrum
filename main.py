from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import psycopg2
from psycopg2.extras import RealDictCursor
import json
import os
from typing import List, Dict, Any
from datetime import datetime

app = FastAPI(title="Gene Analyzer")

# Настройка шаблонов
templates = Jinja2Templates(directory="templates")

# Статические файлы
app.mount("/static", StaticFiles(directory="static"), name="static")

# Конфигурация базы данных
DB_CONFIG = {
    "host": "localhost",
    "database": "genes_database",
    "user": "postgres",
    "password": "12345",
    "port": "5432"
}

def get_db_connection():
    """Создание подключения к базе данных"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        raise

def gene_row_to_dict(row: Dict) -> Dict[str, Any]:
    """Преобразование строки гена в словарь JSON-формата"""
    return {
        "symbol": row["symbol"],
        "locus_group": row["locus_group"],
        "last_modified": row["last_modified"].isoformat() if row["last_modified"] else None,
        "cytoband": row["cytoband"],
        "n_papers": row["n_papers"],
        "refseq_summary": row["refseq_summary"],
        "llm_summary": row["llm_summary"],
        "hgnc_name": row["hgnc_name"],
        "omim": [],
        "mane_select": [],
        "gene_ids": [],
        "orthologs": [],
        "hgnc_alias_symbols": [],
        "hgnc_alias_names": [],
        "hgnc_prev_name": [],
        "hgnc_prev_symbols": [],
        "uniprot_full_names": []
    }

async def get_gene_identifiers(gene_id: int, conn) -> List[Dict]:
    """Получение идентификаторов гена"""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id_type, id_value 
                FROM gene_identifier 
                WHERE gene_id = %s
            """, (gene_id,))
            identifiers = []
            for row in cur.fetchall():
                identifiers.append({
                    "name": row["id_type"],
                    "value": row["id_value"]
                })
            return identifiers
    except Exception as e:
        print(f"Error fetching gene identifiers: {e}")
        return []

async def get_gene_aliases(gene_id: int, conn) -> Dict[str, List[str]]:
    """Получение алиасов гена"""
    aliases = {
        "alias_symbols": [],
        "alias_names": [],
        "prev_name": [],
        "prev_symbols": []
    }
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT alias_type, alias_value 
                FROM gene_alias 
                WHERE gene_id = %s
            """, (gene_id,))
            
            for row in cur.fetchall():
                alias_type = row["alias_type"]
                value = row["alias_value"]
                
                if alias_type == "alias_symbol":
                    aliases["alias_symbols"].append(value)
                elif alias_type == "alias_name":
                    aliases["alias_names"].append(value)
                elif alias_type == "prev_name":
                    aliases["prev_name"].append(value)
                elif alias_type == "prev_symbol":
                    aliases["prev_symbols"].append(value)
                    
    except Exception as e:
        print(f"Error fetching gene aliases: {e}")
    
    return aliases

async def get_orthologs(gene_id: int, conn) -> List[Dict]:
    """Получение ортологов гена"""
    orthologs = []
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Получаем основные данные ортологов
            cur.execute("""
                SELECT id, taxname, common_name, tax_id, symbol, description, last_modified
                FROM ortholog 
                WHERE gene_id = %s
            """, (gene_id,))
            
            ortholog_rows = cur.fetchall()
            
            for ortholog in ortholog_rows:
                ortholog_id = ortholog["id"]
                
                # Получаем синонимы
                cur.execute("""
                    SELECT synonym 
                    FROM ortholog_synonym 
                    WHERE ortholog_id = %s
                """, (ortholog_id,))
                synonyms = [row["synonym"] for row in cur.fetchall()]
                
                # Получаем описания
                cur.execute("""
                    SELECT description 
                    FROM ortholog_summary 
                    WHERE ortholog_id = %s
                """, (ortholog_id,))
                summaries = [{"description": row["description"]} for row in cur.fetchall()]
                
                ortholog_data = {
                    "query_gene": "",  # Заполнится позже
                    "taxname": ortholog["taxname"],
                    "common_name": ortholog["common_name"],
                    "tax_id": ortholog["tax_id"],
                    "symbol": ortholog["symbol"],
                    "synonyms": synonyms,
                    "description": ortholog["description"],
                    "summary": summaries,
                    "last_modified": ortholog["last_modified"].isoformat() if ortholog["last_modified"] else None,
                    "prev_modified": []
                }
                
                orthologs.append(ortholog_data)
                
    except Exception as e:
        print(f"Error fetching orthologs: {e}")
    
    return orthologs

async def get_mane_transcripts(gene_id: int, conn) -> List[str]:
    """Получение MANE транскриптов"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT transcript_id 
                FROM mane_transcript 
                WHERE gene_id = %s
            """, (gene_id,))
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Error fetching MANE transcripts: {e}")
        return []

async def get_uniprot_references(gene_id: int, conn) -> List[str]:
    """Получение Uniprot ссылок"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT uniprot_id, full_name 
                FROM uniprot_reference 
                WHERE gene_id = %s
            """, (gene_id,))
            return [row[1] for row in cur.fetchall() if row[1]]  # Только непустые full_name
    except Exception as e:
        print(f"Error fetching Uniprot references: {e}")
        return []

async def get_ccds_references(gene_id: int, conn) -> List[str]:
    """Получение CCDS ссылок"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ccds_id 
                FROM ccds_reference 
                WHERE gene_id = %s
            """, (gene_id,))
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Error fetching CCDS references: {e}")
        return []

async def get_gene_by_symbol(symbol: str) -> Dict[str, Any]:
    """Получение полных данных гена по символу"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Получаем основную информацию о гене
            cur.execute("SELECT * FROM gene WHERE symbol = %s", (symbol.upper(),))
            gene_row = cur.fetchone()
            
            if not gene_row:
                return None
            
            gene_data = gene_row_to_dict(gene_row)
            gene_id = gene_row["id"]
            
            # Получаем дополнительные данные
            gene_data["gene_ids"] = await get_gene_identifiers(gene_id, conn)
            gene_data["orthologs"] = await get_orthologs(gene_id, conn)
            
            aliases = await get_gene_aliases(gene_id, conn)
            gene_data["hgnc_alias_symbols"] = aliases["alias_symbols"]
            gene_data["hgnc_alias_names"] = aliases["alias_names"]
            gene_data["hgnc_prev_name"] = aliases["prev_name"]
            gene_data["hgnc_prev_symbols"] = aliases["prev_symbols"]
            
            gene_data["mane_select"] = await get_mane_transcripts(gene_id, conn)
            gene_data["uniprot_full_names"] = await get_uniprot_references(gene_id, conn)
            
            # Добавляем CCDS в gene_ids
            ccds_ids = await get_ccds_references(gene_id, conn)
            if ccds_ids:
                gene_data["gene_ids"].append({
                    "name": "ccds_id",
                    "value": ccds_ids
                })
            
            # Добавляем OMIM из gene_ids
            omim_ids = []
            for id_obj in gene_data["gene_ids"]:
                if id_obj.get("name") == "omim_id":
                    value = id_obj.get("value")
                    if isinstance(value, list):
                        omim_ids.extend(value)
                    else:
                        omim_ids.append(value)
            gene_data["omim"] = omim_ids
            
            return gene_data
            
    except Exception as e:
        print(f"Error fetching gene data: {e}")
        import traceback
        print(f"Detailed traceback: {traceback.format_exc()}")
        raise
    finally:
        conn.close()

async def search_genes_in_db(query: str) -> Dict[str, Any]:
    """Поиск генов в базе данных"""
    conn = get_db_connection()
    try:
        query = query.upper().strip()
        
        # Проверка точного совпадения
        exact_gene = await get_gene_by_symbol(query)
        if exact_gene:
            return {
                "exact_match": True,
                "gene": exact_gene
            }
        
        # Поиск частичных совпадений
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            search_pattern = f"%{query}%"
            
            # Поиск по символу, названию и алиасам
            cur.execute("""
                SELECT DISTINCT g.symbol, g.hgnc_name, g.cytoband
                FROM gene g
                LEFT JOIN gene_alias ga ON g.id = ga.gene_id
                WHERE g.symbol ILIKE %s 
                   OR g.hgnc_name ILIKE %s
                   OR ga.alias_value ILIKE %s
                LIMIT 10
            """, (search_pattern, search_pattern, search_pattern))
            
            suggestions = []
            for row in cur.fetchall():
                suggestions.append({
                    "symbol": row["symbol"],
                    "name": row["hgnc_name"],
                    "location": row["cytoband"]
                })
            
            return {
                "exact_match": False,
                "suggestions": suggestions
            }
            
    except Exception as e:
        print(f"Search error: {e}")
        import traceback
        print(f"Detailed search error: {traceback.format_exc()}")
        return {"exact_match": False, "suggestions": []}
    finally:
        conn.close()

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/about", response_class=HTMLResponse)
async def about(request: Request):
    return templates.TemplateResponse("about.html", {"request": request})

@app.get("/api/search/{query}")
async def search_genes(query: str):
    """API endpoint для поиска генов"""
    try:
        print(f"Search request received for: {query}")
        result = await search_genes_in_db(query)
        print(f"Search result: exact_match={result.get('exact_match')}, suggestions_count={len(result.get('suggestions', []))}")
        return JSONResponse(result)
    except Exception as e:
        print(f"API search error: {e}")
        import traceback
        print(f"Detailed API error: {traceback.format_exc()}")
        return JSONResponse(
            {"error": "Internal server error"}, 
            status_code=500
        )

@app.get("/api/gene/{symbol}")
async def get_gene(symbol: str):
    """API endpoint для получения данных гена"""
    try:
        print(f"Gene data request received for: {symbol}")
        gene_data = await get_gene_by_symbol(symbol)
        if gene_data:
            return JSONResponse({"gene": gene_data})
        else:
            return JSONResponse(
                {"error": "Gene not found"}, 
                status_code=404
            )
    except Exception as e:
        print(f"API gene error: {e}")
        import traceback
        print(f"Detailed gene error: {traceback.format_exc()}")
        return JSONResponse(
            {"error": "Internal server error"}, 
            status_code=500
        )

# Эндпоинт для проверки подключения к БД
@app.get("/api/health")
async def health_check():
    """Проверка здоровья приложения и подключения к БД"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        return JSONResponse({"status": "healthy", "database": "connected"})
    except Exception as e:
        return JSONResponse(
            {"status": "unhealthy", "database": "disconnected", "error": str(e)},
            status_code=503
        )

# Эндпоинт для отладки - проверка существования гена в БД
@app.get("/api/debug/gene/{symbol}")
async def debug_gene(symbol: str):
    """Отладочный эндпоинт для проверки наличия гена в БД"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Проверяем наличие гена
            cur.execute("SELECT * FROM gene WHERE symbol = %s", (symbol.upper(),))
            gene_row = cur.fetchone()
            
            if gene_row:
                # Проверяем связанные данные
                gene_id = gene_row["id"]
                
                cur.execute("SELECT COUNT(*) as count FROM gene_identifier WHERE gene_id = %s", (gene_id,))
                identifiers_count = cur.fetchone()["count"]
                
                cur.execute("SELECT COUNT(*) as count FROM gene_alias WHERE gene_id = %s", (gene_id,))
                aliases_count = cur.fetchone()["count"]
                
                cur.execute("SELECT COUNT(*) as count FROM ortholog WHERE gene_id = %s", (gene_id,))
                orthologs_count = cur.fetchone()["count"]
                
                return JSONResponse({
                    "exists": True,
                    "gene_data": dict(gene_row),
                    "related_data": {
                        "identifiers": identifiers_count,
                        "aliases": aliases_count,
                        "orthologs": orthologs_count
                    }
                })
            else:
                return JSONResponse({"exists": False})
                
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        conn.close()

# Эндпоинт для проверки всех генов в БД
@app.get("/api/debug/genes")
async def debug_all_genes():
    """Отладочный эндпоинт для проверки всех генов в БД"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT symbol, hgnc_name FROM gene ORDER BY symbol")
            genes = cur.fetchall()
            return JSONResponse({
                "total_genes": len(genes),
                "genes": [dict(gene) for gene in genes]
            })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")