from fastapi import FastAPI, Request, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import json
import os
from typing import List, Dict, Any
import glob

app = FastAPI(title="Gene Analyzer")

# Настройка шаблонов
templates = Jinja2Templates(directory="templates")

# Статические файлы
app.mount("/static", StaticFiles(directory="static"), name="static")

# Путь к папке с генами
GENES_DATA_PATH = "/srv/data/202510221102_complete_genes"

# Кэш для быстрого поиска
genes_cache = {}

def load_genes_cache():
    """Загрузка всех генов в кэш при старте приложения"""
    global genes_cache
    try:
        if not os.path.exists(GENES_DATA_PATH):
            print(f"Warning: Genes data path {GENES_DATA_PATH} does not exist")
            return
        
        json_files = glob.glob(os.path.join(GENES_DATA_PATH, "*.json"))
        print(f"Found {len(json_files)} gene files")
        
        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    gene_data = json.load(f)
                    symbol = gene_data.get('symbol', '').upper()
                    if symbol:
                        genes_cache[symbol] = gene_data
                        
                        # Также добавляем алиасы в кэш для поиска
                        for alias in gene_data.get('hgnc_alias_symbols', []):
                            if alias and alias.upper() not in genes_cache:
                                genes_cache[alias.upper()] = gene_data
                                
            except Exception as e:
                print(f"Error loading gene file {file_path}: {e}")
                
        print(f"Successfully loaded {len(genes_cache)} genes into cache")
        
    except Exception as e:
        print(f"Error initializing genes cache: {e}")

# Загружаем кэш при старте
load_genes_cache()

async def get_gene_by_symbol(symbol: str) -> Dict[str, Any]:
    """Получение данных гена по символу из JSON файлов"""
    symbol_upper = symbol.upper()
    
    # Ищем в кэше
    if symbol_upper in genes_cache:
        return genes_cache[symbol_upper]
    
    # Если нет в кэше, пытаемся загрузить из файла
    try:
        file_path = os.path.join(GENES_DATA_PATH, f"{symbol_upper}.json")
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                gene_data = json.load(f)
                # Добавляем в кэш для будущих запросов
                genes_cache[symbol_upper] = gene_data
                return gene_data
    except Exception as e:
        print(f"Error loading gene file for {symbol}: {e}")
    
    return None

async def search_genes_in_json(query: str) -> Dict[str, Any]:
    """Поиск генов в JSON файлах"""
    query_upper = query.upper().strip()
    
    if not query_upper:
        return {"exact_match": False, "suggestions": []}
    
    # Проверка точного совпадения
    exact_gene = await get_gene_by_symbol(query_upper)
    if exact_gene:
        return {
            "exact_match": True,
            "gene": exact_gene
        }
    
    # Поиск частичных совпадений
    suggestions = []
    
    # Ищем в кэше
    for symbol, gene_data in genes_cache.items():
        if (query_upper in symbol or 
            query_upper in gene_data.get('hgnc_name', '').upper() or
            any(query_upper in alias.upper() for alias in gene_data.get('hgnc_alias_symbols', []))):
            
            # Убедимся, что это основной символ, а не алиас
            main_symbol = gene_data.get('symbol', '').upper()
            if symbol == main_symbol:  # Показываем только основные символы
                suggestions.append({
                    "symbol": main_symbol,
                    "name": gene_data.get('hgnc_name', ''),
                    "location": gene_data.get('cytoband', '')
                })
    
    # Ограничиваем количество подсказок и убираем дубликаты
    seen_symbols = set()
    unique_suggestions = []
    for suggestion in suggestions:
        if suggestion['symbol'] not in seen_symbols:
            seen_symbols.add(suggestion['symbol'])
            unique_suggestions.append(suggestion)
    
    return {
        "exact_match": False,
        "suggestions": unique_suggestions[:10]  # Ограничиваем 10 подсказками
    }

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/about", response_class=HTMLResponse)
async def about(request: Request):
    return templates.TemplateResponse("about.html", {"request": request})

# Добавьте этот маршрут для отображения страницы гена
@app.get("/gene/{gene_symbol}", response_class=HTMLResponse)
async def gene_detail_page(request: Request, gene_symbol: str):
    """HTML страница с детальной информацией о гене"""
    gene_data = await get_gene_by_symbol(gene_symbol)
    return templates.TemplateResponse("gene_detail.html", {
        "request": request, 
        "gene": gene_data,
        "gene_symbol": gene_symbol.upper()
    })

@app.get("/api/search/{query}")
async def search_genes(query: str):
    """API endpoint для поиска генов"""
    try:
        print(f"Search request received for: {query}")
        result = await search_genes_in_json(query)
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

# Эндпоинт для проверки здоровья приложения
@app.get("/api/health")
async def health_check():
    """Проверка здоровья приложения"""
    try:
        genes_count = len(genes_cache)
        data_path_exists = os.path.exists(GENES_DATA_PATH)
        
        status = "healthy" if genes_count > 0 and data_path_exists else "degraded"
        
        return JSONResponse({
            "status": status,
            "genes_loaded": genes_count,
            "data_path_exists": data_path_exists,
            "data_path": GENES_DATA_PATH
        })
    except Exception as e:
        return JSONResponse(
            {"status": "unhealthy", "error": str(e)},
            status_code=503
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")