from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import json
import os

app = FastAPI(title="Gene Analyzer")

# Настройка шаблонов
templates = Jinja2Templates(directory="templates")

# Статические файлы
app.mount("/static", StaticFiles(directory="static"), name="static")

# Загрузка данных генов при старте приложения
genes_data = {}

def load_genes_data():
    """Load all gene data from JSON files"""
    global genes_data
    genes_data = {}
    try:
        genes_dir = "data"
        if os.path.exists(genes_dir):
            for filename in os.listdir(genes_dir):
                if filename.endswith('.json'):
                    gene_symbol = filename.replace('.json', '').upper()
                    with open(os.path.join(genes_dir, filename), 'r', encoding='utf-8') as f:
                        gene_data = json.load(f)
                        genes_data[gene_symbol] = gene_data
        print(f"Loaded {len(genes_data)} genes")
    except Exception as e:
        print(f"Error loading genes data: {e}")

# Загружаем данные при импорте
load_genes_data()

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/about", response_class=HTMLResponse)
async def about(request: Request):
    return templates.TemplateResponse("about.html", {"request": request})

@app.get("/api/search/{query}")
async def search_genes(query: str):
    """API endpoint for gene search"""
    query = query.upper().strip()
    results = []
    
    # Exact match
    if query in genes_data:
        return JSONResponse({
            "exact_match": True,
            "gene": genes_data[query]
        })
    
    # Partial matches for autocomplete
    for symbol, gene_data in genes_data.items():
        if (query in symbol or 
            query in gene_data.get('hgnc_name', '').upper() or
            any(query in alias.upper() for alias in gene_data.get('hgnc_alias_symbols', []))):
            results.append({
                "symbol": symbol,
                "name": gene_data.get('hgnc_name', ''),
                "location": gene_data.get('cytoband', '')
            })
    
    return JSONResponse({
        "exact_match": False,
        "suggestions": results[:10]  # Limit to 10 suggestions
    })

@app.get("/api/gene/{symbol}")
async def get_gene(symbol: str):
    """API endpoint to get specific gene data"""
    symbol = symbol.upper()
    if symbol in genes_data:
        return JSONResponse({"gene": genes_data[symbol]})
    else:
        return JSONResponse({"error": "Gene not found"}, status_code=404)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)