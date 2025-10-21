from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import json
import os

app = FastAPI(title="Gene Analyzer")

# Настройка шаблонов
templates = Jinja2Templates(directory="templates")

# Статические файлы
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/about", response_class=HTMLResponse)
async def about(request: Request):
    return templates.TemplateResponse("about.html", {"request": request})

@app.get("/users/{user_id}", response_class=HTMLResponse)
async def user_profile(request: Request, user_id: int):
    user_data = {
        "id": user_id,
        "name": f"User {user_id}",
        "email": f"user{user_id}@example.com"
    }
    return templates.TemplateResponse("user.html", {"request": request, "user": user_data})

@app.get("/genes", response_class=HTMLResponse)
async def genes_list(request: Request):
    """Genes list page"""
    genes_data = []
    try:
        # Load gene data from JSON files
        genes_dir = "data"
        if os.path.exists(genes_dir):
            for filename in os.listdir(genes_dir):
                if filename.endswith('.json'):
                    with open(os.path.join(genes_dir, filename), 'r', encoding='utf-8') as f:
                        gene_data = json.load(f)
                        genes_data.append(gene_data)
    except Exception as e:
        print(f"Error loading genes data: {e}")
    
    return templates.TemplateResponse("genes.html", {
        "request": request, 
        "genes": genes_data
    })

@app.get("/genes/{gene_symbol}", response_class=HTMLResponse)
async def gene_detail(request: Request, gene_symbol: str):
    """Gene detail page"""
    gene_data = None
    try:
        file_path = f"data/{gene_symbol.upper()}.json"
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                gene_data = json.load(f)
    except Exception as e:
        print(f"Error loading gene data: {e}")
    
    return templates.TemplateResponse("gene_detail.html", {
        "request": request, 
        "gene": gene_data,
        "gene_symbol": gene_symbol.upper()
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)