from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Мой сайт")

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
        "name": f"Пользователь {user_id}",
        "email": f"user{user_id}@example.com"
    }
    return templates.TemplateResponse("user.html", {"request": request, "user": user_data})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
