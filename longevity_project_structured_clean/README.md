# DVVVA Sequence to function Project 

## Установка
```bash
make setup
```

## Запуск (единая точка входа)
```bash
python main.py

# Примеры
python main.py pipeline -- --gene FOXO3
python main.py genage -- --help
python main.py proteinkb -- harvest --protein FOXO3 --syn FOXO --out out/foxo
```
*Пояснение:* всё после `--` передается «как есть» (pass-through) в исходные скрипты,
которые парсят свои аргументы сами.

## Конфиги
- `.env` — переменные окружения.
- `configs/config.yaml` — пользовательские настройки.

## Обратная совместимость
- Старые импорты вроде `import config` и `import pipeline` работают благодаря `sitecustomize.py`,
  который добавляет `src/longevity` в `sys.path`.
- Новый стиль импортов: `from longevity import pipeline` и т.п.
- Старые команды запуска отдельных скриптов заменены на:
  - `python main.py pipeline -- ...`
  - `python main.py genage -- ...`
  - `python main.py proteinkb -- ...`
