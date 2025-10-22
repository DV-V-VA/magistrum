#!/usr/bin/env python3
"""Единая точка входа проекта (dispatcher — диспетчер подкоманд).

Команды:
  - pipeline [-- ...]  → запускает оригинальный longevity/pipeline.py как скрипт
  - genage   [-- ...]  → запускает longevity/genage_parser.py
  - proteinkb [-- ...] → запускает longevity/proteinkb_parse/main.py (проксирование CLI)

Примечание: опции после названия подкоманды передаются "как есть" (pass-through —
«прямой проброс») в оригинальные скрипты. Ключевые модули проекта не изменяются.
"""
import argparse
import os
import sys
import runpy
import traceback

# .env (переменные окружения): не обязателен
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# Логирование: используем модуль проекта, если есть; иначе fallback
def _fallback_setup_logging(log_dir: str = "data/logs"):
    import logging, os
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "pipeline.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    return logging.getLogger("longevity")

try:
    from longevity.logging_config import setup_logging 
    logger = setup_logging()
except Exception:
    logger = _fallback_setup_logging()

def _run_module(module: str, args: list[str]) -> int:
    old_argv = sys.argv[:]
    try:
        sys.argv = [module] + args
        runpy.run_module(module, run_name="__main__")
        return 0
    except SystemExit as e:
        code = int(e.code) if isinstance(e.code, int) else 1
        return code
    except Exception:
        logger.error("Необработанное исключение при запуске %s", module)
        logger.debug("TRACEBACK:\n%s", traceback.format_exc())
        print(f"Ошибка: см. подробности в логе. Модуль: {module}", file=sys.stderr)
        return 1
    finally:
        sys.argv = old_argv

def main() -> int:
    parser = argparse.ArgumentParser(prog="main.py", description="Единая точка входа")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("pipeline", help="Запустить pipeline.py")
    p1.add_argument("args", nargs=argparse.REMAINDER, help="Аргументы для pipeline.py")

    p2 = sub.add_parser("genage", help="Запустить genage_parser.py")
    p2.add_argument("args", nargs=argparse.REMAINDER, help="Аргументы для genage_parser.py")

    p3 = sub.add_parser("proteinkb", help="Запустить proteinkb_parse/main.py")
    p3.add_argument("args", nargs=argparse.REMAINDER, help="Аргументы для proteinkb_parse")

    ns = parser.parse_args()

    if ns.cmd == "pipeline":
        return _run_module("longevity.pipeline", ns.args)
    elif ns.cmd == "genage":
        return _run_module("longevity.genage_parser", ns.args)
    elif ns.cmd == "proteinkb":
        return _run_module("longevity.proteinkb_parse.main", ns.args)
    else:
        parser.print_help()
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
