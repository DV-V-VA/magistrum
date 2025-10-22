import os
from pathlib import Path
from typing import Any, Dict
try:
    import yaml
except Exception:
    yaml = None

def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]  # .../project/src/longevity -> project

def load_user_config() -> Dict[str, Any]:
    """Читает configs/config.yaml (если есть). Возвращает dict либо {}."""
    cfg_path = _project_root() / "configs" / "config.yaml"
    if cfg_path.exists() and yaml is not None:
        with open(cfg_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}
