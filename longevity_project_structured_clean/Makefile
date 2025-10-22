SHELL := /bin/bash

VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: setup run test clean

setup:
	python3 -m venv $(VENV)
	. $(VENV)/bin/activate && $(PIP) install --upgrade pip
	. $(VENV)/bin/activate && $(PIP) install -r requirements.txt

run:
	$(PY) main.py --help

test:
	$(PY) -m pytest -q || true

clean:
	rm -rf $(VENV) __pycache__ .pytest_cache
