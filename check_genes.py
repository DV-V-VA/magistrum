#!/usr/bin/env python3
"""
Скрипт для проверки структуры JSON файлов генов
"""

import json
import os
import glob
from pathlib import Path

GENES_DATA_PATH = "/srv/data/202510221102_complete_genes"

def check_genes_structure():
    """Проверка структуры JSON файлов генов"""
    
    if not os.path.exists(GENES_DATA_PATH):
        print(f"❌ Папка {GENES_DATA_PATH} не существует!")
        return
    
    json_files = glob.glob(os.path.join(GENES_DATA_PATH, "*.json"))
    print(f"📁 Найдено {len(json_files)} JSON файлов")
    
    if not json_files:
        print("❌ Нет JSON файлов в указанной папке")
        return
    
    # Проверяем несколько файлов
    sample_files = json_files[:5]  # Проверяем первые 5 файлов
    
    for file_path in sample_files:
        print(f"\n🔍 Проверка файла: {os.path.basename(file_path)}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                gene_data = json.load(f)
            
            # Проверяем обязательные поля
            symbol = gene_data.get('symbol')
            hgnc_name = gene_data.get('hgnc_name')
            llm_summary = gene_data.get('llm_summary')
            
            print(f"   ✓ Symbol: {symbol}")
            print(f"   ✓ HGNC Name: {hgnc_name}")
            print(f"   ✓ LLM Summary: {'Есть' if llm_summary else 'Нет'}")
            print(f"   ✓ Orthologs: {len(gene_data.get('orthologs', []))}")
            print(f"   ✓ Gene IDs: {len(gene_data.get('gene_ids', []))}")
            
        except Exception as e:
            print(f"   ❌ Ошибка чтения файла: {e}")
    
    # Проверяем наличие конкретных генов
    test_genes = ['TP53', 'BRCA1', 'NFE2L2', 'EGFR']
    print(f"\n🔎 Поиск тестовых генов:")
    
    for gene in test_genes:
        file_path = os.path.join(GENES_DATA_PATH, f"{gene}.json")
        exists = os.path.exists(file_path)
        print(f"   {gene}: {'✅ Найден' if exists else '❌ Не найден'}")

if __name__ == "__main__":
    check_genes_structure()