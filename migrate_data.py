import psycopg
import json
import os
from datetime import datetime

# Конфигурация базы данных
DB_CONFIG = {
    "host": "localhost",
    "dbname": "genes_database",
    "user": "postgres",
    "password": "12345",
    "port": "5432"
}

def get_db_connection():
    return psycopg.connect(**DB_CONFIG)

def migrate_gene_data():
    """Миграция данных из JSON файлов в PostgreSQL"""
    conn = get_db_connection()
    
    try:
        data_dir = "data"
        if not os.path.exists(data_dir):
            print("Data directory not found")
            return
        
        json_files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
        print(f"Found {len(json_files)} JSON files to migrate")
        
        for filename in json_files:
            gene_symbol = filename.replace('.json', '').upper()
            print(f"Migrating {gene_symbol}...")
            
            with open(os.path.join(data_dir, filename), 'r', encoding='utf-8') as f:
                gene_data = json.load(f)
            
            # Вставляем основную информацию о гене
            with conn.cursor() as cur:
                # Проверяем, существует ли уже ген
                cur.execute("SELECT id FROM gene WHERE symbol = %s", (gene_symbol,))
                existing_gene = cur.fetchone()
                
                if existing_gene:
                    gene_id = existing_gene[0]
                    print(f"Gene {gene_symbol} already exists, updating...")
                    
                    # Обновляем основную информацию
                    cur.execute("""
                        UPDATE gene SET
                            locus_group = %s,
                            last_modified = %s,
                            cytoband = %s,
                            n_papers = %s,
                            refseq_summary = %s,
                            llm_summary = %s,
                            hgnc_name = %s
                        WHERE symbol = %s
                    """, (
                        gene_data.get('locus_group'),
                        parse_datetime(gene_data.get('last_modified')),
                        gene_data.get('cytoband'),
                        gene_data.get('n_papers', 0),
                        gene_data.get('refseq_summary'),
                        gene_data.get('llm_summary'),
                        gene_data.get('hgnc_name'),
                        gene_symbol
                    ))
                else:
                    # Вставляем новую запись
                    cur.execute("""
                        INSERT INTO gene (
                            symbol, locus_group, last_modified, cytoband,
                            n_papers, refseq_summary, llm_summary, hgnc_name
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        gene_symbol,
                        gene_data.get('locus_group'),
                        parse_datetime(gene_data.get('last_modified')),
                        gene_data.get('cytoband'),
                        gene_data.get('n_papers', 0),
                        gene_data.get('refseq_summary'),
                        gene_data.get('llm_summary'),
                        gene_data.get('hgnc_name')
                    ))
                    
                    gene_id = cur.fetchone()[0]
                    print(f"Created new gene record with ID: {gene_id}")
                
                # Мигрируем идентификаторы (удаляем старые и вставляем новые)
                cur.execute("DELETE FROM gene_identifier WHERE gene_id = %s", (gene_id,))
                migrate_gene_identifiers(cur, gene_id, gene_data.get('gene_ids', []))
                
                # Мигрируем алиасы (удаляем старые и вставляем новые)
                cur.execute("DELETE FROM gene_alias WHERE gene_id = %s", (gene_id,))
                migrate_gene_aliases(cur, gene_id, gene_data)
                
                # Мигрируем ортологи (удаляем старые и вставляем новые)
                cur.execute("""
                    DELETE FROM ortholog WHERE gene_id = %s
                """, (gene_id,))
                migrate_orthologs(cur, gene_id, gene_data.get('orthologs', []))
                
                # Мигрируем MANE транскрипты (удаляем старые и вставляем новые)
                cur.execute("DELETE FROM mane_transcript WHERE gene_id = %s", (gene_id,))
                migrate_mane_transcripts(cur, gene_id, gene_data.get('mane_select', []))
                
                # Мигрируем Uniprot ссылки (удаляем старые и вставляем новые)
                cur.execute("DELETE FROM uniprot_reference WHERE gene_id = %s", (gene_id,))
                migrate_uniprot_references(cur, gene_id, gene_data)
                
                # Мигрируем CCDS ссылки (удаляем старые и вставляем новые)
                cur.execute("DELETE FROM ccds_reference WHERE gene_id = %s", (gene_id,))
                migrate_ccds_references(cur, gene_id, gene_data)
                
                print(f"Successfully migrated {gene_symbol}")
        
        conn.commit()
        print("Migration completed successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"Migration error: {e}")
        raise
    finally:
        conn.close()

def parse_datetime(datetime_str):
    """Парсинг строки datetime"""
    if not datetime_str:
        return None
    try:
        # Убираем Z и добавляем временную зону если нужно
        if datetime_str.endswith('Z'):
            datetime_str = datetime_str[:-1] + '+00:00'
        return datetime.fromisoformat(datetime_str)
    except ValueError as e:
        print(f"Warning: Could not parse datetime {datetime_str}: {e}")
        return None

def migrate_gene_identifiers(cur, gene_id, gene_ids):
    """Миграция идентификаторов гена"""
    for id_obj in gene_ids:
        id_type = id_obj.get('name')
        id_value = id_obj.get('value')
        
        if not id_type or id_value is None:
            continue
            
        if isinstance(id_value, list):
            for value in id_value:
                if value:  # Пропускаем пустые значения
                    try:
                        cur.execute("""
                            INSERT INTO gene_identifier (gene_id, id_type, id_value)
                            VALUES (%s, %s, %s)
                        """, (gene_id, id_type, str(value)))
                    except Exception as e:
                        print(f"Warning: Could not insert identifier {id_type}: {value} - {e}")
        else:
            try:
                cur.execute("""
                    INSERT INTO gene_identifier (gene_id, id_type, id_value)
                    VALUES (%s, %s, %s)
                """, (gene_id, id_type, str(id_value)))
            except Exception as e:
                print(f"Warning: Could not insert identifier {id_type}: {id_value} - {e}")

def migrate_gene_aliases(cur, gene_id, gene_data):
    """Миграция алиасов гена"""
    # Alias symbols
    for alias in gene_data.get('hgnc_alias_symbols', []):
        if alias:
            cur.execute("""
                INSERT INTO gene_alias (gene_id, alias_type, alias_value)
                VALUES (%s, 'alias_symbol', %s)
            """, (gene_id, alias))
    
    # Alias names
    for alias in gene_data.get('hgnc_alias_names', []):
        if alias:
            cur.execute("""
                INSERT INTO gene_alias (gene_id, alias_type, alias_value)
                VALUES (%s, 'alias_name', %s)
            """, (gene_id, alias))
    
    # Previous names
    for alias in gene_data.get('hgnc_prev_name', []):
        if alias:
            cur.execute("""
                INSERT INTO gene_alias (gene_id, alias_type, alias_value)
                VALUES (%s, 'prev_name', %s)
            """, (gene_id, alias))
    
    # Previous symbols
    for alias in gene_data.get('hgnc_prev_symbols', []):
        if alias:
            cur.execute("""
                INSERT INTO gene_alias (gene_id, alias_type, alias_value)
                VALUES (%s, 'prev_symbol', %s)
            """, (gene_id, alias))

def migrate_orthologs(cur, gene_id, orthologs):
    """Миграция ортологов"""
    for ortholog in orthologs:
        try:
            cur.execute("""
                INSERT INTO ortholog (
                    gene_id, taxname, common_name, tax_id, symbol, 
                    description, last_modified
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                gene_id,
                ortholog.get('taxname'),
                ortholog.get('common_name'),
                ortholog.get('tax_id'),
                ortholog.get('symbol'),
                ortholog.get('description'),
                parse_datetime(ortholog.get('last_modified'))
            ))
            
            ortholog_id = cur.fetchone()[0]
            
            # Мигрируем синонимы
            for synonym in ortholog.get('synonyms', []):
                if synonym:
                    cur.execute("""
                        INSERT INTO ortholog_synonym (ortholog_id, synonym)
                        VALUES (%s, %s)
                    """, (ortholog_id, synonym))
            
            # Мигрируем описания
            for summary in ortholog.get('summary', []):
                description = summary.get('description')
                if description:
                    cur.execute("""
                        INSERT INTO ortholog_summary (ortholog_id, description)
                        VALUES (%s, %s)
                    """, (ortholog_id, description))
                    
        except Exception as e:
            print(f"Warning: Could not migrate ortholog {ortholog.get('symbol')}: {e}")

def migrate_mane_transcripts(cur, gene_id, mane_select):
    """Миграция MANE транскриптов"""
    for transcript in mane_select:
        if transcript:
            try:
                cur.execute("""
                    INSERT INTO mane_transcript (gene_id, transcript_id)
                    VALUES (%s, %s)
                """, (gene_id, transcript))
            except Exception as e:
                print(f"Warning: Could not insert MANE transcript {transcript}: {e}")

def migrate_uniprot_references(cur, gene_id, gene_data):
    """Миграция Uniprot ссылок"""
    # Из gene_ids
    for id_obj in gene_data.get('gene_ids', []):
        if id_obj.get('name') == 'uniprot_ids':
            uniprot_ids = id_obj.get('value')
            if isinstance(uniprot_ids, list):
                for uniprot_id in uniprot_ids:
                    if uniprot_id:
                        try:
                            cur.execute("""
                                INSERT INTO uniprot_reference (gene_id, uniprot_id)
                                VALUES (%s, %s)
                            """, (gene_id, uniprot_id))
                        except Exception as e:
                            print(f"Warning: Could not insert Uniprot ID {uniprot_id}: {e}")
            elif uniprot_ids:
                try:
                    cur.execute("""
                        INSERT INTO uniprot_reference (gene_id, uniprot_id)
                        VALUES (%s, %s)
                    """, (gene_id, uniprot_ids))
                except Exception as e:
                    print(f"Warning: Could not insert Uniprot ID {uniprot_ids}: {e}")

def migrate_ccds_references(cur, gene_id, gene_data):
    """Миграция CCDS ссылок"""
    for id_obj in gene_data.get('gene_ids', []):
        if id_obj.get('name') == 'ccds_id':
            ccds_ids = id_obj.get('value')
            if isinstance(ccds_ids, list):
                for ccds_id in ccds_ids:
                    if ccds_id:
                        try:
                            cur.execute("""
                                INSERT INTO ccds_reference (gene_id, ccds_id)
                                VALUES (%s, %s)
                            """, (gene_id, ccds_id))
                        except Exception as e:
                            print(f"Warning: Could not insert CCDS ID {ccds_id}: {e}")
            elif ccds_ids:
                try:
                    cur.execute("""
                        INSERT INTO ccds_reference (gene_id, ccds_id)
                        VALUES (%s, %s)
                    """, (gene_id, ccds_ids))
                except Exception as e:
                    print(f"Warning: Could not insert CCDS ID {ccds_ids}: {e}")

if __name__ == "__main__":
    migrate_gene_data()