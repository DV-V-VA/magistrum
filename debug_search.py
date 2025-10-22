import psycopg2
import sys

DB_CONFIG = {
    "host": "localhost",
    "database": "genes_database", 
    "user": "postgres",
    "password": "12345",
    "port": "5432"
}

def debug_search():
    """Диагностика поиска генов"""
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        with conn.cursor() as cur:
            # 1. Проверим существование таблиц
            print("=== Checking tables ===")
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [row[0] for row in cur.fetchall()]
            print(f"Tables in database: {tables}")
            
            # 2. Проверим данные в таблице gene
            print("\n=== Checking gene data ===")
            cur.execute("SELECT symbol, hgnc_name FROM gene LIMIT 5")
            genes = cur.fetchall()
            print(f"Sample genes: {genes}")
            
            # 3. Проверим конкретный ген (например, GH1)
            test_gene = "GH1"
            print(f"\n=== Checking gene '{test_gene}' ===")
            cur.execute("SELECT * FROM gene WHERE symbol = %s", (test_gene,))
            gene_data = cur.fetchone()
            print(f"Gene data: {gene_data}")
            
            # 4. Проверим поисковый запрос
            print(f"\n=== Testing search for '{test_gene}' ===")
            cur.execute("""
                SELECT DISTINCT g.symbol, g.hgnc_name, g.cytoband
                FROM gene g
                LEFT JOIN gene_alias ga ON g.id = ga.gene_id
                WHERE g.symbol ILIKE %s 
                   OR g.hgnc_name ILIKE %s
                   OR ga.alias_value ILIKE %s
            """, (f"%{test_gene}%", f"%{test_gene}%", f"%{test_gene}%"))
            search_results = cur.fetchall()
            print(f"Search results: {search_results}")
            
    except Exception as e:
        print(f"Debug error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    debug_search()