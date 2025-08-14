import psycopg2
import sys

# String de conexão com o banco de dados fornecida pelo usuário
DATABASE_URL = "postgresql://bd_arbovirose_user:cKDg5cWiNxkIfrGAVX5242NpLhJEaqdO@dpg-d2f0hn8dl3ps73e2a1l0-a.oregon-postgres.render.com/bd_arbovirose"

def print_table_names():
    """
    Conecta ao banco de dados e imprime o nome de todas as tabelas.
    """
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        print("Conexão com o banco de dados estabelecida com sucesso.")
        print("-" * 50)

        # Query para listar todas as tabelas no schema 'public'
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()

        if not tables:
            print("Nenhuma tabela encontrada no schema 'public'.")
        else:
            print(f"Tabelas encontradas ({len(tables)}):")
            for table in tables:
                table_name = table[0]
                print(f"  - {table_name}")

    except psycopg2.OperationalError as e:
        print(f"Erro de conexão com o banco de dados: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("-" * 50)
        print("Conexão encerrada.")

if __name__ == "__main__":
    print_table_names()