import pandas as pd
import pyodbc

# Definir o caminho do arquivo CSV
recursos_repassados = "csv/recursos_repassados_2012_2017.csv"

# Carregar o arquivo CSV diretamente em um DataFrame
df = pd.read_csv(recursos_repassados, encoding='utf-8')

# Mostrar o DataFrame para verificação
print(df.head())
print(df.tail())
print(df.shape)

chunksize = 10000

# Configurar a conexão com o SQL Server com fast_executemany habilitado
try:
    conn = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=gustavorossin.database.windows.net;"  # Atualize com o nome do servidor
        "Database=gustavorossin;"
        "UID=gustavorossin;"
        "PWD=s8a)sLk20$;",
        autocommit=False
    )
    print("Conexão bem-sucedida com o banco de dados.")
except pyodbc.Error as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    exit()

# Definir o SQL para inserção
insert_sql = """
    INSERT INTO RecursosRepassados (CodRecursosRepassados, Ano, UF, Municipio, EsferaGoverno, ModalidadeEnsino, VlTotalEscolas)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

# Criar a lista de tuplas a partir do DataFrame
data_to_insert = [
    (
        row['Co_recursos_repassados'], 
        int(row['Ano']),
        row['Estado'], 
        row['Municipio'], 
        row['Esfera_governo'], 
        row['Modalidade_ensino'], 
        row['Vl_total_escolas']
    )
    for index, row in df.iterrows()
]

# Inserir os dados no SQL Server em chunks com fast_executemany
try:
    cursor = conn.cursor()
    cursor.fast_executemany = True  # Ativar fast_executemany
    for i in range(0, len(data_to_insert), chunksize):
        chunk = data_to_insert[i:i+chunksize]
        cursor.executemany(insert_sql, chunk)
        print(f"Chunk {i // chunksize + 1} inserido com sucesso.")
    
    conn.commit()
    print("Transação confirmada.")
    
except Exception as e:
    conn.rollback()
    print(f"Erro durante a inserção: {e}")

finally:
    cursor.close()
    conn.close()
    print("Conexão fechada.")
