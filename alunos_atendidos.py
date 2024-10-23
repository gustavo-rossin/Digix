import pandas as pd
import pyodbc

# Definir o caminho do arquivo CSV
alunos_atendidos = "./alunos_atendidos_pnae_2018_2022.csv"

# Carregar o arquivo CSV diretamente em um DataFrame
df = pd.read_csv(alunos_atendidos, encoding='utf-8')

# Mostrar o DataFrame para verificação
print(df.head())

# Carregar o arquivo CSV em pedaços (chunks)

chunksize = 10000  # Número de linhas por pedaço (ajuste conforme necessário)

# Configurar a conexão com o SQL Server
try:
    conn = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=gustavorossin.database.windows.net;"
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
    INSERT INTO AlunosAtendidos (CodAlunoAtendido, Ano, UF, Municipio, Regiao, EsferaGoverno, EtapaEnsino, QtdAlunos)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

# Criar a lista de tuplas a partir do DataFrame
data_to_insert = [
    (
        row['CodAlunoAtendido'], 
        int(row['Ano']),
        row['UF'], 
        row['Municipio'],
        row['Regiao'], 
        row['EsferaGoverno'], 
        row['EtapaEnsino'], 
        row['QtdAlunos']
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