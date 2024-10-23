import pandas as pd
import pyodbc
import requests

# Definir a URL do OData
url = """https://www.fnde.gov.br/olinda-ide/servico/PNAE_Numero_Alunos_Atendidos/versao/v1/odata/Alunos_Atendidos?$filter=Ano%20ge%20%272012%27%20and%20Ano%20le%20%272017%27&$format=json"""


# Fazer a requisição HTTP para obter os dados
response = requests.get(url)
data = response.json()

df = pd.json_normalize(data['value'])

# Mostrar o DataFrame para verificação
print(df.head())
print(df.tail())
print(df.shape)

chunksize = 10000

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
        row['Co_alunos_atendidos'],
        int(row['Ano']),
        row['Estado'],
        row['Municipio'],
        row['Regiao'], 
        row['Esfera_governo'],
        row['Etapa_ensino'], 
        int(row['Qt_alunos_pnae'])
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