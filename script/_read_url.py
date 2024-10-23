import requests
import pandas as pd

# Definir a URL do OData
url = """https://www.fnde.gov.br/olinda-ide/servico/PDA_Escolas_Atendidas/versao/v1/odata/EscolasAtendidas?$filter=Ano%20ge%20%272012%27%20and%20Ano%20le%20%272017%27&$format=json"""


# Fazer a requisição HTTP para obter os dados
response = requests.get(url)
data = response.json()

df = pd.json_normalize(data['value'])

print(df.head())
print(df.tail())
print(df.shape)