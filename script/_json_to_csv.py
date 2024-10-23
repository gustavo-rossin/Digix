import pandas as pd
import json

# Definir o caminho do arquivo JSON de entrada e o arquivo CSV de saída
json_file = 'escolas_atendidas_2018_2022.json'
csv_file = 'escolas_atendidas_2018_2022.csv'

chunk_size = 100000

# Abrir o arquivo JSON e processá-lo em partes
with open(json_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Normalizar os dados JSON em um DataFrame e gravar no CSV por partes
chunks = pd.json_normalize(data['value'], max_level=1)

# Se o arquivo for grande, você pode quebrá-lo em chunks para não usar muita memória
for i in range(0, len(chunks), chunk_size):
    chunk = chunks[i:i + chunk_size]
    
    # Salvar o DataFrame no CSV, em modo de append para gravar blocos incrementalmente
    chunk.to_csv(csv_file, mode='a', index=False, header=(i == 0), encoding='utf-8')

print(f"Conversão concluída! Arquivo CSV salvo em: {csv_file}")
