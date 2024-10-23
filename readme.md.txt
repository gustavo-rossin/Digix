
# Projeto E2E: Indicadores Estratégicos do PNAE

Este projeto **End-to-End (E2E)** visa fornecer indicadores estratégicos para o **Programa Nacional de Alimentação Escolar (PNAE)**, com a leitura de dados sobre Escolas Atendidas, Alunos Atendidos e dos Recursos Repassados. A base de dados contém os anos de 2012 até 2022, mas o período analisado está filtrado pelos últimos 5 anos por solicitação do cliente. A visualização dos indicadores está disponível no PowerBI, o qual cobre todo o ciclo de vida dos dados, incluindo a obtenção, transformação, carga e visualização dos dados, permitindo uma análise completa e integrada.

## Funcionalidades

- **Ingestão de dados via CSV e JSON**: Processamento de arquivos CSV contendo informações de recursos repassados do PNAE entre os anos de 2018 e 2022. Para os anos de 2012 até 2017 foram feitos requests na própria URL disponibilizada no FNDE.
- **Conexão com SQL Server**: Inserção eficiente dos dados em um banco de dados SQL Server utilizando a funcionalidade `fast_executemany` para alta performance em grandes volumes de dados.
- **Automação de Processos**: Pipeline que automatiza o processo de ETL (Extração, Transformação e Carga) de dados.
- **Criação de Dashboards no Power BI**: Utilização do Power BI para criar relatórios e dashboards que visualizam os dados carregados, permitindo uma análise detalhada pelos indicadores solicitados.

---

**Autor**: Gustavo Rossin
