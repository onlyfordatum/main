# 📘 Orquestrador de Pipeline de Dados - Databricks

Este notebook (nb_pipeline_orchestrator) tem como objetivo coordenar de forma sequencial e automatizada o processo de ingestão, tratamento e transformação de dados, seguindo a arquitetura em camadas Medallion: Bronze → Silver → Gold. Foi criado um scheduler job no Databricks que executa este fluxo diariamente à meia-noite, garantindo a atualização contínua e programada dos dados.

---

## 📌 1. Definição dos Objetos de Ingestão

O pipeline começa com a definição da lista de datasets que serão ingeridos. Essa lista permite a expansão do processo para múltiplos arquivos com a mesma estrutura ou origem.

```python
ingestion_objects = ['global-super-store-dataset']
```

---

## 📥 2. Ingestão via API do Kaggle

Para cada objeto da lista, é executado o notebook `nb_ingestion_call_api`, responsável por baixar os dados diretamente da API do Kaggle.

Este processo é encapsulado em um bloco `try/except`, garantindo tolerância a falhas.

```python
./ingestion/nb_ingestion_call_api
```

---

## 🪙 3. Camada Bronze

Os dados brutos obtidos são enviados para a camada **Bronze**, onde são armazenados sem alterações, funcionando como uma réplica segura dos dados de origem.

```python
./medalion/bronze/nb_bronze
```

---

## 🥈 4. Camada Silver

Na camada **Silver**, os dados passam por tratamento, limpeza e padronização. Isso garante integridade e estrutura adequada para uso analítico.

```python
./medalion/silver/nb_silver_global_superstore
```

---

## 🥇 5. Camada Gold

A camada **Gold** é responsável pela transformação dos dados em insights. São aplicadas agregações, cálculos e métricas específicas para gerar outputs prontos para consumo.

Notebooks executados nesta etapa:

```python
[
  './medalion/gold/nb_gold_annual_metrics_delta',
  './medalion/gold/nb_gold_annual_metrics_parquet',
  './medalion/gold/nb_gold_category_delta',
  './medalion/gold/nb_gold_category_parquet',
  './medalion/gold/nb_gold_total_order_value_delta',
  './medalion/gold/nb_gold_total_order_value_parquet'
]
```

---

## ⚙️ Mecanismo de Execução

Todos os notebooks são chamados com `dbutils.notebook.run`, com timeout definido e parâmetros de entrada quando necessário. Em caso de falhas, o erro é capturado e a execução é encerrada com uma mensagem clara.

---
# Projeto de Ingestão de Dados do Global Superstore

## Visão Geral

Este projeto implementa um pipeline de ingestão de dados no Databricks para extrair, processar e carregar o conjunto de dados **Global Superstore Dataset** do Kaggle em uma tabela estruturada na camada Bronze de uma arquitetura de data lakehouse. O pipeline é projetado para lidar com dados no formato CSV, utilizando o PySpark para definição de esquemas e a API do Kaggle para obtenção do dataset.

## Estrutura do Projeto

- **Configuração de Esquemas**: Define o esquema para o dataset Global Superstore e os parâmetros de ingestão.
- **Lógica de Ingestão**: Implementa o processo de extração de dados do Kaggle, cópia de configurações seguras e carga dos dados em uma tabela no catálogo Bronze.

## Configuração do Ambiente

### Pré-requisitos
- Databricks Runtime com suporte ao PySpark.
- Biblioteca `kaggle` instalada (`%pip install kaggle`).
- Credenciais de API do Kaggle configuradas em um arquivo JSON seguro.
- Permissões adequadas para criar catálogos, esquemas e volumes no Databricks.

### Estrutura de Diretórios
- `/Volumes/bronze/ingestion/config/`: Armazena o arquivo de configuração da API do Kaggle (`kaggle.json`).
- `/Volumes/bronze/ingestion/landing_zone/`: Diretório onde os arquivos CSV baixados são armazenados.

## Configuração de Esquemas

O esquema do dataset **Global Superstore** é definido em um notebook auxiliar (`nb_schemas`) usando `StructType` do PySpark. 

### Parâmetros de Ingestão
Os parâmetros de ingestão são configurados no dicionário `_schemas` e incluem:
- **Fonte de Dados**:
  - API: Kaggle
  - Dataset: `apoorvaappz/global-super-store-dataset`
  - Nome do arquivo: `Global_Superstore2.csv`
  - Formato: CSV
  - Local de origem: `/Volumes/bronze/ingestion/landing_zone/`
- **Destino**:
  - Catálogo: `bronze`
  - Esquema: `ingestion`
  - Tabela: `global_superstore`

## Lógica de Ingestão

O processo de ingestão segue as seguintes etapas:

1. **Criação de Estruturas no Databricks**:
   - Criação do catálogo `bronze`, esquema `ingestion` e volumes `config` e `landing_zone`, se não existirem.
   ```sql
   CREATE CATALOG IF NOT EXISTS bronze;
   CREATE SCHEMA IF NOT EXISTS bronze.ingestion;
   CREATE VOLUME IF NOT EXISTS bronze.ingestion.config;
   CREATE VOLUME IF NOT EXISTS bronze.ingestion.landing_zone;
   ```

2. **Configuração da API do Kaggle**:
   - Um arquivo JSON com credenciais da API (`kaggle.json`) é criado e armazenado em `/Volumes/bronze/ingestion/config/`.
   - O arquivo é copiado para o diretório `~/.kaggle/` com permissões restritas (`0o600`) para segurança.
   ```python
   shutil.copy(source_path, destination_path)
   os.chmod(destination_path, 0o600)
   ```

3. **Download do Dataset**:
   - A biblioteca `kaggle` é utilizada para baixar o dataset e descompactá-lo diretamente no diretório `/Volumes/bronze/ingestion/landing_zone/`.
   ```python
   api.dataset_download_files(src_api_dataset, path="/Volumes/bronze/ingestion/landing_zone/", unzip=True)
   ```

4. **Validação**:
   - Os arquivos baixados são listados para verificar a integridade do processo.
   ```python
   display(dbutils.fs.ls('/Volumes/bronze/ingestion/landing_zone/'))
   ```

# Camada Bronze - Ingestão do Global Superstore Dataset

## Visão Geral

Este projeto implementa a camada Bronze de um pipeline de dados no Databricks, responsável por carregar os dados brutos do **Global Superstore Dataset** (obtidos via API do Kaggle) em uma tabela Delta no catálogo `bronze`. A camada Bronze serve como a camada inicial de armazenamento, preservando os dados em seu formato bruto com mínimas transformações, garantindo rastreabilidade e consistência para pipelines downstream.

## Objetivo

- Carregar os dados brutos do arquivo CSV (`Global_Superstore2.csv`) para uma tabela Delta.
- Adicionar um carimbo de ingestão (`ingestion_timestamp`) para rastreamento.
- Padronizar nomes de colunas, substituindo caracteres especiais por underscores e convertendo para letras minúsculas.
- Persistir os dados na tabela `bronze.ingestion.global_superstore` no formato Delta.
- Remover os arquivos de origem da zona de landing após a persistência.

## Estrutura do Projeto

- **Configuração de Esquemas**: Reutiliza as definições de esquema e parâmetros do notebook `nb_schemas` (definido na camada de ingestão).
- **Processamento na Camada Bronze**: Lê o arquivo CSV, aplica transformações básicas e persiste os dados em uma tabela Delta.

## Pré-requisitos

- Databricks Runtime com suporte ao PySpark e Delta Lake.
- Notebook `nb_schemas` configurado com as definições de esquema e parâmetros de ingestão.
- Arquivo CSV (`Global_Superstore2.csv`) disponível em `/Volumes/bronze/ingestion/landing_zone/`.
- Permissões adequadas para criar catálogos, esquemas e tabelas no Databricks.

## Configuração

### Parâmetros de Ingestão
Os parâmetros são herdados do notebook `nb_schemas` e incluem:

- **Fonte de Dados**:
  - API: `kaggle`
  - Dataset: `apoorvaappz/global-super-store-dataset`
  - Nome do arquivo: `Global_Superstore2.csv`
  - Formato: `csv`
  - Local de origem: `/Volumes/bronze/ingestion/landing_zone/`
- **Destino**:
  - Catálogo: `bronze`
  - Esquema: `ingestion`
  - Tabela: `global_superstore`

O esquema do dataset é definido como um `StructType` com 24 campos, todos do tipo `StringType`, conforme detalhado no notebook de ingestão.

## Lógica do Pipeline

O processo da camada Bronze segue as seguintes etapas:

1. **Carregamento de Configurações**:
   - O notebook `nb_schemas` é executado para obter o esquema e os parâmetros de ingestão.
   ```python
   %run ./../../ingestion/structs/nb_schemas
   ```

2. **Geração do Carimbo de Ingestão**:
   - Um carimbo de tempo (`ingestion_timestamp`) é gerado no fuso horário de São Paulo (`America/Sao_Paulo`) no formato `YYYYMMDDHHMMSS`.
   ```python
   ingestion_timestamp = datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y%m%d%H%M%S')
   ```

3. **Identificação do Arquivo de Origem**:
   - O pipeline verifica os arquivos no diretório `/Volumes/bronze/ingestion/landing_zone/` e seleciona aquele que contém o nome `Global_Superstore2.csv`.
   ```python
   for file in dbutils.fs.ls(src_file_internal_location):
       if file.name.__contains__(src_file_name_contains):
           source_file = file.path.replace("dbfs:","")
   ```

4. **Leitura do Arquivo CSV**:
   - O arquivo CSV é lido utilizando o formato `csv`, com opções de separador (`,`), cabeçalho (`true`) e o esquema predefinido.
   ```python
   df = spark.read.format(src_file_format).option('sep', ',').option('header', 'true').schema(schema).load(source_file)
   ```

5. **Transformações Básicas**:
   - Os nomes das colunas são padronizados, substituindo caracteres especiais (espaços, vírgulas, etc.) por underscores e convertendo para letras minúsculas.
   - Uma coluna `ingestion_timestamp` é adicionada com o carimbo de ingestão (tipo `long`).
   ```python
   for col_name in df.columns:
       new_name = re.sub(r"[ ,;{}\(\)\n\t=]", "_", col_name.lower())
       if col_name != new_name:
           df = df.withColumnRenamed(col_name, new_name)
   df = df.withColumn("ingestion_timestamp", f.lit(ingestion_timestamp).cast("long"))
   ```

6. **Criação de Estruturas no Databricks**:
   - O catálogo `bronze` e o esquema `ingestion` são criados, se ainda não existirem.
   ```sql
   CREATE CATALOG IF NOT EXISTS bronze;
   CREATE SCHEMA IF NOT EXISTS bronze.ingestion;
   ```

7. **Persistência dos Dados**:
   - Os dados são salvos como uma tabela Delta no modo `append`, garantindo que novos dados sejam adicionados sem sobrescrever registros existentes.
   ```python
   df.write.format("delta").mode("append").saveAsTable(f"{dest_catalog_name}.{dest_schema_name}.{dest_table_name}")
   ```

8. **Limpeza da Zona de Landing**:
   - Após a persistência, o arquivo de origem é removido da pasta `/Volumes/bronze/ingestion/landing_zone/` para evitar redundância.
   ```python
   for file in dbutils.fs.ls('/Volumes/bronze/ingestion/landing_zone/'):
       if file.name.__contains__(src_file_name_contains.replace('.csv', '')):
           dbutils.fs.rm(file.path.replace('dbfs:', ''))
   ```

## Estrutura da Tabela Bronze

A tabela `bronze.ingestion.global_superstore` contém todos os 24 campos do dataset original, com nomes de colunas padronizados (ex.: `row_id`, `order_date`, `product_name`) e a coluna adicional `ingestion_timestamp` para rastreamento.

## Notas

- **Integridade dos Dados**: A camada Bronze mantém os dados brutos com mínimas transformações, garantindo que pipelines downstream possam realizar processamentos adicionais.
- **Escalabilidade**: O pipeline pode ser adaptado para outros datasets, ajustando o esquema e os parâmetros no notebook `nb_schemas`.
- **Manutenção**: Monitore a pasta de landing para garantir que apenas os arquivos esperados sejam processados e removidos.

---

# Camada Silver - Refinamento do Global Superstore Dataset

## Visão Geral

Este projeto implementa a camada Silver de um pipeline de dados no Databricks, responsável por refinar os dados brutos da tabela `bronze.ingestion.global_superstore` e armazená-los em uma tabela Delta na camada Silver (`silver.refined.global_superstore`). A camada Silver aplica transformações para padronizar tipos de dados, tratar valores nulos e limpar inconsistências, garantindo maior qualidade e usabilidade para análises e pipelines downstream.

## Objetivo

- Filtrar os dados mais recentes da camada Bronze com base no `ingestion_timestamp`.
- Converter colunas para tipos de dados apropriados (ex.: `date`, `int`, `decimal`).
- Tratar valores nulos e inconsistências, como strings não numéricas na coluna `sales`.
- Persistir os dados refinados na tabela Delta `silver.refined.global_superstore` no modo `overwrite`.

## Estrutura do Projeto

- **Configuração de Esquemas**: Reutiliza os parâmetros do notebook `nb_schemas` para definir a origem (camada Bronze) e o destino (camada Silver).
- **Processamento na Camada Silver**: Aplica transformações de dados, incluindo conversão de tipos, tratamento de nulos e limpeza de strings.


## Configuração

### Parâmetros de Processamento
Os parâmetros são herdados do notebook `nb_schemas` e adaptados para a camada Silver:

- **Origem (Camada Bronze)**:
  - Catálogo: `bronze`
  - Esquema: `ingestion`
  - Tabela: `global_superstore`
- **Destino (Camada Silver)**:
  - Catálogo: `silver`
  - Esquema: `refined`
  - Tabela: `global_superstore`

### Estrutura da Tabela de Origem
A tabela `bronze.ingestion.global_superstore` contém 24 campos de dados brutos (todos como `StringType`) e uma coluna `ingestion_timestamp` (tipo `long`).

## Lógica do Pipeline

O processo da camada Silver segue as seguintes etapas:

1. **Carregamento de Configurações**:
   - O notebook `nb_schemas` é executado para obter os parâmetros de origem e destino.
   ```python
   %run ./../../ingestion/structs/nb_schemas
   ```

2. **Criação de Estruturas no Databricks**:
   - O catálogo `silver` e o esquema `refined` são criados, se ainda não existirem.
   ```sql
   CREATE CATALOG IF NOT EXISTS silver;
   CREATE SCHEMA IF NOT EXISTS silver.refined;
   ```

3. **Filtragem dos Dados Mais Recentes**:
   - O pipeline identifica o `ingestion_timestamp` mais recente da tabela Bronze e filtra os dados correspondentes.
   ```python
   filter_max = spark.table(f"{src_catalog_name}.{src_schema_name}.{src_table_name}").select(f.max("ingestion_timestamp")).collect()[0][0]
   df = spark.table(f"{src_catalog_name}.{src_schema_name}.{src_table_name}").filter(f"""ingestion_timestamp = {filter_max}""")
   ```

4. **Transformações de Dados**:
   - **Colunas de Data (`order_date`, `ship_date`)**:
     - Convertidas para o tipo `date` usando o formato `dd-MM-yyyy`.
     - Valores nulos são substituídos por `1900-01-01`.
     ```python
     for col in ['order_date', 'ship_date']:
         df = df.withColumn(col, f.trim(f.to_date(f.col(col), 'dd-MM-yyyy')))
         df = df.withColumn(col, f.when(f.col(col).isNull(), f.lit('1900-01-01').cast('date')).otherwise(f.col(col)))
     ```
   - **Colunas Inteiras (`row_id`)**:
     - Convertidas para o tipo `int`.
     - Valores nulos são substituídos por `0`.
     ```python
     for col in ['row_id']:
         df = df.withColumn(col, f.col(col).cast('int'))
         df = df.withColumn(col, f.when(f.col(col).isNull(), f.lit(0).cast('int')).otherwise(f.col(col)))
     ```
   - **Colunas Decimais (`quantity`, `discount`, `profit`, `shipping_cost`)**:
     - Convertidas para o tipo `decimal(12,2)`.
     - Valores nulos são substituídos por `0.00`.
     ```python
     for col in ['quantity', 'discount', 'profit', 'shipping_cost']:
         df = df.withColumn(col, f.col(col).cast('decimal(12,2)'))
         df = df.withColumn(col, f.when(f.col(col).isNull(), f.lit(0.00).cast('decimal(12,2)')).otherwise(f.col(col)))
     ```
   - **Coluna `sales`**:
     - Registros com valores não numéricos são filtrados usando uma expressão regular (`^-?\d+(\.\d+)?$`).
     - Convertida para `decimal(12,2)`.
     ```python
     sales_regex_clean = r'^-?\d+(\.\d+)?$'
     df = df.filter(f.col("sales").rlike(sales_regex_clean)).withColumn('sales', f.col('sales').cast('decimal(12,2)'))
     ```
   - **Colunas de String**:
     - Valores nulos são substituídos por `'N/A'`.
     - Espaços em branco nas extremidades são removidos com `trim`.
     ```python
     for col in df.dtypes:
         if col[1] == 'string':
             df = df.withColumn(col[0], f.when(f.col(col[0]).isNull(), f.lit('N/A')).otherwise(f.trim(f.col(col[0]))))
     ```

5. **Persistência dos Dados**:
   - Os dados refinados são salvos como uma tabela Delta no modo `overwrite`, substituindo qualquer versão anterior.
   ```python
   df.write.format("delta").mode("overwrite").saveAsTable(f"{dest_catalog_name}.{dest_schema_name}.{dest_table_name}")
   ```

## Estrutura da Tabela Silver

A tabela `silver.refined.global_superstore` contém os mesmos 24 campos da tabela Bronze, mas com tipos de dados refinados.


# Camada Gold - Análises do Global Superstore Dataset

## Visão Geral

A camada Gold do pipeline de dados no Databricks processa os dados refinados da tabela `silver.refined.global_superstore` para gerar visões analíticas otimizadas para análises de negócio. Os resultados são armazenados em tabelas Delta no catálogo `gold.analytics` e em arquivos Parquet particionados em `/Volumes/gold/analytics/parquets_files`. Este pipeline calcula métricas como vendas anuais por produto, vendas mensais por categoria e valor total por pedido, suportando decisões estratégicas baseadas em dados.

## Objetivo

- Transformar dados refinados da camada Silver em visões agregadas para análises de negócio.
- Gerar métricas de vendas, quantidades e valores médios, organizadas por produto, categoria e pedido.
- Persistir os resultados em tabelas Delta e arquivos Parquet, otimizados para consultas analíticas.

## Estrutura do Projeto

- **Fonte de Dados**: Tabela `silver.refined.global_superstore`, contendo dados limpos e tipados.
- **Destino**:
  - Tabelas Delta: Catálogo `gold.analytics`.
  - Arquivos Parquet: Diretório `/Volumes/gold/analytics/parquets_files`.
- **Notebooks**:
  - `nb_gold_annual_metrics_delta`: Métricas anuais por produto (tabela Delta).
  - `nb_gold_annual_metrics_parquet`: Métricas anuais por produto (arquivo Parquet).
  - `nb_gold_category_delta`: Vendas mensais por categoria (tabela Delta).
  - `nb_gold_category_parquet`: Vendas mensais por categoria (arquivo Parquet).
  - `nb_gold_total_order_value_delta`: Valor total por pedido (tabela Delta).
  - `nb_gold_total_order_value_parquet`: Valor total por pedido (arquivo Parquet).

## Configuração

### Parâmetros
Os parâmetros são definidos nos notebooks e incluem:

- **Origem (Camada Silver)**:
  - Catálogo: `silver`
  - Esquema: `refined`
  - Tabela: `global_superstore`
- **Destino (Camada Gold)**:
  - Catálogo: `gold`
  - Esquema: `analytics`
  - Prefixo das tabelas: `global_superstore`
  - Diretório de Parquet: `/Volumes/gold/analytics/parquets_files`

### Estrutura de Armazenamento
- **Tabelas Delta**: Armazenadas em `gold.analytics` com nomes como `global_superstore_annual_metrics`, `global_superstore_category` e `global_superstore_total_order_value`.
- **Arquivos Parquet**: Armazenados em `/Volumes/gold/analytics/parquets_files`, com particionamento por `year` ou `month` quando aplicável.

## Lógica do Pipeline

Os notebooks da camada Gold seguem uma estrutura comum:

1. **Carregamento de Configurações**:
   - O notebook `nb_schemas` é executado para obter os parâmetros de origem.
   ```python
   %run ./../../ingestion/structs/nb_schemas
   ```

2. **Criação de Estruturas**:
   - Criação do catálogo `gold`, esquema `analytics` e volume `parquets_files`, se necessário.
   ```sql
   CREATE CATALOG IF NOT EXISTS gold;
   CREATE SCHEMA IF NOT EXISTS gold.analytics;
   CREATE VOLUME IF NOT EXISTS gold.analytics.parquets_files;
   ```

3. **Leitura dos Dados**:
   - Os dados são lidos da tabela `silver.refined.global_superstore`.
   ```python
   df = spark.read.table(f"{src_catalog_name}.{src_schema_name}.{src_table_name}")
   ```

4. **Transformações e Persistência**:
   Cada notebook realiza agregações específicas e persiste os resultados em tabelas Delta ou arquivos Parquet. Detalhes abaixo:

### 1. Métricas Anuais por Produto
- **Notebooks**: `nb_gold_annual_metrics_delta`, `nb_gold_annual_metrics_parquet`
- **Descrição**: Calcula métricas anuais por produto, incluindo total de vendas, quantidade vendida e média de vendas.
- **Saída**:
  - **Delta**: Tabela `gold.analytics.global_superstore_annual_metrics` (modo `overwrite`, com `overwriteSchema` habilitado).
  - **Parquet**: Arquivos em `/Volumes/gold/analytics/parquets_files/annual_metrics`, particionados por `year` (coalesced para 1 partição).

### 2. Vendas Mensais por Categoria
- **Notebooks**: `nb_gold_category_delta`, `nb_gold_category_parquet`
- **Descrição**: Calcula o total de vendas por categoria e mês.
- **Saída**:
  - **Delta**: Tabela `gold.analytics.global_superstore_category` (modo `overwrite`, com `overwriteSchema` habilitado).
  - **Parquet**: Arquivos em `/Volumes/gold/analytics/parquets_files/category`, particionados por `month` (coalesced para 1 partição).

### 3. Valor Total por Pedido
- **Notebooks**: `nb_gold_total_order_value_delta`, `nb_gold_total_order_value_parquet`
- **Descrição**: Calcula o valor total de cada pedido.
- **Saída**:
  - **Delta**: Tabela `gold.analytics.global_superstore_total_order_value` (modo `overwrite`).
  - **Parquet**: Arquivos em `/Volumes/gold/analytics/parquets_files/total_order_value` (coalesced para 1 partição, sem particionamento).

## Notas

- **Performance**: O uso de `coalesce(1)` nos arquivos Parquet reduz o número de partições para facilitar a leitura em sistemas externos, mas pode impactar a performance em grandes datasets. Considere ajustar para datasets maiores.
- **Escalabilidade**: A estrutura dos notebooks permite a adição de novas métricas analíticas, mantendo os parâmetros e padrões de armazenamento.
- **Manutenção**: Monitore a tabela Silver para garantir que os dados de origem estejam atualizados antes de executar os notebooks Gold.
