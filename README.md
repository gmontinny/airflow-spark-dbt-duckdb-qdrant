# Pipeline de Documentos Não Estruturados

Pipeline de dados moderno aplicado a documentos **PDF, DOCX e TXT** usando **Arquitetura Medallion** com embeddings semânticos em português, orquestrado pelo Apache Airflow 3.x.

---

## Stack

| Componente | Versão | Função |
|---|---|---|
| Apache Airflow | 3.3.1 | Orquestração do pipeline (CeleryExecutor) |
| Apache Tika | latest | Extração de texto e metadados via servidor REST |
| Apache Spark | 4.0.1 | Processamento distribuído (Spark Connect) |
| DuckDB | 1.0+ | Lakehouse local — camadas bronze / silver / gold |
| dbt-duckdb | 1.8+ | Transformações SQL declarativas |
| PORTULAN/serafim-100m | — | Embeddings semânticos em português (dim 768) |
| Qdrant | latest | Vector store para busca semântica |
| PostgreSQL | 16 | Metadados do Airflow |
| Redis | 7.2 | Broker de tarefas Celery |

---

## Suporte a GPU

O pipeline detecta automaticamente se há GPU disponível em runtime:

| Ambiente | Device | Comportamento |
|---|---|---|
| Máquina com NVIDIA GPU + CUDA | `cuda` | Embeddings acelerados por GPU |
| Máquina sem GPU | `cpu` | Embeddings processados por CPU |

O log da task `gerar_embeddings` informa o device em uso:
```
# Com GPU
INFO - Device para embeddings: cuda
INFO - GPU: NVIDIA GeForce RTX 3060 (12.0 GB VRAM)

# Sem GPU
INFO - Device para embeddings: cpu
```

O `Dockerfile` do worker instala `torch` CPU via `requirements.txt` e tenta sobrescrever com a versão CUDA 12.4. Se a máquina não tiver suporte CUDA, o build não falha — mantém a versão CPU silenciosamente.

> **Requisito para GPU**: [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html) instalado no host.

---

## Arquitetura Medallion

```
datas/
 ├── *.pdf
 ├── *.docx
 └── *.txt
       │
       ▼ Apache Tika REST (tika-server:9998)
  [BRONZE] bronze.documentos_raw        ← DuckDB
       │
       ▼ dbt (limpeza + normalização)
  [SILVER] main_silver.documentos_clean ← DuckDB
       │
       ▼ dbt (agregações analíticas)
  [GOLD]   main_gold.resumo_por_tipo    ← DuckDB
       │
       ▼ sentence-transformers (serafim-100m) — GPU ou CPU
  [GOLD]   gold.documentos_embeddings   ← DuckDB
       │
       ▼ Qdrant (indexação vetorial)
  [QDRANT] collection: documentos_pt    ← busca semântica
```

---

## Fluxo da DAG

A DAG `pipeline_documentos_nao_estruturados` executa 5 tasks em sequência:

```
extrair_bronze → transformar_silver → transformar_gold → gerar_embeddings → indexar_qdrant
```

| Task | O que faz |
|---|---|
| `extrair_bronze` | Lê todos os arquivos de `datas/`, chama o Tika REST (`service=text` + `service=meta`), persiste em `bronze.documentos_raw` no DuckDB |
| `transformar_silver` | Executa `dbt run --select silver` — limpa, normaliza e filtra documentos com menos de 50 chars |
| `transformar_gold` | Executa `dbt run --select gold` — agrega métricas por tipo de documento em `main_gold.resumo_por_tipo` |
| `gerar_embeddings` | Detecta GPU/CPU, carrega `PORTULAN/serafim-100m`, gera vetores dim 768 e persiste em `gold.documentos_embeddings` |
| `indexar_qdrant` | Cria (ou recria) a coleção `documentos_pt` no Qdrant e faz upsert de todos os vetores com metadados |

---

## Estrutura do Projeto

```
airflow_dbt_tikas/
├── dags/
│   ├── pipeline_documentos.py       # DAG principal — documentos não estruturados
│   └── pipeline_zap.py              # DAG auxiliar — ZAP imóveis (CSV)
├── datas/                           # Documentos de entrada (PDF / DOCX / TXT)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                 # Conexão DuckDB
│   └── models/
│       ├── silver/
│       │   ├── sources.yml          # Declara bronze.documentos_raw como fonte
│       │   └── documentos_clean.sql # Limpeza e normalização
│       └── gold/
│           └── resumo_por_tipo.sql  # Agregação analítica por tipo
├── spark-connect/
│   ├── Dockerfile                   # Spark 4.x
│   └── conf/
│       └── spark-defaults.conf
├── airflow-worker/
│   └── Dockerfile                   # Worker com torch CPU + tentativa CUDA
├── warehouse/
│   └── lakehouse.duckdb             # Arquivo DuckDB (bronze/silver/gold)
├── docker-compose-airflow.yml
├── requirements.txt
└── .env
```

---

## Serviços e Portas

| Serviço | Container | Porta | Descrição |
|---|---|---|---|
| Airflow UI | `airflow-apiserver` | 8085 | Interface web do Airflow |
| Spark UI | `spark-connect` | 4040 | Monitor de jobs Spark |
| Spark Connect | `spark-connect` | 15002 | gRPC endpoint |
| Tika REST | `tika-server` | 9998 | Extração de texto e metadados |
| Qdrant UI | `qdrant` | 6333 | Dashboard + API REST vetorial |
| Qdrant gRPC | `qdrant` | 6334 | Interface gRPC |

---

## Variáveis de Ambiente

Definidas no `.env` e injetadas em todos os containers Airflow:

| Variável | Valor padrão | Descrição |
|---|---|---|
| `AIRFLOW_UID` | `50000` | UID do usuário Airflow |
| `DUCKDB_PATH` | `/opt/airflow/work-dir/warehouse/lakehouse.duckdb` | Caminho do banco DuckDB |
| `SPARK_CONNECT_URL` | `sc://spark-connect:15002` | Endpoint Spark Connect |
| `QDRANT_HOST` | `qdrant` | Host do Qdrant |
| `QDRANT_PORT` | `6333` | Porta do Qdrant |
| `TIKA_SERVER_JAR` | `http://tika-server:9998` | URL do servidor Tika REST |
| `TIKA_CLIENT_ONLY` | `True` | Força modo cliente REST (sem download de JAR) |

---

## Como Executar

**1. Adicione documentos à pasta `datas/`**
```bash
cp meus_documentos.pdf datas/
```
Formatos suportados: `.pdf`, `.docx`, `.txt`

**2. Suba o ambiente**
```bash
docker compose -f docker-compose-airflow.yml up -d --build
```

**3. Aguarde todos os containers ficarem healthy**
```bash
docker compose -f docker-compose-airflow.yml ps
```

**4. Acesse o Airflow UI**
```
http://localhost:8085
usuário: airflow
senha:   airflow
```

**5. Execute a DAG manualmente**
- Localize `pipeline_documentos_nao_estruturados`
- Clique em **Unpause** e depois em **Trigger DAG**

---

## Modelo de Embeddings

O modelo **[PORTULAN/serafim-100m-portuguese-pt-sentence-encoder](https://huggingface.co/PORTULAN/serafim-100m-portuguese-pt-sentence-encoder)** é especializado em português brasileiro e europeu, gerando vetores de dimensão **768** otimizados para similaridade semântica.

- Download automático do HuggingFace na primeira execução
- Device selecionado automaticamente: CUDA (GPU) ou CPU
- Similaridade por cosseno no Qdrant
- Coleção: `documentos_pt`

---

## Modelos DBT

### `silver.documentos_clean` → criado como `main_silver.documentos_clean`
Limpeza e normalização dos dados extraídos pelo Tika:
- Remove documentos com conteúdo vazio ou menor que 50 caracteres
- Converte tipos (`num_pages` → INTEGER, `created_at` → TIMESTAMP via `TRY_CAST` + `NULLIF`)
- Fonte: `bronze.documentos_raw`

### `gold.resumo_por_tipo` → criado como `main_gold.resumo_por_tipo`
Agregação analítica por tipo de documento:
- Total de documentos, média de páginas, média de caracteres
- Timestamps de primeira e última ingestão
- Fonte: `silver.documentos_clean`

> O dbt-duckdb prefixa os schemas com o nome do database (`main_`). As queries da DAG já consideram esse comportamento.

---

## Dependências Python (requirements.txt)

```
pyspark==4.0.0
duckdb>=1.0.0
dbt-duckdb>=1.8.0
pyarrow>=10.0.0
pandas>=2.0.0
grpcio>=1.74.0
grpcio-status>=1.74.0
apache-airflow-providers-fab
tika>=2.6.0
sentence-transformers>=3.0.0
torch>=2.0.0
qdrant-client>=1.9.0
```

> O `Dockerfile` do worker sobrescreve `torch` com a versão CUDA 12.4 quando disponível.

---

## Parar o Ambiente

```bash
# Para os containers
docker compose -f docker-compose-airflow.yml down

# Para os containers e remove volumes (reset completo)
docker compose -f docker-compose-airflow.yml down -v
```

---

## Visualizando os Dados

### DuckDB — DataGrip / DBeaver / TablePlus

O arquivo `lakehouse.duckdb` fica na pasta `warehouse/` do projeto e pode ser aberto diretamente por qualquer cliente que suporte DuckDB.

**DataGrip**
1. `File` → `New` → `Data Source` → `DuckDB`
2. Em `File` informe o caminho absoluto do arquivo:
   ```
   C:\seu-caminho\airflow_dbt_tikas\warehouse\lakehouse.duckdb
   ```
3. Clique em `Test Connection` → `OK`
4. Os schemas disponíveis após rodar a DAG:

| Schema | Tabela | Descrição |
|---|---|---|
| `bronze` | `documentos_raw` | Dados brutos extraídos pelo Tika |
| `main_silver` | `documentos_clean` | Dados limpos e normalizados (dbt) |
| `main_gold` | `resumo_por_tipo` | Agregação por tipo de documento (dbt) |
| `gold` | `documentos_embeddings` | Vetores dim 768 gerados pelo modelo |

**DBeaver (gratuito)**
1. `Nova Conexão` → pesquise `DuckDB`
2. Em `Path` informe o caminho do arquivo `lakehouse.duckdb`
3. Clique em `Testar Conexão` → `Finalizar`

**Queries úteis:**
```sql
-- Documentos extraídos pelo Tika
SELECT file_name, file_type, num_pages, content_length, author, title
FROM bronze.documentos_raw;

-- Documentos limpos pela camada silver
SELECT file_name, file_type, num_pages, content_length, created_at
FROM main_silver.documentos_clean;

-- Resumo analítico por tipo
SELECT * FROM main_gold.resumo_por_tipo;

-- Embeddings gerados (vetores truncados para visualização)
SELECT doc_id, file_name, file_type, title,
       len(embedding) AS embedding_dim
FROM gold.documentos_embeddings;
```

---

### Qdrant — Dashboard Web

O Qdrant possui uma interface web nativa acessível diretamente pelo browser, sem necessidade de instalar nada.

**Acesso:**
```
http://localhost:6333/dashboard
```

No dashboard você pode:
- Ver a coleção `documentos_pt` com todos os vetores indexados
- Inspecionar os payloads de cada ponto (`doc_id`, `file_name`, `file_type`, `title`, `author`)
- Executar buscas semânticas diretamente pela interface
- Ver métricas da coleção (total de pontos, dimensão dos vetores, função de distância)

**Busca semântica via API REST** (curl ou Postman):
```bash
curl -X POST http://localhost:6333/collections/documentos_pt/points/search \
  -H 'Content-Type: application/json' \
  -d '{
    "vector": [0.1, 0.2, ...],
    "limit": 5,
    "with_payload": true
  }'
```

**Listar todos os pontos indexados:**
```bash
curl http://localhost:6333/collections/documentos_pt/points/scroll \
  -H 'Content-Type: application/json' \
  -d '{"limit": 100, "with_payload": true}'
```

**Informações da coleção:**
```bash
curl http://localhost:6333/collections/documentos_pt
```
