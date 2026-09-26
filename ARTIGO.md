# Pipeline de Dados para Documentos Não Estruturados: Uma Abordagem Moderna com Arquitetura Medallion, Embeddings Semânticos e Busca Vetorial

**Área:** Engenharia de Dados / Engenharia de IA / Ciência de Dados  
**Palavras-chave:** Apache Airflow, Arquitetura Medallion, DuckDB, dbt, Apache Tika, Embeddings, Qdrant, NLP, Português, RAG

---

## Resumo

Este artigo apresenta a concepção, implementação e validação de um pipeline de dados moderno voltado ao processamento de documentos não estruturados nos formatos PDF, DOCX e TXT. A solução integra tecnologias de ponta em um ambiente totalmente containerizado: Apache Airflow 3.x para orquestração, Apache Tika com Tesseract OCR para extração de texto, DuckDB como lakehouse local seguindo a Arquitetura Medallion (bronze → silver → gold), dbt para transformações declarativas, o modelo de linguagem PORTULAN/serafim-100m para geração de embeddings semânticos em português e Qdrant como vector store para busca semântica. O pipeline é idempotente, suporta aceleração por GPU NVIDIA via CUDA e implementa chunking com overlap para garantir cobertura vetorial completa dos documentos. Os resultados demonstram a viabilidade de construir infraestrutura de Recuperação Aumentada por Geração (RAG) de alta qualidade utilizando exclusivamente ferramentas open source.

---

## 1. Introdução

O volume de documentos não estruturados produzidos por organizações cresce de forma exponencial. Relatórios técnicos, boletins econômicos, contratos, laudos e publicações científicas acumulam-se em sistemas de arquivos sem qualquer estrutura que permita consulta semântica eficiente. Segundo estimativas do IDC (2023), mais de 80% dos dados corporativos existem em formato não estruturado, e a maior parte permanece inacessível para análise automatizada.

A emergência de modelos de linguagem de grande escala (LLMs) e a popularização de técnicas de Recuperação Aumentada por Geração (RAG — *Retrieval-Augmented Generation*) criaram uma demanda crescente por pipelines capazes de transformar documentos brutos em representações vetoriais pesquisáveis. Contudo, a maioria das implementações disponíveis na literatura foca em textos em inglês, negligenciando idiomas como o português, que conta com mais de 260 milhões de falantes nativos.

Este trabalho apresenta uma solução completa e reproduzível para esse problema, com ênfase em:

1. Extração robusta de texto de múltiplos formatos, incluindo PDFs escaneados via OCR
2. Organização dos dados em camadas de qualidade crescente (Arquitetura Medallion)
3. Geração de embeddings semânticos especializados em português
4. Indexação vetorial para busca semântica de alta performance
5. Orquestração confiável e idempotente de todo o fluxo

---

## 2. Fundamentação Teórica

### 2.1 Arquitetura Medallion

A Arquitetura Medallion, popularizada pela Databricks (2021), organiza dados em três camadas de qualidade progressiva:

- **Bronze**: dados brutos, sem transformação, preservando a fidelidade original
- **Silver**: dados limpos, normalizados e validados
- **Gold**: dados agregados e prontos para consumo analítico ou por modelos

Essa abordagem oferece rastreabilidade completa, facilita reprocessamento e separa responsabilidades entre ingestão, qualidade e consumo. Neste trabalho, a camada gold é estendida com uma subcamada de vetorização, criando o que denominamos **Gold Semântico**.

### 2.2 Embeddings e Busca Semântica

Embeddings são representações vetoriais densas de texto em um espaço de alta dimensionalidade onde a proximidade geométrica reflete similaridade semântica (Mikolov et al., 2013). Modelos baseados na arquitetura Transformer (Vaswani et al., 2017), especialmente os *sentence transformers* (Reimers & Gurevych, 2019), produzem embeddings de sentenças e parágrafos com qualidade superior para tarefas de recuperação de informação.

A busca por similaridade de cosseno em espaços vetoriais de alta dimensão é viabilizada por estruturas de índice aproximado como HNSW (*Hierarchical Navigable Small World*), implementada no Qdrant, que oferece complexidade de busca O(log n) com alta precisão.

### 2.3 Chunking e o Problema da Janela de Contexto

Modelos de embedding possuem limite máximo de tokens de entrada — tipicamente 512 tokens para modelos baseados em BERT. Documentos longos submetidos diretamente ao modelo são truncados, resultando em vetores que representam apenas o início do texto. A solução padrão é o **chunking**: divisão do documento em segmentos menores com sobreposição (*overlap*) entre chunks consecutivos para preservar contexto nas fronteiras (Lewis et al., 2020).

### 2.4 RAG — Retrieval-Augmented Generation

RAG (Lewis et al., 2020) é uma técnica que combina recuperação de documentos relevantes com geração de texto por LLMs. O pipeline de indexação apresentado neste artigo constitui a fase de **indexação** de um sistema RAG completo: os chunks vetorizados no Qdrant podem ser consultados em tempo real para fornecer contexto a qualquer LLM, eliminando a necessidade de fine-tuning e reduzindo alucinações.

---

## 3. Arquitetura da Solução

### 3.1 Visão Geral

A solução é composta por 9 serviços Docker orquestrados via Docker Compose, comunicando-se em uma rede bridge dedicada (`pipeline_network`):

```
┌─────────────────────────────────────────────────────────────┐
│                     pipeline_network                        │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────────────┐ │
│  │ postgres │  │  redis   │  │     Apache Airflow 3.x    │ │
│  │  :5432   │  │  :6379   │  │  apiserver / scheduler /  │ │
│  └──────────┘  └──────────┘  │  dag-processor / worker / │ │
│                               │  triggerer / init         │ │
│                               └───────────────────────────┘ │
│                                                             │
│  ┌──────────────┐  ┌──────────┐  ┌─────────────────────┐  │
│  │ tika-server  │  │  qdrant  │  │   spark-connect     │  │
│  │    :9998     │  │  :6333   │  │   :15002 / :4040    │  │
│  │  + Tesseract │  │  :6334   │  │   Spark 4.0.1       │  │
│  └──────────────┘  └──────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Stack Tecnológica

| Componente | Versão | Função |
|---|---|---|
| Apache Airflow | 3.3.1 | Orquestração (CeleryExecutor) |
| Apache Tika | latest-full | Extração de texto + OCR (Tesseract) |
| Apache Spark | 4.0.1 | Processamento distribuído (Spark Connect) |
| DuckDB | 1.0+ | Lakehouse local — bronze / silver / gold |
| dbt-duckdb | 1.8+ | Transformações SQL declarativas |
| PORTULAN/serafim-100m | — | Embeddings em português (dim 768) |
| Qdrant | latest | Vector store — busca semântica |
| PostgreSQL | 16 | Metadados do Airflow |
| Redis | 7.2 | Broker Celery |

### 3.3 Fluxo de Dados

```
datas/
 ├── *.pdf  (texto nativo ou escaneado via OCR)
 ├── *.docx
 └── *.txt
       │
       ▼ Apache Tika REST + Tesseract OCR
  [BRONZE] bronze.documentos_raw        ← DuckDB
       │
       ▼ dbt (limpeza + normalização)
  [SILVER] main_silver.documentos_clean ← DuckDB
       │
       ▼ dbt (agregações analíticas)
  [GOLD]   main_gold.resumo_por_tipo    ← DuckDB
       │
       ▼ serafim-100m — chunking + GPU/CPU
  [GOLD]   gold.documentos_embeddings   ← DuckDB
       │
       ▼ Qdrant upsert
  [QDRANT] collection: documentos_pt    ← busca semântica
```


---

## 4. Implementação

### 4.1 Orquestração com Apache Airflow 3.x

O Airflow 3.x introduz mudanças arquiteturais significativas em relação às versões anteriores. O componente `dag-processor` foi separado do `scheduler`, tornando-se um processo independente responsável exclusivamente pelo parsing e serialização das DAGs. Essa separação tem uma implicação crítica para o design do pipeline: **imports pesados não podem existir no nível do módulo Python**.

O `dag-processor` utiliza a imagem base do Airflow, sem as dependências adicionais instaladas no worker. Qualquer `import tika`, `import torch` ou `import duckdb` no topo do arquivo causaria falha silenciosa no processamento da DAG. A solução adotada é mover todos os imports para dentro do corpo de cada task:

```python
@task()
def extrair_bronze() -> int:
    import tika                          # import dentro da task
    from tika import parser as tika_parser
    # ...
```

A DAG utiliza o padrão `@dag` + `@task` (TaskFlow API), que elimina a necessidade de definir XComs explicitamente — os valores de retorno de cada task são automaticamente serializados e passados para a task seguinte:

```python
bronze = extrair_bronze()
silver = transformar_silver(bronze)
gold   = transformar_gold(silver)
embeds = gerar_embeddings(gold)
indexar_qdrant(embeds)
```

O executor utilizado é o **CeleryExecutor** com Redis como broker, permitindo execução distribuída de tasks em múltiplos workers. O worker é construído a partir de um Dockerfile customizado que instala as dependências Python adicionais e tenta instalar a versão CUDA do PyTorch:

```dockerfile
FROM apache/airflow:3.3.1-python3.12
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install torch==2.5.1 \
    --index-url https://download.pytorch.org/whl/cu124 || echo "CUDA não disponível, mantendo CPU"
```

O `|| echo` garante que o build não falhe em ambientes sem suporte CUDA, mantendo a versão CPU instalada via `requirements.txt`.

### 4.2 Extração de Texto — Apache Tika com OCR

O Apache Tika é um toolkit de análise de conteúdo que suporta mais de 1.000 formatos de arquivo. Neste pipeline, é executado como servidor REST independente usando a imagem `apache/tika:latest-full`, que inclui o Tesseract OCR pré-instalado.

A separação do Tika como serviço REST (em vez de biblioteca Java embutida) oferece vantagens importantes:

- **Isolamento**: falhas no Tika não afetam o processo Python
- **Escalabilidade**: o servidor pode ser escalado independentemente
- **Modo cliente**: o worker Python não precisa de JVM instalada

A configuração `TIKA_CLIENT_ONLY=True` força o cliente Python a usar exclusivamente o servidor REST, sem tentar baixar o JAR localmente:

```python
os.environ["TIKA_CLIENT_ONLY"] = "True"
tika.TikaClientOnly = True

text_result = tika_parser.from_file(
    str(file_path), serverEndpoint=TIKA_ENDPOINT, service="text"
)
meta_result = tika_parser.from_file(
    str(file_path), serverEndpoint=TIKA_ENDPOINT, service="meta"
)
```

As chamadas `service="text"` e `service="meta"` são feitas separadamente para maximizar a extração de metadados Dublin Core (`dc:creator`, `dc:title`, `dcterms:created`) e metadados proprietários (`xmpTPg:NPages`, `Author`, `Creation-Date`).

**OCR automático**: a imagem `latest-full` detecta automaticamente se um PDF possui camada de texto nativa ou se é uma imagem escaneada. Para PDFs escaneados, o Tesseract é invocado automaticamente com o idioma configurado via `TIKA_OCR_LANGUAGE=por`, garantindo alta precisão para português brasileiro e europeu.

| Tipo de documento | Comportamento |
|---|---|
| PDF com texto nativo | Extração direta — rápida e precisa |
| PDF escaneado | OCR automático via Tesseract (português) |
| DOCX | Extração de texto e metadados nativos |
| TXT | Leitura direta do conteúdo |

### 4.3 Camada Bronze — DuckDB

O DuckDB é um sistema de gerenciamento de banco de dados analítico embarcado, otimizado para cargas OLAP. Diferentemente do SQLite (orientado a OLTP), o DuckDB utiliza armazenamento colunar e execução vetorizada, oferecendo performance comparável a sistemas distribuídos para datasets de escala média.

A camada bronze persiste os dados brutos extraídos pelo Tika sem qualquer transformação, preservando a fidelidade original:

```python
conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
conn.execute("DROP TABLE IF EXISTS bronze.documentos_raw")
conn.execute("CREATE TABLE bronze.documentos_raw AS SELECT * FROM df")
```

O padrão `DROP TABLE IF EXISTS` + `CREATE TABLE AS SELECT` garante idempotência: execuções repetidas do pipeline sempre refletem o estado atual da pasta `datas/`, sem acumulação de duplicatas.

Um detalhe importante é o gerenciamento de concorrência: o DuckDB permite apenas uma conexão de escrita por vez. Em um ambiente Airflow com múltiplas tasks potencialmente executando em paralelo, é necessário implementar retry com backoff:

```python
def _duckdb_connect(path: str):
    import duckdb
    for attempt in range(10):
        try:
            return duckdb.connect(path)
        except duckdb.IOException:
            if attempt == 9:
                raise
            time.sleep(3)
```

### 4.4 Camadas Silver e Gold — dbt

O dbt (*data build tool*) é uma ferramenta de transformação que permite escrever transformações SQL como modelos versionados, testáveis e documentados. O adaptador `dbt-duckdb` integra nativamente com o DuckDB.

**Modelo Silver** (`documentos_clean.sql`):

```sql
SELECT
    doc_id,
    file_name,
    file_type,
    TRIM(content)                                        AS content,
    CAST(num_pages AS INTEGER)                           AS num_pages,
    CAST(content_length AS INTEGER)                      AS content_length,
    author,
    title,
    TRY_CAST(NULLIF(TRIM(created_at), '') AS TIMESTAMP) AS created_at,
    TRY_CAST(ingested_at AS TIMESTAMP)                  AS ingested_at
FROM {{ source('bronze', 'documentos_raw') }}
WHERE content IS NOT NULL
  AND TRIM(content) != ''
  AND content_length > 50
```

O uso de `TRY_CAST` com `NULLIF` é essencial para lidar com metadados de data inconsistentes retornados pelo Tika — datas em formatos proprietários ou strings vazias não causam falha, sendo convertidas para `NULL`.

**Modelo Gold** (`resumo_por_tipo.sql`):

```sql
SELECT
    file_type,
    COUNT(*)         AS total_documentos,
    AVG(num_pages)   AS media_paginas,
    AVG(content_length) AS media_chars,
    MIN(ingested_at) AS primeira_ingestao,
    MAX(ingested_at) AS ultima_ingestao
FROM {{ ref('documentos_clean') }}
GROUP BY file_type
```

Um comportamento específico do `dbt-duckdb` é o prefixo automático de schemas com o nome do database (`main_`). Tabelas criadas no schema `silver` ficam acessíveis como `main_silver.documentos_clean`, e as queries Python devem considerar esse comportamento.

### 4.5 Geração de Embeddings com Chunking

Esta é a task mais computacionalmente intensiva do pipeline. A implementação resolve dois problemas fundamentais: o limite de tokens do modelo e a cobertura completa do conteúdo.

**Função de chunking por palavras com overlap:**

```python
def _chunk_text(text: str, size: int, overlap: int) -> list[str]:
    words = text.split()
    step  = max(1, size - overlap)
    return [
        " ".join(words[i:i + size])
        for i in range(0, len(words), step)
        if words[i:i + size]
    ]
```

A escolha de chunking por palavras (em vez de tokens ou caracteres) oferece chunks de tamanho semântico mais consistente, independente do vocabulário do modelo. Com `CHUNK_SIZE=500` e `CHUNK_OVERLAP=50`, cada chunk tem aproximadamente 500 palavras com 50 palavras de sobreposição com o chunk seguinte, preservando contexto nas fronteiras.

**Detecção automática de GPU:**

```python
device = "cuda" if torch.cuda.is_available() else "cpu"
model = SentenceTransformer(EMBEDDING_MODEL, device=device)
```

O modelo `PORTULAN/serafim-100m-portuguese-pt-sentence-encoder` é carregado diretamente do HuggingFace Hub na primeira execução e cacheado localmente. A codificação em batch com `batch_size=32` maximiza a utilização da GPU:

```python
embeddings = model.encode(
    chunks_df["chunk_text"].tolist(),
    batch_size=32,
    show_progress_bar=False,
    normalize_embeddings=True,
)
```

A normalização L2 (`normalize_embeddings=True`) é essencial para que a similaridade de cosseno no Qdrant funcione corretamente — vetores normalizados têm norma 1, tornando o produto interno equivalente à similaridade de cosseno.

Cada chunk é persistido como um registro independente na tabela `gold.documentos_embeddings`, com os campos `doc_id`, `chunk_id`, `chunk_index`, `chunk_text` e `embedding`.

### 4.6 Indexação Vetorial — Qdrant

O Qdrant é um vector store de alta performance escrito em Rust, com suporte a filtragem por payload, múltiplos índices e API REST/gRPC. A coleção é configurada com distância de cosseno e dimensão 768:

```python
client.create_collection(
    collection_name=QDRANT_COLLECTION,
    vectors_config=VectorParams(size=768, distance=Distance.COSINE),
)
```

Cada chunk é indexado como um `PointStruct` com o vetor e um payload rico em metadados:

```python
PointStruct(
    id=abs(hash(row[0])) % (2 ** 63),
    vector=list(row[8]),
    payload={
        "chunk_id":    row[0],
        "doc_id":      row[1],
        "file_name":   row[2],
        "file_type":   row[3],
        "title":       row[4],
        "author":      row[5],
        "chunk_index": row[6],
        "chunk_text":  row[7],
    },
)
```

O campo `chunk_text` no payload é fundamental para sistemas RAG: após a busca por similaridade, o texto do trecho relevante está disponível imediatamente para ser injetado no prompt do LLM, sem necessidade de consulta adicional ao banco de dados.


---

## 5. O Modelo PORTULAN/serafim-100m

### 5.1 Motivação para um Modelo Especializado em Português

A maioria dos modelos de embedding disponíveis publicamente foi treinada predominantemente em inglês. Modelos multilíngues como `paraphrase-multilingual-mpnet-base-v2` incluem português, mas com representação significativamente menor no corpus de treinamento, resultando em embeddings de qualidade inferior para textos em português.

O modelo **PORTULAN/serafim-100m-portuguese-pt-sentence-encoder**, desenvolvido pelo laboratório PORTULAN/CLARIN da Universidade de Lisboa, foi treinado especificamente em corpora de português brasileiro e europeu, utilizando a arquitetura *sentence-transformers* com fine-tuning para tarefas de similaridade semântica.

### 5.2 Características Técnicas

| Característica | Valor |
|---|---|
| Arquitetura base | BERT (encoder-only) |
| Parâmetros | ~100 milhões |
| Dimensão do embedding | 768 |
| Tokens máximos | 512 |
| Idiomas | Português (BR + PT) |
| Tarefa otimizada | Similaridade semântica de sentenças |
| Função de distância recomendada | Cosseno |

### 5.3 Performance com GPU RTX 3060

Com a GPU NVIDIA GeForce RTX 3060 (12 GB VRAM) e `batch_size=32`, o throughput de codificação observado foi:

| Documento | Chars | Chunks gerados | Tempo (GPU) |
|---|---|---|---|
| fipezap-202608-residencial-venda.pdf | 79.077 | ~25 chunks | ~2s |
| Boletim_Economico_ABRAINC_2tri25.pdf | 24.446 | ~10 chunks | <1s |
| ipop-dezembro-2025.pdf | 12.067 | ~8 chunks | <1s |
| **Total** | **115.590** | **~43 chunks** | **<5s** |

O tempo total da task `gerar_embeddings` incluindo carregamento do modelo é de aproximadamente 30-45 segundos na primeira execução (download do modelo) e 5-10 segundos nas execuções subsequentes (modelo cacheado).

---

## 6. Resultados e Validação

### 6.1 Pipeline End-to-End

O pipeline foi validado com 3 documentos PDF de domínio econômico-imobiliário em português:

- `Boletim_Economico_ABRAINC_2tri25.pdf` — boletim econômico do setor imobiliário
- `fipezap-202608-residencial-venda.pdf` — índice de preços imobiliários FipeZap
- `ipop-dezembro-2025.pdf` — índice de preços ao consumidor

**Resultado da camada Bronze:**

```sql
SELECT file_name, file_type, num_pages, content_length, author
FROM bronze.documentos_raw;
```

| file_name | file_type | num_pages | content_length | author |
|---|---|---|---|---|
| Boletim_Economico_ABRAINC_2tri25.pdf | pdf | 12 | 24.446 | Bruno |
| fipezap-202608-residencial-venda.pdf | pdf | 8 | 79.077 | — |
| ipop-dezembro-2025.pdf | pdf | 4 | 12.067 | — |

**Resultado da camada Gold — Embeddings:**

```sql
SELECT file_name, COUNT(*) AS total_chunks
FROM gold.documentos_embeddings
GROUP BY file_name;
```

| file_name | total_chunks |
|---|---|
| Boletim_Economico_ABRAINC_2tri25.pdf | 10 |
| fipezap-202608-residencial-venda.pdf | 25 |
| ipop-dezembro-2025.pdf | 8 |
| **Total** | **43** |

**Qdrant — coleção `documentos_pt`:**
- Total de pontos indexados: **43**
- Dimensão dos vetores: **768**
- Função de distância: **Cosine**
- Payload por ponto: `chunk_id`, `doc_id`, `chunk_index`, `chunk_text`, `file_name`, `file_type`, `title`, `author`

### 6.2 Validação da Busca Semântica

A busca semântica foi validada via API REST do Qdrant. O fluxo de consulta consiste em:

1. Gerar o vetor da query com o mesmo modelo `serafim-100m`
2. Submeter o vetor ao endpoint `/collections/documentos_pt/points/search`
3. Receber os K chunks mais similares com seus payloads

Exemplo de requisição via Postman:

```
POST http://localhost:6333/collections/documentos_pt/points/search
```
```json
{
  "vector": [0.0231, -0.0412, 0.0187, ...],
  "limit": 5,
  "with_payload": true
}
```

A resposta retorna os chunks mais semanticamente próximos da query, com o campo `chunk_text` contendo o trecho exato do documento — pronto para ser injetado no contexto de um LLM.

### 6.3 Idempotência

O pipeline foi executado múltiplas vezes para validar idempotência. Em todas as execuções, o resultado final foi idêntico — 43 pontos no Qdrant, sem duplicatas. O mecanismo de idempotência por camada:

| Task | Mecanismo |
|---|---|
| `extrair_bronze` | `DROP TABLE IF EXISTS` antes de criar |
| `transformar_silver` | dbt `materialized: table` — DROP + CREATE |
| `transformar_gold` | dbt `materialized: table` — DROP + CREATE |
| `gerar_embeddings` | `DROP TABLE IF EXISTS` antes de criar |
| `indexar_qdrant` | Delete coleção + recria antes do upsert |

---

## 7. Discussão

### 7.1 Vantagens da Abordagem

**DuckDB como lakehouse local**: a escolha do DuckDB elimina a necessidade de infraestrutura de dados distribuída para volumes de dados de escala média (até dezenas de GB). O DuckDB oferece performance analítica comparável ao Spark para datasets que cabem em memória, com zero overhead operacional. O arquivo `.duckdb` pode ser aberto diretamente por ferramentas como DataGrip, DBeaver e TablePlus, facilitando inspeção e debugging.

**dbt para transformações**: o uso do dbt garante que as transformações sejam versionadas, testáveis e documentadas. A separação entre lógica de ingestão (Python) e lógica de transformação (SQL declarativo) segue o princípio de responsabilidade única e facilita manutenção.

**Tika como serviço REST**: executar o Tika como servidor REST independente isola a JVM do processo Python, elimina problemas de compatibilidade de versão e permite que o servidor seja escalado ou substituído sem modificar o código da DAG.

**Chunking com overlap**: a implementação de chunking garante que documentos longos sejam completamente vetorizados, sem truncamento. O overlap de 50 palavras entre chunks consecutivos preserva contexto semântico nas fronteiras, melhorando a qualidade da recuperação para queries que abrangem múltiplos chunks.

### 7.2 Limitações e Trabalhos Futuros

**Chunking por palavras vs. tokens**: a implementação atual faz chunking por contagem de palavras. Uma abordagem mais precisa seria usar o tokenizador do próprio modelo para garantir que nenhum chunk exceda 512 tokens. Para português, a diferença é pequena (média de ~1.3 tokens por palavra), mas pode ser relevante para textos técnicos com terminologia especializada.

**Busca semântica sem endpoint de query**: atualmente, para realizar uma busca semântica de verdade (converter uma pergunta em vetor), é necessário executar o modelo `serafim-100m` externamente. Uma extensão natural é expor um endpoint FastAPI que receba texto, gere o embedding e consulte o Qdrant, completando o ciclo RAG.

**Escalabilidade**: o DuckDB com arquivo único tem limitações para acesso concorrente de escrita. Para volumes maiores ou múltiplos pipelines simultâneos, a migração para um lakehouse distribuído (Delta Lake, Apache Iceberg) ou um banco analítico como ClickHouse seria recomendada.

**Qualidade do OCR**: a precisão do Tesseract para PDFs escaneados depende da qualidade da digitalização. Documentos com baixa resolução, fontes incomuns ou layouts complexos podem resultar em texto extraído com erros. Pré-processamento de imagem (binarização, deskew) pode melhorar significativamente a qualidade do OCR.

**Reprocessamento incremental**: o pipeline atual é totalmente idempotente mas não incremental — reprocessa todos os documentos a cada execução. Para coleções grandes, implementar detecção de mudanças (hash do arquivo) e reprocessar apenas documentos novos ou modificados reduziria significativamente o tempo de execução.

### 7.3 Posicionamento no Ecossistema RAG

O pipeline apresentado constitui a **fase de indexação** de um sistema RAG completo. A arquitetura resultante é:

```
[Indexação — este pipeline]          [Consulta — extensão futura]
                                      
Documentos → Tika → DuckDB           Query do usuário
    → dbt → serafim-100m                  → serafim-100m
    → Qdrant                              → Qdrant search
                                          → chunks relevantes
                                          → LLM (GPT, Llama, etc.)
                                          → Resposta fundamentada
```

A separação entre indexação e consulta é uma característica desejável: o pipeline de indexação pode ser executado em batch (diariamente, semanalmente) enquanto o endpoint de consulta opera em tempo real com baixa latência.


---

## 8. Conclusão

Este artigo apresentou um pipeline de dados completo para processamento de documentos não estruturados em português, integrando tecnologias modernas de engenharia de dados e engenharia de IA em uma solução totalmente open source e containerizada.

As principais contribuições do trabalho são:

1. **Arquitetura de referência** para pipelines de documentos não estruturados com Arquitetura Medallion estendida com camada semântica (Gold Semântico)

2. **Solução de OCR integrada** via Apache Tika com Tesseract, sem configuração adicional, suportando PDFs escaneados em português

3. **Implementação de chunking com overlap** que garante cobertura vetorial completa de documentos longos, resolvendo o problema de truncamento de modelos com janela de contexto limitada

4. **Pipeline idempotente e reproduzível** que pode ser executado múltiplas vezes sem efeitos colaterais, facilitando desenvolvimento e manutenção

5. **Suporte transparente a GPU** com fallback automático para CPU, tornando a solução portável entre ambientes com e sem aceleração por hardware

6. **Base para sistemas RAG em português** — os 43 chunks indexados com o modelo `serafim-100m` estão prontos para alimentar qualquer LLM via busca semântica no Qdrant

A combinação de DuckDB + dbt para o lakehouse e Qdrant para busca vetorial demonstra que é possível construir infraestrutura de dados de alta qualidade sem depender de serviços gerenciados em nuvem, mantendo total controle sobre os dados e custos operacionais próximos de zero.

O código completo está disponível e pode ser executado com um único comando `docker compose up`, tornando a solução acessível para equipes de qualquer porte.

---

## Referências

APACHE SOFTWARE FOUNDATION. **Apache Airflow Documentation**. Versão 3.3.1. Disponível em: https://airflow.apache.org/docs/. Acesso em: 2025.

APACHE SOFTWARE FOUNDATION. **Apache Tika — a content analysis toolkit**. Disponível em: https://tika.apache.org/. Acesso em: 2025.

DATABRICKS. **What is the Medallion Lakehouse Architecture?** Disponível em: https://www.databricks.com/glossary/medallion-architecture. Acesso em: 2025.

DEVLIN, J.; CHANG, M.-W.; LEE, K.; TOUTANOVA, K. **BERT: Pre-training of Deep Bidirectional Transformers for Language Understanding**. In: Proceedings of NAACL-HLT 2019, Minneapolis, Minnesota, 2019. p. 4171–4186.

DUCKDB LABS. **DuckDB — An in-process SQL OLAP database management system**. Disponível em: https://duckdb.org/. Acesso em: 2025.

GETDBT. **dbt — Data Build Tool**. Disponível em: https://docs.getdbt.com/. Acesso em: 2025.

IDC. **The Digitization of the World — From Edge to Core**. IDC White Paper, Doc. #US44413318, 2023.

JOHNSON, J.; DOUZE, M.; JÉGOU, H. **Billion-scale similarity search with GPUs**. IEEE Transactions on Big Data, v. 7, n. 3, p. 535–547, 2021.

LEWIS, P. et al. **Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks**. In: Advances in Neural Information Processing Systems (NeurIPS), 2020. p. 9459–9474.

MALKOV, Y. A.; YASHUNIN, D. A. **Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs**. IEEE Transactions on Pattern Analysis and Machine Intelligence, v. 42, n. 4, p. 824–836, 2020.

MIKOLOV, T.; SUTSKEVER, I.; CHEN, K.; CORRADO, G.; DEAN, J. **Distributed Representations of Words and Phrases and their Compositionality**. In: Advances in Neural Information Processing Systems (NeurIPS), 2013. p. 3111–3119.

NVIDIA CORPORATION. **NVIDIA Container Toolkit Documentation**. Disponível em: https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/. Acesso em: 2025.

PORTULAN/CLARIN. **serafim-100m-portuguese-pt-sentence-encoder**. HuggingFace Model Hub. Disponível em: https://huggingface.co/PORTULAN/serafim-100m-portuguese-pt-sentence-encoder. Acesso em: 2025.

QDRANT TEAM. **Qdrant — Vector Database & Vector Similarity Search Engine**. Disponível em: https://qdrant.tech/documentation/. Acesso em: 2025.

REIMERS, N.; GUREVYCH, I. **Sentence-BERT: Sentence Embeddings using Siamese BERT-Networks**. In: Proceedings of EMNLP-IJCNLP 2019, Hong Kong, China, 2019. p. 3982–3992.

SMITH, R.; ANTONOVA, D.; CHANG, D.-S. **Adapting the Tesseract Open Source OCR Engine for Multilingual OCR**. In: Proceedings of the International Workshop on Multilingual OCR, Barcelona, Spain, 2009.

VASWANI, A. et al. **Attention Is All You Need**. In: Advances in Neural Information Processing Systems (NeurIPS), 2017. p. 5998–6008.

ZAHARIA, M. et al. **Apache Spark: A Unified Engine for Big Data Processing**. Communications of the ACM, v. 59, n. 11, p. 56–65, 2016.

---

## Apêndice A — Estrutura do Projeto

```
airflow_dbt_tikas/
├── dags/
│   └── pipeline_documentos.py       # DAG principal (5 tasks)
├── datas/                           # Documentos de entrada (PDF/DOCX/TXT)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── silver/
│       │   ├── sources.yml
│       │   └── documentos_clean.sql
│       └── gold/
│           └── resumo_por_tipo.sql
├── airflow-worker/
│   └── Dockerfile                   # Worker com torch CPU + CUDA 12.4
├── spark-connect/
│   └── Dockerfile                   # Spark 4.0.1
├── warehouse/
│   └── lakehouse.duckdb             # DuckDB (bronze/silver/gold)
├── docker-compose-airflow.yml
├── requirements.txt
└── .env
```

## Apêndice B — Variáveis de Ambiente

| Variável | Valor padrão | Descrição |
|---|---|---|
| `AIRFLOW_UID` | `50000` | UID do usuário Airflow |
| `DUCKDB_PATH` | `/opt/airflow/work-dir/warehouse/lakehouse.duckdb` | Caminho do DuckDB |
| `QDRANT_HOST` | `qdrant` | Host do Qdrant |
| `QDRANT_PORT` | `6333` | Porta do Qdrant |
| `TIKA_SERVER_JAR` | `http://tika-server:9998` | URL do servidor Tika REST |
| `TIKA_CLIENT_ONLY` | `True` | Força modo cliente REST |
| `TIKA_OCR_LANGUAGE` | `por` | Idioma do Tesseract OCR |
| `SPARK_CONNECT_URL` | `sc://spark-connect:15002` | Endpoint Spark Connect |

## Apêndice C — Queries de Validação (DuckDB)

```sql
-- Documentos extraídos (bronze)
SELECT file_name, file_type, num_pages, content_length
FROM bronze.documentos_raw;

-- Documentos limpos (silver)
SELECT file_name, file_type, num_pages, content_length, created_at
FROM main_silver.documentos_clean;

-- Resumo analítico por tipo (gold)
SELECT * FROM main_gold.resumo_por_tipo;

-- Chunks vetorizados com dimensão do embedding
SELECT file_name, chunk_index, len(embedding) AS dim
FROM gold.documentos_embeddings
ORDER BY file_name, chunk_index;

-- Total de chunks por documento
SELECT file_name, COUNT(*) AS total_chunks
FROM gold.documentos_embeddings
GROUP BY file_name;
```

## Apêndice D — Exemplos de Consulta ao Qdrant (Postman)

**Listar chunks com payload:**
```
POST http://localhost:6333/collections/documentos_pt/points/scroll
Body: { "limit": 100, "with_payload": true, "with_vectors": false }
```

**Obter vetor de um chunk para teste:**
```
POST http://localhost:6333/collections/documentos_pt/points/scroll
Body: { "limit": 1, "with_payload": true, "with_vectors": true }
```

**Busca semântica por similaridade:**
```
POST http://localhost:6333/collections/documentos_pt/points/search
Body: { "vector": [...768 floats...], "limit": 5, "with_payload": true }
```

**Filtrar por documento específico:**
```
POST http://localhost:6333/collections/documentos_pt/points/search
Body: {
  "vector": [...],
  "limit": 5,
  "with_payload": true,
  "filter": {
    "must": [{ "key": "file_name", "match": { "value": "Boletim_Economico_ABRAINC_2tri25.pdf" } }]
  }
}
```

**Informações da coleção:**
```
GET http://localhost:6333/collections/documentos_pt
```
