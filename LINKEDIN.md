# Post LinkedIn

---

🚀 **De PDF a busca semântica em português — pipeline completo com Airflow, DuckDB, dbt e Qdrant**

No projeto anterior construí um pipeline com Apache Spark + dbt + DuckDB seguindo a Arquitetura Medallion para dados estruturados. Dessa vez o desafio foi diferente: processar **documentos não estruturados** (PDF, DOCX, TXT) e torná-los pesquisáveis semanticamente em português.

A arquitetura foi elaborada e pensada para o **Apache Airflow 3.x** como motor de orquestração — CeleryExecutor, workers customizados e DAG de 5 tasks em sequência dentro da Arquitetura Medallion.

---

**🏗️ 9 containers Docker, uma rede, um pipeline**

→ PostgreSQL + Redis — metadados e broker Celery
→ Airflow 3.x — apiserver, scheduler, dag-processor, worker, triggerer
→ Apache Tika (latest-full) — extração de texto + OCR Tesseract em português
→ DuckDB — lakehouse local bronze / silver / gold
→ Qdrant — vector store para busca semântica
→ Apache Spark 4.x — Spark Connect para processamento distribuído

**Detalhe crítico do Airflow 3.x**: o dag-processor é um processo separado sem as dependências do worker. Imports como `tika`, `torch` e `duckdb` precisam ficar **dentro das tasks** — nunca no nível do módulo.

---

**⚙️ O fluxo**

`extrair_bronze → transformar_silver → transformar_gold → gerar_embeddings → indexar_qdrant`

**Bronze** → Tika REST extrai texto e metadados. PDFs escaneados passam por OCR automático via Tesseract (`TIKA_OCR_LANGUAGE=por`).

**Silver/Gold** → dbt limpa, normaliza e agrega. `TRY_CAST + NULLIF` lida com metadados inconsistentes sem quebrar o pipeline.

**Gold Semântico** → modelo **PORTULAN/serafim-100m** (especializado em português, dim 768). Cada documento é dividido em chunks de 500 palavras com overlap de 50 — cobertura vetorial completa, sem truncamento. Com GPU RTX 3060: 43 chunks em menos de 5 segundos.

**Qdrant** → cada chunk indexado com `chunk_text` no payload, pronto para alimentar um LLM em um sistema RAG.

---

**🔍 Por que chunking é essencial**

Sem chunking, um PDF de 79k caracteres gera um único vetor representando só o início do documento. Com chunking + overlap, o mesmo PDF gera 25 vetores independentes — a busca semântica encontra informações em qualquer parte do conteúdo.

---

**📌 Principais aprendizados**

✅ Airflow 3.x — isolamento de imports entre dag-processor e worker é obrigatório
✅ Tika como serviço REST é mais robusto do que biblioteca embutida
✅ DuckDB + dbt = lakehouse local sem overhead operacional
✅ Chunking com overlap não é opcional para RAG de qualidade
✅ serafim-100m entrega embeddings reais em português sem modelos multilíngues genéricos

---

Esse pipeline é a **fase de indexação de um sistema RAG completo em português**. 100% open source. Sobe com um único `docker compose up`.

💬 Trabalha com documentos não estruturados em português? Comenta aqui.

#EngenhariadeDados #EngenhariadIA #ApacheAirflow #DuckDB #dbt #Qdrant #RAG #Embeddings #NLP #Python #OpenSource #DataScience
