# Post LinkedIn — Pipeline de Documentos Não Estruturados

---

🚀 **De documento PDF a busca semântica em português — pipeline completo com Airflow, DuckDB, dbt e Qdrant**

No projeto anterior compartilhei como construí um pipeline de dados com **Apache Spark**, dbt e DuckDB seguindo a Arquitetura Medallion. Dessa vez fui além: o desafio era processar **documentos não estruturados** — PDFs, DOCX e TXT — e torná-los pesquisáveis semanticamente em português.

A diferença fundamental entre os dois projetos está na **infraestrutura de orquestração**. No projeto Spark, o foco era processamento distribuído de dados estruturados. Aqui, a arquitetura foi elaborada e pensada para o **Apache Airflow 3.x** como motor de orquestração, com CeleryExecutor, workers customizados e uma DAG de 5 tasks em sequência — cada uma com responsabilidade bem definida dentro da Arquitetura Medallion.

---

**🏗️ Como foi arquitetado**

O ambiente roda em **9 containers Docker** em rede bridge dedicada:

→ **PostgreSQL + Redis** — metadados e broker Celery do Airflow  
→ **Airflow 3.x** — apiserver, scheduler, dag-processor, worker, triggerer  
→ **Apache Tika** (latest-full) — extração de texto e OCR via servidor REST  
→ **DuckDB** — lakehouse local com camadas bronze / silver / gold  
→ **Qdrant** — vector store para busca semântica  
→ **Apache Spark 4.x** — Spark Connect disponível para processamento distribuído  

Uma decisão arquitetural importante: no Airflow 3.x o **dag-processor é um processo separado** que usa a imagem base sem dependências extras. Isso significa que imports como `tika`, `torch` e `duckdb` precisam ficar **dentro das tasks**, nunca no nível do módulo — caso contrário a DAG falha silenciosamente no parsing.

---

**⚙️ O fluxo da DAG**

```
extrair_bronze → transformar_silver → transformar_gold → gerar_embeddings → indexar_qdrant
```

**Bronze**: Apache Tika REST extrai texto e metadados de cada documento. PDFs escaneados passam por OCR automático via Tesseract em português (`TIKA_OCR_LANGUAGE=por`) — sem nenhuma configuração adicional.

**Silver**: dbt limpa, normaliza e filtra os dados. `TRY_CAST` com `NULLIF` lida com metadados de data inconsistentes do Tika sem quebrar o pipeline.

**Gold analítico**: dbt agrega métricas por tipo de documento — total, média de páginas, média de caracteres.

**Gold semântico**: aqui entra o modelo **PORTULAN/serafim-100m**, especializado em português brasileiro e europeu. Cada documento é dividido em chunks de 500 palavras com overlap de 50 — garantindo que documentos longos sejam **completamente vetorizados**, sem truncamento. Com GPU RTX 3060, 43 chunks de 3 PDFs são codificados em menos de 5 segundos.

**Qdrant**: cada chunk vira um ponto com vetor de 768 dimensões e payload rico — `chunk_text`, `file_name`, `chunk_index`, `author`, `title`. O texto do trecho fica no payload, pronto para alimentar um LLM em um sistema RAG.

---

**🔍 Por que chunking importa**

Esse foi um ponto crítico. Modelos de embedding têm limite de ~512 tokens. Sem chunking, um PDF de 79k caracteres gera **um único vetor que representa só o início do documento**. Com chunking + overlap, o mesmo documento gera 25 vetores independentes — cada um representando um trecho real do conteúdo. A busca semântica passa a encontrar informações em qualquer parte do documento.

---

**📦 Stack completa open source**

| Componente | Função |
|---|---|
| Apache Airflow 3.3.1 | Orquestração (CeleryExecutor) |
| Apache Tika latest-full | Extração de texto + OCR Tesseract |
| DuckDB 1.0+ | Lakehouse local |
| dbt-duckdb 1.8+ | Transformações SQL declarativas |
| PORTULAN/serafim-100m | Embeddings em português (dim 768) |
| Qdrant | Vector store — busca semântica |
| Apache Spark 4.0.1 | Processamento distribuído |

100% open source. Sobe com um único `docker compose up`.

---

**📌 O que aprendi**

✅ Airflow 3.x exige atenção ao isolamento de imports entre dag-processor e worker  
✅ Tika como serviço REST é muito mais robusto do que como biblioteca embutida  
✅ DuckDB + dbt é uma combinação poderosa para lakehouse local sem overhead operacional  
✅ Chunking com overlap não é opcional — é o que torna a busca semântica realmente útil  
✅ O modelo serafim-100m entrega qualidade real para português sem depender de modelos multilíngues genéricos  

---

Esse pipeline forma a **fase de indexação de um sistema RAG completo em português**. O próximo passo natural é expor um endpoint que receba uma pergunta, gere o embedding com o mesmo modelo e consulte o Qdrant — retornando os trechos mais relevantes para alimentar um LLM.

Se você trabalha com documentos não estruturados em português e quer construir busca semântica ou RAG, essa arquitetura é um ponto de partida sólido.

💬 Ficou com alguma dúvida sobre alguma parte da implementação? Comenta aqui.

---

#EngenhariadeDados #EngenhariadIA #DataEngineering #ApacheAirflow #DuckDB #dbt #Qdrant #NLP #RAG #Embeddings #Python #OpenSource #MachineLearning #DataScience #Português
