"""
Pipeline: Documentos não estruturados → Tika → Bronze → Silver (DBT) → Gold + Embeddings → Qdrant
Arquitetura Medallion com embeddings semânticos em português (PORTULAN/serafim-100m).
"""

from __future__ import annotations

import hashlib
import logging
import os
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

log = logging.getLogger("airflow.task")

DUCKDB_PATH       = os.getenv("DUCKDB_PATH", "/opt/airflow/work-dir/warehouse/lakehouse.duckdb")
DATA_DIR          = "/opt/airflow/work-dir/data/datas"
DBT_PROJECT_DIR   = "/opt/airflow/dbt"
QDRANT_HOST       = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT       = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_COLLECTION = "documentos_pt"
EMBEDDING_MODEL   = "PORTULAN/serafim-100m-portuguese-pt-sentence-encoder"
EMBEDDING_DIM     = 768
CHUNK_SIZE        = 500
CHUNK_OVERLAP     = 50
TIKA_ENDPOINT     = os.getenv("TIKA_SERVER_JAR", "http://tika-server:9998")
SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".txt"}

DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def _doc_id(file_path: str) -> str:
    return hashlib.md5(file_path.encode()).hexdigest()


def _str(v) -> str:
    """Normaliza valores que podem ser lista ou None."""
    if isinstance(v, list):
        return v[0] if v else ""
    return str(v) if v else ""


def _duckdb_connect(path: str):
    """Abre conexão DuckDB com retry para evitar conflito de lock entre tasks."""
    import duckdb
    for attempt in range(10):
        try:
            return duckdb.connect(path)
        except duckdb.IOException:
            if attempt == 9:
                raise
            time.sleep(3)


def _run_dbt(select: str) -> None:
    result = subprocess.run(
        ["dbt", "run", "--select", select,
         "--project-dir", DBT_PROJECT_DIR,
         "--profiles-dir", DBT_PROJECT_DIR],
        capture_output=True, text=True,
    )
    log.info(result.stdout)
    if result.returncode != 0:
        log.error(result.stderr)
        raise RuntimeError(f"dbt run '{select}' falhou:\n{result.stdout}\n{result.stderr}")


@dag(
    dag_id="pipeline_documentos_nao_estruturados",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tika", "dbt", "duckdb", "qdrant", "embeddings", "nlp"],
    doc_md="""
    ## Pipeline de Documentos Não Estruturados
    Extrai texto e metadados com **Apache Tika**, persiste em **DuckDB** seguindo
    arquitetura **Medallion** (bronze → silver → gold), gera embeddings semânticos
    com **PORTULAN/serafim-100m** e indexa no **Qdrant**.
    """,
)
def pipeline_documentos_nao_estruturados():

    @task()
    def extrair_bronze() -> int:
        """
        Camada Bronze: extrai texto e metadados via Tika REST (TIKA_CLIENT_ONLY)
        e persiste em bronze.documentos_raw no DuckDB.
        """
        import tika
        import pandas as pd
        from tika import parser as tika_parser

        # Modo cliente REST — usa o serviço tika-server sem baixar JAR localmente
        os.environ["TIKA_CLIENT_ONLY"] = "True"
        tika.TikaClientOnly = True

        data_path = Path(DATA_DIR)
        if not data_path.exists():
            raise FileNotFoundError(f"Diretório não encontrado: {DATA_DIR}")

        files = [f for f in data_path.iterdir() if f.suffix.lower() in SUPPORTED_EXTENSIONS]
        if not files:
            raise ValueError(f"Nenhum documento suportado em {DATA_DIR}")

        log.info("Extraindo %d documentos via Tika REST em %s", len(files), TIKA_ENDPOINT)

        docs = []
        for file_path in files:
            try:
                text_result = tika_parser.from_file(
                    str(file_path), serverEndpoint=TIKA_ENDPOINT, service="text",
                )
                meta_result = tika_parser.from_file(
                    str(file_path), serverEndpoint=TIKA_ENDPOINT, service="meta",
                )
                content = (text_result.get("content") or "").strip()
                meta    = meta_result.get("metadata") or {}

                docs.append({
                    "doc_id":         _doc_id(str(file_path)),
                    "file_name":      file_path.name,
                    "file_type":      file_path.suffix.lower().lstrip("."),
                    "content":        content,
                    "num_pages":      int(_str(meta.get("xmpTPg:NPages", meta.get("Page-Count", "0"))) or 0),
                    "content_length": len(content),
                    "author":         _str(meta.get("dc:creator", meta.get("Author", ""))),
                    "title":          _str(meta.get("dc:title", meta.get("title", file_path.stem))),
                    "created_at":     _str(meta.get("dcterms:created", meta.get("Creation-Date", ""))),
                    "ingested_at":    datetime.utcnow().isoformat(),
                })
                log.info("Extraído: %s (%d chars)", file_path.name, len(content))
            except Exception as exc:
                log.warning("Falha ao extrair %s: %s", file_path.name, exc)

        if not docs:
            raise RuntimeError("Nenhum documento extraído com sucesso.")

        df = pd.DataFrame(docs)
        os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

        conn = _duckdb_connect(DUCKDB_PATH)
        try:
            conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            conn.execute("DROP TABLE IF EXISTS bronze.documentos_raw")
            conn.execute("CREATE TABLE bronze.documentos_raw AS SELECT * FROM df")
            count = conn.execute("SELECT COUNT(*) FROM bronze.documentos_raw").fetchone()[0]
        finally:
            conn.close()

        log.info("bronze.documentos_raw: %d documentos", count)
        return count

    @task()
    def transformar_silver(bronze_count: int) -> int:
        """Camada Silver: limpeza e normalização via dbt."""
        log.info("dbt run silver (%d docs na bronze)...", bronze_count)
        _run_dbt("silver")

        conn = _duckdb_connect(DUCKDB_PATH)
        try:
            count = conn.execute("SELECT COUNT(*) FROM main_silver.documentos_clean").fetchone()[0]
        finally:
            conn.close()

        log.info("silver.documentos_clean: %d documentos", count)
        return count

    @task()
    def transformar_gold(silver_count: int) -> int:
        """Camada Gold: agregações analíticas via dbt."""
        log.info("dbt run gold (%d docs na silver)...", silver_count)
        _run_dbt("gold")

        conn = _duckdb_connect(DUCKDB_PATH)
        try:
            count = conn.execute("SELECT COUNT(*) FROM main_gold.resumo_por_tipo").fetchone()[0]
        finally:
            conn.close()

        log.info("gold.resumo_por_tipo: %d linhas", count)
        return count

    @task(execution_timeout=timedelta(minutes=30))
    def gerar_embeddings(gold_count: int) -> int:
        """
        Camada Gold — Vetorização: divide cada documento em chunks com overlap,
        gera embeddings semânticos com PORTULAN/serafim-100m e persiste em
        gold.documentos_embeddings (um registro por chunk).
        """
        import pandas as pd
        from sentence_transformers import SentenceTransformer
        import torch

        def _chunk_text(text: str, size: int, overlap: int) -> list[str]:
            words = text.split()
            step  = max(1, size - overlap)
            return [" ".join(words[i:i + size]) for i in range(0, len(words), step) if words[i:i + size]]

        device = "cuda" if torch.cuda.is_available() else "cpu"
        log.info("Device para embeddings: %s", device)
        if device == "cuda":
            log.info("GPU: %s (%.1f GB VRAM)",
                     torch.cuda.get_device_name(0),
                     torch.cuda.get_device_properties(0).total_memory / 1e9)

        log.info("Carregando modelo %s...", EMBEDDING_MODEL)
        model = SentenceTransformer(EMBEDDING_MODEL, device=device)

        conn = _duckdb_connect(DUCKDB_PATH)
        try:
            silver_df = conn.execute(
                "SELECT doc_id, file_name, file_type, content, title, author "
                "FROM main_silver.documentos_clean"
            ).df()
        finally:
            conn.close()

        if silver_df.empty:
            raise RuntimeError("Nenhum documento na silver para vetorizar.")

        chunks = []
        for _, row in silver_df.iterrows():
            for idx, chunk in enumerate(_chunk_text(row["content"], CHUNK_SIZE, CHUNK_OVERLAP)):
                chunks.append({
                    "doc_id":      row["doc_id"],
                    "chunk_id":    f"{row['doc_id']}_{idx}",
                    "chunk_index": idx,
                    "file_name":   row["file_name"],
                    "file_type":   row["file_type"],
                    "title":       row["title"],
                    "author":      row["author"],
                    "chunk_text":  chunk,
                })

        log.info("Total de chunks: %d (de %d documentos)", len(chunks), len(silver_df))
        chunks_df = pd.DataFrame(chunks)

        embeddings = model.encode(
            chunks_df["chunk_text"].tolist(),
            batch_size=32,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        chunks_df["embedding"] = [emb.tolist() for emb in embeddings]

        conn = _duckdb_connect(DUCKDB_PATH)
        try:
            conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
            conn.execute("DROP TABLE IF EXISTS gold.documentos_embeddings")
            conn.execute("CREATE TABLE gold.documentos_embeddings AS SELECT * FROM chunks_df")
            count = conn.execute("SELECT COUNT(*) FROM gold.documentos_embeddings").fetchone()[0]
        finally:
            conn.close()

        log.info("gold.documentos_embeddings: %d chunks vetorizados", count)
        return count

    @task()
    def indexar_qdrant(embeddings_count: int) -> dict:
        """Indexa os embeddings no Qdrant para busca semântica."""
        from qdrant_client import QdrantClient
        from qdrant_client.models import Distance, PointStruct, VectorParams

        log.info("Conectando ao Qdrant %s:%d", QDRANT_HOST, QDRANT_PORT)
        client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

        existing = [c.name for c in client.get_collections().collections]
        if QDRANT_COLLECTION in existing:
            client.delete_collection(QDRANT_COLLECTION)

        client.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(size=EMBEDDING_DIM, distance=Distance.COSINE),
        )

        conn = _duckdb_connect(DUCKDB_PATH)
        try:
            rows = conn.execute(
                "SELECT chunk_id, doc_id, file_name, file_type, title, author, "
                "chunk_index, chunk_text, embedding "
                "FROM gold.documentos_embeddings"
            ).fetchall()
        finally:
            conn.close()

        points = [
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
            for row in rows
        ]

        client.upsert(collection_name=QDRANT_COLLECTION, points=points)
        indexed = client.get_collection(QDRANT_COLLECTION).points_count

        log.info("Qdrant '%s': %d pontos indexados", QDRANT_COLLECTION, indexed)
        return {"collection": QDRANT_COLLECTION, "pontos_indexados": indexed}

    bronze = extrair_bronze()
    silver = transformar_silver(bronze)
    gold   = transformar_gold(silver)
    embeds = gerar_embeddings(gold)
    indexar_qdrant(embeds)


pipeline_documentos_nao_estruturados()
