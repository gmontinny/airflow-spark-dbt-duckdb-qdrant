-- Resumo analítico por tipo de documento
SELECT
    file_type,
    COUNT(*)                        AS total_documentos,
    AVG(num_pages)                  AS media_paginas,
    AVG(content_length)             AS media_chars,
    MIN(ingested_at)                AS primeira_ingestao,
    MAX(ingested_at)                AS ultima_ingestao
FROM {{ ref('documentos_clean') }}
GROUP BY file_type
