
    

    create  table
      "lakehouse"."main_silver"."documentos_clean__dbt_tmp"
  
    
    as (
      -- Normaliza e limpa os documentos extraídos pelo Tika
SELECT
    doc_id,
    file_name,
    file_type,
    TRIM(content)                                           AS content,
    CAST(num_pages AS INTEGER)                              AS num_pages,
    CAST(content_length AS INTEGER)                         AS content_length,
    author,
    title,
    TRY_CAST(NULLIF(TRIM(created_at), '') AS TIMESTAMP)    AS created_at,
    TRY_CAST(ingested_at AS TIMESTAMP)                      AS ingested_at
FROM "lakehouse"."bronze"."documentos_raw"
WHERE content IS NOT NULL
  AND TRIM(content) != ''
  AND content_length > 50
    );
    
  