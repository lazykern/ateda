{{ config(
  materialized = 'incremental',
  file_format = 'iceberg',
  incremental_strategy = 'merge',
  unique_key = 'candid',
  partition_by = ['observation_date'],
) }}

{% set observation_date_str = var('observation_date', '2024-02-01') %}

SELECT
    *,
    current_timestamp() AS _created_ts
FROM {{ source('landing','alerts') }} src
WHERE src.observation_date = to_date('{{ observation_date_str }}')