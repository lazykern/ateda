{{ config(
  materialized = 'incremental',
  file_format = 'iceberg',
  incremental_strategy = 'merge',
  unique_key = ['objectid', 'candid'],
  partition_by = ['days(ts)'],
  tblproperties = {
    'write.target-file-size-bytes': '536870912',
    'write.distribution-mode': 'range'
  },
  post_hook = "ALTER TABLE {{ this }} WRITE ORDERED BY objectid, ts;"
) }}

{% set observation_date_str = var('observation_date', '2024-02-01') %}

WITH src AS (
  SELECT
    candid,
    objectId as objectid,
    candidate.jd,
    candidate.ssdistnr,
    candidate.ssmagnr,
    candidate.ssnamenr
  FROM {{ ref('ztf_alert') }}
  WHERE observation_date = to_date('{{ observation_date_str }}')
    AND candidate.ssdistnr IS NOT NULL
    AND candidate.ssmagnr IS NOT NULL
    AND candidate.ssnamenr IS NOT NULL
    AND candidate.ssdistnr > 0
    AND candidate.ssmagnr > 0
)
SELECT
  candid,
  objectid,
  ssdistnr,
  ssmagnr,
  ssnamenr,
  to_timestamp((jd - 2440587.5) * 86400) as ts,
  current_timestamp() AS _created_ts
FROM src