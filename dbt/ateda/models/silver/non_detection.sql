{{ config(
  materialized = 'incremental',
  file_format = 'iceberg',
  incremental_strategy = 'merge',
  unique_key = ['objectid', 'fid', 'pid', 'mjd'],
  partition_by = ['fid', 'days(ts)'],
  tblproperties = {
    'write.target-file-size-bytes': '536870912',
    'write.distribution-mode': 'range'
  },
  post_hook = "ALTER TABLE {{ this }} WRITE ORDERED BY objectid, ts;"
) }}

{% set observation_date_str = var('observation_date', '2024-02-01') %}

WITH src AS (
  SELECT
    objectId as objectid,
    pc.jd,
    pc.fid,
    pc.pid,
    pc.diffmaglim,
    pc.nid,
    pc.rcid,
    pc.field,
    pc.magzpsci,
    pc.magzpsciunc,
    pc.magzpscirms,
    pc.clrcoeff,
    pc.clrcounc
  FROM {{ ref('ztf_alert') }}
  LATERAL VIEW explode(prv_candidates) exploded_table AS pc
  WHERE observation_date = to_date('{{ observation_date_str }}')
    AND pc.candid IS NULL
)
SELECT
  objectid,
  (jd - 2400000.5) as mjd,
  fid,
  pid,
  diffmaglim,
  nid,
  rcid,
  field,
  magzpsci,
  magzpsciunc,
  magzpscirms,
  clrcoeff,
  clrcounc,
  to_timestamp((jd - 2440587.5) * 86400) as ts,
  current_timestamp() as _created_ts
FROM src