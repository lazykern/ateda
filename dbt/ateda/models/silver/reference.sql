{{ config(
  materialized = 'incremental',
  file_format = 'iceberg',
  incremental_strategy = 'merge',
  unique_key = ['objectid', 'fid', 'rfid'],
  partition_by = ['fid', 'days(startref_ts)'],
  tblproperties = {
    'write.target-file-size-bytes': '536870912',
    'write.distribution-mode': 'range'
  },
  post_hook = "ALTER TABLE {{ this }} WRITE ORDERED BY objectid, startref_ts;"
) }}

{% set observation_date_str = var('observation_date', '2024-02-01') %}

WITH source_data AS (
  SELECT
    objectId as objectid,
    candidate.fid,
    candidate.rfid,
    candidate.rcid,
    candidate.field,
    candidate.magnr,
    candidate.sigmagnr,
    candidate.chinr,
    candidate.sharpnr,
    candidate.ranr,
    candidate.decnr,
    candidate.jdstartref,
    candidate.jdendref,
    candidate.nframesref
  FROM {{ ref('ztf_alert') }}
  WHERE observation_date = to_date('{{ observation_date_str }}')
),

deduplicated_matches AS (
  SELECT
    objectid,
    fid,
    rfid,
    first(rcid, true) as rcid,
    first(field, true) as field,
    first(magnr, true) as magnr,
    first(sigmagnr, true) as sigmagnr,
    first(chinr, true) as chinr,
    first(sharpnr, true) as sharpnr,
    first(ranr, true) as ranr,
    first(decnr, true) as decnr,
    first(jdstartref, true) as jdstartref,
    first(jdendref, true) as jdendref,
    first(nframesref, true) as nframesref
  FROM source_data
  GROUP BY objectid, fid, rfid
)

SELECT
  objectid,
  fid,
  rfid,
  rcid,
  field,
  magnr,
  sigmagnr,
  chinr,
  sharpnr,
  ranr,
  decnr,
  (jdstartref - 2400000.5) as mjdstartref,
  (jdendref - 2400000.5) as mjdendref,
  to_timestamp((jdstartref - 2440587.5) * 86400) as startref_ts,
  to_timestamp((jdendref - 2440587.5) * 86400) as endref_ts,
  nframesref,
  current_timestamp() AS _created_ts
FROM deduplicated_matches
