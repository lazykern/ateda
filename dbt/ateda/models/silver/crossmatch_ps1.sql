{{ config(
  materialized = 'incremental',
  file_format = 'iceberg',
  incremental_strategy = 'merge',
  unique_key = ['objectid', 'candid', 'objectidps'],
  partition_by = ['days(ts)'],
  tblproperties = {
    'write.target-file-size-bytes': '536870912',
    'write.distribution-mode': 'range'
  },
  post_hook = "ALTER TABLE {{ this }} WRITE ORDERED BY objectid, ts;"
) }}

{% set observation_date_str = var('observation_date', '2024-02-01') %}

WITH source_data AS (
  SELECT
    candid,
    objectId as objectid,
    candidate.jd,
    candidate.nmtchps,
    -- Match 1
    candidate.objectidps1,
    candidate.sgmag1,
    candidate.srmag1,
    candidate.simag1,
    candidate.szmag1,
    candidate.sgscore1,
    candidate.distpsnr1,
    -- Match 2
    candidate.objectidps2,
    candidate.sgmag2,
    candidate.srmag2,
    candidate.simag2,
    candidate.szmag2,
    candidate.sgscore2,
    candidate.distpsnr2,
    -- Match 3
    candidate.objectidps3,
    candidate.sgmag3,
    candidate.srmag3,
    candidate.simag3,
    candidate.szmag3,
    candidate.sgscore3,
    candidate.distpsnr3
  FROM {{ ref('ztf_alert') }}
  WHERE observation_date = to_date('{{ observation_date_str }}')
    AND candidate.nmtchps > 0
),

-- Combine all valid matches directly, avoiding separate CTEs
unpivoted_matches AS (
  SELECT candid, objectid, jd, objectidps1 as objectidps, sgmag1 as sgmag, srmag1 as srmag, simag1 as simag, szmag1 as szmag, sgscore1 as sgscore, distpsnr1 as distpsnr
  FROM source_data
  WHERE objectidps1 IS NOT NULL
  UNION ALL
  SELECT candid, objectid, jd, objectidps2 as objectidps, sgmag2 as sgmag, srmag2 as srmag, simag2 as simag, szmag2 as szmag, sgscore2 as sgscore, distpsnr2 as distpsnr
  FROM source_data
  WHERE objectidps2 IS NOT NULL
  UNION ALL
  SELECT candid, objectid, jd, objectidps3 as objectidps, sgmag3 as sgmag, srmag3 as srmag, simag3 as simag, szmag3 as szmag, sgscore3 as sgscore, distpsnr3 as distpsnr
  FROM source_data
  WHERE objectidps3 IS NOT NULL
),

-- Deduplicate using GROUP BY and first()
deduplicated_matches AS (
  SELECT
    candid,
    objectidps,
    first(objectid, true) as objectid,
    first(jd, true) as jd,
    first(sgmag, true) as sgmag,
    first(srmag, true) as srmag,
    first(simag, true) as simag,
    first(szmag, true) as szmag,
    first(sgscore, true) as sgscore,
    first(distpsnr, true) as distpsnr
  FROM unpivoted_matches
  GROUP BY candid, objectidps
)

SELECT
  candid,
  objectid,
  objectidps,
  sgmag,
  srmag,
  simag,
  szmag,
  sgscore,
  distpsnr,
  to_timestamp((jd - 2440587.5) * 86400) as ts,
  current_timestamp() AS _created_ts
FROM deduplicated_matches
