{{ config(
  materialized = 'incremental',
  file_format = 'iceberg',
  incremental_strategy = 'merge',
  unique_key = ['objectid', 'fid', 'pid', 'rfid', 'mjd'],
  partition_by = ['fid', 'days(ts)'],
  tblproperties = {
    'write.target-file-size-bytes': '536870912',
    'write.distribution-mode': 'range'
  },
  post_hook = "ALTER TABLE {{ this }} WRITE ORDERED BY objectid, ts;"
) }}

{% set observation_date_str = var('observation_date', '2024-02-01') %}

WITH fp_exploded AS (
  SELECT
    objectId,
    fp.*
  FROM {{ ref('ztf_alert') }}
  LATERAL VIEW explode(fp_hists) exploded_table AS fp
  WHERE observation_date = to_date('{{ observation_date_str }}')
    AND fp.procstatus = '0'
    AND fp.forcediffimflux IS NOT NULL AND fp.forcediffimflux != -99999
    AND fp.forcediffimfluxunc IS NOT NULL AND fp.forcediffimfluxunc != -99999 AND fp.forcediffimfluxunc > 0
    AND fp.distnr IS NOT NULL AND fp.distnr != -99999
    AND fp.ranr IS NOT NULL AND fp.ranr != -99999
    AND fp.decnr IS NOT NULL AND fp.decnr != -99999
    AND fp.magnr IS NOT NULL AND fp.magnr != -99999
    AND fp.sigmagnr IS NOT NULL AND fp.sigmagnr != -99999
    AND fp.chinr IS NOT NULL AND fp.chinr != -99999
    AND fp.sharpnr IS NOT NULL AND fp.sharpnr != -99999
),
fp_ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY objectId, fid, pid, rfid
      ORDER BY
        abs(forcediffimflux / forcediffimfluxunc) DESC,
        forcediffimfluxunc ASC
    ) as rn
  FROM fp_exploded
),
fp_source AS (
  SELECT *
  FROM fp_ranked
  WHERE rn = 1
)
SELECT
  objectId as objectid,
  (jd - 2400000.5) as mjd,
  field,
  rcid,
  fid,
  pid,
  rfid,
  sciinpseeing,
  scibckgnd,
  scisigpix,
  magzpsci,
  magzpsciunc,
  magzpscirms,
  clrcoeff,
  clrcounc,
  exptime,
  adpctdif1,
  adpctdif2,
  diffmaglim,
  forcediffimflux,
  forcediffimfluxunc,
  distnr,
  ranr,
  decnr,
  magnr,
  sigmagnr,
  chinr,
  sharpnr,
  (forcediffimflux / forcediffimfluxunc) as snr,
  (magzpsci - 2.5 * log10(abs(forcediffimflux))) as magpsf,
  CASE
    WHEN abs(forcediffimflux) > 1e-9 THEN ((2.5 / ln(10.0)) * forcediffimfluxunc / abs(forcediffimflux))
    ELSE NULL
  END as sigmapsf,
  ((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magzpsci)) * forcediffimflux as diff_flux_ujy,
  ((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magzpsci)) * forcediffimfluxunc as diff_flux_err_ujy,
  ((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr)) +
  (((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magzpsci)) * forcediffimflux)
  as total_flux_ujy,
  CASE
    WHEN (((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr)) +
          (((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magzpsci)) * forcediffimflux)) > 0
    THEN -2.5 * log10(
        (((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr)) +
         (((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magzpsci)) * forcediffimflux))
        / (pow(10.0, 3.56) * 1e6)
      )
    ELSE NULL
  END as apparent_mag,
  CASE
    WHEN (((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr)) +
          (((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magzpsci)) * forcediffimflux)) > 0 THEN
      (2.5 / ln(10.0)) * forcediffimfluxunc / (
        abs(((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr)) +
        (((pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magzpsci)) * forcediffimflux))
      )
    ELSE NULL
  END as apparent_mag_err,
  to_timestamp((jd - 2440587.5) * 86400.0) as ts,
  current_timestamp() as _created_ts
FROM fp_source;

