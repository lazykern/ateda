{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = ['objectid', 'candid'],
  file_format = 'iceberg',
  partition_by = ['fid', 'days(ts)'],
  tblproperties = {
    'write.target-file-size-bytes': '536870912',
    'write.distribution-mode': 'range'
  },
  post_hook = "ALTER TABLE {{ this }} WRITE ORDERED BY objectid, ts;"
) }}

{% set observation_date_str = var('observation_date', '2024-02-01') %}


WITH reference AS (
  SELECT
    objectid,
    fid,
    rfid,
    magnr,
    sigmagnr
  FROM {{ ref('reference') }}
),
filtered_alerts AS (
  SELECT
    objectId as objectid,
    candidate,
    prv_candidates
  FROM {{ ref('ztf_alert') }}
  WHERE observation_date = to_date('{{ observation_date_str }}')
),
candidate AS (
  SELECT
    fa.objectid,
    fa.candidate.candid,
    fa.candidate.jd,
    (fa.candidate.jd - 2400000.5) as mjd,
    to_timestamp((fa.candidate.jd - 2440587.5) * 86400.0) as ts,
    fa.candidate.fid,
    fa.candidate.pid,
    fa.candidate.diffmaglim,
    CASE
      WHEN lower(trim(fa.candidate.isdiffpos)) IN ('t', '1') THEN true
      WHEN lower(trim(fa.candidate.isdiffpos)) IN ('f', '0') THEN false
      ELSE NULL
    END as isdiffpos,
    fa.candidate.nid,
    fa.candidate.xpos,
    fa.candidate.ypos,
    fa.candidate.ra,
    fa.candidate.dec,
    fa.candidate.magpsf,
    fa.candidate.sigmapsf,
    fa.candidate.chipsf,
    fa.candidate.magap,
    fa.candidate.sigmagap,
    fa.candidate.distnr,
    fa.candidate.sky,
    fa.candidate.fwhm,
    fa.candidate.classtar,
    fa.candidate.mindtoedge,
    fa.candidate.seeratio,
    fa.candidate.aimage,
    fa.candidate.bimage,
    fa.candidate.aimagerat,
    fa.candidate.bimagerat,
    fa.candidate.nneg,
    fa.candidate.nbad,
    fa.candidate.rb,
    fa.candidate.rbversion,
    fa.candidate.drb,
    fa.candidate.drbversion,
    fa.candidate.sumrat,
    fa.candidate.magapbig,
    fa.candidate.sigmagapbig,
    fa.candidate.scorr,
    fa.candidate.rfid,
    fa.candidate.dsnrms,
    fa.candidate.ssnrms,
    fa.candidate.magzpsci,
    fa.candidate.magzpsciunc,
    fa.candidate.magzpscirms,
    fa.candidate.nmatches,
    fa.candidate.clrcoeff,
    fa.candidate.clrcounc,
    fa.candidate.zpclrcov,
    fa.candidate.zpmed,
    fa.candidate.clrmed,
    fa.candidate.clrrms,
    fa.candidate.exptime
  FROM filtered_alerts fa
  WHERE fa.candidate.candid IS NOT NULL
),
candidate_ids AS (
  SELECT DISTINCT candid from candidate
),
exploded_prv_candidates AS (
  SELECT
    fa.objectid,
    pc.candid,
    pc.jd,
    pc.fid,
    pc.pid,
    pc.diffmaglim,
    pc.isdiffpos,
    pc.nid,
    pc.xpos,
    pc.ypos,
    pc.ra,
    pc.dec,
    pc.magpsf,
    pc.sigmapsf,
    pc.chipsf,
    pc.magap,
    pc.sigmagap,
    pc.distnr,
    pc.sky,
    pc.fwhm,
    pc.classtar,
    pc.mindtoedge,
    pc.seeratio,
    pc.aimage,
    pc.bimage,
    pc.aimagerat,
    pc.bimagerat,
    pc.nneg,
    pc.nbad,
    pc.rb,
    pc.rbversion,
    pc.sumrat,
    pc.magapbig,
    pc.sigmagapbig,
    pc.scorr,
    pc.magzpsci,
    pc.magzpsciunc
  FROM
    filtered_alerts fa
    LATERAL VIEW explode(fa.prv_candidates) exploded_table AS pc
  WHERE pc.candid IS NOT NULL
),
prv_candidate AS (
  SELECT DISTINCT
    epc.objectid,
    epc.candid,
    epc.jd,
    (epc.jd - 2400000.5) as mjd,
    to_timestamp((epc.jd - 2440587.5) * 86400.0) as ts,
    epc.fid,
    epc.pid,
    epc.diffmaglim,
    CASE
      WHEN lower(trim(epc.isdiffpos)) IN ('t', '1') THEN true
      WHEN lower(trim(epc.isdiffpos)) IN ('f', '0') THEN false
      ELSE NULL
    END as isdiffpos,
    epc.nid,
    epc.xpos,
    epc.ypos,
    epc.ra,
    epc.dec,
    epc.magpsf,
    epc.sigmapsf,
    epc.chipsf,
    epc.magap,
    epc.sigmagap,
    epc.distnr,
    epc.sky,
    epc.fwhm,
    epc.classtar,
    epc.mindtoedge,
    epc.seeratio,
    epc.aimage,
    epc.bimage,
    epc.aimagerat,
    epc.bimagerat,
    epc.nneg,
    epc.nbad,
    epc.rb,
    epc.rbversion,
    NULL AS drb,
    NULL AS drbversion,
    epc.sumrat,
    epc.magapbig,
    epc.sigmagapbig,
    epc.scorr,
    NULL AS rfid,
    NULL AS dsnrms,
    NULL AS ssnrms,
    epc.magzpsci,
    epc.magzpsciunc,
    NULL AS magzpscirms,
    NULL AS nmatches,
    NULL AS clrcoeff,
    NULL AS clrcounc,
    NULL AS zpclrcov,
    NULL AS zpmed,
    NULL AS clrmed,
    NULL AS clrrms,
    NULL AS exptime
  FROM
    exploded_prv_candidates epc
  LEFT ANTI JOIN candidate_ids ci ON epc.candid = ci.candid
),
all_candidates AS (
  SELECT * FROM candidate
  UNION
  SELECT * FROM prv_candidate
)
SELECT
  candid,
  a.objectid,
  mjd,
  a.fid,
  pid,
  diffmaglim,
  isdiffpos,
  nid,
  xpos,
  ypos,
  ra,
  dec,
  magpsf,
  sigmapsf,
  chipsf,
  magap,
  sigmagap,
  distnr,
  magnr,
  sigmagnr,
  sky,
  fwhm,
  classtar,
  mindtoedge,
  seeratio,
  aimage,
  bimage,
  aimagerat,
  bimagerat,
  nneg,
  nbad,
  rb,
  rbversion,
  drb,
  drbversion,
  sumrat,
  magapbig,
  sigmagapbig,
  scorr,
  a.rfid,
  dsnrms,
  ssnrms,
  magzpsci,
  magzpsciunc,
  magzpscirms,
  nmatches,
  clrcoeff,
  clrcounc,
  zpclrcov,
  zpmed,
  clrmed,
  clrrms,
  exptime,
  ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) ) as diff_flux_ujy,
  ( (0.4 * ln(10.0)) * sigmapsf * ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf)) ) as diff_flux_err_ujy,
  CASE
      WHEN isdiffpos THEN ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) + ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) )
      ELSE ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) - ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) )
  END as total_flux_ujy,
  sqrt(
      pow( ( (0.4 * ln(10.0)) * sigmapsf * ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) ) ), 2) +
      pow( ( (0.4 * ln(10.0)) * sigmagnr * ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) ), 2)
  ) as total_flux_err_ujy,
  CASE
      WHEN (
          CASE
              WHEN isdiffpos THEN ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) + ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) )
              ELSE ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) - ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) )
          END
      ) > 0 THEN -2.5 * log10(
          (
              CASE
                  WHEN isdiffpos THEN ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) + ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) )
                  ELSE ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) - ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) )
              END
          ) / (pow(10.0, 3.56) * 1e6)
      )
      ELSE NULL
  END as apparent_mag,
  CASE
      WHEN (
          CASE
              WHEN isdiffpos THEN ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) + ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) )
              ELSE ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) - ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) )
          END
      ) > 0 THEN (2.5 / ln(10.0)) * sqrt(
          pow( ( (0.4 * ln(10.0)) * sigmapsf * ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) ) ), 2) +
          pow( ( (0.4 * ln(10.0)) * sigmagnr * ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) ), 2)
      ) / (
          CASE
              WHEN isdiffpos THEN ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) + ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) )
              ELSE ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magnr) ) - ( (pow(10.0, 3.56) * 1e6) * pow(10.0, -0.4 * magpsf) )
          END
      )
      ELSE NULL
  END as apparent_mag_err,
  ts,
  current_timestamp() AS _created_ts
FROM all_candidates a
LEFT JOIN reference r ON a.objectid = r.objectid AND a.fid = r.fid AND a.rfid = r.rfid AND a.distnr < 1.4