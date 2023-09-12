INSERT INTO WOW_REWARDS.SBX_REWARDS.SEGMENTACION_ACTIVOS
WITH

INTERVALO_FECHAS AS (
    -- Fecha de inicio y final que vamos a usar para la segmentacion
    SELECT
        to_date('2023-05-22') AS FECHA_INICIO,
        to_date('2023-08-20') AS FECHA_FIN
),

TRANSACCIONES_BY_USER AS (
    SELECT
        EMAIL,
        sum(CHECK_AMOUNT / 1.16) AS VENTAS,
        count(DISTINCT TRANSACTION_ID) AS FREQ,
        VENTAS / FREQ AS AOV
    FROM
        SEGMENT_EVENTS.SESSIONM_SBX.FACT_TRANSACTIONS
    INNER JOIN
        INTERVALO_FECHAS
    ON
        to_date(CREATED_AT) BETWEEN FECHA_INICIO AND FECHA_FIN
    WHERE
        EMAIL IS NOT null
    GROUP BY
        EMAIL
),

SEGMENTACION_BASE_ACTIVOS AS (
    SELECT 
        TRANSACCIONES_BY_USER.*,
        CASE
            WHEN FREQ <= PERCENTILE_CONT(0.20) WITHIN GROUP (ORDER BY FREQ) OVER () THEN 'LIGHT'
            WHEN FREQ <= PERCENTILE_CONT(0.40) WITHIN GROUP (ORDER BY FREQ) OVER () THEN 'MIDL'
            WHEN FREQ <= PERCENTILE_CONT(0.60) WITHIN GROUP (ORDER BY FREQ) OVER () THEN 'MIDH'
            WHEN FREQ <= PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY FREQ) OVER () THEN 'HEAVY'
            ELSE 'SUPER'
        END AS SEGMENTO,
        CASE
            WHEN AOV <= ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY AOV) OVER ()) THEN 'LOW'
            WHEN AOV <= ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY AOV) OVER ()) THEN 'AVG'
            ELSE 'HIGH'
        END AS TAG_GRUPO,
        SEGMENTO || '_' || TAG_GRUPO AS FULL_SEGMENTO,
        CASE
            WHEN FULL_SEGMENTO IN ('LIGHT_LOW', 'LIGHT_AVG', 'LIGHT_HIGH', 'MIDL_LOW', 'MIDL_AVG', 'MIDH_LOW') THEN 'LOW'
            WHEN FULL_SEGMENTO IN ('MIDL_HIGH', 'MIDH_AVG', 'MIDH_HIGH', 'HEAVY_LOW', 'HEAVY_AVG') THEN 'MEDIUM' 
            WHEN FULL_SEGMENTO IN ('HEAVY_HIGH', 'SUPER_LOW', 'SUPER_AVG') THEN 'HIGH'
            ELSE 'TOP'
        END AS ULTRASEGMENTO
    FROM
        TRANSACCIONES_BY_USER
),

SEGMENTACION_GC AS (
    SELECT
        EMAIL,
        'GC' AS FLAG_GC
    FROM
        SEGMENTACION_BASE_ACTIVOS tablesample bernoulli (10)
),

EXTERNAL_USER_MAPPINGS AS (
    SELECT DISTINCT
        USER_ID,
        first_value(EXTERNAL_USER_ID) OVER (PARTITION BY USER_ID ORDER BY EXTERNAL_USER_ID) AS EXTERNAL_ID
    FROM
        SEGMENT_EVENTS.SESSIONM_SBX.SM_EXTERNAL_USER_MAPPINGS
    WHERE
        EXTERNAL_USER_ID LIKE 'MEX_ALSEA:%'
)


SELECT
    EMAIL,
    '2023-33' AS YEAR_WEEK,
    ULTRASEGMENTO,
    CASE
        WHEN ULTRASEGMENTO = 'LOW' THEN split_part(YEAR_WEEK, '-', 1) || split_part(YEAR_WEEK, '-', 2) || 1
        WHEN ULTRASEGMENTO = 'MEDIUM' THEN split_part(YEAR_WEEK, '-', 1) || split_part(YEAR_WEEK, '-', 2) || 2
        WHEN ULTRASEGMENTO = 'HIGH' THEN split_part(YEAR_WEEK, '-', 1) || split_part(YEAR_WEEK, '-', 2) || 3
        WHEN ULTRASEGMENTO = 'TOP' THEN split_part(YEAR_WEEK, '-', 1) || split_part(YEAR_WEEK, '-', 2) || 4
    END AS PROMO,
    TAG_GRUPO,
    SEGMENTO,
    ifnull(FLAG_GC, 'GP') AS FLAG_GC,
    EXTERNAL_ID,
    USER_ID
FROM
    SEGMENTACION_BASE_ACTIVOS
LEFT JOIN 
    SEGMENTACION_GC 
USING(
    EMAIL
)
LEFT JOIN
    SEGMENT_EVENTS.SESSIONM_SBX.SM_USERS
USING(
    EMAIL
)
LEFT JOIN
    EXTERNAL_USER_MAPPINGS
USING(
    USER_ID
)
;