WITH

SEMANAS AS (
    SELECT
        SEM_ALSEA,
        MES_ALSEA,
        ANIO_ALSEA,
        CASE MES_ALSEA
            WHEN 1 OR 2 OR 3 THEN 'Q1'
            WHEN 4 OR 5 OR 6 THEN 'Q2'
            WHEN 7 OR 8 OR 9 THEN 'Q3'
            WHEN 10 OR 11 OR 12 THEN 'Q4'
        END AS TRIMESTRE,
        max(to_date(FECHA)) AS FECHA_FIN,
        min(to_date(FECHA)) AS FECHA_INICIO
    FROM
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    GROUP BY
        SEM_ALSEA,
        MES_ALSEA,
        ANIO_ALSEA
)

SELECT
    ANIO_ALSEA,
    TRIMESTRE,
    MES_ALSEA,
    SEM_ALSEA,
    count(DISTINCT lower(EMAIL))
FROM
    SEGMENT_EVENTS.SESSIONM_SBX.FACT_TRANSACTIONS
INNER JOIN
    SEMANAS
ON
    to_date(CREATED_AT) <= FECHA_FIN
AND 
    FECHA_INICIO <= to_date(CREATED_AT)
GROUP BY
    ANIO_ALSEA,
    TRIMESTRE,
    MES_ALSEA,
    SEM_ALSEA
ORDER BY
    ANIO_ALSEA DESC,
    SEM_ALSEA DESC
;