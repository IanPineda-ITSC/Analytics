WITH

PRIMERAS_COMPRAS AS (
    SELECT
        lower(EMAIL) AS EMAIL,
        min(to_date(CREATED_AT)) AS FECHA_PRIMERA_COMPRA
    FROM
        SEGMENT_EVENTS.SESSIONM_SBX.FACT_TRANSACTIONS
    GROUP BY
        lower(EMAIL)
)

SELECT
    ANIO_ALSEA,
    MES_ALSEA,
    count(DISTINCT EMAIL) AS PRIMERAS_COMPRAS
FROM
    PRIMERAS_COMPRAS
INNER JOIN
    WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
ON
    FECHA = FECHA_PRIMERA_COMPRA
GROUP BY
    ANIO_ALSEA,
    MES_ALSEA
ORDER BY
    ANIO_ALSEA DESC,
    MES_ALSEA DESC