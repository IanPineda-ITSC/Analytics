WITH

MESES AS (
    SELECT
        ANIO_ALSEA,
        MES_ALSEA,
        max(to_date(FECHA)) AS FECHA_FIN,
        FECHA_FIN - 90 AS FECHA_INICIO
    FROM
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    WHERE
    (
        ANIO_ALSEA = 2023
        AND
        MES_ALSEA <= 8
    )
    OR
    (
        ANIO_ALSEA = 2022
    )
    GROUP BY
        MES_ALSEA,
        ANIO_ALSEA
)

SELECT
    ANIO_ALSEA,
    MES_ALSEA,
    count(DISTINCT lower(EMAIL))
FROM
    SEGMENT_EVENTS.SESSIONM_SBX.FACT_TRANSACTIONS
INNER JOIN
    MESES
ON
    to_date(CREATED_AT) <= FECHA_FIN
AND 
    FECHA_INICIO <= to_date(CREATED_AT)
GROUP BY
    MES_ALSEA,
    ANIO_ALSEA
ORDER BY
    ANIO_ALSEA,
    MES_ALSEA
;