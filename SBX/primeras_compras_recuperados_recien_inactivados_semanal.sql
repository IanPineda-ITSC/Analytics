WITH

SEMANAS AS (
    SELECT
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA,
        to_date(max(FECHA)) AS FECHA_FIN_SEMANA_ACTUAL,
        FECHA_FIN_SEMANA_ACTUAL - 90 AS FECHA_INICIO_SEMANA_ACTUAL,
        FECHA_FIN_SEMANA_ACTUAL - 7 AS FECHA_FIN_SEMANA_PREVIA,
        FECHA_FIN_SEMANA_PREVIA - 90 AS FECHA_INICIO_SEMANA_PREVIA
    FROM
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    WHERE
    (
        ANIO_ALSEA = 2023
        AND
        SEM_ALSEA <= 37
    )
    GROUP BY
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA
),

TRANSACCIONES_TOTAL AS (
    SELECT
        to_date(CREATED_AT) AS FECHA,
        lower(EMAIL) AS EMAIL
    FROM 
        SEGMENT_EVENTS.SESSIONM_SBX.FACT_TRANSACTIONS
),

PRIMERAS_COMPRAS AS (
    SELECT
        lower(EMAIL) AS EMAIL,
        min(FECHA) AS FECHA
    FROM
        TRANSACCIONES_TOTAL
    GROUP BY
        EMAIL
),

PRIMERAS_COMPRAS_CON_SEMANA AS (
    SELECT
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA,
        EMAIL
    FROM
        PRIMERAS_COMPRAS
    INNER JOIN
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    USING(
        FECHA
    )
),

ACTIVOS_EN_SEMANA_PREVIA AS (
    SELECT
        EMAIL,
        SEM_ALSEA,
        MES_ALSEA,
        ANIO_ALSEA
    FROM
        TRANSACCIONES_TOTAL
    INNER JOIN
        SEMANAS
    ON
        FECHA_INICIO_SEMANA_PREVIA < FECHA
    AND
        FECHA <= FECHA_FIN_SEMANA_PREVIA 
    GROUP BY
        EMAIL,
        SEM_ALSEA,
        MES_ALSEA,
        ANIO_ALSEA
),

ACTIVOS_EN_SEMANA_ACTUAL AS (
    SELECT
        EMAIL,
        SEM_ALSEA,
        MES_ALSEA,
        ANIO_ALSEA
    FROM
        TRANSACCIONES_TOTAL
    INNER JOIN
        SEMANAS
    ON
        FECHA_INICIO_SEMANA_ACTUAL < FECHA
    AND
        FECHA <= FECHA_FIN_SEMANA_ACTUAL 
    GROUP BY
        EMAIL,
        SEM_ALSEA,
        MES_ALSEA,
        ANIO_ALSEA
),

PRE_PIVOT AS (
    SELECT
        coalesce(ACTIVOS_EN_SEMANA_PREVIA.ANIO_ALSEA, ACTIVOS_EN_SEMANA_ACTUAL.ANIO_ALSEA) AS ANIO,
        coalesce(ACTIVOS_EN_SEMANA_PREVIA.MES_ALSEA, ACTIVOS_EN_SEMANA_ACTUAL.MES_ALSEA) AS MES,
        coalesce(ACTIVOS_EN_SEMANA_PREVIA.SEM_ALSEA, ACTIVOS_EN_SEMANA_ACTUAL.SEM_ALSEA) AS SEM,
        CASE
            WHEN ACTIVOS_EN_SEMANA_PREVIA.EMAIL IS null AND PRIMERAS_COMPRAS_CON_SEMANA.EMAIL IS null THEN 'RECUPERADOS'
            WHEN ACTIVOS_EN_SEMANA_PREVIA.EMAIL IS null THEN 'PRIMERAS_COMPRAS'
            WHEN ACTIVOS_EN_SEMANA_ACTUAL.EMAIL IS null THEN 'RECIEN_INACTIVADOS'
            ELSE 'ACTIVOS_EN_AMBOS_PERIODOS'
        END AS CATEGORIA,
        count(DISTINCT lower(coalesce(ACTIVOS_EN_SEMANA_PREVIA.EMAIL, ACTIVOS_EN_SEMANA_ACTUAL.EMAIL))) AS USUARIOS
    FROM
        ACTIVOS_EN_SEMANA_ACTUAL
    FULL OUTER JOIN
        ACTIVOS_EN_SEMANA_PREVIA
    USING(
        EMAIL,
        SEM_ALSEA,
        MES_ALSEA,
        ANIO_ALSEA
    )
    FULL OUTER JOIN
        PRIMERAS_COMPRAS_CON_SEMANA
    USING(
        EMAIL,
        SEM_ALSEA,
        MES_ALSEA,
        ANIO_ALSEA
    )
    GROUP BY
        SEM,
        MES,
        ANIO,
        CATEGORIA
)

SELECT
    *
FROM
    PRE_PIVOT
PIVOT(sum(USUARIOS) FOR CATEGORIA IN ('RECUPERADOS','PRIMERAS_COMPRAS','RECIEN_INACTIVADOS'))
AS PIVOTED (ANIO_ALSEA, MES_ALSEA, SEM_ALSEA, RECUPERADOS, PRIMERAS_COMPRAS, RECIEN_INACTIVADOS)
ORDER BY
    ANIO_ALSEA DESC,
    SEM_ALSEA DESC
;