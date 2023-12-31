WITH

PERIODO_ACTUAL AS (
    SELECT
        to_date(max(FECHA)) AS FECHA_FIN,
        FECHA_FIN - 180 AS FECHA_INICIO,
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA
    FROM
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    WHERE
        ANIO_ALSEA = 2023
    AND
        SEM_ALSEA = 30
    GROUP BY
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA
),

SEMANA_PREVIA AS (
    SELECT
        to_date(max(FECHA)) AS FECHA_FIN,
        FECHA_FIN - 180 AS FECHA_INICIO
    FROM
    (
        SELECT
            CASE WHEN SEM_ALSEA = 1 THEN ANIO_ALSEA - 1 ELSE ANIO_ALSEA END AS ANIO_ALSEA,
            CASE WHEN SEM_ALSEA = 1 THEN 52             ELSE SEM_ALSEA - 1 END AS SEM_ALSEA
        FROM
            PERIODO_ACTUAL
    )
    INNER JOIN
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    USING(
        ANIO_ALSEA,
        SEM_ALSEA
    )
),

MES_PREVIO AS (
    SELECT
        to_date(max(FECHA)) AS FECHA_FIN,
        FECHA_FIN - 180 AS FECHA_INICIO
    FROM
    (
        SELECT
            CASE WHEN MES_ALSEA = 1 THEN ANIO_ALSEA - 1 ELSE ANIO_ALSEA END AS ANIO_ALSEA,
            CASE WHEN MES_ALSEA = 1 THEN 12             ELSE MES_ALSEA - 1 END AS MES_ALSEA
        FROM
            PERIODO_ACTUAL
    )
    INNER JOIN
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    USING(
        ANIO_ALSEA,
        MES_ALSEA
    )
),

ANIO_PREVIO AS (
    SELECT
        to_date(max(FECHA)) AS FECHA_FIN,
        FECHA_FIN - 180 AS FECHA_INICIO
    FROM
    (
        SELECT
            ANIO_ALSEA - 1 AS ANIO_ALSEA
        FROM
            PERIODO_ACTUAL
    )
    INNER JOIN
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    USING(
        ANIO_ALSEA
    )
),

REGISTROS_CLOUD AS (
    SELECT lower(EMAIL) AS EMAIL, to_date(RECEIVED_AT) AS FECHA FROM SEGMENT_EVENTS.GOLO_WEB.SIGNUP_SUCCESS
    UNION ALL
    SELECT lower(EMAIL) AS EMAIL, to_date(RECEIVED_AT) AS FECHA FROM SEGMENT_EVENTS.GOLO_ANDROID_PROD.SIGNUP_SUCCESS
    UNION ALL
    SELECT lower(EMAIL) AS EMAIL, to_date(RECEIVED_AT) AS FECHA FROM SEGMENT_EVENTS.GOLO_IOS_PROD.SIGNUP_SUCCESS
),

REGISTROS_OLO AS (
    SELECT lower(CONTEXT_TRAITS_EMAIL) AS EMAIL, to_date(TIMESTAMP) AS FECHA FROM SEGMENT_EVENTS.DOMINOS_ANDROID_APP_PRODUCCION.SIGNED_IN WHERE CONTEXT_TRAITS_NAME IS NOT NULL          
    UNION ALL
    SELECT lower(CONTEXT_TRAITS_EMAIL) AS EMAIL, to_date(TIMESTAMP) AS FECHA FROM SEGMENT_EVENTS.DOMINOS_APP_PRODUCCION.SIGNED_IN WHERE CONTEXT_TRAITS_FIRST_NAME IS NOT NULL 
),

REGISTROS_OLO_Y_CLOUD AS (
    SELECT
        EMAIL,
        min(FECHA) AS FECHA_REGISTRO
    FROM
    (
        SELECT * FROM REGISTROS_CLOUD
        UNION ALL
        SELECT * FROM REGISTROS_OLO
    )
    WHERE 
        EMAIL IS NOT null
    AND 
        EMAIL <>'NULL'
    GROUP BY
        EMAIL
),

REGISTROS_PERIODO_ACTUAL AS (
    SELECT
        count(DISTINCT EMAIL) AS CLIENTES
    FROM
        REGISTROS_OLO_Y_CLOUD
    INNER JOIN
        PERIODO_ACTUAL
    ON
        FECHA_REGISTRO BETWEEN FECHA_INICIO AND FECHA_FIN
),

REGISTROS_SEMANA_PREVIA AS (
    SELECT
        count(DISTINCT EMAIL) AS CLIENTES
    FROM
        REGISTROS_OLO_Y_CLOUD
    INNER JOIN
        SEMANA_PREVIA
    ON
        FECHA_REGISTRO BETWEEN FECHA_INICIO AND FECHA_FIN
),

REGISTROS_MES_PREVIO AS (
    SELECT
        count(DISTINCT EMAIL) AS CLIENTES
    FROM
        REGISTROS_OLO_Y_CLOUD
    INNER JOIN
        MES_PREVIO
    ON
        FECHA_REGISTRO BETWEEN FECHA_INICIO AND FECHA_FIN
),

REGISTROS_ANIO_PREVIO AS (
    SELECT
        count(DISTINCT EMAIL) AS CLIENTES
    FROM
        REGISTROS_OLO_Y_CLOUD
    INNER JOIN
        ANIO_PREVIO
    ON
        FECHA_REGISTRO BETWEEN FECHA_INICIO AND FECHA_FIN
)

SELECT
    REGISTROS_PERIODO_ACTUAL.CLIENTES AS CLIENTES_REGISTRADOS_PERIODO_ACTUAL,

    REGISTROS_SEMANA_PREVIA.CLIENTES AS CLIENTES_REGISTRADOS_SEMANA_PREVIA,
    CLIENTES_REGISTRADOS_PERIODO_ACTUAL / CLIENTES_REGISTRADOS_SEMANA_PREVIA AS PERCENT_V_SEMANA_PREVIA,

    REGISTROS_MES_PREVIO.CLIENTES AS CLIENTES_REGISTRADOS_MES_PREVIO,
    CLIENTES_REGISTRADOS_PERIODO_ACTUAL / CLIENTES_REGISTRADOS_MES_PREVIO AS PERCENT_V_MES_PREVIO,

    REGISTROS_ANIO_PREVIO.CLIENTES AS CLIENTES_REGISTRADOS_ANIO_PREVIO,
    CLIENTES_REGISTRADOS_PERIODO_ACTUAL / CLIENTES_REGISTRADOS_ANIO_PREVIO AS PERCENT_V_ANIO_PREVIO
FROM
    REGISTROS_PERIODO_ACTUAL
JOIN
    REGISTROS_SEMANA_PREVIA
JOIN
    REGISTROS_MES_PREVIO
JOIN
    REGISTROS_ANIO_PREVIO
;