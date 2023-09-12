WITH

FECHAS_BASE AS (
    SELECT
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA,
        max(to_date(FECHA)) AS FECHA_BASE
    FROM
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    WHERE 
        ANIO_ALSEA = 2022
    OR
    (
        ANIO_ALSEA = 2023
        AND
        SEM_ALSEA <= 33
    )
    GROUP BY
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA
),

ULTIMA_VENTA AS (
    SELECT
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA,
        EMAIL,
        to_date(max(CREATED_AT)) FECHA_ULTIMA_VENTA
    FROM
        SEGMENT_EVENTS.SESSIONM_SBX.FACT_TRANSACTIONS
    INNER JOIN
        FECHAS_BASE
    ON
        to_date(CREATED_AT) <= FECHA_BASE
    GROUP BY
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA,
        EMAIL
),

STATUS_MEMBERS AS (
    SELECT
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA,
        EMAIL,
        CASE
            WHEN ULTIMA_VENTA.FECHA_ULTIMA_VENTA >= FECHA_BASE - 91  THEN 'ACTIVE'
            ELSE 'INACTIVE'
        END AS STATUS
    FROM
        ULTIMA_VENTA
    INNER JOIN
        FECHAS_BASE
    USING(
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA
    )
)

SELECT 
    ANIO_ALSEA,
    MES_ALSEA,
    SEM_ALSEA,
    STATUS,
    count(DISTINCT EMAIL)
FROM 
    STATUS_MEMBERS
GROUP BY    
    ANIO_ALSEA,
    MES_ALSEA,
    SEM_ALSEA,
    STATUS
;