WITH 

RANGO_DE_FECHAS AS (
    SELECT
        max(to_date(FECHA)) AS FECHA_FIN,
        FECHA_FIN - 180 AS FECHA_INICIO
        -- min(to_date(FECHA)) AS FECHA_INICIO
    FROM
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    WHERE
        ANIO_ALSEA = 2022
    AND
        MES_ALSEA = 8
),

VENTAS_RESTAURANTES AS (
    SELECT
        CASE 
            WHEN MARCA = 'VIPS MEXICO' THEN 'VIPS MEXICO'
            ELSE 'CASUALES'
        END AS MARCA,
        TRANSACTION_ID,
        VENTAS,
        lower(EMAIL) AS EMAIL,
        to_date(DATETIME) AS FECHA
    FROM
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_VENTAS_ORDENES_WOW
    INNER JOIN
        RANGO_DE_FECHAS
    ON
        to_date(DATETIME) <= FECHA_FIN
    AND
        FECHA_INICIO <= to_date(DATETIME)
    WHERE
        POS_EMPLOYEE_ID NOT IN ('Power', '1 service cloud')
    AND
        MARCA IN (
            'THE CHEESECAKE FACTORY MEXICO',
            'ITS JUST WINGS',
            'CHILIS MEXICO',
            'ITALIANNIS MEXICO',
            'P.F. CHANGS MEXICO',
            'VIPS MEXICO'
        )
)

SELECT
    MARCA,
    sum(VENTAS) / count(DISTINCT EMAIL) AS CLV
FROM
    VENTAS_RESTAURANTES
GROUP BY
    MARCA