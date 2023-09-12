WITH 

RANGO_DE_FECHAS AS (
    -- Seleccionamos el rango de fechas para el cual queremos generar la segmentacion
    SELECT
        to_date('2023-07-30') - 365 AS FECHA_INICIO_ACTIVOS,
        to_date('2023-07-30') AS FECHA_FIN
)

SELECT
    count(DISTINCT EMAIL, MARCA) AS ACTIVOS_RESTAURANTES
FROM
    WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_VENTAS_ORDENES_WOW
INNER JOIN
    RANGO_DE_FECHAS
ON
    to_date(DATETIME) <= FECHA_FIN
AND
    FECHA_INICIO_ACTIVOS < to_date(DATETIME)
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
;