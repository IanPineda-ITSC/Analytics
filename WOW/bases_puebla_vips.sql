-- INSERT INTO WOW_REWARDS.SEGMENTACIONES_WOW.SEGMENTACIONES
WITH 

RANGO_DE_FECHAS AS (
    -- Seleccionamos el rango de fechas para el cual queremos generar la segmentacion
    SELECT
        -- to_date('2023-09-18') - 365 AS FECHA_INICIO_INACTIVOS,
        to_date('2023-01-01') - 180 AS FECHA_INICIO_ACTIVOS,
        to_date('2023-09-18') AS FECHA_FIN
        -- to_date('2023-08-27') - 365 AS FECHA_INICIO_INACTIVOS,
        -- to_date('2023-08-27') - 180 AS FECHA_INICIO_ACTIVOS,
        -- to_date('2023-08-27') AS FECHA_FIN
),

VENTAS_ANY_BRAND AS (
    -- Filtramos solo las compras que sean validas
    SELECT
        *
    FROM
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_VENTAS_ORDENES_WOW
    WHERE
        POS_EMPLOYEE_ID NOT IN ('Power', '1 service cloud')
),

VENTAS_RESTAURANTES AS (
    SELECT DISTINCT
        CASE 
            WHEN MARCA = 'VIPS MEXICO' THEN MARCA
            ELSE 'CASUALES'
        --     WHEN MARCA IN ('ITS JUST WINGS','CHILIS MEXICO') THEN 'CHILIS'
        --     ELSE MARCA
        END AS BRAND,
        TRANSACTION_ID,
        VENTAS,
        EMAIL,
        SUCURSAL_ID,
        to_date(DATETIME) AS FECHA
    FROM
        VENTAS_ANY_BRAND
    WHERE
        MARCA IN (
            'VIPS MEXICO'
        )
),

REGISTROS_TOTALES AS (
    SELECT
        EMAIL,
        min(to_date(TIMESTAMP)) AS FECHA_REGISTRO
    FROM
       SEGMENT_EVENTS.COREALSEA_PROD.REGISTRATION
    GROUP BY
        EMAIL
),

NUEVOS_ACTIVOS AS (
    SELECT
        REGISTROS_TOTALES.*,
        'NUEVOS_ACTIVOS' AS SEGMENTO,
        min(BRAND) AS MARCA,
        min(SUCURSAL_ID) AS SUCURSAL
    FROM
        REGISTROS_TOTALES
    INNER JOIN
        RANGO_DE_FECHAS
    ON
        FECHA_REGISTRO BETWEEN FECHA_INICIO_ACTIVOS AND FECHA_FIN
    LEFT JOIN
        VENTAS_RESTAURANTES
    ON
        lower(REGISTROS_TOTALES.EMAIL) = lower(VENTAS_RESTAURANTES.EMAIL)
    AND
        VENTAS_RESTAURANTES.FECHA BETWEEN FECHA_INICIO_ACTIVOS AND FECHA_FIN
    GROUP BY
        REGISTROS_TOTALES.EMAIL,
        FECHA_REGISTRO
    HAVING
        count(DISTINCT TRANSACTION_ID) = 1
),

VENTAS_ACTIVOS AS (
    SELECT
        BRAND,
        EMAIL,
        COUNT(DISTINCT TRANSACTION_ID) AS ORD,
        SUM(VENTAS) AS VENTAS,
        mode(SUCURSAL_ID) AS SUCURSAL
    FROM
        VENTAS_RESTAURANTES
    JOIN
        RANGO_DE_FECHAS
    WHERE
        FECHA BETWEEN FECHA_INICIO_ACTIVOS AND FECHA_FIN
    AND
        EMAIL NOT IN (SELECT EMAIL FROM NUEVOS_ACTIVOS)
    GROUP BY
        BRAND,
        EMAIL
),

ACTIVOS AS (
    SELECT
        EMAIL,
        ORD,
        BRAND AS MARCA,
        SUCURSAL,
        VENTAS AS SALES,
        VENTAS / ORD AS TICKET_PROMEDIO,
        CASE
            WHEN BRAND = 'VIPS MEXICO' AND ORD <= 3 THEN 'LOW VALUE'
            WHEN BRAND = 'VIPS MEXICO' AND ORD <= 12 THEN 'MEDIUM VALUE'
            WHEN BRAND = 'VIPS MEXICO' AND ORD <= 18 THEN 'HIGH VALUE'
            WHEN BRAND = 'VIPS MEXICO' THEN 'TOP VALUE'
            
            WHEN ORD < 2 THEN 'LOW VALUE'
            WHEN ORD < 4 THEN 'MEDIUM VALUE'
            WHEN ORD < 5 THEN 'HIGH VALUE'
            ELSE 'TOP VALUE'
        END AS SEGMENTO
    FROM
        VENTAS_ACTIVOS
),

SEGMENTACION_TOTAL AS (
    SELECT EMAIL, SEGMENTO, MARCA, SUCURSAL, ORD FROM ACTIVOS
    UNION ALL
    SELECT EMAIL, SEGMENTO, MARCA, SUCURSAL, 1 AS ORD FROM NUEVOS_ACTIVOS
),

TIENDAS_PUEBLA AS (
    SELECT
        *
    FROM
        WOW_REWARDS.SBX_REWARDS.DS_CAT_CC_VIPS
    WHERE
        ESTADO = 'PUEBLA'
)

SELECT
    SEGMENTACION_TOTAL.*,
    USUARIOS_SESSIONM.FIRST_NAME,
    USUARIOS_SESSIONM.LAST_NAME,
    NOMBRE_TIENDA
FROM
    SEGMENTACION_TOTAL
INNER JOIN
    TIENDAS_PUEBLA
ON
    SUCURSAL = CECO
LEFT JOIN 
    WOW_REWARDS.WORK_SPACE_WOW_REWARDS.USUARIOS_SESSIONM
ON
    lower(SEGMENTACION_TOTAL.EMAIL) = lower(USUARIOS_SESSIONM.EMAIL)
;