WITH

CATALOGO_SUCURSALES AS (
    SELECT DISTINCT
        CC AS RETAILER_STORE_ID,
        REGION,
        CASE
            WHEN ESTADO LIKE '%Baja California' THEN 'Baja California'
            WHEN ESTADO LIKE '%Baja California Sur' THEN 'Baja California Sur'
            WHEN ESTADO LIKE '%Chihuahua' THEN 'Chihuahua'
            WHEN ESTADO LIKE '%Jalisco' THEN 'Jalisco'
            WHEN ESTADO LIKE '%Nayarit' THEN 'Nayarit'
            WHEN ESTADO LIKE '%Sinaloa' THEN 'Sinaloa'
            WHEN ESTADO LIKE '%Sonora' THEN 'Sonora'
            ELSE ESTADO
        END AS ESTADO,
        CASE
            WHEN CIUDAD LIKE 'Quer%' THEN 'Queretaro'
            WHEN CIUDAD LIKE 'Tlanepantla' THEN 'Tlalnepantla'
            WHEN CIUDAD LIKE 'Tuxla Gutierrez' THEN 'Tuxtla Gutierrez'
            ELSE CIUDAD
        END AS CIUDAD,
        CC_NOMBRE,
        DM
    FROM
        SEGMENT_EVENTS.SESSIONM_SBX.SBX_DIRECTORIO
),

TRANSACCIONES AS (
    SELECT
        to_date(CREATED_AT) AS FECHA,
        CASE
            WHEN RETAILER_STORE_ID = 8957 THEN 38957
            ELSE RETAILER_STORE_ID
        END AS RETAILER_STORE_ID,
        CHECK_AMOUNT
    FROM
        SEGMENT_EVENTS.SESSIONM_SBX.FACT_TRANSACTIONS
),

TOTAL_ALSEA_POR_CIUDAD_Y_SEMANA AS (
    SELECT
        T.SEM_ALSEA,
        CATALOGO_SUCURSALES.REGION,
        CATALOGO_SUCURSALES.ESTADO,
        CATALOGO_SUCURSALES.CIUDAD,
        CATALOGO_SUCURSALES.RETAILER_STORE_ID,
        CATALOGO_SUCURSALES.CC_NOMBRE,
        SUM(TOT.VENTAS) VENTA_TOTAL
    FROM 
        "WOW_REWARDS"."WORK_SPACE_WOW_REWARDS"."DATOS_TOTAL_ALSEA" TOT
    INNER JOIN 
        "WOW_REWARDS"."WORK_SPACE_WOW_REWARDS"."DS_DIM_TIME" T
    ON 
        T.FECHA = TO_CHAR(TO_DATE(TOT.FECHA,'DD/MM/YYYY'),'YYYY-MM-DD')
    AND 
        T.ANIO_ALSEA = 2022    ---ADECUAR EL AÑO QUE SE NECESITE
    AND 
        MARCA = 'STARBUCKS COFFEE MEXICO'
    INNER JOIN
        CATALOGO_SUCURSALES
    ON
        TOT.TIENDA = CATALOGO_SUCURSALES.RETAILER_STORE_ID
    GROUP BY 
        T.ANIO_ALSEA,
        T.SEM_ALSEA,
        CATALOGO_SUCURSALES.REGION,
        CATALOGO_SUCURSALES.ESTADO,
        CATALOGO_SUCURSALES.CIUDAD,
        CATALOGO_SUCURSALES.RETAILER_STORE_ID,
        CATALOGO_SUCURSALES.CC_NOMBRE,
        CATALOGO_SUCURSALES.DM
),

VENTA_SR_POR_CIUDAD_Y_SEMANA AS (
    SELECT
        DS_DIM_TIME.SEM_ALSEA,
        CATALOGO_SUCURSALES.REGION,
        CATALOGO_SUCURSALES.ESTADO,
        CATALOGO_SUCURSALES.CIUDAD,
        CATALOGO_SUCURSALES.RETAILER_STORE_ID,
        CATALOGO_SUCURSALES.CC_NOMBRE,
        CATALOGO_SUCURSALES.DM,
        sum(TRANSACCIONES.CHECK_AMOUNT) / 1.16 AS VENTA_SR
    FROM
        TRANSACCIONES
    LEFT JOIN
        CATALOGO_SUCURSALES
    USING(
        RETAILER_STORE_ID
    )
    INNER JOIN
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    USING(
        FECHA
    )
    WHERE
        ANIO_ALSEA = 2022
    GROUP BY
        DS_DIM_TIME.ANIO_ALSEA,
        DS_DIM_TIME.SEM_ALSEA,
        CATALOGO_SUCURSALES.REGION,
        CATALOGO_SUCURSALES.ESTADO,
        CATALOGO_SUCURSALES.CIUDAD,
        CATALOGO_SUCURSALES.RETAILER_STORE_ID,
        CATALOGO_SUCURSALES.CC_NOMBRE,
        CATALOGO_SUCURSALES.DM
),

PRE_PIVOT AS (
    SELECT
        SEM_ALSEA,
        REGION,
        ESTADO,
        CIUDAD,
        RETAILER_STORE_ID,
        CC_NOMBRE,
        DM, 
        VENTA_SR / VENTA_TOTAL AS TENDER
    FROM
        TOTAL_ALSEA_POR_CIUDAD_Y_SEMANA
    INNER JOIN
        VENTA_SR_POR_CIUDAD_Y_SEMANA
    USING(
        SEM_ALSEA,
        REGION,
        ESTADO,
        CIUDAD,
        RETAILER_STORE_ID,
        CC_NOMBRE
        
    )
    WHERE
        34 <= SEM_ALSEA 
    AND 
        SEM_ALSEA <= 44
),

PIVOTED AS (
    SELECT 
        CECO,
        NOMBRE,
        W34,
        W35,
        W36,
        W37,
        W38,
        W39,
        W40,
        W41,
        W42,
        W43,
        W44
    FROM 
        PRE_PIVOT
    PIVOT (
        AVG(TENDER)
        FOR SEM_ALSEA IN (
            34, 35, 36, 37, 38, 39, 40,
            41, 42, 43, 44
        )
    )
    AS T (REGION, ESTADO, CIUDAD, CECO, NOMBRE, DM, W34, W35, W36, W37, W38, W39, W40, W41, W42, W43, W44)
),

TENDER_1_33 AS (
    SELECT
        RETAILER_STORE_ID AS CECO,
        CC_NOMBRE AS NOMBRE,
        sum(VENTA_SR) / sum(VENTA_TOTAL) AS TENDER
    FROM
        TOTAL_ALSEA_POR_CIUDAD_Y_SEMANA
    INNER JOIN
        VENTA_SR_POR_CIUDAD_Y_SEMANA
    USING(
        SEM_ALSEA,
        REGION,
        ESTADO,
        CIUDAD,
        RETAILER_STORE_ID,
        CC_NOMBRE
        
    )
    WHERE
        1 <= SEM_ALSEA 
    AND 
        SEM_ALSEA <= 33
    GROUP BY
        RETAILER_STORE_ID,
        CC_NOMBRE
),

TENDER_34_44 AS (
    SELECT
        RETAILER_STORE_ID AS CECO,
        CC_NOMBRE AS NOMBRE,
        sum(VENTA_SR) / sum(VENTA_TOTAL) AS TENDER
    FROM
        TOTAL_ALSEA_POR_CIUDAD_Y_SEMANA
    INNER JOIN
        VENTA_SR_POR_CIUDAD_Y_SEMANA
    USING(
        SEM_ALSEA,
        REGION,
        ESTADO,
        CIUDAD,
        RETAILER_STORE_ID,
        CC_NOMBRE
        
    )
    WHERE
        34 <= SEM_ALSEA 
    AND 
        SEM_ALSEA <= 44
    GROUP BY
        RETAILER_STORE_ID,
        CC_NOMBRE
)
SELECT
    PIVOTED.*,
    TENDER_1_33.TENDER AS TENDER_1_33,
    TENDER_34_44.TENDER AS TENDER_34_44
FROM
    PIVOTED
LEFT JOIN
    TENDER_1_33
USING(
    CECO,
    NOMBRE
)
LEFT JOIN
    TENDER_34_44
USING(
    CECO,
    NOMBRE
)
;