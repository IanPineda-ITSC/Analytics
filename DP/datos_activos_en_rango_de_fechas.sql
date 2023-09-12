WITH 

RANGO_DE_FECHAS AS (
    SELECT
        FECHA
    FROM
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    WHERE
        FECHA >= '2023-02-12'
     AND 
        FECHA <= '2023-08-13'
),

TRANSACCIONES_OLO AS (
-- Filtramos las transacciones para recibir solo las que sean de los sources y el rango de fechas que queremos.
    SELECT
        lower(MXP.EMAIL) AS EMAIL,
        ORDER_DATE AS FECHA,
        TO_CHAR(ORDER_DATE,'YYYY-MM-DD')||LOCATION_CODE||OLO.ORDER_NUMBER AS ORDER_ID,
        OLO.ORDERFINALPRICE / 1.16 AS ORDER_AMOUNT
    FROM
        "SEGMENT_EVENTS"."DOMINOS_OLO"."DPMSALES_FULL" OLO
    INNER JOIN
        "SEGMENT_EVENTS"."DOMINOS_OLO"."MXPOWERSALESLOG" MXP 
    ON 
        OLO.ORDER_NUMBER = MXP.ORDERNUMBER 
    AND 
        OLO.LOCATION_CODE = MXP.STORENUMBER 
    AND 
        TO_CHAR(OLO.ORDER_DATE,'YYYY-MM-DD') = MXP.ORDERDATE
    WHERE
        ORDER_STATUS_CODE = 4
    AND 
        SOURCE_CODE  IN ('ANDROID2','DESKTOP2','IOSAPP','DESKTOP','MOBILE2','ANDROID')
    AND 
        OLO.SOURCE_CODE IS NOT NULL
    AND 
        UPPER(COMMENTS) NOT LIKE '%DIDI%'
    AND 
        UPPER(COMMENTS) NOT LIKE '%UBER%'
    AND 
        UPPER(COMMENTS) NOT LIKE '%RAPPI%'          
),

TRANSACCIONES_CLOUD AS (
    SELECT DISTINCT 
        lower(A.EMAIL) AS EMAIL,
        to_date(SUBSTRING(A.STOREORDERID,1,10)) AS FECHA,
        to_char(FECHA) || A.STOREID ||A.StoreOrderID AS ORDER_ID,
        PAYMENTSAMOUNT / 1.16 AS ORDER_AMOUNT
    FROM 
        "SEGMENT_EVENTS"."DOMINOS_GOLO"."VENTA_CLOUD" A
    WHERE 
        A.STOREID NOT LIKE '9%'
    AND 
        A.SOURCEORGANIZATIONURI IN ('order.dominos.com','resp-order.dominos.com','iphone.dominos.mx','android.dominos.mx') 
    AND 
        A.SOURCEORGANIZATIONURI IS NOT NULL
),

TRANSACCIONES_TOTAL AS (
    SELECT * FROM TRANSACCIONES_OLO
    UNION ALL
    SELECT * FROM TRANSACCIONES_CLOUD
),

TRANSACCIONES_ACTIVOS AS (
    SELECT
        TRANSACCIONES_TOTAL.*
    FROM
        TRANSACCIONES_TOTAL
    INNER JOIN 
        RANGO_DE_FECHAS
    USING(
        FECHA
    )
),

TRANSACCIONES_ACTIVOS_BY_USER AS (
-- Agrupamos las transacciones por usuario, para calcular la frecuencia, las ventas totales y el ticket promedio del cliente.
    SELECT
        EMAIL,
        count(DISTINCT ORDER_ID) AS FREQ,
        sum(ORDER_AMOUNT) AS VENTAS,
        VENTAS / FREQ AS AOV       
    FROM 
        TRANSACCIONES_ACTIVOS
    GROUP BY
        EMAIL
),
PERCENTILES_AOV AS (
-- Generamos el 25 y 75 percentil de AOV para saber las delimitaciones de los segmentos
    SELECT
        round(percentile_cont(0.25) WITHIN GROUP (ORDER BY TRANSACCIONES_ACTIVOS_BY_USER.AOV)) p25th_AOV,
        round(percentile_cont(0.75) WITHIN GROUP (ORDER BY TRANSACCIONES_ACTIVOS_BY_USER.AOV)) p75th_AOV
    FROM
        TRANSACCIONES_ACTIVOS_BY_USER
),

SEGMENTACION_ACTIVOS AS (
-- Basado en el criterio de frecuencia y en los percentiles de AOV, etiquetamos cada uno de los usuarios con su respectivo segmento
    SELECT 
        EMAIL,
        CASE
            WHEN FREQ <= 1 THEN 'NUEVOS'
            WHEN FREQ <  4 THEN 'LIGHT'
            WHEN FREQ <  6 THEN 'MID'
            ELSE                'HEAVY'
        END AS SEGMENTO_FREQUENCY,
        CASE
            WHEN AOV <= p25th_AOV THEN 'LOW'
            WHEN AOV <= p75th_AOV THEN 'AVG'
            ELSE                       'HIGH'
        END AS SEGMENTO_AOV,
        (SEGMENTO_FREQUENCY || '_' || SEGMENTO_AOV) AS FULL_SEGMENTO,
        CASE 
            WHEN FULL_SEGMENTO IN ('NUEVOS_LOW','NUEVOS_AVG','LIGHT_LOW') THEN 'LOW_VALUE'
            WHEN FULL_SEGMENTO IN ('LIGHT_AVG','NUEVOS_HIGH','LIGHT_HIGH') THEN 'MEDIUM_LOW_VALUE'
            WHEN FULL_SEGMENTO IN ('MID_LOW','MID_AVG','HEAVY_LOW') THEN 'MEDIUM_HIGH_VALUE'
            WHEN FULL_SEGMENTO IN ('HEAVY_AVG','MID_HIGH') THEN 'HIGH_VALUE'
            WHEN FULL_SEGMENTO IN ('HEAVY_HIGH') THEN 'TOP_VALUE'
        END  AS SEGMENTO, 
        FREQ,
        VENTAS,
        AOV, 
        FREQ*AOV AS CLV
    FROM
        TRANSACCIONES_ACTIVOS_BY_USER
    INNER JOIN
        PERCENTILES_AOV
)
------------CALCULO DE DATA DE ACTIVOS  ----
SELECT   
    SEGMENTO,
    COUNT(DISTINCT EMAIL) AS CLIENTES,
    AVG(FREQ) AS FREQ,
    SUM(VENTAS) AS VENTAS,
    AVG(AOV) AS AOV,  
    AVG(FREQ)*AVG(AOV) AS CLV
FROM
    SEGMENTACION_ACTIVOS 
GROUP BY 
    SEGMENTO
;