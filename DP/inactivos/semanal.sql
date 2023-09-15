WITH

RANGO_DE_FECHAS AS (
    SELECT
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA,
        max(to_date(FECHA)) AS FECHA_FIN,
        FECHA_FIN - 180 AS FECHA_INICIO_ACTIVOS,
        FECHA_FIN - 365 AS FECHA_INICIO_INACTIVOS
    FROM
        WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
    WHERE
    (
        ANIO_ALSEA = 2022
    )
    OR
    (
        ANIO_ALSEA = 2023
        AND
        SEM_ALSEA <= 36
    )
    GROUP BY
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA
),

TRANSACCIONES_OLO AS (
    SELECT DISTINCT
        lower(MXP.EMAIL) AS EMAIL,
        ORDER_DATE AS FECHA,
        TO_CHAR(ORDER_DATE,'YYYY-MM-DD') || '-' || LOCATION_CODE || '-' || OLO.ORDER_NUMBER AS ORDER_ID,
        OLO.ORDERFINALPRICE / 1.16 AS ORDER_AMOUNT,
        PHONENUMBER AS PHONE,
        FIRSTNAME
    FROM
       "SEGMENT_EVENTS"."DOMINOS_OLO"."MXPOWERSALESLOG" MXP
    INNER JOIN 
        "SEGMENT_EVENTS"."DOMINOS_OLO"."DPMSALES_FULL" OLO
    ON 
        OLO.ORDER_NUMBER = MXP.ORDERNUMBER AND OLO.LOCATION_CODE = MXP.STORENUMBER 
    AND 
        TO_CHAR(OLO.ORDER_DATE,'YYYY-MM-DD') = MXP.ORDERDATE
    WHERE 
        OLO.ORDER_STATUS_CODE = 4
    AND 
        OLO.LOCATION_CODE NOT IN ('13001' , '13006', '13021', '11000')
    AND 
        UPPER(OLO.SOURCE_CODE) IN (
            'ANDROID', 
            'DESKTOP', 
            'IOS', 
            'MOBILE', 
            'WEB', 
            'ANDROID2', 
            'DESKTOP2', 
            'IOSAPP', 
            'MOBILE2', 
            'WHATSAPP'
        )
),

TRANSACCIONES_CLOUD AS (
    SELECT DISTINCT 
        lower(A.EMAIL) AS EMAIL,
        to_date(SUBSTRING(A.STOREORDERID,1,10)) AS FECHA,
        split_part(A.STOREORDERID, '#', 1) || '-' || A.STOREID || '-' || split_part(A.STOREORDERID, '#', 2) AS ORDER_ID,
        PAYMENTSAMOUNT / 1.16 AS ORDER_AMOUNT,
        PHONE,
        FIRSTNAME
    FROM 
        "SEGMENT_EVENTS"."DOMINOS_GOLO"."VENTA_CLOUD" A
    WHERE 
        A.STOREID NOT LIKE '9%'
    AND 
        A.SOURCEORGANIZATIONURI IN (
            'order.dominos.com', 
            'resp-order.dominos.com', 
            'iphone.dominos.mx', 
            'android.dominos.mx'
        )
),

TRANSACCIONES_OLO_Y_CLOUD AS (
    SELECT * FROM TRANSACCIONES_OLO
    UNION ALL
    SELECT * FROM TRANSACCIONES_CLOUD
),

SEGMENTACION_BASE_INACTIVOS AS (
    SELECT
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA,
        EMAIL,
        'INACTIVOS' AS SEGMENTO
    FROM
        TRANSACCIONES_OLO_Y_CLOUD
    JOIN
        RANGO_DE_FECHAS
    ON
        FECHA < FECHA_FIN
    GROUP BY
        ANIO_ALSEA,
        MES_ALSEA,
        SEM_ALSEA,
        EMAIL,
        FECHA_INICIO_ACTIVOS,
        FECHA_INICIO_INACTIVOS
    HAVING
        max(FECHA) < FECHA_INICIO_ACTIVOS
    AND
        FECHA_INICIO_INACTIVOS <= max(FECHA)
)

SELECT
    ANIO_ALSEA,
    MES_ALSEA,
    SEM_ALSEA,
    SEGMENTO,
    count(DISTINCT EMAIL),
    count(DISTINCT lower(EMAIL)),
    count(*)
FROM
    SEGMENTACION_BASE_INACTIVOS
GROUP BY
    ANIO_ALSEA,
    MES_ALSEA,
    SEM_ALSEA,
    SEGMENTO
;
--  902,435