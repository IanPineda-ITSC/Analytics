SELECT
    USERS.EMAIL,
    USERS.USER_ID,
    ULTRASEGMENTO || '_' || GRUPO AS SEGMENTO
FROM
    USERS
LEFT JOIN
(
    SELECT 
        EMAIL,
        Q1.USER_ID,
        RECENCY,
        FREQ,
        AOV,
        JOINING_DATE,
        SEGMENTO,
        TAG_GRUPO,
        FULL_SEGMENTO,
        CASE WHEN (FULL_SEGMENTO = 'LIGHT_LOW')
                OR (FULL_SEGMENTO = 'LIGHT_AVG')
                OR (FULL_SEGMENTO = 'LIGHT_HIGH')
                OR (FULL_SEGMENTO = 'MIDL_LOW')
                OR (FULL_SEGMENTO = 'MIDL_AVG')
                OR (FULL_SEGMENTO = 'MIDH_LOW')
            THEN 'LOW'
            WHEN (FULL_SEGMENTO = 'MIDL_HIGH')
                OR (FULL_SEGMENTO = 'MIDH_AVG')
                OR (FULL_SEGMENTO = 'MIDH_HIGH')
                OR (FULL_SEGMENTO = 'HEAVY_LOW')
                OR (FULL_SEGMENTO = 'HEAVY_AVG')
            THEN 'MEDIUM'
            WHEN (FULL_SEGMENTO = 'HEAVY_HIGH')
                OR (FULL_SEGMENTO = 'SUPER_LOW')
                OR (FULL_SEGMENTO = 'SUPER_AVG')
            THEN 'HIGH'
            ELSE 'TOP'
        END                                        AS ULTRASEGMENTO,
        CASE
            WHEN Q2.USER_ID IS NOT NULL THEN 'CONTROL'
            ELSE 'PROMOCION'
        END AS GRUPO
    FROM 
    (
        SELECT
            MAIN_SBQ.EMAIL,
            MAIN_SBQ.USER_ID,
            MAIN_SBQ.RECENCY,
            MAIN_SBQ.FREQ,
            MAIN_SBQ.AOV,
            MAIN_SBQ.JOINING_DATE,

            CASE WHEN MAIN_SBQ.FREQ <= PERCENTILE_SBQ.P20TH_FREQ THEN 'LIGHT'
                WHEN MAIN_SBQ.FREQ <= PERCENTILE_SBQ.P40TH_FREQ THEN 'MIDL'
                WHEN MAIN_SBQ.FREQ <= PERCENTILE_SBQ.P60TH_FREQ THEN 'MIDH'
                WHEN MAIN_SBQ.FREQ <= PERCENTILE_SBQ.P80TH_FREQ THEN 'HEAVY'
                ELSE 'SUPER'
            END                                                AS SEGMENTO,

            CASE WHEN MAIN_SBQ.AOV <= PERCENTILE_SBQ.P25TH_AOV THEN 'LOW'
                WHEN MAIN_SBQ.AOV <= PERCENTILE_SBQ.P75TH_AOV THEN 'AVG'
                ELSE 'HIGH'
            END                                                AS TAG_GRUPO,

            (CASE WHEN MAIN_SBQ.FREQ <= PERCENTILE_SBQ.P20TH_FREQ THEN 'LIGHT'
                WHEN MAIN_SBQ.FREQ <= PERCENTILE_SBQ.P40TH_FREQ THEN 'MIDL'
                WHEN MAIN_SBQ.FREQ <= PERCENTILE_SBQ.P60TH_FREQ THEN 'MIDH'
                WHEN MAIN_SBQ.FREQ <= PERCENTILE_SBQ.P80TH_FREQ THEN 'HEAVY'
                ELSE 'SUPER'
            END) || '_' ||
            (CASE WHEN MAIN_SBQ.AOV <= PERCENTILE_SBQ.P25TH_AOV THEN 'LOW'
                WHEN MAIN_SBQ.AOV <= PERCENTILE_SBQ.P75TH_AOV THEN 'AVG'
                ELSE 'HIGH'
            END)                                               AS FULL_SEGMENTO

        FROM 
        (
            SELECT
                USER_ID,
                EMAIL,
                JOINING_DATE,
                FREQ,
                VENTAS,
                RECENCY,
                AOV
            FROM 
            (
                SELECT
                    USERS_TABLE.USER_ID,
                    USERS_TABLE.EMAIL,
                    MIN(USERS_TABLE.REGISTERED_TIMESTAMP)                                                  AS JOINING_DATE,
                    COUNT(DISTINCT TRANSACTION_TABLE.TRANSACTION_ID)                                       AS FREQ,
                    SUM(TRANSACTION_TABLE.CHECK_AMOUNT)                                                    AS VENTAS,
                    CURRENT_DATE - MAX(DATE(TRANSACTION_TABLE.TRANSACTION_DATE))                           AS RECENCY,
                    SUM(TRANSACTION_TABLE.CHECK_AMOUNT)/COUNT(DISTINCT TRANSACTION_TABLE.TRANSACTION_ID)   AS AOV
                FROM
                (
                    SELECT 
                        TRANSACTION_ID,
                        CHECK_AMOUNT,
                        TRANSACTION_DATE
                    FROM 
                        TRANSACTION_HEADERS
                )                                                            AS TRANSACTION_TABLE
                INNER JOIN
                (
                    SELECT DISTINCT
                        TRANSACTION_ID,
                        USER_ID
                    FROM 
                        TRANSACTION_PAYMENTS
                )                                                            AS TRANSACTION_PAYMENTS
                ON 
                    LOWER(TRANSACTION_PAYMENTS.TRANSACTION_ID) = LOWER(TRANSACTION_TABLE.TRANSACTION_ID)
                INNER JOIN
                (
                    SELECT 
                        USER_ID,
                        EMAIL,
                        REGISTERED_TIMESTAMP
                    FROM 
                        USERS
                )                                                            AS USERS_TABLE
                ON 
                    LOWER(USERS_TABLE.USER_ID) = LOWER(TRANSACTION_PAYMENTS.USER_ID)
                GROUP BY 
                    USERS_TABLE.USER_ID,
                    USERS_TABLE.EMAIL
            )                                               AS   USERS_TRANSACTION
            GROUP BY
                USER_ID,
                EMAIL,
                JOINING_DATE,
                FREQ,
                VENTAS,
                RECENCY,
                AOV
        ) MAIN_SBQ
        LEFT OUTER JOIN
        (
            SELECT
                APPROX_PERCENTILE(FREQ, ARRAY[0.20, 0.40, 0.60, 0.80])[1]    AS P20TH_FREQ,
                APPROX_PERCENTILE(FREQ, ARRAY[0.20, 0.40, 0.60, 0.80])[2]    AS P40TH_FREQ,
                APPROX_PERCENTILE(FREQ, ARRAY[0.20, 0.40, 0.60, 0.80])[3]    AS P60TH_FREQ,
                APPROX_PERCENTILE(FREQ, ARRAY[0.20, 0.40, 0.60, 0.80])[4]    AS P80TH_FREQ,
                APPROX_PERCENTILE(AOV, ARRAY[0.25, 0.75])[1]                 AS P25TH_AOV,
                APPROX_PERCENTILE(AOV, ARRAY[0.25, 0.75])[2]                 AS P75TH_AOV
            FROM 
            (
                SELECT 
                    USERS_TABLE.USER_ID,
                    USERS_TABLE.EMAIL,
                    MIN(USERS_TABLE.REGISTERED_TIMESTAMP)                                                  AS JOINING_DATE,
                    COUNT(DISTINCT TRANSACTION_TABLE.TRANSACTION_ID)                                       AS FREQ,
                    SUM(TRANSACTION_TABLE.CHECK_AMOUNT)                                                    AS VENTAS,
                    CURRENT_DATE - MAX(DATE(TRANSACTION_TABLE.TRANSACTION_DATE))                           AS RECENCY,
                    SUM(TRANSACTION_TABLE.CHECK_AMOUNT)/count(DISTINCT TRANSACTION_TABLE.TRANSACTION_ID)   AS AOV
                FROM 
                (
                    SELECT 
                        TRANSACTION_ID,
                        CHECK_AMOUNT,
                        TRANSACTION_DATE
                    FROM 
                        TRANSACTION_HEADERS
                )                                                            AS TRANSACTION_TABLE
                INNER JOIN
                (
                    SELECT DISTINCT
                        TRANSACTION_ID,
                        USER_ID
                    FROM 
                        TRANSACTION_PAYMENTS
                )                                                            AS TRANSACTION_PAYMENTS
                ON 
                    LOWER(TRANSACTION_PAYMENTS.TRANSACTION_ID) = LOWER(TRANSACTION_TABLE.TRANSACTION_ID)
                INNER JOIN
                (
                    SELECT 
                        USER_ID,
                        EMAIL,
                        REGISTERED_TIMESTAMP
                    FROM 
                        USERS
                )                                                            AS USERS_TABLE
                ON 
                    LOWER(USERS_TABLE.USER_ID) = LOWER(TRANSACTION_PAYMENTS.USER_ID)
                GROUP BY 
                    USERS_TABLE.USER_ID,
                    USERS_TABLE.EMAIL
            )    AS   USERS_TRANSACTION
        ) PERCENTILE_SBQ
        ON 
            TRUE
    ) AS Q1
    LEFT JOIN
    (
        SELECT
            USER_ID
        FROM
            USERS
        ORDER BY
            RAND()
        LIMIT
            285398
    ) AS Q2
    ON
        Q1.USER_ID = Q2.USER_ID
) AS SEGMENTACION_TOTAL
ON
    USERS.USER_ID = SEGMENTACION_TOTAL.USER_ID