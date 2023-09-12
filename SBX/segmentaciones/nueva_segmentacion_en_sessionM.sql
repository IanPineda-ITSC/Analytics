SELECT
    *
FROM
( 
    SELECT
        USER_ID,
        CASE
            WHEN FREQ <= P20TH_FREQ THEN 'LOW'
            WHEN FREQ <= P40TH_FREQ AND AOV <= P75TH_AOV THEN 'LOW'
            WHEN FREQ <= P40TH_FREQ AND AOV >  P75TH_AOV THEN 'MEDIUM'
            WHEN FREQ <= P60TH_FREQ AND AOV <= P25TH_AOV THEN 'LOW'
            WHEN FREQ <= P60TH_FREQ AND AOV >  P25TH_AOV THEN 'MEDIUM'
            WHEN FREQ <= P80TH_FREQ AND AOV <= P75TH_AOV THEN 'MEDIUM'
            WHEN FREQ <= P80TH_FREQ AND AOV >  P75TH_AOV THEN 'HIGH'
            WHEN FREQ >  P80TH_FREQ AND AOV <= P75TH_AOV THEN 'HIGH'
            WHEN FREQ >  P80TH_FREQ AND AOV >  P75TH_AOV THEN 'TOP'
        END AS ULTRASEGMENTO
    FROM
    (
        SELECT
            USER_ID,
            count(DISTINCT TRANSACTION_HEADERS.TRANSACTION_ID) AS FREQ,
            sum(CHECK_AMOUNT) / count(DISTINCT TRANSACTION_HEADERS.TRANSACTION_ID) AS AOV
        FROM
            TRANSACTION_HEADERS
        INNER JOIN
            TRANSACTION_PAYMENTS
        ON
            lower(TRANSACTION_PAYMENTS.TRANSACTION_ID) = lower(TRANSACTION_HEADERS.TRANSACTION_ID)
        WHERE
            to_char(TRANSACTION_HEADERS.TRANSACTION_DATE, 'yyyy-mm-dd') >= '2023-05-29'
        AND
            to_char(TRANSACTION_HEADERS.TRANSACTION_DATE, 'yyyy-mm-dd') <= '2023-08-20'
        GROUP BY
            USER_ID
    ) 
    JOIN
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
                count(DISTINCT TRANSACTION_HEADERS.TRANSACTION_ID) AS FREQ,
                sum(CHECK_AMOUNT) / count(DISTINCT TRANSACTION_HEADERS.TRANSACTION_ID) AS AOV
            FROM
                TRANSACTION_HEADERS
            INNER JOIN
                TRANSACTION_PAYMENTS
            ON
                lower(TRANSACTION_PAYMENTS.TRANSACTION_ID) = lower(TRANSACTION_HEADERS.TRANSACTION_ID)
            WHERE
                to_char(TRANSACTION_HEADERS.TRANSACTION_DATE, 'yyyy-mm-dd') >= '2023-05-29'
            AND
                to_char(TRANSACTION_HEADERS.TRANSACTION_DATE, 'yyyy-mm-dd') <= '2023-08-20'
            GROUP BY
                USER_ID
        )
    ) AS PERCENTILES
    ON
        TRUE
)
WHERE
    ULTRASEGMENTO = 'LOW'
ORDER BY
    rand()
LIMIT
    360000
;

WHERE
    date_format (LAST_UPDATED_AT, '%Y-%m-%d') >= date_format(
    date_add('MONTH', -3, date_add('HOUR', -1, current_timestamp)),
    '%Y-%m-%d'
    )