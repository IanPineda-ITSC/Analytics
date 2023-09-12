SELECT
    ANIO_ALSEA,
    MES_ALSEA,
    (sum(CHECK_AMOUNT) / 1.16)/ count(DISTINCT EMAIL) AS CLV
FROM
    SEGMENT_EVENTS.SESSIONM_SBX.FACT_TRANSACTIONS
INNER JOIN
    WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
ON
    to_date(CREATED_AT) = FECHA
WHERE
    ANIO_ALSEA >= 2022
GROUP BY
    ANIO_ALSEA,
    MES_ALSEA
ORDER BY
    ANIO_ALSEA DESC,
    MES_ALSEA DESC
;

-- CLV SBX      2023/08 Mensual  $ 516.98
-- CLV SBX      2023/08 90 dias  $ 1,161.01 

-- CLV DP       2023/08 Mensual  $ 347.51
-- CLV DP       2023/08 180 dias $ 670.31

-- CLV Vips     2023/08 Mensual  $ 433.39
-- CLV Vips     2023/08 180 dias $ 1,248.49

-- CLV Casuales 2023/08 Mensual  $ 1131.87
-- CLV Casuales 2023/08 180 dias $ 2,330.57

-- CLV SBX      2022/08 Mensual  $ 467.44
-- CLV SBX      2022/08 90 dias  $ 1017.66

-- CLV DP       2022/08 Mensual  $ 342.63
-- CLV DP       2022/08 180 dias $ 649.06

-- CLV Vips     2022/08 Mensual  $ 444.94
-- CLV Vips     2022/08 180 dias $ 1,150.80

-- CLV Casuales 2022/08 Mensual  $ 1,048.06
-- CLV Casuales 2022/08 180 dias $ 1,777.63