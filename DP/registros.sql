    WITH

    SEMANA_ACTUAL AS (
        SELECT
            count(DISTINCT EMAIL) AS SEMANA_ACTUAL
        FROM
            SEGMENT_EVENTS.DOMINOS_UNIFIED.SIGNUP_SUCCESS
        INNER JOIN
            WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
        ON
            to_date(SENT_AT)
        WHERE
            ANIO_ALSEA = {ANIO_ACTUAL}
        AND
            SEM_ALSEA = {SEMANA_ACTUAL}
    ),

    SEMANA_PREVIA AS (
        SELECT
            count(DISTINCT EMAIL) AS SEMANA_PREVIA
        FROM
            SEGMENT_EVENTS.DOMINOS_UNIFIED.SIGNUP_SUCCESS
        INNER JOIN
            WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
        ON
            to_date(SENT_AT) = FECHA
        WHERE
            ANIO_ALSEA = {ANIO_SEMANA_PREVIA}
        AND
            SEM_ALSEA = {SEMANA_PREVIA}
    ),

    SEMANAL AS (
        SELECT
            *,
            (SEMANA_ACTUAL - SEMANA_PREVIA) / SEMANA_PREVIA AS PERCENT_V_SEMANA_PREVIA
        FROM
            SEMANA_ACTUAL
        JOIN
            SEMANA_PREVIA
    ),

    MES_ACTUAL AS (
        SELECT
            count(DISTINCT EMAIL) AS MES_ACTUAL
        FROM
            SEGMENT_EVENTS.DOMINOS_UNIFIED.SIGNUP_SUCCESS
        INNER JOIN
            WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
        ON
            to_date(SENT_AT) = FECHA
        WHERE
            ANIO_ALSEA = {ANIO_ACTUAL}
        AND
            MES_ALSEA = {MES_ACTUAL}
        AND
            SEM_ALSEA <= {SEMANA_ACTUAL}
    ),

    MES_PREVIO AS (
        SELECT
            count(DISTINCT EMAIL) AS MES_PREVIO
        FROM
            SEGMENT_EVENTS.DOMINOS_UNIFIED.SIGNUP_SUCCESS
        INNER JOIN
            WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
        ON
            to_date(SENT_AT) = FECHA
        WHERE
            ANIO_ALSEA = {ANIO_MES_PREVIO}
        AND
            MES_ALSEA = {MES_PREVIO}
    ),

    MENSUAL AS (
        SELECT
            *,
            (MES_ACTUAL - MES_PREVIO) / MES_PREVIO AS PERCENT_V_MES_PREVIO
        FROM
            MES_ACTUAL
        JOIN
            MES_PREVIO
    ),

    ANIO_ACTUAL AS (
        SELECT
            count(DISTINCT EMAIL) AS ANIO_ACTUAL
        FROM
           SEGMENT_EVENTS.DOMINOS_UNIFIED.SIGNUP_SUCCESS
        INNER JOIN
            WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
        ON
            to_date(SENT_AT) = FECHA
        WHERE
            ANIO_ALSEA = {ANIO_ACTUAL}
        AND
            SEM_ALSEA <= {SEMANA_ACTUAL}
    ),

    ANIO_PREVIO AS (
        SELECT
            count(DISTINCT EMAIL) AS ANIO_PREVIO
        FROM
           SEGMENT_EVENTS.DOMINOS_UNIFIED.SIGNUP_SUCCESS 
        INNER JOIN
            WOW_REWARDS.WORK_SPACE_WOW_REWARDS.DS_DIM_TIME
        ON
            to_date(SENT_AT) = FECHA
        WHERE
            ANIO_ALSEA = {ANIO_PREVIO}
        AND
            SEM_ALSEA <= {SEMANA_ACTUAL}
    ),

    ANUAL AS (
        SELECT
            *,
            (ANIO_ACTUAL - ANIO_PREVIO) / ANIO_PREVIO AS PERCENT_V_ANIO_PREVIO
        FROM
            ANIO_ACTUAL
        JOIN
            ANIO_PREVIO
    )

    SELECT
        *
    FROM
        SEMANAL
    JOIN
        MENSUAL
    JOIN
        ANUAL
    ;