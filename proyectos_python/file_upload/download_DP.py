import os

from typing import Optional

from configparser import ConfigParser

from pandas import read_excel #type: ignore
from pandas import read_csv #type: ignore
from pandas import concat #type: ignore
from pandas import DataFrame
from pandas import to_numeric#type: ignore

from snowflake.connector import connect #type: ignore
import snowflake.connector.snow_logging #type: ignore

def bool_to_app_instalada(input: bool) -> str:
    return 'PUSH' if input else 'EMAIL'

def main() -> None:
    config = ConfigParser()
    config.read('config.ini')

    conn = connect(
        user = config.get('SNOWFLAKE', 'USER'),
        password = config.get('SNOWFLAKE', 'PASSWORD'),
        account = config.get('SNOWFLAKE', 'ACCOUNT'),
        database = config.get('SNOWFLAKE', 'DATABASE'),
        warehouse = config.get('SNOWFLAKE', 'WAREHOUSE'),
        schema = config.get('SNOWFLAKE', 'SCHEMA'),
        role = config.get('SNOWFLAKE', 'ROLE'),
    )

    cur = conn.cursor()
    cur.execute("""
        SELECT
            *
        FROM
            WOW_REWARDS.SEGMENTACION_DOMINOS.SEGMENTACION_JULIO
        WHERE
            SEMANA = 37
        AND
        (
            PHONENUMBER IS NOT null
            AND
            APP_INSTALADA
        )
        AND
            NAME IS NOT null
        ORDER BY
            PHONENUMBER
        -- PUSH + EMAIL (Personalizable)
        ;
    """)

    df = cur.fetch_pandas_all()

    df['TIPO'] = 'PUSH+EMAIL(PERSONALIZABLE)'

    print(df.head())

    df_list:list[tuple[tuple[str, str, str], DataFrame]] = [tup for tup in df.groupby(['GRUPO', 'SEGMENTO', 'TIPO'])]

    for item in df_list:
        name =  'Segmentacion_DOMINOSMANIA_W37' + '_' + item[0][0] + '_' + item[0][1] + '_' + item[0][2] + '.csv'

        dir = './' + item[0][0]
        if not os.path.exists(dir):
            os.mkdir(dir)

        dir = './' + item[0][0] + '/' + item[0][2]
        if not os.path.exists(dir):
            os.mkdir(dir)

        item[1].to_csv(dir + '/' + name, index=False)

    cur = conn.cursor()
    cur.execute("""
        SELECT
            *
        FROM
            WOW_REWARDS.SEGMENTACION_DOMINOS.SEGMENTACION_JULIO
        WHERE
            SEMANA = 37
        AND
        (
            PHONENUMBER IS null
            OR
            NOT APP_INSTALADA
        )
        AND
            NAME IS NOT null
        ;
    """)

    df = cur.fetch_pandas_all()

    df['TIPO'] = 'SOLO_EMAIL(PERSONALIZABLE)'

    print(df.head())

    df_list:list[tuple[tuple[str, str, str], DataFrame]] = [tup for tup in df.groupby(['GRUPO', 'SEGMENTO', 'TIPO'])]

    for item in df_list:
        name =  'Segmentacion_DOMINOSMANIA_W37' + '_' + item[0][0] + '_' + item[0][1] + '_' + item[0][2] + '.csv'

        dir = './' + item[0][0]
        if not os.path.exists(dir):
            os.mkdir(dir)

        dir = './' + item[0][0] + '/' + item[0][2]
        if not os.path.exists(dir):
            os.mkdir(dir)

        item[1].to_csv(dir + '/' + name, index=False)

        cur = conn.cursor()
    
    
    cur.execute("""
        SELECT
            *
        FROM
            WOW_REWARDS.SEGMENTACION_DOMINOS.SEGMENTACION_JULIO
        WHERE
            SEMANA = 37
        AND
            NAME IS null
        -- SOLO EMAIL (No personalizable)
        ;
    """)

    df = cur.fetch_pandas_all()

    df['TIPO'] = 'SOLO_EMAIL(NO_PERSONALIZABLE)'

    print(df.head())

    df_list:list[tuple[tuple[str, str, str], DataFrame]] = [tup for tup in df.groupby(['GRUPO', 'SEGMENTO', 'TIPO'])]

    for item in df_list:
        name =  'Segmentacion_DOMINOSMANIA_W37' + '_' + item[0][0] + '_' + item[0][1] + '_' + item[0][2] + '.csv'

        dir = './' + item[0][0]
        if not os.path.exists(dir):
            os.mkdir(dir)

        dir = './' + item[0][0] + '/' + item[0][2]
        if not os.path.exists(dir):
            os.mkdir(dir)

        item[1].to_csv(dir + '/' + name, index=False)

if __name__ == '__main__':
    print('Build Version: 23/07/20 12:01 \n')
    main()