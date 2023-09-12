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
    return 'APP_INSTALADA' if input else 'APP_NO_INSTALADA'

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
    cur.execute("""SELECT * FROM WOW_REWARDS.SEGMENTACION_DOMINOS.SEGMENTACION WHERE PROMO = 'PROMO_SEMANA_28';""")

    df = cur.fetch_pandas_all()

    df['APP_INSTALADA'] = df['APP_INSTALADA'].apply(bool_to_app_instalada)

    print(df.head())

    # df_list:list[tuple[tuple[str, str], DataFrame]] = [tup for tup in df.groupby(['GRUPO', 'SEGMENTO', 'APP_INSTALADA'])]
    df_list:list[tuple[tuple[str, str], DataFrame]] = [tup for tup in df.groupby(['GRUPO', 'MARCA', 'APP_INSTALADA'])]
    # df_list:list[tuple[tuple[str, str], DataFrame]] = [tup for tup in df.groupby(['GRUPO', 'SEGMENTO'])]
    # df_list:list[tuple[tuple[str, str], DataFrame]] = [tup for tup in df.groupby(['GRUPO', 'MARCA'])]

    for item in df_list:
        app_instalada = 'APP_INSTALADA' if item[0][2] else 'APP_NO_INSTALADA'
        # name =  'Segmentacion_dominosmania_' + item[0][0] + '_' + item[0][1] + '_' + app_instalada + '.csv'
        name =  'Segmentacion_' + item[0][1] + '_' + item[0][0] + '_' + app_instalada + '.csv'
        # name =  'Segmentacion_W26' + '_' + item[0][0] + '_' + item[0][1] +  '.csv'
        # name =  'SEGMENTACION_RECUPERACION_' + item[0][0] + '_' + item[0][1] + '.csv'

        # dir = './' + app_instalada
        # if not os.path.exists(dir):
        #     os.mkdir(dir)

        # dir = './' + app_instalada + '/' + item[0][0]
        # if not os.path.exists(dir):
        #     os.mkdir(dir)

        dir = './' + item[0][1]
        if not os.path.exists(dir):
            os.mkdir(dir)

        # dir = './' + item[0][1] + '/' + item[0][0]
        # if not os.path.exists(dir):
        #     os.mkdir(dir)

        item[1].to_csv(dir + '/' + name, index=False)

if __name__ == '__main__':
    print('Build Version: 23/04/27 01:17 \n')
    main()