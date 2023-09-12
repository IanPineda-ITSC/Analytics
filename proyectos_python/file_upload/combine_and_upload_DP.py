from pandas import read_csv #type: ignore
from pandas import read_excel #type: ignore
from pandas import DataFrame
from pandas import concat #type: ignore
from configparser import ConfigParser
from snowflake.connector import connect #type: ignore
from snowflake.connector.pandas_tools import write_pandas
from unidecode import unidecode

import glob

def read_csv_files_from_dir(dir:str) -> list[DataFrame]:
    df_list:list[DataFrame] = []
    for filename in glob.iglob(dir + '**/*.csv', recursive=True):
        print(filename)
        df = read_csv(filename, encoding = 'unicode_escape', dtype={'NAME': str})
        df_list.append(df)
    
    return df_list

def read_excel_files_from_dir(dir:str) -> list[DataFrame]:
    df_list:list[DataFrame] = []
    for filename in glob.iglob(dir + '**/*.xlsx', recursive=True):
        print(filename)
        df = read_excel(filename, header = None, names=['FOLIO', 'NOMBRE_CAMPANIA'])
        df_list.append(df)
    
    return df_list


def main() -> None:
    config = ConfigParser()
    config.read('config.ini')

    df_list = read_csv_files_from_dir('./BASES_DP_W30/')

    df = concat(df_list)

    # df['APP_INSTALADA'] = df['APP_INSTALADA'].apply(lambda x: x == 'APP_INSTALADA')

    # df['NAME'] = df['NAME'].apply(unidecode)

    df.drop(columns=['TIPO', 'TIEMPO_DE_ENTREGA'], inplace=True)

    print('Escribiendo a snowflake...')
    print(f'DB: {config.get("SNOWFLAKE", "DATABASE")}')
    print(f'Schema: {config.get("SNOWFLAKE", "SCHEMA")}')
    print(f'Tabla: {config.get("SNOWFLAKE", "TABLE")}')

    print(df.head())

    conn = connect(
        user = config.get('SNOWFLAKE', 'USER'),
        warehouse = config.get('SNOWFLAKE', 'WAREHOUSE'),
        password = config.get('SNOWFLAKE', 'PASSWORD'),
        account = config.get('SNOWFLAKE', 'ACCOUNT'),
        role = config.get('SNOWFLAKE', 'ROLE'),
        database = 'WOW_REWARDS',
        schema = 'SEGMENTACION_DOMINOS'
    )

    write_pandas(conn, df, 'SEGMENTACION')
    print('Escritura terminada \n')

if __name__ == '__main__':
    main()