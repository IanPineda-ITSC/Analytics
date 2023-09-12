from pandas import read_csv #type: ignore
from pandas import read_excel #type: ignore
from pandas import DataFrame
from pandas import concat #type: ignore
from configparser import ConfigParser
from snowflake.connector import connect #type: ignore
from snowflake.connector.pandas_tools import write_pandas

import glob

def read_csv_files_from_dir(dir:str) -> list[DataFrame]:
    df_list:list[DataFrame] = []
    for filename in glob.iglob(dir + '**/*.csv', recursive=True):
        print(filename)
        df = read_csv(filename, encoding = 'unicode_escape')
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

    # df_list = read_csv_files_from_dir('./semana 23/')
    # df_list = read_excel_files_from_dir('./CUPONES 200-600 RECUPERACIÓN CASUAL 2023/')
    df_list = read_csv_files_from_dir('./SEGMENTACIONES_JULIO/')

    df = concat(df_list)

    for index, row in df.iterrows():
        if len(row['EMAIL']) > 64:
            print(row['EMAIL'])
            df.drop(index, inplace = True)
        
        if type(row['FIRST_NAME']) != float and len(row['FIRST_NAME']) > 128:
            print(row['FIRST_NAME'])
            df.drop(index, inplace = True)

        if type(row['LAST_NAME']) != float and len(row['LAST_NAME']) > 128:
            print(row['LAST_NAME'])
            df.drop(index, inplace = True)

    print('Escribiendo a snowflake...')
    print(f'DB: {config.get("SNOWFLAKE", "DATABASE")}')
    print(f'Schema: {config.get("SNOWFLAKE", "SCHEMA")}')
    print(f'Tabla: {config.get("SNOWFLAKE", "TABLE")}')

    print(df.head())

    conn = connect(
        user = config.get('SNOWFLAKE', 'USER'),
        password = config.get('SNOWFLAKE', 'PASSWORD'),
        account = config.get('SNOWFLAKE', 'ACCOUNT'),
        database = config.get('SNOWFLAKE', 'DATABASE'),
        warehouse = config.get('SNOWFLAKE', 'WAREHOUSE'),
        schema = config.get('SNOWFLAKE', 'SCHEMA'),
        role = config.get('SNOWFLAKE', 'ROLE'),
    )

    write_pandas(conn, df, config.get('SNOWFLAKE', 'TABLE'))
    print('Escritura terminada \n')

if __name__ == '__main__':
    main()