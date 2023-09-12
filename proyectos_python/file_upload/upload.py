import os

from typing import Optional

from configparser import ConfigParser

from pandas import read_excel #type: ignore
from pandas import read_csv #type: ignore
from pandas import concat #type: ignore
from pandas import DataFrame
from pandas import to_numeric#type: ignore

from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect #type: ignore
import snowflake.connector.snow_logging #type: ignore


def read(file_type:str, file_name:str) -> Optional[list[tuple[DataFrame, str]]]:
    print('Leyendo archivo...')
    print(f'Archivo a leer: {file_name}')

    file:list[tuple[DataFrame, str]]
    if file_type == 'CSV':
        file = [(read_csv(file_name), file_name)]
    elif file_type == 'XLSX':
        file = [(read_excel(file_name), file_name)]
    elif file_type == 'DIR-CSV':
        file = []
        for filename in os.listdir(file_name):
            file.append((read_csv(os.path.join(file_name, filename)), filename))
    elif file_type == 'DIR-XLSX':
        file = []
        for filename in os.listdir(file_name):
            file.append((read_excel(os.path.join(file_name, filename)), filename))
    else:
        print('Por favor introduce un tipo de archivos que sea csv | xlsx')
        return None

    print('Lectura terminada \n')
    return file

def main() -> None:
    config = ConfigParser()
    config.read('config.ini')

    df = read_csv("w21v2.csv",  encoding = 'unicode_escape')

    df.to_csv('file.csv')

    df['PHONENUMBER'] = to_numeric(df['PHONENUMBER'])

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
    print('Build Version: 23/04/27 01:17 \n')
    main()