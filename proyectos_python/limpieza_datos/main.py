import os

from typing import Optional

from configparser import ConfigParser

from pandas import read_excel #type: ignore
from pandas import read_csv #type: ignore
from pandas import concat #type: ignore
from pandas import DataFrame

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

def clean(file:DataFrame, file_name:str) -> DataFrame:
    print('Limpiando archivo...')
    # Eliminamos todos los trailing whitespace
    file = file.applymap(lambda x: x.strip() if isinstance(x, str) else x) #type: ignore

    # Convertimos todo el whitespace restante (que gracias al paso anterior, estamos
    # seguros que es entre cadenas) para las columnas que pueden tener multiples valores
    file['Payments{}.Type']             = file['Payments{}.Type']           .replace(r'\s', '#', regex=True) #type: ignore
    file['Payments{}.CardType']         = file['Payments{}.CardType']       .replace(r'\s', '#', regex=True) #type: ignore
    file['Coupons{}.Code']              = file['Coupons{}.Code']            .replace(r'\s', '#', regex=True) #type: ignore
    file['Payments{}.TransactionID']    = file['Payments{}.TransactionID']  .replace(r'\s', '#', regex=True) #type: ignore
    file['Payments{}.ProviderID']       = file['Payments{}.ProviderID']     .replace(r'\s', '#', regex=True) #type: ignore

    # Para Payment Amount, sumamos todos los valores numericos que aparezcan
    # Si no hay ningun valor numerico, solamente llenamos con zero
    file['Payments{}.Amount'] = file['Payments{}.Amount'].str.split().apply(lambda x: sum(float(i) for i in x) if type(x) == list else 0) #type: ignore

    if 'Phone' not in file.columns:
        file['Phone'] = None
    file['Phone'] = file['Phone'].astype(str) #type: ignore
    
    file['FILE_NAME'] = file_name 

    file = file.rename(columns = {
        'StorePlaceOrderTime': 'STOREPLACEORDERTIME',
        'PlaceOrderTime': 'PLACEORDERTIME',
        'StoreID': 'STOREID',
        'StoreOrderID': 'STOREORDERID',
        'FutureOrderTime': 'FUTUREORDERTIME',
        'CustomerID': 'CUSTOMERID',
        'Phone': 'PHONE',
        'Email': 'EMAIL',
        'FirstName': 'FIRSTNAME',
        'LastName': 'LASTNAME',
        'ServiceMethod': 'SERVICEMETHOD',
        'SourceOrganizationURI': 'SOURCEORGANIZATIONURI',
        'Coupons{}.Code': 'COUPONSCODE',
        'Payments{}.Amount': 'PAYMENTSAMOUNT',
        'Payments{}.Type': 'PAYMENTSTYPE',
        'Payments{}.CardType': 'PAYMENTSCARDTYPE',
        'Payments{}.TransactionID': 'PAYMENTSTRANSACTIONID',
        'Payments{}.ProviderID': 'PAYMENTSPROVIDERID'
    })

    print('Limpieza terminada \n')

    return file


def main() -> None:
    config = ConfigParser()
    config.read('config.ini')

    df_list = read(
        file_type = config.get('FILE', 'TYPE'),
        file_name = config.get('FILE', 'NAME')
    )

    if not df_list:
        return

    df_list = [clean(df, file_name) for df,file_name in df_list]

    df = concat(df_list)
    df.to_csv('output.csv', index = False)

    print('Escribiendo a snowflake...')
    print(f'DB: {config.get("SNOWFLAKE", "DATABASE")}')
    print(f'Schema: {config.get("SNOWFLAKE", "SCHEMA")}')
    print(f'Tabla: {config.get("SNOWFLAKE", "TABLE")}')

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
    print('Build Version: 23/08/18 16:55 \n')
    main()