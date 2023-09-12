# Programa para limpiar datos de GOLO y cargarlos a una tabla en snowflake

## Crear ejecutable

Para compilar el programa, ejecutar

```pyinstaller --onefile main.py```

## Ejecutar 

Para ejecutar el programa, primero es necesario tener un archivo llamado

```config.ini```


El cual debe tener los siguientes valores

```
[FILE]
TYPE =
NAME =
[SNOWFLAKE]
USER =
PASSWORD =
ROLE =
WAREHOUSE =
ACCOUNT =
DATABASE =
SCHEMA =
TABLE =
```

Donde:

TYPE es el tipo de archivo a procesar, puede ser cualquiera de las siguientes opciones
```
CSV (Archivo CSV)
XLSX (Archivo de excel)
DIR-CSV (Directorio con archivos CSV)
DIR-XLSX (Directorio con archivos de excel)
```

NAME es el path del archivo o directorio a procesar

Y los demas son los datos de acceso a snowflake

Ya teniendo esto, simplemente hay que ejecutar el archivo ejecutable, o el script de python con un interprete, y teniendo el archivo config en el mismo directorio