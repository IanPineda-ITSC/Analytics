from snowflake.connector import connect #type: ignore
from configparser import ConfigParser
from tkinter import ttk
import tkinter as tk

class App(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title('Generar CSV segmentos')
        self.geometry('800x600')

        self.frame_activo:ttk.Frame = Loading()
        self.frame_activo.pack()
    
class Loading(ttk.Frame):
    def __init__(self):
        super().__init__()

        self.rowconfigure(0, weight = 1)
        self.columnconfigure(0, weight = 1)

        label = ttk.Label(master = self, text = 'Descargando datos de snowflake ...')
        label.grid(column = 0, row = 0)

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
        cur.execute("""SELECT DISTINCT ANIO, MES, SEGMENTO, APP_INSTALADA FROM WOW_REWARDS.SEGMENTACIONES_WOW.SEGMENTACIONES;""")

        df = cur.fetch_pandas_all()

        print(df.head())

if __name__ == '__main__':
    app = App()
    app.mainloop()