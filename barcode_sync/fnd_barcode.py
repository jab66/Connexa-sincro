import os
import pandas as pd
import psycopg2 
from psycopg2 import extras


class BarcodeFunc:
    """
    Clase utilitaria para la sincronizacion de los barcodes entre Diarco y Connexa.
    """


    def __init__(self):

        self.db_origen = "dbname='diarco_data' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"
        self.db_destino = "dbname='connexa_platform' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"
        self.conn_origen = ""
        self.cursor_origen = ""
        self.conn_destino = ""
        self.cursor_origen = ""
        
        self.connect_db_origen()
        self.connect_db_destino()

        self.folder = f"{os.path.dirname(os.path.abspath(__file__))}{os.sep}"

        self.folder_query = f"{os.path.dirname(os.path.abspath(__file__))}{os.sep}qry_barcode"

        self.qry_find_all_m_3_articulos = f"{self.folder_query}{os.sep}find_all_m_3_articulos.sql"
        self.qry_find_all_fnd_product = f"{self.folder_query}{os.sep}find_all_fnd_product.sql"
        self.qry_insert_fnd_barcode = f"{self.folder_query}{os.sep}insert_fnd_barcode.sql"
        self.qry_find_all_fnd_barcode = f"{self.folder_query}{os.sep}find_all_fnd_barcode.sql"


    def connect_db_origen(self):
        """
        Metodo para conectarse a la base de datos origen

        Return: cursor
        """         
        self.conn_origen = psycopg2.connect(self.db_origen)
        self.cursor_origen = self.conn_origen.cursor()
        return self.cursor_origen


    def connect_db_destino(self):
        """
        Metodo para conectarse a la base de datos destino

        Return: cursor
        """      
        self.conn_destino = psycopg2.connect(self.db_destino)
        self.cursor_destino = self.conn_destino.cursor()
        return self.cursor_destino
    

    def find_all_m_3_articulos(self):
        """
        Metodo para obtener los productos de Diarco para obtener los EAN

        Return: lista de tuplas
        """ 
        with open(self.qry_find_all_m_3_articulos, 'r') as archivo:
            qry = archivo.read()

        self.cursor_origen.execute(qry)
        return self.cursor_origen.fetchall()
    

    def find_all_fnd_barcode(self):
        """
        Metodo para obtener los barcode de Connexa

        Return: lista de tuplas
        """ 
        with open(self.qry_find_all_fnd_barcode, 'r') as archivo:
            qry = archivo.read()

        self.cursor_destino.execute(qry)
        return self.cursor_destino.fetchall()
    

    def commit(self):
        """
        Metodo para registrar los cambios en la base de datos destino

        Return: None
        """
        self.conn_destino.commit()  


    def close_connections (self):
        """
        Metodo para cerrar las conexiones y cursores abiertos de origen y destino

        Return: None
        """
        if (self.cursor_origen.closed==0):
            self.cursor_origen.close()
        if (self.cursor_destino.closed==0):
            self.cursor_destino.close()
        if (self.conn_origen.closed==0):
            self.conn_origen.close()
        if (self.conn_destino.closed==0):
            self.conn_destino.close()

    def create_df_fnd_product(self):
        """
        Metodo para crear el Dataframe de los productos de Connexa

        Return: dataframe
        """
        # definir las columnas de la consulta
        columns = ['id', 'ext_code']
        # obtengo los productos
        lista = self.find_all_fnd_product()
        # crear el dataframe
        df = pd.DataFrame(lista, columns=columns)     
        return df 
    
    def find_all_fnd_product(self):
        """
        Metodo para obtener los articulos de Connexa

        Return: lista de tuplas
        """ 
        with open(self.qry_find_all_fnd_product, 'r') as archivo:
            qry = archivo.read()

        self.cursor_destino.execute(qry)
        return self.cursor_destino.fetchall()


    def create_df_m_3_articulos(self):
        """
        Metodo para crear el Dataframe de los productos de Diarco

        Return: dataframe
        """
        # definir las columnas de la consulta
        columns = ['c_articulo', 'n_articulo', 'ean', 'ean1', 'ean2', 'ean3', 'ean4', 'dun14']
        # obtengo los productos
        lista = self.find_all_m_3_articulos()
        # crear el dataframe
        df = pd.DataFrame(lista, columns=columns)     
        return df 
    

    def create_df_fnd_barcode(self):
        """
        Metodo para crear el Dataframe de los barcode de Connexa

        Return: dataframe
        """
        # definir las columnas de la consulta
        columns = ['barcode']
        # obtengo los productos
        lista = self.find_all_fnd_barcode()
        # crear el dataframe
        df = pd.DataFrame(lista, columns=columns)     
        return df 
    

    def insert_barcode_from_csv(self, df):
        """
        Metodo para insertar un dataframe de barcodes en Connexa

        Return: None
        """   
        archivo = f'{self.folder}datos.csv'
        df.to_csv(archivo, index=False, header=False, quotechar="'")

        with open(archivo, 'r') as f:
            self.cursor_destino.copy_from(open(archivo, 'r'), 
                                        'fnd_barcode', 
                                        sep=',', 
                                        columns=('id', 'barcode', 'timestamp', 'product_id', 
                                                 'quantity', 'uom_id', 'barcode_type_id')
                            )
            

    def insert_barcode_from_dataframe(self, df):
        """
        Metodo para insertar un dataframe de barcodes en Connexa

        Return: None
        """         
        with open(self.qry_insert_fnd_barcode, 'r') as archivo:
            insert = archivo.read()

        # Convertir el DataFrame en una lista de tuplas
        data = list(df.itertuples(index=False, name=None))

        self.cursor_destino.executemany(insert, data)


    def control_code_calculator(self, ean):
        char_digits = list(ean[:-1])  # Tomamos todos los caracteres excepto el último
        ean_multiplicador = [0, 0]

        if len(ean) == 8:
            ean_multiplicador[0] = 3
            ean_multiplicador[1] = 1
        elif len(ean) == 12:
            ean_multiplicador[0] = 3
            ean_multiplicador[1] = 1
            char_digits.reverse()  # Invertir los caracteres si es EAN-12
        elif len(ean) == 13:
            ean_multiplicador[0] = 1
            ean_multiplicador[1] = 3
        elif len(ean) == 14:
            ean_multiplicador[0] = 3
            ean_multiplicador[1] = 1
            char_digits.reverse()  # Invertir los caracteres si es EAN-14
        else:
            return False

        total_sum = 0
        for i, digit in enumerate(char_digits):
            if digit.isdigit():
                total_sum += int(digit) * ean_multiplicador[i % 2]
            else:
                return False

        checksum = 10 - total_sum % 10
        if checksum == 10:
            checksum = 0

        if checksum == int(ean[-1]):  # Compara el checksum con el último dígito
            return True
        else:
            return False

