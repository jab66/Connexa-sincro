import os

import pandas as pd
import psycopg2 
from psycopg2 import extras


class ProductFunc:
    """
    Clase utilitaria para la sincronizacion de los productos entre Diarco y Connexa.
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

        self.folder_query = f"{os.path.dirname(os.path.abspath(__file__))}{os.sep}qry_product"
        self.folder = f"{os.path.dirname(os.path.abspath(__file__))}{os.sep}"

        self.qry_find_all_m_3_articulos = f"{self.folder_query}{os.sep}find_all_m_3_articulos.sql"
        self.qry_find_fnd_product_by_extcode = f"{self.folder_query}{os.sep}find_fnd_product_by_extcode.sql"
        self.qry_find_all_fnd_product = f"{self.folder_query}{os.sep}find_all_fnd_product.sql"
        self.qry_insert_fnd_product = f"{self.folder_query}{os.sep}insert_fnd_product.sql"
        self.qry_find_all_fnd_category = f"{self.folder_query}{os.sep}find_all_fnd_category.sql"
        self.qry_update_fnd_product = f"{self.folder_query}{os.sep}update_fnd_product.sql"
        self.qry_update_fnd_product_inactive = f"{self.folder_query}{os.sep}update_fnd_product_inactive.sql"



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
        Metodo para obtener los articulos de Diarco

        Return: lista de tuplas
        """ 
        with open(self.qry_find_all_m_3_articulos, 'r') as archivo:
            qry = archivo.read()

        self.cursor_origen.execute(qry)
        return self.cursor_origen.fetchall()


    def find_all_fnd_product(self):
        """
        Metodo para obtener los articulos de Connexa

        Return: lista de tuplas
        """ 
        with open(self.qry_find_all_fnd_product, 'r') as archivo:
            qry = archivo.read()

        self.cursor_destino.execute(qry)
        return self.cursor_destino.fetchall()
    

    def find_all_fnd_category(self):
        """
        Metodo para obtener las categorias de Connexa

        Return: lista de tuplas
        """ 
        with open(self.qry_find_all_fnd_category, 'r') as archivo:
            qry = archivo.read()

        self.cursor_destino.execute(qry)
        return self.cursor_destino.fetchall()
    

    def find_fnd_product_by_extcode(self, extcode):
        """
        Metodo para obtener un producto Connexa a partir del campo ext_code

        Return: una tupla
        """         
        with open(self.qry_find_fnd_product_by_extcode, 'r') as archivo:
            qry = archivo.read()
                
        self.cursor_destino.execute(qry, (extcode,))
        return self.cursor_destino.fetchone()

    def create_df_fnd_product(self):
        """
        Metodo para crear el Dataframe de los productos de Connexa

        Return: dataframe
        """
        # definir las columnas de la consulta
        columns = ['id', 'base_price', 'description', 'ext_code', 'label_value', 'sku', 'timestamp', 'brand_id', 'category_id','currency_id','label_uom_id', 'manufacturer_id', 'sales_uom_id', 'status_id', 'image']
        # obtengo los productos
        lista = self.find_all_fnd_product()
        # crear el dataframe
        df = pd.DataFrame(lista, columns=columns)     
        return df    


    def create_df_m_3_articulos(self):
        """
        Metodo para crear el Dataframe de los productos de Diarco

        Return: dataframe
        """
        # definir las columnas de la consulta
        columns = ['c_articulo', 'n_articulo', 'precio_compra', 'ean', 'f_proc', 'rama']
        # obtengo los productos
        lista = self.find_all_m_3_articulos()
        # crear el dataframe
        df = pd.DataFrame(lista, columns=columns)     
        return df 
    
    
    def create_df_fnd_category(self):
        """
        Metodo para crear el Dataframe de las categorias de Connexa

        Return: dataframe
        """
        # definir las columnas de la consulta
        columns = ['ext_code', 'id']
        # obtengo las categorias
        lista = self.find_all_fnd_category()
        # crear el dataframe
        df = pd.DataFrame(lista, columns=columns)     
        return df   
    

    def insert_product_from_dataframe(self, df):
        """
        Metodo para insertar un dataframe de productos en Connexa

        Return: None
        """         
        with open(self.qry_insert_fnd_product, 'r') as archivo:
            insert = archivo.read()

        # Convertir el DataFrame en una lista de tuplas
        data = list(df.itertuples(index=False, name=None))

        self.cursor_destino.executemany(insert, data)


    def insert_product_from_csv(self, df):
        """
        Metodo para insertar un dataframe de productos en Connexa

        Return: None
        """   
        archivo = f'{self.folder}datos.csv'
        df.to_csv(archivo, index=False, header=False, quotechar="'")

        # df.to_csv(archivo, index=False, header=False, quotechar="'")

        with open(archivo, 'r') as f:
            self.cursor_destino.copy_from(open(archivo, 'r'), 
                                        'fnd_product', 
                                        sep=',', 
                                        columns=('id', 'base_price', 'description', 'ext_code', 'sku', 'timestamp', 'category_id', 'status_id')
                            )


    def create_file_sin_categorizar(self, sin_categorizar):
        """
        Metodo para crear el archivo con los productos que no se puedieron categorizar en Connexa

        Return: None
        """   
        file_name =  f"{self.folder}sin_categorizar.txt"

        with open(file_name, "a") as archivo:
            for nombre in sin_categorizar:
                archivo.write(f"{nombre}\n") 

            

    def update_fnd_product(self, datos):
        """
        Metodo para actualizar los articulos de Diarco

        Return: None
        """ 
        with open(self.qry_update_fnd_product, 'r') as archivo:
            qry = archivo.read()

        extras.execute_batch(self.cursor_destino, qry, datos)


    def update_fnd_product_status(self, datos):
        """
        Metodo para actualizar los estados de los articulos en Connexa

        Return: None
        """ 
        with open(self.qry_update_fnd_product_inactive, 'r') as archivo:
            qry = archivo.read()

        extras.execute_batch(self.cursor_destino, qry, datos)


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

