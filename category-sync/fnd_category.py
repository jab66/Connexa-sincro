from datetime import datetime
import psycopg2
import pandas as pd
import os

class CategoryFunc:
    """
    Clase utilitaria para la sincronizacion de las categorias de productos entre Diarco y Connexa.
    """

    def __init__(self):

        print("Se inicializa la clase")

        self.db_origen = "dbname='diarco_data' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"
        self.db_destino = "dbname='connexa_platform' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"
        self.conn_origen = ""
        self.cursor_origen = ""
        self.conn_destino = ""
        self.cursor_origen = ""
        
        self.connect_db_origen()
        self.connect_db_destino()

        self.folder_query = f"{os.path.dirname(os.path.abspath(__file__))}{os.sep}qry_category"

        self.qry_find_all_t114_rubros = f"{self.folder_query}{os.sep}find_all_t114_rubros.sql"
        self.qry_find_all_fnd_category = f"{self.folder_query}{os.sep}find_all_fnd_category.sql"
        self.qry_find_category_by_extcode = f"{self.folder_query}{os.sep}find_category_by_extcode.sql"
        self.qry_insert_fnd_category = f"{self.folder_query}{os.sep}insert_fnd_category.sql"
        self.qry_update_fnd_category = f"{self.folder_query}{os.sep}update_fnd_category.sql"
        self.qry_delete_fnd_category = f"{self.folder_query}{os.sep}delete_fnd_category.sql"
        self.qry_update_fnd_product = f"{self.folder_query}{os.sep}update_fnd_product.sql"
        self.qry_find_fnd_category_join_fnd_product = f"{self.folder_query}{os.sep}find_fnd_category_join_fnd_product.sql"
        self.qry_find_all_fnd_product = f"{self.folder_query}{os.sep}find_all_fnd_product.sql"
        self.qry_find_branch_by_extcode = f"{self.folder_query}{os.sep}find_branch_by_extcode.sql"


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

   
    def find_all_t114_rubros(self):
        """
        Metodo para obtener el arbol de categorias de Diarco

        Return: lista de tuplas
        """ 
        with open(self.qry_find_all_t114_rubros, 'r') as archivo:
            qry = archivo.read()

        self.cursor_origen.execute(qry)
        return self.cursor_origen.fetchall()

    
    def find_all_fnd_category(self):
        """
        Metodo para obtener el arbol de categorias de Connexa

        Return: lista de tuplas
        """ 
        with open(self.qry_find_all_fnd_category, 'r') as archivo:
            qry = archivo.read()

        self.cursor_destino.execute(qry)
        return self.cursor_destino.fetchall()


    def find_branch_by_extcode(self, rama):
        """
        Metodo para obtener una rama del arbol de categorias de Connexa

        Return: lista de tuplas
        """ 
        with open(self.qry_find_branch_by_extcode, 'r') as archivo:
            qry = archivo.read()

        self.cursor_destino.execute(qry, (rama + '%', ))
        return self.cursor_destino.fetchall()


    def find_category_by_extcode(self, extcode):
        """
        Metodo para obtener una categoria de Connexa a partir del campo ext_code

        Return: una tupla
        """         
        with open(self.qry_find_category_by_extcode, 'r') as archivo:
            qry = archivo.read()
                
        if extcode is None:
            qry = qry.replace('-- CONDICION', "ext_code is null")
        else:
            qry = qry.replace('-- CONDICION', "ext_code = %s")

        self.cursor_destino.execute(qry, (extcode,))
        return self.cursor_destino.fetchone()


    def insert_category(self, id, ext_code, name, parent_id):
        """
        Metodo para insertar una categoria en Connexa

        Return: None
        """         
        with open(self.qry_insert_fnd_category, 'r') as archivo:
            insert = archivo.read()

        self.cursor_destino.execute(insert, (id, ext_code, name, datetime.now(), parent_id))


    def update_category(self, name, ext_code):
        """
        Metodo para actualizar una categoria en Connexa

        Return: None
        """         
        with open(self.qry_update_fnd_category, 'r') as archivo:
            act = archivo.read()

        self.cursor_destino.execute(act, (name, ext_code))


    def delete_category(self, id):
        """
        Metodo para eliminar una categoria en Connexa

        Return: None
        """         
        with open(self.qry_delete_fnd_category, 'r') as archivo:
            delete = archivo.read()

        self.cursor_destino.execute(delete, (id, )
                                    )


    def get_parent_extcode(self, extcode):
        """
        Retorna el id del padre a partir de un ext_code

        Return: string
        """
        retorno = ""
        indice_guion = extcode.rfind('-')
        if indice_guion != -1:
            retorno = extcode[:indice_guion]
        longitud = len(retorno)
        if longitud == 0:
            return None
        else:
            return retorno
    

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


    def commit(self):
        """
        Metodo para registrar los cambios en la base de datos destino

        Return: None
        """
        self.conn_destino.commit()    
        

    def create_df_t114_rubros (self):
        """
        Metodo para crear el Dataframe de las categorias de Diarco

        Return: dataframe
        """
        # definir las columnas de la consulta
        columns = ['c_rubro', 'c_rubro_padre', 'nombre_categoria', 'ruta', 'id_unico', 'a', 'b', 'c', 'd']
        # obtengo las categorias
        lista = self.find_all_t114_rubros()
        # crear el dataframe
        df = pd.DataFrame(lista, columns=columns)
        # eliminar las columnas que no se van a usar
        df.drop(['a', 'b', 'c', 'd', 'ruta'], axis=1, inplace=True)
        return df
    

    def create_df_fnd_category(self):
        """
        Metodo para crear el Dataframe de las categorias de Connexa

        Return: dataframe
        """
        # definir las columnas de la consulta
        columns = ['id', 'ext_code', 'name', 'timestamp', 'parent_id']
        # obtengo las categorias
        lista = self.find_all_fnd_category()
        # crear el dataframe
        df = pd.DataFrame(lista, columns=columns)
        # eliminar las columnas que no se van a usar
        df.drop(['timestamp'], axis=1, inplace=True)       
        return df
     

    def create_df_fnd_product(self):
        """
        Metodo para crear el Dataframe de productos de Connexa

        Return: dataframe
        """
        # definir las columnas de la consulta
        columns = ['id', 'description', 'ext_code', 'category_id']
        # obtengo los productos
        lista_productos = self.find_all_fnd_product()
        # crear el dataframe
        df = pd.DataFrame(lista_productos, columns=columns)   
        return df


    def find_all_fnd_product(self):
        """
        Metodo para obtener los productos de Connexa

        Return: lista de tuplas
        """ 
        with open(self.qry_find_all_fnd_product, 'r') as archivo:
            qry_productos = archivo.read()

        self.cursor_destino.execute(qry_productos)
        return self.cursor_destino.fetchall()      
    

    def update_fnd_product(self, id):
        """
        Metodo para actualizar en NULL la categoria de un producto en Connexa

        Return: None
        """         
        with open(self.qry_update_fnd_product, 'r') as archivo:
            upd = archivo.read()

        self.cursor_destino.execute(upd, (id, ))


    def find_fnd_category_join_fnd_product(self, idCategoria):
        """
        Metodo para obtener los productos relacionados a una categoria en Connexa

        Return: lista de tuplas
        """ 
        with open(self.qry_find_fnd_category_join_fnd_product, 'r') as archivo:
            qry = archivo.read()

        self.cursor_destino.execute(qry, (idCategoria,))
        return self.cursor_destino.fetchall()  
    
