import os
import pandas as pd
import psycopg2


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

        self.qry_find_all_fnd_product = f"{self.folder_query}{os.sep}find_all_fnd_product.sql"
        self.qry_find_all_articulos_valkimia = f"{self.folder_query}{os.sep}find_all_articulos_valkimia.sql"
        self.qry_delete_fnd_barcode = f"{self.folder_query}{os.sep}delete_fnd_barcode.sql"


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

    def create_df_fnd_product(self):
        """
        Metodo para crear el Dataframe de los productos de Connexa

        Return: dataframe
        """
        # definir las columnas de la consulta
        columns = ['id', 'ext_code', 'description']
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


    def create_df_articulos_valkimia(self):
        """
        Metodo para crear el Dataframe de los articulos de valkimia

        Return: dataframe
        """
        # definir las columnas de la consulta
        columns = ['codigo_articulo','unidad_medida','barcode','longitud_barcode',
                   'unidades_x_bulto', 'id_barcode', 'id_um']
        # obtengo los productos
        lista = self.find_all_articulos_valkimia()
        # crear el dataframe
        df = pd.DataFrame(lista, columns=columns)     
        return df 
    
    def find_all_articulos_valkimia(self):
        """
        Metodo para obtener los articulos de valkimia

        Return: lista de tuplas
        """ 
        with open(self.qry_find_all_articulos_valkimia, 'r') as archivo:
            qry = archivo.read()

        self.cursor_origen.execute(qry)
        return self.cursor_origen.fetchall()
    

    def insert_barcode_from_csv(self, df):
        """
        Metodo para insertar un dataframe de barcodes en Connexa

        Return: None
        """  
        carpeta_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "barcode_sync", "csv")
        os.makedirs(carpeta_csv, exist_ok=True)
        ruta_archivo = os.path.join(carpeta_csv, "insert_barcode.csv")

        df.to_csv(ruta_archivo, 
                  columns = ['uuid_barcode', 'barcode', 'descripcion_ean',
                             'timestamp', 'id_articulo_connexa', 'unidades_x_bulto',
                             'id_um', 'id_barcode'],
                  index=False, header=False, quotechar="'")

        with open(ruta_archivo, 'r') as f:
            self.cursor_destino.copy_from(open(ruta_archivo, 'r'), 
                                        'fnd_barcode', 
                                        sep=',', 
                                        columns=('id', 'barcode', 'description', 'timestamp', 'product_id', 
                                                 'quantity', 'uom_id', 'barcode_type_id')
                            )



    def delete_fnd_barcode(self):
        """
        Metodo para eliminar la tabla barcode en Connexa

        Return: None
        """    

        with open(self.qry_delete_fnd_barcode, 'r') as archivo:
            delete = archivo.read()

        self.cursor_destino.execute(delete)


    def dataFrameToCsv(self, dataframe, nombre_archivo):
        """
        Genera un archivo csv con la informacion enviada en el dataframe.
        """
        carpeta_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "barcode_sync", "csv")
        os.makedirs(carpeta_csv, exist_ok=True)
        ruta_archivo = os.path.join(carpeta_csv, nombre_archivo)

        dataframe.to_csv(f'{ruta_archivo}.csv', index=False)


    def control_code_calculator(self, ean: str) -> bool:

        # validar si es null
        ean = ean if ean is not None else "ean123"

        # Obtener los dígitos, excluyendo el último
        char_digits = list(ean[:-1])  # Similar a toCharArray()
        
        # Definir el multiplicador
        ean_multiplicador = [0, 0]

        # Validar el tamaño y asignar multiplicadores
        if len(ean) == 8:
            ean_multiplicador[0] = 3
            ean_multiplicador[1] = 1
        elif len(ean) == 12:
            ean_multiplicador[0] = 3
            ean_multiplicador[1] = 1
            char_digits.reverse()  # Invertir los dígitos
        elif len(ean) == 13:
            ean_multiplicador[0] = 1
            ean_multiplicador[1] = 3
        elif len(ean) == 14:
            ean_multiplicador[0] = 3
            ean_multiplicador[1] = 1
            char_digits.reverse()  # Invertir los dígitos
        else:
            return False
        
        # Calcular la suma de los productos
        suma = 0
        for i, char in enumerate(char_digits):
            if char.isdigit():
                suma += int(char) * ean_multiplicador[i % 2]
            else:
                return False
        
        # Calcular el dígito de verificación (checksum)
        checksum = 10 - (suma % 10)
        if checksum == 10:
            checksum = 0
        
        # Comparar con el último dígito del EAN
        if checksum == int(ean[-1]):  # Comparamos con el último dígito
            return True
        else:
            return False
        
    def descripcionEAN (self, descricpion, quantity, longitud):

        nombre = descricpion

        if longitud == 14 and quantity > 1:
            nombre = f"CAJA * {quantity} - {descricpion}"
        
        if longitud == 13 and quantity > 1:
            nombre = f"PACK * {quantity} - {descricpion}"

        return nombre
    

