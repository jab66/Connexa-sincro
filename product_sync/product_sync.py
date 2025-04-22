import pandas as pd
import uuid
from util.logger import Logger

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from product_sync.fnd_product import ProductFunc

# Función para sincronizar los datos
def run_product_sync():
    """
    Funcion para sincronizar productos
    Se recuperan los productos de Diarco y se actualiza en Connexa.
    Tabla de productos de Diarco: M_3_ARTICULOS
    Tabla de productos de Connexa: FND_PRODUCT
    """
    logger = Logger(
        log_dir="product_sync/log",  
        log_filename="product.log"
    )

    product_func = ProductFunc()

    # obtener los productos de Diarco
    productos_diarco = product_func.find_all_m_3_articulos()
    logger.info(f"Total de Productos recuperados en Diarco: {len(productos_diarco)}")   

    # obtener el dataframe de los productos de Connexa
    df_destino = product_func.create_df_fnd_product() 

    # obtener el dataframe de las categorias de Connexa
    df_categorias = product_func.create_df_fnd_category() 

    # Lista vacía para almacenar las filas
    filas=[]
    filas_upd=[]
    no_categorizados = 0
    sin_categorizar = []

    for articulo in productos_diarco:
        c_articulo, n_articulo, precio_compra, ean, f_proc, rama, m_baja = articulo

        # Generar un UUID para el producto si es necesario
        product_id = str(uuid.uuid4())
            
        # Definir el SKU y el código externo
        sku = f"{c_articulo}"
        ext_code = f"{c_articulo}"
            
        # Definir estado de los productos 
        estado = 1 if m_baja == 'N' else 2

        # buscar el id de la categoria del producto
        reg = df_categorias.query(f'ext_code == "{rama}"') 
        if reg.empty:
            no_categorizados += 1
            sin_categorizar.append(f"Articulo: {c_articulo}, Rama: {rama}, Descripcion: {n_articulo}")
            #continue

        try:
            if (reg.size != 0):
                id_value = reg.iloc[0]['id']
            else:
                id_value == None
        except Exception as e:
            Logger.error(reg, c_articulo, reg.size, e)
            raise RuntimeError(e)
        
        # buscar el producto de diarco en connexa
        reg = df_destino.query(f'ext_code == "{c_articulo}"') 

        nueva_fila = {
            "id": product_id,
            "base_price":precio_compra, 
            "description":n_articulo, 
            "ext_code":ext_code, 
            "sku": sku,
            "timestamp": f_proc,
            "categoy_id": id_value,
            "status_id": estado
                }

        if reg.empty:
            # agregar el producto a la lista para insertar
            filas.append(nueva_fila)
        else:
            # agregar el producto a la lista para modificar
            filas_upd.append(nueva_fila)

    logger.info(f"Cantidad de productos a insertar: {len(filas)}")
    if filas:
        # Crear el DataFrame con todas las filas a insertar
        df_new = pd.DataFrame(filas)
        # FORMA 1: usar el comando executemany
        product_func.insert_product_from_dataframe(df=df_new)
        # FORMA 2: generar un csv con dataframe (recomendable para cargas muy grandes)
        # product_func.insert_product_from_csv(df=df_new)
    
    logger.info(f"Cantidad de productos a modificar: {len(filas_upd)}")
    if filas_upd:  # modificar informacion de los productos
        # Convertir a tuplas
        tuples = [
            (record['base_price'], record['description'],  
            record['timestamp'], record['status_id'], record['categoy_id'],record['ext_code'])
            for record in filas_upd
            ]  
        product_func.update_fnd_product(datos=tuples)

    logger.info(f"Cantidad de productos no categorizados: {no_categorizados}")

    if sin_categorizar:

        carpeta_txt = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "product_sync", "txt")
        os.makedirs(carpeta_txt, exist_ok=True)
        ruta_archivo = os.path.join(carpeta_txt, "sin_categorizar.txt")
    
        product_func.create_file_sin_categorizar(ruta_archivo, sin_categorizar)
        with open(ruta_archivo, "a") as archivo:
            for nombre in sin_categorizar:
                archivo.write(f"{nombre}\n") 

    product_func.commit()


    # Productos en connexa que no están en diarco (se inactivan)
    
    # obtener los productos de Diarco
    df_diarco = product_func.create_df_m_3_articulos()
    logger.info(f"Total de Productos recuperados en Diarco: {len(df_diarco)}")  

     # obtener el dataframe de los productos de Connexa
    df_connexa = product_func.create_df_fnd_product() 
    logger.info(f"Total de Productos recuperados en Connexa: {len(df_connexa)}")  

    # cambiar el tipo de campo a aquellos campos que serán de igualdad en el df_resultado
    df_connexa['ext_code'] = df_connexa['ext_code'].astype(str)
    df_diarco['c_articulo'] = df_diarco['c_articulo'].astype(str)

    # Filtrar los registros de df_connexa que no están en df_diarco
    df_resultado = df_connexa[~df_connexa['ext_code'].isin(df_diarco['c_articulo'])]
    logger.info(f"Total de Productos en Connexa para marcar de inactivos: {len(df_resultado)}")  

    # Guardar el DataFrame en un archivo CSV
    carpeta_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "product_sync", "csv")
    os.makedirs(carpeta_csv, exist_ok=True)
    ruta_archivo = os.path.join(carpeta_csv, "productos_inactivos.csv")

    df_resultado.to_csv(f'{ruta_archivo}', index=False)
    
    # Convertir el DataFrame a una lista de diccionarios
    lista_dict = df_resultado.to_dict(orient='records')

    if lista_dict:  # modificar informacion de los productos
        # Convertir a tuplas
        tuples = [
            (2, record['ext_code'])
            for record in lista_dict
            ]  
        product_func.update_fnd_product_status(datos=tuples)


    # actualizar la unidad de venta
    df_uv = product_func.create_df_unidad_venta()
    tuplas = list(df_uv.itertuples(index=False))
    product_func.update_unidad_venta(tuplas)

    product_func.commit()
    product_func.close_connections()


   