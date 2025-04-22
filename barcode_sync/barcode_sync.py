import datetime
import uuid
import pandas as pd

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from util.logger import Logger
from barcode_sync.fnd_barcode import BarcodeFunc 

# Función para sincronizar los datos
def run_barcode_sync():
    """
    Funcion para sincronizar barcode
    """
    logger = Logger(
        log_dir="barcode_sync/log",  # o una ruta absoluta si preferís
        log_filename="barcode.log"
    )

    barcode_func = BarcodeFunc()

    # obtener el dataframe de los productos de Connexa (solo los activos)
    logger.info("creanado df_productos_connexa")
    df_productos_connexa = barcode_func.create_df_fnd_product() 
    logger.info(len(df_productos_connexa))

    # obtener el dataframe de los productos de Valkimia
    logger.info("creando df_productos_valkimia")
    df_productos_valkimia = barcode_func.create_df_articulos_valkimia() 
    logger.info(len(df_productos_valkimia))

    df_all = pd.DataFrame()

    logger.info("se recorre el dataframe df_productos_connexa")
    # se recorre el dataframe de productos de connexa
    for index, row_prod_connexa in df_productos_connexa.iterrows():

        # buscar el producto en valkimia 
        # el producto puede estar mas de una vez
        regs_valkimia = df_productos_valkimia[df_productos_valkimia['codigo_articulo'] == row_prod_connexa["ext_code"]].copy()

        if (not regs_valkimia.empty):
            # agregar al dataframe de valkimia la informacion de connexa
            regs_valkimia['id_articulo_connexa'] = row_prod_connexa["id"]
            regs_valkimia['ext_code'] = row_prod_connexa["ext_code"]
            regs_valkimia['uuid_barcode'] = regs_valkimia.apply(lambda _: uuid.uuid4(), axis=1)
            regs_valkimia['timestamp'] = regs_valkimia.apply(lambda _: datetime.datetime.now(), axis=1)
            regs_valkimia['validacion_ean'] = regs_valkimia['barcode'].apply(barcode_func.control_code_calculator)
            regs_valkimia['descripcion_producto'] = row_prod_connexa['description']
        else:
            regs_valkimia['id_articulo_connexa'] = 0

        df_all = pd.concat([df_all, regs_valkimia], ignore_index=True) 

    # se agrega la descripcion del EAN
    df_all['descripcion_ean'] = df_all.apply(
                    lambda row: barcode_func.descripcionEAN
                            (row['descripcion_producto'], 
                                row['unidades_x_bulto'],
                                row['longitud_barcode']),
                                axis=1)
        
    # Se cuentan las ocurrencias de cada EAN
    df_all['ean_ocurrencias'] = df_all['barcode'].map(df_all['barcode'].value_counts())
    
    #
    # Se crea el df_all en csv
    #
    if (len(df_all)>0):
        barcode_func.dataFrameToCsv(nombre_archivo="df_all", dataframe=df_all)
        logger.info(f"df_all = {df_all.shape}")

        #
        # Productos de Connexa no encontrados en Valkimia
        #
        # condicion para eliminar
        condicion = df_all['id_articulo_connexa'] == 0
        # almacenar los registros en otro dataframe
        df_articulos_no_encontrados_en_valkimia = df_all[condicion]
        # eliminar los registros de df_all
        df_all = df_all[~condicion]

        logger.info(f"df_articulos_no_encontrados_en_valkimia = {df_articulos_no_encontrados_en_valkimia.shape}")

        if (len(df_articulos_no_encontrados_en_valkimia)>0):
            barcode_func.dataFrameToCsv(nombre_archivo="articulos_no_encontrados_en_valkimia", 
                                            dataframe=df_articulos_no_encontrados_en_valkimia)
            logger.info("se creo el archivo articulos_no_encontrados_en_valkimia")
        else:
            logger.info('no se creo el archivo articulos_no_encontrados_en_valkimia')


        #
        # EAN no pasa la validacion
        #
        # condicion para eliminar
        condicion = df_all['validacion_ean'] == False
        # almacenar los registros en otro dataframe
        df_ean_validacion_false = df_all[condicion]
        # eliminar los registros de df_all
        df_all = df_all[~condicion]

        logger.info(f"df_ean_validacion_false = {df_ean_validacion_false.shape}")

        if (len(df_ean_validacion_false)>0):
            barcode_func.dataFrameToCsv(nombre_archivo="ean_validacion_false", 
                                            dataframe=df_ean_validacion_false)
            logger.info("se creo el archivo ean_validacion_false")
        else:
            logger.info('no se creo el archivo ean_validacion_false')


        #
        # Ocurrencia de los ean superior a 1 
        #
        # condicion para eliminar
        condicion = df_all['ean_ocurrencias'] > 1
        # almacenar los registros en otro dataframe
        df_ean_ocurrencias = df_all[condicion]
        # eliminar los registros de df_all
        df_all = df_all[~condicion]

        logger.info(f"df_ean_ocurrencias = {df_ean_ocurrencias.shape}")

        if (len(df_ean_ocurrencias)>0):
            barcode_func.dataFrameToCsv(nombre_archivo="ean_ocurrencias", 
                                            dataframe=df_ean_ocurrencias)
            logger.info("se creo el archivo ean_ocurrencias")
        else:
            logger.info('no se creo el archivo ean_ocurrencias')



    else:
        logger.info("df_all sin registros")

    logger.info(f"df_all = {df_all.shape}")
    barcode_func.dataFrameToCsv(nombre_archivo="df_all_procesado", dataframe=df_all)
    logger.info("Se genera el df_all_procesado ")

    # eliminar los registros en la tabla fnd_barcode
    barcode_func.delete_fnd_barcode()

    # insertar los EAN en la tabla de Connexa
    barcode_func.insert_barcode_from_csv(df=df_all)

    barcode_func.commit()
    barcode_func.close_connections()

