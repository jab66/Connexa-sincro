import uuid
import psycopg2
# from psycopg2 import sql
import pandas as pd
from util.logger import Logger
from psycopg2 import extras


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from supplier_sync.fnd_supplier import SupplierFunc

# Función para sincronizar los datos
def run_supplier_sync():
    """
    Funcion para sincronizar proveedores
    """
    logger = Logger(
        log_dir="supplier_sync/log",  
        log_filename="supplier.log"
    )

    suppler_func = SupplierFunc()

    upd = 0
    ins = 0

    try:

        # Conectar a la base de datos origen
        conn_origen = psycopg2.connect(suppler_func.db_origen)
        cursor_origen = conn_origen.cursor()
        
        # Ejecutar la consulta en la base de datos origen
        cursor_origen.execute(suppler_func.query_origen)
        proveedores_origen = cursor_origen.fetchall()
        cantidad = len(proveedores_origen)
        logger.info(f"Total de proveedorers obtenidos desde origen: {cantidad}")

        # Conectar a la base de datos destino
        conn_destino = psycopg2.connect(suppler_func.db_destino)
        cursor_destino = conn_destino.cursor()

        # Ejecutar la consulta en la base de datos destino 
        cursor_destino.execute(suppler_func.query_destino)
        proveedores_destino = cursor_destino.fetchall()
        cantidad = len(proveedores_destino)
        logger.info(f"Total de proveedorers obtenidos desde destino: {cantidad}")

        # Cargar el dataframe con la informacion de la base de datos destino
        df_proveedores_destino = pd.DataFrame(proveedores_destino)

        filas_ins=[]
        filas_upd=[]

        #Iterar sobre los datos obtenidos y sincronizarlos
        for proveedor_origen in proveedores_origen:
            c_proveedor, c_cuit, n_proveedor, f_proc = proveedor_origen
            
            # Generar un UUID para el producto si es necesario
            supplier_id = str(uuid.uuid4())
            
            # Definir el código externo
            ext_code = f"{c_proveedor}"
            
            # Definir id de proveedor activo (tabla fnd_supplier_status)
            activo = "23a92ea4-423a-43f0-9fab-efacef4a574e"
        
            # Verificar si el proveedor ya existe en el dataframe
            result = suppler_func.search(df_proveedores_destino, 'ext_code', ext_code)

            nueva_fila = {
                "id": supplier_id,
                "name": n_proveedor,
                "tax_identification": c_cuit,
                "ext_code": ext_code,
                "timestamp": f_proc,
                "supplier_status_id": activo
            }

            if result:
                filas_upd.append(nueva_fila)
                upd = upd + 1
            else:
                filas_ins.append(nueva_fila)
                ins = ins + 1
                
        logger.info(f"Cantidad de proveedores a insertar: {len(filas_ins)}")
        logger.info(f"Cantidad de proveedores a actualizar: {len(filas_upd)}")

        if filas_ins:
            df_new = pd.DataFrame(filas_ins)
            data = list(df_new.itertuples(index=False, name=None))
            insert_query = """
                INSERT INTO public.fnd_supplier (id, name, tax_identification, ext_code, "timestamp", supplier_status_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor_destino.executemany(insert_query, data)
            conn_destino.commit()

        if filas_upd:
            # Convertir a tuplas
            tuples = [
                (record['name'], record['tax_identification'],  
                record['timestamp'], record['ext_code'])
                for record in filas_upd
                ]  
            # Definir el update
            update_query = """
                UPDATE public.fnd_supplier
                SET name = %s,
                    tax_identification = %s,
                    "timestamp" = %s
                WHERE ext_code = %s
            """                      
            extras.execute_batch(cursor_destino, update_query, tuples)
            conn_destino.commit()


    except Exception as e:
        logger.error(e) 
        raise Exception(e)       
