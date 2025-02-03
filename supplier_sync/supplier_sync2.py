import uuid
import psycopg2
from psycopg2 import sql
import pandas as pd

# Conexiones a las bases de datos
db_origen  = "dbname='diarco_data' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"
db_destino = "dbname='connexa_platform' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"

# Consulta para obtener datos desde la base de datos origen
query_origen = """
    SELECT c_proveedor, c_cuit, n_proveedor, f_proc
	FROM public.m_10_proveedores 
"""

# Consulta para obtener datos desde la base de datos destino (cargar el dataframe)
query_destino = """
    SELECT ext_code FROM public.fnd_supplier
"""

# Funcion para buscar id de proveedor en el dataframe de destino
def search (dataframe, columna, valor):
    if dataframe.empty:
        return False
    else:
        campo = f"{columna}"
        dataframe = dataframe.set_axis([campo], axis=1)
        dataframe[campo] = dataframe[campo].astype('string')
        resultado = dataframe[dataframe[campo] == f"{valor}"]
        if resultado.empty:
            return False
        else:
            return True
        
# Función para sincronizar los datos
def sincronizar_datos():
    try:
        # Conectar a la base de datos origen
        conn_origen = psycopg2.connect(db_origen)
        cursor_origen = conn_origen.cursor()
        
        # Ejecutar la consulta en la base de datos origen
        cursor_origen.execute(query_origen)
        proveedores = cursor_origen.fetchall()
        
        # Conectar a la base de datos destino
        conn_destino = psycopg2.connect(db_destino)
        cursor_destino = conn_destino.cursor()

        # Ejecutar la consulta en la base de datos destino (dataframe)
        cursor_destino.execute(query_destino)
        proveedores_destino = cursor_destino.fetchall()
        
        # Cargar el dataframe con la informacion de la base de datos destino
        df = pd.DataFrame(proveedores_destino)

        registros = 0

        #Iterar sobre los datos obtenidos y sincronizarlos
        for proveedor in proveedores:
            c_proveedor, c_cuit, n_proveedor, f_proc = proveedor
            
            # Generar un UUID para el producto si es necesario
            supplier_id = str(uuid.uuid4())
            
            # Definir el código externo
            ext_code = f"{c_proveedor}"
            
            # Definir id de proveedor activo (tabla fnd_supplier_status)
            activo = "23a92ea4-423a-43f0-9fab-efacef4a574e"
     
            # Verificar si el proveedor ya existe en el dataframe
            result = search(df, 'ext_code', ext_code)


            if result:
                # Si el proveedor existe, actualizar los datos
                update_query = """
                    UPDATE public.fnd_supplier
                    SET name = %s,
                        tax_identification = %s,
                        "timestamp" = %s
                    WHERE ext_code = %s
                """
                cursor_destino.execute(update_query, (n_proveedor, c_cuit, f_proc, ext_code))
            else:
                # Si el producto no existe, insertarlo
                insert_query = """
                    INSERT INTO public.fnd_supplier (id, name, tax_identification, ext_code, "timestamp", supplier_status_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor_destino.execute(insert_query, (supplier_id, n_proveedor, c_cuit, ext_code, f_proc, activo))
            
            registros = registros + 1
            if registros == 500:
                conn_destino.commit()
                registros = 0
            
        
        # Confirmar los cambios en la base de datos destino
        conn_destino.commit()
        
    except Exception as e:
        print(f"Ocurrió un error: {e}")
    finally:
        # Cerrar los cursores y las conexiones
        cursor_origen.close()
        conn_origen.close()
        cursor_destino.close()
        conn_destino.close()


if __name__ == "__main__":
    sincronizar_datos()

