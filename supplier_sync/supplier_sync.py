import uuid
import psycopg2
from psycopg2 import sql
import time

# Conexiones a las bases de datos
db_origen  = "dbname='diarco_data' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"
db_destino = "dbname='connexa_platform' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"

# Consulta para obtener datos desde la base de datos origen
query_origen = """
    SELECT c_proveedor, c_cuit, n_proveedor, f_proc
	FROM public.m_10_proveedores 
"""

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
        
        # Iterar sobre los datos obtenidos y sincronizarlos
        for proveedor in proveedores:
            c_proveedor, c_cuit, n_proveedor, f_proc = proveedor
            
            # Generar un UUID para el producto si es necesario
            supplier_id = str(uuid.uuid4())
            
            # Definir el código externo
            ext_code = f"{c_proveedor}"

            # Definir id de proveedor activo (tabla fnd_supplier_status)
            activo = "23a92ea4-423a-43f0-9fab-efacef4a574e"
            
            # Verificar si el proveedor ya existe en la base de datos destino
            cursor_destino.execute("SELECT id FROM public.fnd_supplier WHERE ext_code = %s", (ext_code,))
            result = cursor_destino.fetchone()
            
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

