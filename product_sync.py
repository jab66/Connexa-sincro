import uuid
import psycopg2
from psycopg2 import sql

# Conexiones a las bases de datos
db_origen = "dbname='diarco_data' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"
db_destino = "dbname='connexa_platform' user='postgres' password='aladelta10$' host='goldfarb.connexa-cloud.com'"

# Consulta para obtener datos desde la base de datos origen
query_origen = """
    SELECT c_articulo, n_articulo, precio_compra, ean, f_proc
    FROM public.m_3_articulos
"""

# Función para sincronizar los datos
def sincronizar_datos():
    try:
        # Conectar a la base de datos origen
        conn_origen = psycopg2.connect(db_origen)
        cursor_origen = conn_origen.cursor()
        
        # Ejecutar la consulta en la base de datos origen
        cursor_origen.execute(query_origen)
        articulos = cursor_origen.fetchall()
        
        # Conectar a la base de datos destino
        conn_destino = psycopg2.connect(db_destino)
        cursor_destino = conn_destino.cursor()
        
        # Iterar sobre los datos obtenidos y sincronizarlos
        for articulo in articulos:
            c_articulo, n_articulo, precio_compra, ean, f_proc = articulo
            
            # Generar un UUID para el producto si es necesario
            product_id = str(uuid.uuid4())
            
            # Definir el SKU y el código externo
            sku = f"{c_articulo}"
            ext_code = f"{c_articulo}"
            
            # Definir todos los productos activos (fnd_product_status, 1=Activo)
            estado = 1

            # Verificar si el producto ya existe en la base de datos destino
            cursor_destino.execute("SELECT id FROM public.fnd_product WHERE ext_code = %s", (ext_code,))
            result = cursor_destino.fetchone()
            
            if result:
                # Si el producto existe, actualizar los datos
                update_query = """
                    UPDATE public.fnd_product
                    SET base_price = %s,
                        description = %s,
                        "timestamp" = %s,
                        status_id = %s
                    WHERE ext_code = %s
                """
                cursor_destino.execute(update_query, (precio_compra, n_articulo, f_proc, estado, ext_code))
            else:
                # Si el producto no existe, insertarlo
                insert_query = """
                    INSERT INTO public.fnd_product (id, base_price, description, ext_code, sku, "timestamp", status_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor_destino.execute(insert_query, (product_id, precio_compra, n_articulo, ext_code, sku, f_proc, estado))
        
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