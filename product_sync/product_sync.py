import uuid
import pandas as pd
from fnd_product import ProductFunc

# Función para sincronizar los datos
def sincronizar_datos():
    """
    Funcion para sincronizar productos
    """

    product_func = ProductFunc()

    # obtener los productos de Diarco
    productos_diarco = product_func.find_all_m_3_articulos()
    print(f"Total de Productos recuperados en Diarco: {len(productos_diarco)}")   

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
        c_articulo, n_articulo, precio_compra, ean, f_proc, rama = articulo

        # Generar un UUID para el producto si es necesario
        product_id = str(uuid.uuid4())
            
        # Definir el SKU y el código externo
        sku = f"{c_articulo}"
        ext_code = f"{c_articulo}"
            
        # Definir todos los productos activos (fnd_product_status, 1=Activo)
        estado = 1
        
        # buscar el id de la categoria del producto
        reg = df_categorias.query(f'ext_code == "{rama}"') 
        if reg.empty:
            no_categorizados += 1
            sin_categorizar.append(f"Articulo: {c_articulo}, Rama: {rama}, Descripcion: {n_articulo}")
            continue
        id_value = reg.iloc[0]['id']

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

    print(f"Cantidad de productos a insertar: {len(filas)}")
    if filas:
        # Crear el DataFrame con todas las filas a insertar
        df_new = pd.DataFrame(filas)
        # FORMA 1: usar el comando executemany
        product_func.insert_product_from_dataframe(df=df_new)
        # FORMA 2: generar un csv con dataframe (recomendable para cargas muy grandes)
        # product_func.insert_product_from_csv(df=df_new)
    
    print(f"Cantidad de productos a modificar: {len(filas_upd)}")
    if filas_upd:  # modificar informacion de los productos
        # Convertir a tuplas
        tuples = [
            (record['base_price'], record['description'],  
            record['timestamp'], record['status_id'], record['categoy_id'],record['ext_code'])
            for record in filas_upd
            ]  
        product_func.update_fnd_product(datos=tuples)

    print(f"Productos no categorizados correctamente: {no_categorizados}")

    if sin_categorizar:
        product_func.create_file_sin_categorizar(sin_categorizar=sin_categorizar)
        # with open("sin_categorizar.txt", "a") as archivo:
        #     for nombre in sin_categorizar:
        #         archivo.write(f"{nombre}\n") 

    product_func.commit()
    product_func.close_connections()




if __name__ == "__main__":

    sincronizar_datos()