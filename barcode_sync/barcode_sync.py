import datetime
from uuid import uuid4
import uuid
import pandas as pd
from fnd_barcode import BarcodeFunc

# Función para sincronizar los datos
def sincronizar_datos():
    """
    Funcion para sincronizar barcode
    """

    barcode_func = BarcodeFunc()

    # obtener el dataframe de los productos de Connexa
    df_productos_connexa = barcode_func.create_df_fnd_product() 
    print(f"df productos connexa: {len(df_productos_connexa)}")

    # obtener el dataframe de los barcode de Connexa
    df_barcode_connexa = barcode_func.create_df_fnd_barcode() 
    print(f"df barcode connexa: {len(df_barcode_connexa)}")

    # obtener el dataframe de los productos de Diarco
    df_productos_diarco = barcode_func.create_df_m_3_articulos()
    print(f"df productos diarco: {len(df_productos_diarco)}")


    filas=[]
    filas_upd=[]

    count_barcode_fail = 0
    not_found_product_connexa = 0
    quantity = 0

    # se recorre el dataframe de productos de diarco
    for index, row in df_productos_diarco.iterrows():

        quantity = row["q_factor_cpra_sucu"]
        unidad_compra = row["d_codigo_abrev_cpra"]

        # buscar el producto en connexa 
        reg = df_productos_connexa.query(f'ext_code == "{row["c_articulo"]}"') 

        if not reg.empty:
            # recupero el id de producto
            id_product = reg.iloc[0]['id']
            
            # Recorriendo las columnas ean, ean1, ean2, ean3, ean4
            for col in ['ean', 'ean1', 'ean2', 'ean3', 'ean4','dun14']:
                valor = row[col]
                if valor != '0':
                    # se controla que sea un EAN valido
                    estado = barcode_func.control_code_calculator(valor)

                    if estado:
                        # determinar el tipo de EAN              
                        codigo_ean = {13: 1, 14: 5, 8: 11}.get(len(valor), 0)
                        uom_id = {"CM3":"cm3", "M2":"m2",
                                  "UNID":"unidad", "ML":"ml",
                                  "GRM":"g", "KG":"kg", "L":"l"}.get(unidad_compra, "--")

                        if codigo_ean != 0:

                            reg_barcode = df_barcode_connexa.query(f'barcode == "{valor}"')

                            nueva_fila = {
                                "id": str(uuid.uuid4()),
                                "barcode": valor,
                                "timestamp": datetime.datetime.now(),
                                "product_id": id_product,
                                "quantity": {'dun14':quantity}.get(col, 1),
                                "uom_id": uom_id,
                                "barcode_type_id": codigo_ean
                            }

                            if reg_barcode.empty:
                                # agregar el producto a la lista para insertar
                                filas.append(nueva_fila)
                            else:
                                # agregar el producto a la lista para modificar
                                filas_upd.append(nueva_fila)
                    else:
                        count_barcode_fail += 1
        else:
            not_found_product_connexa += 1

    print(f"Insertar: {len(filas)}")
    print(f"Update  : {len(filas_upd)}")
    print(f"Barcode fail: {count_barcode_fail}")
    print(f"Not found product in Connexa: {not_found_product_connexa}")

    # se eliminan los barcode duplicados
    # Convertir la lista a un DataFrame
    df = pd.DataFrame(filas, columns=['id', 'barcode','timestamp', 'product_id', 'quantity', 'uom_id', 'barcode_type_id'])
    df_sin_duplicados = df.drop_duplicates(subset="barcode")
    # print(f"DF sin duplicados: {df_sin_duplicados}")


    if (not df_sin_duplicados.empty):
        # FORMA 1     
        # barcode_func.insert_barcode_from_dataframe(df=df_sin_duplicados)
        # FORMA 2
        barcode_func.insert_barcode_from_csv(df=df_sin_duplicados)

    if (filas_upd):
        # Convertir a tuplas
        tuples = [
            (record['quantity'], record['uom_id'], record['barcode'])
            for record in filas_upd
            ]  
        barcode_func.update_fnd_barcode(datos=tuples)



    barcode_func.commit()
    barcode_func.close_connections()

if __name__ == "__main__":

    sincronizar_datos()

