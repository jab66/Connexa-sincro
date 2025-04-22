import uuid
import pandas as pd
from category_sync.fnd_category import CategoryFunc
from util.logger import Logger

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from category_sync.fnd_category import CategoryFunc

# Función para sincronizar los datos
def run_category_sync():
    """
    Funcion para sincronizar categorias de productos.
    Se recuperan las categorias de Diarco y se actualiza en Connexa.
    Tabla de categorias de Diarco: T114_RUBROS
    Tabla de categorias de Connexa: FND_CATEGORY
    """

    logger = Logger(
        log_dir="category_sync/log",  
        log_filename="category.log"
    )

    try:
        # funcion de la aplicacion de sincronización de categorias
        category_func = CategoryFunc()

        logger.info("#")
        logger.info("# Verificacion de las categorias entre Diarco y Connexa")
        logger.info("#")

        # obtener el arbol de categorias de Diarco
        categorias_diarco = category_func.find_all_t114_rubros()
        logger.info(f"Total de Categorias recuperadas en Diarco: {len(categorias_diarco)}")

        # obtener el arbol de categorias de Connexa
        categorias_connexa = category_func.find_all_fnd_category()
        logger.info(f"Total de Categorias recuperadas en Connexa: {len(categorias_connexa)}")

        # obtener el dataframe de las categorias de Connexa
        df_destino = category_func.create_df_fnd_category()
        
        categorias_nuevas = 0
        # Iterar sobre las categorias de Diarco y sincronizarlas
        for categoria in categorias_diarco:
            c_rubro, c_rubro_padre, nombre_categoria, ruta, id_unico, nivel, f_alta, f_baja, m_baja = categoria

            # determinar si la categoria existe en Connexa
            reg = df_destino.query(f'ext_code == "{id_unico}"') 

            # si la categoria no existe en Connexa
            if reg.empty:
                # obtener el id_unico del padre 
                parent = category_func.get_parent_extcode(id_unico)
                # obtener la categoria del padre
                category_parent = category_func.find_category_by_extcode(parent)

                new_uuid = str(uuid.uuid4())
                category_func.insert_category(id=new_uuid,
                                                name=nombre_categoria,
                                                ext_code=id_unico,
                                                parent_id=category_parent[0])
                category_func.commit()
                logger.warning(f"La categoria {id_unico} fue dada de alta en Connexa.")

                categorias_nuevas += 1

        logger.info(f"Total de categorias nuevas: {categorias_nuevas}")

        logger.info("#")
        logger.info("# Verificacion de las descripciones")
        logger.info("#")
        
        # volver a obtener el arbol de categorias de Connexa
        categorias_connexa = category_func.find_all_fnd_category()
        logger.info(f"Total de Categorias recuperadas en Connexa: {len(categorias_connexa)}")
        
        # obtener el dataframe de las categorias de Diarco
        df_origen = category_func.create_df_t114_rubros()

        # obtener el dataframe de las categorias de Connexa
        df_destino = category_func.create_df_fnd_category()

        # Determinar si hay categorias con descripciones distintas, si es así se actualizan en Connexa
        categoiras_modificadas = 0
        registros = []
        for index, row_1 in df_origen.iterrows():
            registro_2 = df_destino.query(f'ext_code == "{row_1.id_unico}"') 
            if registro_2.empty:
                logger.error(f"El id_unico {row_1.id_unico} no se encontro en Connexa para veriifcar su descripcion")
            else:
                cat_diarco = row_1.nombre_categoria
                cat_connexa = registro_2["name"].values[0]
                if cat_diarco != cat_connexa:
                    registro = {'ext_code': row_1.id_unico, 'name':cat_diarco, 'name_connexa':cat_connexa}
                    registros.append(registro)
        
        for registro in registros:
            ext_code_ = registro['ext_code']
            name_ = registro['name']
            name_connexa_ = registro['name_connexa']
            logger.warning(f"En Connexa se reemplaza la descripcion '{name_connexa_}' por '{name_}' ({ext_code_})")
            category_func.update_category(name=name_, ext_code=ext_code_)
            category_func.commit()
            categoiras_modificadas += 1

        logger.info(f"Total de categorias que cambiaron su descripcion: {categoiras_modificadas}")


        logger.info("#")
        logger.info("# Verificacion de las categorias eliminadas en el origen")
        logger.info("#")

        # obtener el arbol de categorias de Connexa
        categorias_connexa = category_func.find_all_fnd_category()
        # obtener el dataframe de las categorias de Connexa
        df_destino = category_func.create_df_fnd_category()

        # obtener el arbol de categorias de Diarco
        categorias_diarco = category_func.find_all_t114_rubros()
        # obtener el dataframe de las categorias de Diarco
        df_origen = category_func.create_df_t114_rubros()

        # Hacemos el join (unión) entre df_destino y df_origen para obtener las diferencias
        df_join = pd.merge(df_destino, df_origen, left_on='ext_code', right_on='id_unico', how='outer', indicator=True)

        # Filtramos las diferencias y eliminamos registros donde 'ext_code' o 'id_unico' sean nulo
        diferencias = df_join[(df_join['_merge'] != 'both') & (df_join['ext_code'].notna() | df_join['id_unico'].notna())]

        # obtener el dataframe de productos de Connexa
        df_productos = category_func.create_df_fnd_product()

        # Hacemos un loop para obtener los valores de la columna 'id'
        categorias_eliminadas = 0
        productos_null = 0
        for index, row in diferencias.iloc[::-1].iterrows():
            id_category = row['id']  
            name_category = row['name']

            lista_relacionada = category_func.find_fnd_category_join_fnd_product(id_category)
            for relacion in lista_relacionada:
                id, ext_code, desc_categoria, id_producto, desc_producto, cod_producto = relacion

                # se pone en null la categoria del producto
                logger.warning(f"Se saco de la rama {desc_categoria} ({id}) el producto {desc_producto} ({cod_producto})")
                category_func.update_fnd_product(id=cod_producto)
                category_func.commit()
                productos_null += 1
           
            # se elimina la categoria en Connexa
            category_func.delete_category(id=id_category)
            category_func.commit()
            categorias_eliminadas += 1
            logger.warning(f"Id de categoria {id_category} ({name_category}) fue eliminada en Connexa.")

        
        logger.info(f"Total de productos seteados en 'null' su id de categoria: {productos_null}")
        logger.info(f"Total de categorias eliminadas en la tabla fnd_category: {categorias_eliminadas}")
        

    except Exception as e:
        logger.error(e)
    finally:
        # Cerrar los cursores y las conexiones
        category_func.close_connections()




