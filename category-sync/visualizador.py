import sys
from fnd_category import CategoryFunc
import os
import logging

def visualizador():
    fnd_category = CategoryFunc()
    archivo = open('salida.txt', 'w')
    # obtener el arbol de categorias de Connexa
    categorias_connexa = fnd_category.find_branch_by_extcode(rama='5')

    resultado = [t for t in categorias_connexa ]

    for tupla in resultado:
        id = tupla[0]
        categoria = tupla[2].strip()  # Eliminar los espacios extra
        # fecha = tupla[3]
        parent_id = tupla[4]
        # print(f"Id: {id}, Categoria: {categoria}, Parent_id: {parent_id}")                                                                                                                                                                             


    # Paso 1: Crear un diccionario con el id como clave
    categorias = {}
    for tupla in resultado:
        id = tupla[0]
        categoria = tupla[2].strip()  # Eliminar los espacios extra
        parent_id = tupla[4]
        categorias[id] = {'categoria': categoria, 'parent_id': parent_id, 'hijas': []}

    # Paso 2: Construir el árbol, asignando hijas a sus padres
    for id, categoria_data in categorias.items():
        parent_id = categoria_data['parent_id']
        if parent_id in categorias:
            categorias[parent_id]['hijas'].append(categoria_data)

    # Paso 3: Imprimir la jerarquía
    def imprimir_categoria(categoria_data, nivel=0):

        print(" " * nivel * 4 + categoria_data['categoria'], file=archivo)  # Indentación según el nivel
        for hija in categoria_data['hijas']:
            imprimir_categoria(hija, nivel + 1)


    # Buscar las categorías principales (sin parent_id)
    for categoria_data in categorias.values():
        if categoria_data['parent_id'] == 'e763e556-b344-4ae9-b51a-81860eb004e3':  # ID raíz de BEBIDAS
            imprimir_categoria(categoria_data)
        
    archivo.close()

if __name__ == "__main__":
    
    visualizador()
    
