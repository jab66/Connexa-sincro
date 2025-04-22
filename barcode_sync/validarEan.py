
# Función para sincronizar los datos
def validarEan():

    ean_code = '17790060234865'
    print(control_code_calculator(ean_code))

    
def control_code_calculator(ean: str) -> bool:
    # Obtener los dígitos, excluyendo el último
    char_digits = list(ean[:-1])  # Similar a toCharArray()
    
    # Definir el multiplicador
    ean_multiplicador = [0, 0]

    # Validar el tamaño y asignar multiplicadores
    if len(ean) == 8:
        ean_multiplicador[0] = 3
        ean_multiplicador[1] = 1
    elif len(ean) == 12:
        ean_multiplicador[0] = 3
        ean_multiplicador[1] = 1
        char_digits.reverse()  # Invertir los dígitos
    elif len(ean) == 13:
        ean_multiplicador[0] = 1
        ean_multiplicador[1] = 3
    elif len(ean) == 14:
        ean_multiplicador[0] = 3
        ean_multiplicador[1] = 1
        char_digits.reverse()  # Invertir los dígitos
    else:
        return False
    
    # Calcular la suma de los productos
    suma = 0
    for i, char in enumerate(char_digits):
        if char.isdigit():
            suma += int(char) * ean_multiplicador[i % 2]
        else:
            return False
    
    # Calcular el dígito de verificación (checksum)
    checksum = 10 - (suma % 10)
    if checksum == 10:
        checksum = 0
    
    # Comparar con el último dígito del EAN
    if checksum == int(ean[-1]):  # Comparamos con el último dígito
        return True
    else:
        return False


if __name__ == "__main__":

    validarEan()