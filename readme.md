# 🛠️ Cómo usar el script de sincronización

Este script te permite ejecutar procesos de sincronización específicos desde la línea de comandos. Usa la opción `--sync` seguida del tipo de sincronización que deseas ejecutar.

## 🧪 Comandos disponibles

### 🔹 Ejecutar solo categorías
```
python main.py --sync category
```

### 🔹 Ejecutar solo productos
```
python main.py --sync product
```

### 🔹 Ejecutar solo códigos de barras
```
python main.py --sync barcode
```

### 🔹 Ejecutar todas las sincronizaciones dependientes de productos
```
python main.py --sync allProduct
```

### 🔹 Ejecutar solo supplier
```
python main.py --sync supplier
```
