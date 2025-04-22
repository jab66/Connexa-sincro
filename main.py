import argparse
from barcode_sync.barcode_sync import run_barcode_sync
from product_sync.product_sync import run_product_sync
from category_sync.category_sync import run_category_sync
from supplier_sync.supplier_sync import run_supplier_sync

def main():
    parser = argparse.ArgumentParser(description="Sincronización de datos")
    parser.add_argument(
        "--sync",
        choices=["category", "product", "barcode", "allProduct", "supplier"],
        required=True,
        help="Selecciona qué sincronización ejecutar"
    )
    args = parser.parse_args()

    print("🚀 Iniciando sincronización...")

    try:
        if args.sync == "category" or args.sync == "allProduct":
            run_category_sync()
            print("✅ Finalizó la sincronización de category.")

        if args.sync == "product" or args.sync == "allProduct":
            run_product_sync()
            print("✅ Finalizó la sincronización de product.")

        if args.sync == "barcode" or args.sync == "allProduct":
            run_barcode_sync()
            print("✅ Finalizó la sincronización de barcode.")

        if args.sync == "supplier":
            run_supplier_sync()
            print("✅ Finalizó la sincronización de supplier.")

    except Exception as e:
        print(f"❌ Error en sincronización: {e}")

if __name__ == '__main__':
    main()
