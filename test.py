import csv
import os
from ISAM import Record, DataFile, IndexFile

DATA_FILENAME = "sales.dat"
INDEX_FILENAME = "sales.idx"
CSV_FILENAME = "sales_dataset_unsorted.csv"


def normalize_date(date_str: str) -> str:
    try:
        d, m, y = date_str.split("/")
        return f"{y}-{m}-{d}"
    except:
        return date_str.strip()


def load_csv_to_datafile(csv_filename, datafile):
    open(datafile.filename, "wb").close()

    with open(csv_filename, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")
        headers = [h.strip().replace("\ufeff", "") for h in reader.fieldnames]
        reader.fieldnames = headers

        for row in reader:
            id_key = "ID de la venta" if "ID de la venta" in headers else "ID"
            nombre_key = "Nombre producto" if "Nombre producto" in headers else "Nombre"
            cantidad_key = "Cantidad vendida" if "Cantidad vendida" in headers else "Cantidad"
            precio_key = "Precio unitario" if "Precio unitario" in headers else "Precio"
            fecha_key = "Fecha de venta" if "Fecha de venta" in headers else "Fecha"

            record = Record(
                int(row[id_key].strip()),
                row[nombre_key].strip(),
                int(row[cantidad_key].strip()),
                float(row[precio_key].strip()),
                normalize_date(row[fecha_key].strip())
            )
            datafile.add(record)

def initialize_datafile():
    df = DataFile(DATA_FILENAME)
    if not os.path.exists(DATA_FILENAME) or os.path.getsize(DATA_FILENAME) == 0:
        print("Inicializando base de datos desde CSV...")
        load_csv_to_datafile(CSV_FILENAME, df)
        df.reorganize()
    return df

def menu():
    df = initialize_datafile()
    while True:
        print("\nMenú:")
        print("1. Ver todos los registros (paginados)")
        print("2. Buscar por ID")
        print("3. Eliminar por ID")
        print("4. Insertar nuevo registro")
        print("5. Salir")
        op = input("Elige una opción: ")

        if op == "1":
            df.scanAll()
        elif op == "2":
            id_venta = int(input("ID de la venta: "))
            rec = df.search(id_venta)
            print(rec if rec else "No encontrado")
        elif op == "3":
            id_venta = int(input("ID a eliminar: "))
            if df.delete(id_venta):
                print("Eliminado con éxito")
            else:
                print("No se encontró el registro")
        elif op == "4":
            id_venta = int(input("ID de la venta: "))
            nombre = input("Nombre producto: ")
            cantidad = int(input("Cantidad vendida: "))
            precio = float(input("Precio unitario: "))
            fecha = input("Fecha de venta (YYYY-MM-DD): ")
            rec = Record(id_venta, nombre, cantidad, precio, fecha)
            df.add(rec)
            df.reorganize()

            print("Registro insertado.")
        elif op == "5":
            break
        else:
            print("Opción inválida")

if __name__ == "__main__":
    menu()