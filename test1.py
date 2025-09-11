import os
import csv
from ISAM_opcion1 import Record, DataFile, IndexFile

DATA_FILENAME = "sales.dat"
CSV_FILENAME = "sales_dataset_unsorted.csv"

def normalize_date(date_str: str) -> str:
    try:
        d, m, y = date_str.split("/")
        return f"{y}-{m}-{d}"
    except:
        return date_str.strip()

def load_some_records(csv_filename, n=5):
    records = []
    with open(csv_filename, newline='', encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")
        headers = [h.strip().replace("\ufeff", "") for h in reader.fieldnames]
        reader.fieldnames = headers

        id_key = "ID de la venta" if "ID de la venta" in headers else "ID"
        nombre_key = "Nombre producto" if "Nombre producto" in headers else "Nombre"
        cantidad_key = "Cantidad vendida" if "Cantidad vendida" in headers else "Cantidad"
        precio_key = "Precio unitario" if "Precio unitario" in headers else "Precio"
        fecha_key = "Fecha de venta" if "Fecha de venta" in headers else "Fecha"

        for i, row in enumerate(reader):
            if i >= n:
                break
            records.append(Record(
                int(row[id_key].strip()),
                row[nombre_key].strip(),
                int(row[cantidad_key].strip()),
                float(row[precio_key].strip()),
                normalize_date(row[fecha_key].strip())
            ))
    return records

def print_index(idx: IndexFile):
    print("\n📖 Índice actual:")
    for k, p in idx.entries:
        print(f"Clave mínima {k} en página {p}")
    print()

def run_tests():
    # Reiniciar archivo
    if os.path.exists(DATA_FILENAME):
        os.remove(DATA_FILENAME)

    df = DataFile(DATA_FILENAME)
    idx = IndexFile()

    # 1) Insertar algunos registros desde CSV
    print("=== Insertando registros iniciales ===")
    for rec in load_some_records(CSV_FILENAME, n=7):  # más que BLOCK_FACTOR para forzar split
        df.add(rec, idx)
        print(f"Insertado ID {rec.id_venta}")
    df.scanAll()
    print_index(idx)

    # 2) Buscar un registro existente
    print("=== Búsqueda de registro ===")
    rec = df.search(idx.entries[0][0], idx)  # tomo la primera clave del índice
    print(f"Resultado búsqueda: {rec.id_venta if rec else 'No encontrado'}")

    # 3) Eliminar un registro
    print("=== Eliminando registro ===")
    clave_a_eliminar = idx.entries[0][0]
    df.delete(clave_a_eliminar, idx)
    df.scanAll()
    print_index(idx)

    # 4) Insertar un nuevo registro (para mostrar reutilización de páginas o splits)
    print("=== Insertando nuevo registro manual ===")
    nuevo = Record(9999, "Producto Nuevo", 12, 45.5, "2025-09-10")
    df.add(nuevo, idx)
    df.scanAll()
    print_index(idx)

if __name__ == "__main__":
    run_tests()
