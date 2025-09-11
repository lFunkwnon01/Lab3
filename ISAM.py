import struct
import os

BLOCK_FACTOR = 3

class Record:
    FORMAT = 'i30sif10s'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, id_venta: int, nombre_producto: str, cantidad_vendida: int, precio_unitario: float,
                 fecha_venta: str):
        self.id_venta = id_venta
        self.nombre_producto = nombre_producto
        self.cantidad_vendida = cantidad_vendida
        self.precio_unitario = precio_unitario
        self.fecha_venta = fecha_venta

    def pack(self):
        return struct.pack(
            self.FORMAT,
            self.id_venta,
            self.nombre_producto[:30].encode(),
            self.cantidad_vendida,
            self.precio_unitario,
            self.fecha_venta[:10].encode()
        )
    
    @staticmethod
    def unpack(data):
        id_venta, nombre_producto, cantidad_vendida, precio_unitario, fecha_venta = struct.unpack(Record.FORMAT, data)
        return Record(
            id_venta,
            nombre_producto.decode().rstrip(),
            cantidad_vendida,
            precio_unitario,
            fecha_venta.decode().rstrip()
        )

    def __repr__(self):
        return f"Record(id={self.id_venta}, nombre='{self.nombre_producto}', cantidad={self.cantidad_vendida}, precio={self.precio_unitario}, fecha='{self.fecha_venta}')"

class Page:
    HEADER_FORMAT = 'ii' # size, next_page
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    SIZE_OF_PAGE = HEADER_SIZE + BLOCK_FACTOR * Record.SIZE_OF_RECORD

    def __init__(self, records=[], next_page=-1):
        self.records = records
        self.next_page = next_page
        
    def pack(self):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.records), self.next_page)
        records_data = b''

        for record in self.records:
            records_data += record.pack()

        i = len(self.records)
        while i < BLOCK_FACTOR: 
            records_data += b'\x00' * Record.SIZE_OF_RECORD
            i += 1

        return header_data + records_data

    @staticmethod
    def unpack(data: bytes):
        size, next_page = struct.unpack(Page.HEADER_FORMAT, data[:Page.HEADER_SIZE])
        records = []

        offset = Page.HEADER_SIZE

        for i in range(size):
            record_data = data[offset:offset + Record.SIZE_OF_RECORD]
            records.append(Record.unpack(record_data))
            offset += Record.SIZE_OF_RECORD

        return Page(records, next_page)

class IndexFile:
    ENTRY_FORMAT = "ii"  # (clave, numero de pagina)
    ENTRY_SIZE = struct.calcsize(ENTRY_FORMAT)

    def __init__(self, filename: str):
        self.filename = filename

    def build_index(self, datafile: 'DataFile'):
        with open(datafile.filename, 'rb') as df, open(self.filename, 'wb') as idxf:
            page_number = 0
            while True:
                data = df.read(Page.SIZE_OF_PAGE)
                if not data:
                    break
                page = Page.unpack(data)
                if page.records:
                    first_record = page.records[0]
                    key = first_record.id_venta
                    idxf.write(struct.pack(self.ENTRY_FORMAT, key, page_number))
                page_number += 1

    def print_index(self):
        with open(self.filename, 'rb') as idxf:
            print("\n--- ÍNDICE ---")
            entry_num = 0
            while True:
                data = idxf.read(self.ENTRY_SIZE)
                if not data:
                    break
                key, page_number = struct.unpack(self.ENTRY_FORMAT, data)
                print(f"Entrada {entry_num}: clave={key}, página={page_number}")
                entry_num += 1

    def search(self, id_venta: int):
        with open(self.filename, 'rb') as idxf:
            while True:
                data = idxf.read(self.ENTRY_SIZE)
                if not data:
                    break
                key, page_number = struct.unpack(self.ENTRY_FORMAT, data)
                if key >= id_venta:
                    return page_number
        return -1



class DataFile:
    def __init__(self, filename: str):
        self.filename = filename
        if not os.path.exists(filename):
            with open(filename, 'wb') as f:
                pass

    def search(self, id_venta: int):
        with open(self.filename, 'rb') as f:
            while True:
                data = f.read(Page.SIZE_OF_PAGE)
                if not data:
                    break
                page = Page.unpack(data)
                for record in page.records:
                    if record.id_venta == id_venta:
                        return record
        return None

    def delete(self, id_venta: int):
        with open(self.filename, 'r+b') as f:
            while True:
                pos = f.tell()
                data = f.read(Page.SIZE_OF_PAGE)
                if not data:
                    break
                page = Page.unpack(data)
                for i, record in enumerate(page.records):
                    if record.id_venta == id_venta:
                        deleted_record = page.records[i]
                        del page.records[i]
                        f.seek(pos)
                        f.write(page.pack())
                        return deleted_record
        return None

    def add(self, record: Record):
        # 1 si el archivo no existe o vacio, lo crea con una page y el registro
        # 2 si no recuperamos la ultima page
        # 2.1 si hay espacio se inserta el neuvo record y excribimos la page
        # 2.2 si esta llena, crear una nueva page al final con un solo record
        
        # 1
        with open(self.filename, 'r+b') as f:
            f.seek(0, 2)
            if f.tell() == 0:
                new_page = Page([record])
                f.write(new_page.pack())
                return

            f.seek(-Page.SIZE_OF_PAGE, 2)
            pos = f.tell()
            page_data = f.read(Page.SIZE_OF_PAGE)
            page = Page.unpack(page_data)

            if len(page.records) < BLOCK_FACTOR:
                page.records.append(record)
                page.records.sort(key=lambda r: r.id_venta)
                f.seek(pos)
                f.write(page.pack())
            else:
                new_page = Page([record])
                f.seek(0, 2)
                f.write(new_page.pack())
                # desbordamiento / overflow
    def reorganize(self):
        all_records = []
        with open(self.filename, "rb") as f:
            while True:
                data = f.read(Page.SIZE_OF_PAGE)
                if not data:
                    break
                page = Page.unpack(data)
                all_records.extend(page.records)

        all_records.sort(key=lambda r: r.id_venta)

        with open(self.filename, "wb") as f:
            i = 0
            while i < len(all_records):
                page_records = all_records[i:i+BLOCK_FACTOR]
                page = Page(page_records)
                f.write(page.pack())
                i += BLOCK_FACTOR


    def scanAll(self):
        # imprimir todos los registros y el numero de page
        # page 1
        # rec 1
        # rec 2
        # rec 3
        with open(self.filename, 'rb') as f:
            f.seek(0, 2)
            numPages = f.tell() // Page.SIZE_OF_PAGE
            f.seek(0, 0)

            for i in range(numPages):
                page_data = f.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)
                print(f"-- Page {i} (size={len(page.records)}, next={page.next_page})")
                for rec in page.records:
                    print("   ", rec)

# obtener los indices 
# buscar un registro por su clave





'''
datafile.add(Record(1, 'Cafetera Inteligente', 31, 1751.2, '04/06/2024'))
datafile.add(Record(2, 'Purificador de Aire', 42, 1938.49, '09/11/2024'))
datafile.add(Record(3, 'Raspberry Pi', 34, 1257.34, '22/11/2024'))
datafile.add(Record(4, 'olaqhace', 37, 156.34, '15/10/2024'))
datafile.add(Record(5, 'Laptop', 10, 1500.0, '01/01/2023'))
datafile.add(Record(6, 'Smartphone', 20, 800.0, '02/02/2023'))
datafile.add(Record(7, 'Tablet', 15, 600.0, '03/03/2023'))
datafile.add(Record(8, 'Monitor', 5, 300.0, '04/04/2023'))
datafile.add(Record(9, 'XD', 5, 300.0, '04/04/2023'))
datafile.add(Record(10, 'BASTA', 5, 300.0, '04/04/2023'))

datafile.scanAll()
print('------------------------------------------------------------------------------------------------')
record = datafile.search(6)
if record:
    print(f'Registro encontrado: ID: {record.id_venta}, Producto: {record.nombre_producto}')
else:
    print('Registro no encontrado.')

record = datafile.search(99)
if record:
    print(f'Registro encontrado: ID: {record.id_venta}, Producto: {record.nombre_producto}')
else:
    print('Registro no encontrado.')

datafile.delete(7)
datafile.delete(8)
datafile.delete(9)
datafile.delete(10)
datafile.scanAll()
print('------------------------------------------------------------------------------------------------')
datafile.add(Record(7, 'Smartphone', 20, 800.0, '02/02/2023'))
datafile.add(Record(8, 'Tablet', 15, 600.0, '03/03/2023'))
datafile.add(Record(9, 'Monitor', 5, 300.0, '04/04/2023'))
datafile.add(Record(10, 'BASTA', 5, 300.0, '04/04/2023'))
datafile.scanAll()
print('------------------------------------------------------------------------------------------------')
indexfile = IndexFile('test_indexfile.idx')
indexfile.build_index(datafile)

# respuesta a la pregunta de eliminación:

La desición del grupo seria marcar la página vacía para que cuando
alguna otra página se llene y necesite una nueva página, use dicha página vacía.

'''