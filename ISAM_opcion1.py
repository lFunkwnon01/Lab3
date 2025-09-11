import struct
import os

BLOCK_FACTOR = 3

class Record:
    FORMAT = 'i30sif10s'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, id_venta: int, nombre_producto: str, cantidad_vendida: int, precio_unitario: float, fecha_venta: str):
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

class Page:
    HEADER_FORMAT = 'ii' # size, next_page
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    SIZE_OF_PAGE = HEADER_SIZE + BLOCK_FACTOR * Record.SIZE_OF_RECORD

    def __init__(self, records=None, next_page=-1):
        self.records = records if records is not None else []
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
    def __init__(self):
        self.entries = []  # (clave, num_pagina)

    def build(self, pages):
        self.entries = []
        for i, page in enumerate(pages):
            if page.records:
                clave = page.records[0].id_venta
                self.entries.append((clave, i))

    def insert(self, clave, page_num):
        self.entries.append((clave, page_num))
        self.entries.sort(key=lambda x: x[0])

    def search(self, clave):
        for idx, (k, page_num) in enumerate(self.entries):
            if clave < k:
                return self.entries[max(0, idx-1)][1]
        return self.entries[-1][1] if self.entries else None

class DataFile:
    def delete(self, id_venta, index: IndexFile):
        pages = self.load_all_pages()
        if not pages or not index.entries:
            print(f'Registro con ID {id_venta} no encontrado.')
            return False
        page_num = index.search(id_venta)
        if page_num is None or page_num >= len(pages):
            print(f'Registro con ID {id_venta} no encontrado.')
            return False
        page = pages[page_num]
        # Eliminar el registro de la página
        original_len = len(page.records)
        page.records = [r for r in page.records if r.id_venta != id_venta]
        if len(page.records) == original_len:
            print(f'Registro con ID {id_venta} no encontrado en la página.')
            return False
        # Si la página queda vacía, márcala como reutilizable (next_page = -2)
        if len(page.records) == 0:
            print(f'Página {page_num+1} quedó vacía. Marcada para reutilización.')
            pages[page_num] = Page([], next_page=-2)
            # Eliminar la clave del índice correspondiente
            index.entries = [(k, n) for (k, n) in index.entries if n != page_num]
        else:
            pages[page_num] = page
        # Guardar todas las páginas
        self.save_all_pages(pages)
        # Reconstruir el índice para reflejar el estado real
        index.build(pages)
        print(f'Registro con ID {id_venta} eliminado.')
        return True
    def search(self, id_venta, index: IndexFile):
        pages = self.load_all_pages()
        if not pages or not index.entries:
            print(f'Registro con ID {id_venta} no encontrado.')
            return None
        page_num = index.search(id_venta)
        if page_num is None or page_num >= len(pages):
            print(f'Registro con ID {id_venta} no encontrado.')
            return None
        page = pages[page_num]
        for record in page.records:
            if record.id_venta == id_venta:
                print(f'Encontrado: ID: {record.id_venta}, Producto: {record.nombre_producto}, '
                      f'Cantidad: {record.cantidad_vendida}, Precio: {record.precio_unitario}, '
                      f'Fecha: {record.fecha_venta}')
                return record
        print(f'Registro con ID {id_venta} no encontrado en la página.')
        return None
    def __init__(self, filename: str):
        self.filename = filename

    def add(self, record: Record, index: IndexFile):
        # Si el archivo está vacío, crea la primera página
        if not os.path.exists(self.filename):
            with open(self.filename, 'wb') as f:
                page = Page([record])
                f.write(page.pack())
            # Construir el índice con la nueva página
            index.build([Page([record])])
            return
        # Buscar la página donde debería ir el registro
        pages = self.load_all_pages()
        # Si el índice está vacío o desactualizado, reconstruirlo
        if not index.entries or len(index.entries) != len(pages):
            index.build(pages)

        # Buscar páginas vacías (next_page == -2)
        empty_page_num = None
        for i, page in enumerate(pages):
            if len(page.records) == 0 and page.next_page == -2:
                empty_page_num = i
                break

        if empty_page_num is not None:
            # Reutilizar la página vacía
            pages[empty_page_num] = Page([record])
            # Actualizar el índice
            index.insert(record.id_venta, empty_page_num)
            self.save_all_pages(pages)
            index.build(pages)
            return

        page_num = index.search(record.id_venta)
        page = pages[page_num]
        # Insertar el registro en la página correspondiente
        page.records.append(record)
        page.records.sort(key=lambda r: r.id_venta)
        # Si la página se desborda, hacer split
        if len(page.records) > BLOCK_FACTOR:
            mid = len(page.records) // 2
            left_records = page.records[:mid]
            right_records = page.records[mid:]
            pages[page_num] = Page(left_records)
            pages.insert(page_num + 1, Page(right_records))
            # Reconstruir el índice para reflejar el estado real
            index.build(pages)
        else:
            pages[page_num] = page
        # Guardar todas las páginas
        self.save_all_pages(pages)

    def load_all_pages(self):
        pages = []
        if not os.path.exists(self.filename):
            return pages
        with open(self.filename, 'rb') as f:
            f.seek(0, 2)
            num_pages = f.tell() // Page.SIZE_OF_PAGE
            f.seek(0)
            for _ in range(num_pages):
                page_data = f.read(Page.SIZE_OF_PAGE)
                pages.append(Page.unpack(page_data))
        return pages

    def save_all_pages(self, pages):
        with open(self.filename, 'wb') as f:
            for page in pages:
                f.write(page.pack())

    def scanAll(self):
        pages = self.load_all_pages()
        for i, page in enumerate(pages):
            print(f'Page {i+1}:')
            for record in page.records:
                print(f'ID: {record.id_venta}, Producto: {record.nombre_producto}, '
                      f'Cantidad: {record.cantidad_vendida}, Precio: {record.precio_unitario}, '
                      f'Fecha: {record.fecha_venta}')

'''
if __name__ == '__main__':
    # Limpiar archivo de datos antes de insertar para evitar duplicados
    data_filename = 'datafile_split.dat'
    if os.path.exists(data_filename):
        os.remove(data_filename)
    datafile = DataFile(data_filename)
    index = IndexFile()
    # Insertar registros
    datafile.add(Record(1, 'Cafetera Inteligente', 31, 1751.2, '04/06/2024'), index)
    datafile.add(Record(2, 'Purificador de Aire', 42, 1938.49, '09/11/2024'), index)
    datafile.add(Record(3, 'Raspberry Pi', 34, 1257.34, '22/11/2024'), index)
    datafile.add(Record(4, 'Aire Acondicionado', 10, 2500.00, '15/07/2024'), index)
    datafile.add(Record(5, 'Smart TV', 5, 3200.00, '20/08/2024'), index)
    datafile.scanAll()
    print('--- Índice (clave, num_pagina) ---')
    print(index.entries)

    # Ejemplo de búsqueda
    print('\n--- Búsqueda de registro con ID 3 ---')
    datafile.search(3, index)

    # Ejemplo de eliminación
    print('\n--- Eliminación de registro con ID 2 ---')
    datafile.delete(2, index)
    datafile.scanAll()
    print('--- Índice (clave, num_pagina) después de eliminar ---')
    print(index.entries)

'''