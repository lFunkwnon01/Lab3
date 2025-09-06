import struct
import os

BLOCK_FACTOR = 3

class Record:
    FORMAT = '20s20si'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, nombre: str, apellido: str, ciclo: int):
        self.nombre = nombre
        self.apellido = apellido
        self.ciclo = ciclo

    def pack(self):
        return struct.pack(self.FORMAT, 
                           self.nombre[:20].encode(), 
                           self.apellido[:20].encode(), 
                           self.ciclo)
    
    @staticmethod
    def unpack(data):
        nombre, apellido, ciclo = struct.unpack(Record.FORMAT, data)
        return Record(nombre.decode().rstrip(), 
                      apellido.decode().rstrip(), 
                      ciclo)
    
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

class DataFile:
    def __init__(self, filename: str):
        self.filename = filename
        self.filename = filename + '_idx'

    def add(self, record: Record):
        # 1 si el archivo no existe o vacio, lo crea con una page y el registro
        # 2 si no recuperamos la ultima page
        # 2.1 si hay espacio se inserta el neuvo record y excribimos la page
        # 2.2 si esta llena, crear una nueva page al final con un solo record
        
        # 1
        if not os.path.exists(self.filename):
            with open(self.filename, 'wb') as f:
                page = Page([record])
                f.write(page.pack())
            return
        
        # 2 
        with open(self.filename, 'r+b') as f:
            f.seek(-Page.SIZE_OF_PAGE, 2)
            page_data = f.read(Page.SIZE_OF_PAGE)
            page = Page.unpack(page_data)
            # 2.1
            if len(page.records) < BLOCK_FACTOR:
                page.records.append(record)
                f.seek(-Page.SIZE_OF_PAGE, 1)
                f.write(page.pack())
            else:
                # 2.2
                new_page = Page([record])
                f.seek(0, 2)
                f.write(new_page.pack())
                # desbordamiento / overflow 
                page.next_page = (os.path.getsize(self.filename) // Page.SIZE_OF_PAGE) - 1
                f.seek(-Page.SIZE_OF_PAGE, 1)
                f.write(page.pack())

    def scanAll(self):
        # imprimir todos los registros y el numero de page
        # page 1
        # rec 1
        # rec 2
        # rec 3
        with open(self.filename, 'rb') as f:
            f.seek(0,2)
            num_pages = f.tell() // Page.SIZE_OF_PAGE
            f.seek(0)

            for i in range(num_pages):
                page_data = f.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)
                
                print(f'Page {i+1}:')
                for record in page.records:
                    record_data = record.unpack(record.pack())
                    print(f'{record_data.nombre}, {record_data.apellido}, {record_data.ciclo}')


# obtener los indices 
# buscar un registro por su clave
 



class IndexFile:
    pass



dataf = DataFile('datafile.dat')
dataf.add(Record('Ana', 'Vera', 9))
dataf.add(Record('Bety', 'Alzamora', 9))
dataf.add(Record('Federico', 'Ninquispe', 9))
dataf.add(Record('James', 'Quispe', 9))
dataf.scanAll()
