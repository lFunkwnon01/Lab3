import struct
import os
import pandas as pd

BLOCK_FACTOR = 3

class Record:
    FORMAT = 'i30sif10s'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, id: int, name: str, cant: int, price: str, date: str):
        self.id = id
        self.name = name
        self.cant = cant
        self.price = price
        self.date = date

    def pack(self):
        return struct.pack(self.FORMAT, self.id, self.name[:30].encode(), 
                           self.cant, self.price, self.date[:10].encode())

    @staticmethod
    def unpack(data):
        id, name, cant, price, date = struct.unpack(Record.FORMAT, data)
        return Record(id, name.decode().rstrip('\x00'), cant, 
                      price, date.decode().rstrip('\x00'))

class Bucket:
    BUCKET_SIZE = (BLOCK_FACTOR * Record.SIZE_OF_RECORD) + 4

    def __init__(self, records=[], next_bucket=-1):
        self.records = records
        self.next_bucket = next_bucket


    def __init__(self, records=[], next_bucket=-1):
        self.records = records
        self.next_bucket = next_bucket

    def pack(self):
        records_data = b''
        
        for record in self.records:
            records_data += record.pack()
            
        i = len(self.records)
        while i < BLOCK_FACTOR: 
            records_data += b'\x00' * Record.SIZE_OF_RECORD
            i += 1
            
        next_bucket_data = struct.pack('i', self.next_bucket)
        return records_data + next_bucket_data
    
    @staticmethod
    def unpack(data: bytes):
        if len(data) < Bucket.BUCKET_SIZE:
            data = data.ljust(Bucket.BUCKET_SIZE, b'\x00')
            
        records = []
        for i in range(BLOCK_FACTOR):
            offset = i * Record.SIZE_OF_RECORD
            record_data = data[offset:offset + Record.SIZE_OF_RECORD]
            record = Record.unpack(record_data)
            if record is not None:
                records.append(record)
        
        next_bucket_data = data[-4:]
        next_bucket = struct.unpack('i', next_bucket_data)[0]
        return Bucket(records, next_bucket)
    
class HashFile:

    def __init__(self, filename: str, num_buckets: int = 6):
        self.filename = filename
        self.num_buckets = num_buckets
        if not os.path.exists(filename):
            with open(filename, 'w+b') as f:
                f.write(b'\x00')
        self._create_buckets()

    def import_from_csv(self, csv_file: str, sep=';'):
        df = pd.read_csv(csv_file, sep=sep)
        for i, row in df.iterrows():
            record = Record(row['ID de la venta'], row['Nombre producto'], row['Cantidad vendida'], 
                            row['Precio unitario'], row['Fecha de venta'])
            self.insert(record)
        print("Imported")

    def _create_buckets(self):
        with open(self.filename, 'w+b') as f:
            for i in range(self.num_buckets):
                empty_bucket = Bucket([], -1)
                f.write(empty_bucket.pack())

    def hash(self, key: int):
        return key % self.num_buckets

    def insert(self, record: Record):
        pos = self.hash(record.id)
        
        with open(self.filename, 'r+b') as f:
            f.seek(0, 2)
            file_size = f.tell()
            total_buckets = file_size // Bucket.BUCKET_SIZE
            
            curr_pos = pos
            curr_bucket = None
            
            while True:
                f.seek(curr_pos * Bucket.BUCKET_SIZE)
                bucket_data = f.read(Bucket.BUCKET_SIZE)
                curr_bucket = Bucket.unpack(bucket_data)
                
                if len(curr_bucket.records) < BLOCK_FACTOR:
                    curr_bucket.records.append(record)
                    f.seek(curr_pos * Bucket.BUCKET_SIZE)
                    f.write(curr_bucket.pack())
                    return
                
                if curr_bucket.next_bucket <= 0:
                    new_pos = total_buckets
                    new_bucket = Bucket([record], -1)
                    f.seek(new_pos * Bucket.BUCKET_SIZE)
                    f.write(new_bucket.pack())
                    
                    curr_bucket.next_bucket = new_pos
                    f.seek(curr_pos * Bucket.BUCKET_SIZE)
                    f.write(curr_bucket.pack())
                    total_buckets += 1
                    return
                
                if curr_bucket.next_bucket >= total_buckets:
                    curr_bucket.next_bucket = -1
                    continue
                    
                curr_pos = curr_bucket.next_bucket
                if curr_pos <= 0:
                    curr_bucket.next_bucket = -1
                    continue

    def search(self, key: int):
        with open(self.filename, 'rb') as f:
            pos = self.hash(key)
            f.seek(pos * Bucket.BUCKET_SIZE)
            bucket_data = f.read(Bucket.BUCKET_SIZE)
            bucket = Bucket.unpack(bucket_data)

            while True:
                for record in bucket.records:
                    if record.id == key:
                        return record.id
                if bucket.next_bucket == -1:
                    break
                f.seek(bucket.next_bucket * Bucket.BUCKET_SIZE)
                bucket_data = f.read(Bucket.BUCKET_SIZE)
                bucket = Bucket.unpack(bucket_data)

    def remove(self, key: int):
        with open(self.filename, 'r+b') as f:
            pos = self.hash(key)
            curr_pos = pos
            f.seek(curr_pos * Bucket.BUCKET_SIZE)
            bucket_data = f.read(Bucket.BUCKET_SIZE)
            bucket = Bucket.unpack(bucket_data)

            while True:
                for record in bucket.records:
                    if record.id == key:
                        bucket.records.remove(record)
                        f.seek(curr_pos * Bucket.BUCKET_SIZE)
                        f.write(bucket.pack())
                        return
                if bucket.next_bucket == -1:
                    break
                curr_pos = bucket.next_bucket
                f.seek(curr_pos * Bucket.BUCKET_SIZE)
                bucket_data = f.read(Bucket.BUCKET_SIZE)
                bucket = Bucket.unpack(bucket_data)     
        print("Removed")

    def remove_file(self):
        try:
            if os.path.exists(self.filename):
                os.remove(self.filename)
        except Exception as e:
            pass