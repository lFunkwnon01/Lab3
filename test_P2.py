import os
import sys
import tempfile
import shutil
from P2 import HashFile, Record

class TestHashFile:
    def __init__(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, 'test_hash.dat')
        self.passed_tests = 0
        self.total_tests = 0
        
    def cleanup(self):
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def assert_equal(self, actual, expected, test_name):
        self.total_tests += 1
        if actual == expected:
            print(f"[PASS] {test_name}: PASSED")
            self.passed_tests += 1
        else:
            print(f"[FAIL] {test_name}: FAILED - Expected {expected}, got {actual}")
    
    def assert_not_none(self, value, test_name):
        self.total_tests += 1
        if value is not None:
            print(f"[PASS] {test_name}: PASSED")
            self.passed_tests += 1
        else:
            print(f"[FAIL] {test_name}: FAILED - Expected non-None value, got None")
    
    def assert_none(self, value, test_name):
        self.total_tests += 1
        if value is None:
            print(f"[PASS] {test_name}: PASSED")
            self.passed_tests += 1
        else:
            print(f"[FAIL] {test_name}: FAILED - Expected None, got {value}")

    def test_basic_insert_and_search(self):
        print("\n=== Test: Inserción y búsqueda básica ===")
        
        hash_file = HashFile(self.test_file, 6)
        
        record1 = Record(100, "Producto A", 10, 25.50, "2024-01-01")
        record2 = Record(200, "Producto B", 5, 15.75, "2024-01-02")
        record3 = Record(300, "Producto C", 8, 30.00, "2024-01-03")
        
        hash_file.insert(record1)
        hash_file.insert(record2)
        hash_file.insert(record3)
        
        result1 = hash_file.search(100)
        result2 = hash_file.search(200)
        result3 = hash_file.search(300)
        
        self.assert_equal(result1, 100, "Búsqueda registro 100")
        self.assert_equal(result2, 200, "Búsqueda registro 200")
        self.assert_equal(result3, 300, "Búsqueda registro 300")

    def test_search_nonexistent_record(self):
        print("\n=== Test: Búsqueda de registro inexistente ===")
        
        hash_file = HashFile(self.test_file, 6)
        
        record1 = Record(100, "Producto A", 10, 25.50, "2024-01-01")
        hash_file.insert(record1)
        
        result = hash_file.search(999)
        self.assert_none(result, "Búsqueda registro inexistente")

    def test_collision_handling(self):
        print("\n=== Test: Manejo de colisiones ===")
        
        hash_file = HashFile(self.test_file, 6)
        
        # Con 6 buckets: 6, 12, 18 todos van al bucket 0
        record1 = Record(6, "Producto 6", 10, 25.50, "2024-01-01")
        record2 = Record(12, "Producto 12", 5, 15.75, "2024-01-02")
        record3 = Record(18, "Producto 18", 8, 30.00, "2024-01-03")
        
        hash_file.insert(record1)
        hash_file.insert(record2)
        hash_file.insert(record3)
        
        result1 = hash_file.search(6)
        result2 = hash_file.search(12)
        result3 = hash_file.search(18)
        
        self.assert_equal(result1, 6, "Búsqueda después de colisión - registro 6")
        self.assert_equal(result2, 12, "Búsqueda después de colisión - registro 12")
        self.assert_equal(result3, 18, "Búsqueda después de colisión - registro 18")

    def test_bucket_overflow(self):
        print("\n=== Test: Desbordamiento de bucket ===")
        
        hash_file = HashFile(self.test_file, 6)
        
        # 5 registros > BLOCK_FACTOR (3), todos van al bucket 0
        records = []
        for i in range(5):
            key = i * 6  # 0, 6, 12, 18, 24
            record = Record(key, f"Producto {key}", i+1, 10.0 + i, f"2024-01-0{i+1}")
            records.append(record)
            hash_file.insert(record)
        
        for i, record in enumerate(records):
            result = hash_file.search(record.id)
            self.assert_equal(result, record.id, f"Búsqueda overflow - registro {record.id}")

    def test_remove_existing_record(self):
        print("\n=== Test: Eliminación de registro existente ===")
        
        hash_file = HashFile(self.test_file, 6)
        
        record1 = Record(100, "Producto A", 10, 25.50, "2024-01-01")
        record2 = Record(200, "Producto B", 5, 15.75, "2024-01-02")
        
        hash_file.insert(record1)
        hash_file.insert(record2)
        
        result1 = hash_file.search(100)
        result2 = hash_file.search(200)
        self.assert_equal(result1, 100, "Registro 100 existe antes de eliminar")
        self.assert_equal(result2, 200, "Registro 200 existe antes de eliminar")
        
        hash_file.remove(100)
        
        result1_after = hash_file.search(100)
        result2_after = hash_file.search(200)
        
        self.assert_none(result1_after, "Registro 100 eliminado correctamente")
        self.assert_equal(result2_after, 200, "Registro 200 sigue existiendo")

    def test_remove_nonexistent_record(self):
        print("\n=== Test: Eliminación de registro inexistente ===")
        
        hash_file = HashFile(self.test_file, 6)
        
        record1 = Record(100, "Producto A", 10, 25.50, "2024-01-01")
        hash_file.insert(record1)
        
        hash_file.remove(999)  # No debería causar error
        
        result = hash_file.search(100)
        self.assert_equal(result, 100, "Registro original no afectado por eliminación inexistente")

    def test_remove_from_overflow_bucket(self):
        print("\n=== Test: Eliminación en bucket de overflow ===")
        
        hash_file = HashFile(self.test_file, 6)
        
        # Llenar bucket principal y crear overflow
        records = []
        for i in range(5):
            key = i * 6
            record = Record(key, f"Producto {key}", i+1, 10.0 + i, f"2024-01-0{i+1}")
            records.append(record)
            hash_file.insert(record)
        
        hash_file.remove(24)  # Último insertado
        
        result_removed = hash_file.search(24)
        self.assert_none(result_removed, "Registro eliminado del overflow")
        
        for record in records[:-1]:
            result = hash_file.search(record.id)
            self.assert_equal(result, record.id, f"Registro {record.id} sigue existiendo")

    def test_sequential_operations(self):
        print("\n=== Test: Operaciones secuenciales mixtas ===")
        
        hash_file = HashFile(self.test_file, 6)
        
        for i in range(1, 11):
            record = Record(i*10, f"Producto {i*10}", i, 10.0 + i, f"2024-01-{i:02d}")
            hash_file.insert(record)
        
        for i in range(1, 11):
            result = hash_file.search(i*10)
            self.assert_equal(result, i*10, f"Registro {i*10} insertado correctamente")
        
        # Eliminar registros pares
        for i in range(2, 11, 2):
            hash_file.remove(i*10)
        
        for i in range(1, 11):
            result = hash_file.search(i*10)
            if i % 2 == 0:
                self.assert_none(result, f"Registro {i*10} eliminado correctamente")
            else:
                self.assert_equal(result, i*10, f"Registro {i*10} sigue existiendo")

    def run_all_tests(self):
        print("Iniciando pruebas funcionales para HashFile")
        print("=" * 60)
        
        try:
            self.test_basic_insert_and_search()
            self.test_search_nonexistent_record()
            self.test_collision_handling()
            self.test_bucket_overflow()
            self.test_remove_existing_record()
            self.test_remove_nonexistent_record()
            self.test_remove_from_overflow_bucket()
            self.test_sequential_operations()
            
        except Exception as e:
            print(f"[ERROR] Error durante las pruebas: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            self.cleanup()
        
        print("\n" + "=" * 60)
        print(f"RESUMEN DE PRUEBAS:")
        print(f"Pruebas pasadas: {self.passed_tests}")
        print(f"Pruebas fallidas: {self.total_tests - self.passed_tests}")
        print(f"Total de pruebas: {self.total_tests}")
        
        if self.passed_tests == self.total_tests:
            print("Todas las pruebas pasaron exitosamente!")
        else:
            print("Algunas pruebas fallaron. Revisa la implementación.")
        
        return self.passed_tests == self.total_tests


if __name__ == "__main__":
    tester = TestHashFile()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)
