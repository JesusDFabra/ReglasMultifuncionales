"""
Pruebas para corroborar la correcta lectura de los archivos en la carpeta de insumos.
Ejecutar desde la raíz del proyecto: python -m pytest tests/test_lectura_insumos.py -v
O: python -m unittest tests.test_lectura_insumos -v
"""
import sys
from pathlib import Path

# Raíz del proyecto (reglas_multifuncionales)
RAIZ = Path(__file__).resolve().parent.parent
if str(RAIZ) not in sys.path:
    sys.path.insert(0, str(RAIZ))

import unittest
import pandas as pd

from src.config.cargador_config import CargadorConfig
from src.insumos.lector_insumos import LectorInsumos


class TestLecturaInsumos(unittest.TestCase):
    """Verifica que la configuración y la lectura de insumos funcionen correctamente."""

    @classmethod
    def setUpClass(cls):
        cls.config = CargadorConfig()
        cls.lector = LectorInsumos(cls.config)
        cls.dir_insumos = cls.config.obtener_directorio_insumos()
        cls.fecha = cls.config.obtener_fecha_proceso()

    def test_config_carga_y_directorio_insumos_existe(self):
        """La configuración carga y el directorio de insumos existe."""
        conf = self.config.cargar()
        self.assertIn("insumos", conf)
        self.assertIn("gestion_erestrad", conf["insumos"])
        self.assertIn("consolidado_cajeros_cuadrados", conf["insumos"])
        self.assertTrue(
            self.dir_insumos.is_dir(),
            f"El directorio de insumos debe existir: {self.dir_insumos}",
        )

    def test_rutas_insumos_se_construyen_correctamente(self):
        """Las rutas de gestión y consolidado se construyen con la fecha de proceso."""
        ruta_gestion = self.lector._ruta_archivo("gestion_erestrad", self.fecha)
        ruta_consolidado = self.lector._ruta_archivo("consolidado_cajeros_cuadrados", self.fecha)
        self.assertEqual(ruta_gestion.parent, self.dir_insumos)
        self.assertEqual(ruta_consolidado.parent, self.dir_insumos)
        self.assertIn(self.fecha, ruta_gestion.name)
        self.assertIn(self.fecha, ruta_consolidado.name)
        self.assertTrue(ruta_gestion.name.endswith(".xlsx"))
        self.assertTrue(ruta_consolidado.name.endswith(".xlsx"))

    def test_leer_gestion_erestrad_si_existe(self):
        """Si existe el archivo de gestión, se lee y devuelve un DataFrame con columna tipo_registro."""
        ruta = self.lector._ruta_archivo("gestion_erestrad", self.fecha)
        if not ruta.exists():
            self.skipTest(f"Archivo de gestión no encontrado: {ruta}. Coloque el archivo para probar la lectura.")
        df = self.lector.leer_gestion_erestrad(fecha=self.fecha)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df.columns), 0, "Debe tener al menos una columna")
        columnas_norm = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
        self.assertIn(
            "tipo_registro",
            columnas_norm,
            msg="El archivo de gestión debe tener columna 'tipo_registro'. Columnas: " + str(list(df.columns)),
        )

    def test_leer_consolidado_cajeros_cuadrados_si_existe(self):
        """Si existe el archivo consolidado, se lee y devuelve un DataFrame con columna tipo_cajero."""
        ruta = self.lector._ruta_archivo("consolidado_cajeros_cuadrados", self.fecha)
        if not ruta.exists():
            self.skipTest(f"Archivo consolidado no encontrado: {ruta}. Coloque el archivo para probar la lectura.")
        df = self.lector.leer_consolidado_cajeros_cuadrados(fecha=self.fecha)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df.columns), 0, "Debe tener al menos una columna")
        columnas_norm = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
        self.assertIn(
            "tipo_cajero",
            columnas_norm,
            msg="El consolidado debe tener columna 'tipo_cajero'. Columnas: " + str(list(df.columns)),
        )

    def test_leer_arqueos_mf_si_existe(self):
        """Si existe el archivo ARQUEOS MF del mes actual, se lee correctamente."""
        from datetime import datetime
        hoy = datetime.now()
        ruta = self.lector._ruta_arqueos_mf(mes=hoy.month, anio=hoy.year)
        if not ruta.exists():
            self.skipTest(f"Archivo ARQUEOS MF no encontrado: {ruta}. Coloque el archivo para probar la lectura.")
        df = self.lector.leer_arqueos_mf(mes=hoy.month, anio=hoy.year)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df.columns), 0, "Debe tener al menos una columna")


if __name__ == "__main__":
    unittest.main(verbosity=2)
