"""
Módulo para administrar conexiones a bases de datos mediante ODBC.
Mismo patrón que reglas_dispensadores (CertificacionArqueo).
"""

import pyodbc
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class AdminBD:
    """
    Clase base para administrar conexiones a bases de datos mediante ODBC.
    """

    def __init__(self, servidor: str, usuario: str, clave: str):
        self.servidor = servidor
        self.usuario = usuario
        self.clave = clave
        self.conn = None
        self._conexion_abierta = False

    def conectar(self) -> pyodbc.Connection:
        """Establece la conexión. Si ya hay una abierta y válida, la reutiliza."""
        if self._conexion_abierta and self.conn:
            try:
                cursor = self.conn.cursor()
                cursor.close()
                logger.debug("Reutilizando conexión existente a DSN: %s", self.servidor)
                return self.conn
            except Exception:
                self._conexion_abierta = False
                self.conn = None

        try:
            self.conn = pyodbc.connect(
                f"DSN={self.servidor}; CCSID=37; TRANSLATE=1; UID={self.usuario}; PWD={self.clave}"
            )
            self._conexion_abierta = True
            logger.info("Conexión establecida a DSN: %s", self.servidor)
            return self.conn
        except Exception as e:
            logger.error("Error al conectar a %s: %s", self.servidor, e)
            self._conexion_abierta = False
            raise

    def consultar(self, consulta: str, mantener_conexion: bool = True) -> pd.DataFrame:
        """Ejecuta una consulta SQL y retorna un DataFrame."""
        try:
            self.conectar()
            logger.debug("Ejecutando consulta en %s", self.servidor)
            df = pd.read_sql(consulta, self.conn)
            logger.debug("Consulta ejecutada. Registros: %d", len(df))
            return df
        except Exception as e:
            logger.error("Error al ejecutar consulta: %s", e)
            self._conexion_abierta = False
            raise
        finally:
            if not mantener_conexion:
                self.desconectar()

    def desconectar(self):
        """Cierra la conexión."""
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
                self._conexion_abierta = False
                logger.info("Conexión cerrada a DSN: %s", self.servidor)
            except Exception as e:
                logger.warning("Error al cerrar conexión: %s", e)
                self._conexion_abierta = False


class AdminBDNacional(AdminBD):
    """Administrador de base de datos para servidor NACIONAL (DSN ODBC)."""

    def __init__(self, usuario: str, clave: str):
        super().__init__("NACIONAL", usuario, clave)


def crear_admin_nacional_desde_config(cargador_config=None):
    """
    Crea una instancia de AdminBDNacional con usuario y clave desde config/insumos.yaml.
    Si usar_bd es False o faltan usuario/clave, retorna None.

    Returns:
        AdminBDNacional o None
    """
    from src.config.cargador_config import CargadorConfig
    config = cargador_config or CargadorConfig()
    bd = config.obtener_config_bd()
    if not bd.get("usar_bd") or not bd.get("usuario") or not bd.get("clave"):
        return None
    return AdminBDNacional(bd["usuario"], bd["clave"])
