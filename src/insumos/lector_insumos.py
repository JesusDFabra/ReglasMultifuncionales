"""
Lee los archivos de insumo de multifuncionales: gestión erestrad, consolidado cajeros cuadrados,
y archivo mensual de arqueos MF (lectura y edición).
"""
from pathlib import Path
from typing import Optional, Tuple, Union
from datetime import datetime, date
import pandas as pd
import logging

from src.config.cargador_config import CargadorConfig, PROYECTO_ROOT

logger = logging.getLogger(__name__)

# Meses en español para el nombre del archivo ARQUEOS MF
MESES_NOMBRE = (
    "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
    "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"
)


class LectorInsumos:
    """Carga los archivos Excel de insumo según la configuración."""

    def __init__(self, config: Optional[CargadorConfig] = None):
        self.config = config or CargadorConfig()

    def _directorio_insumo(self, nombre_insumo: str):
        """Directorio base para un insumo: insumo.directorio si existe, sino directorios.insumos."""
        conf = self.config.cargar()
        insumos = conf.get("insumos", {})
        dir_insumo = insumos.get(nombre_insumo, {}).get("directorio")
        if dir_insumo:
            p = Path(dir_insumo)
            return p if p.is_absolute() else (PROYECTO_ROOT / dir_insumo)
        return self.config.obtener_directorio_insumos()

    def _ruta_archivo(self, nombre_insumo: str, fecha: Optional[str] = None) -> Path:
        """Obtiene la ruta del archivo para un insumo y una fecha DD_MM_YYYY (patron desde config)."""
        conf = self.config.cargar()
        insumos = conf.get("insumos", {})
        if nombre_insumo not in insumos:
            raise KeyError(f"Insumo '{nombre_insumo}' no definido en config")
        if fecha is None:
            fecha = self.config.obtener_fecha_proceso()
        patron = insumos[nombre_insumo].get("patron", "").replace("{fecha}", fecha)
        directorio = self._directorio_insumo(nombre_insumo)
        return directorio / patron

    def leer_gestion_erestrad(self, fecha: Optional[str] = None, hoja: Optional[str] = None) -> pd.DataFrame:
        """
        Lee el archivo gestion_DD_MM_YYYY_erestrad.xlsx.

        Args:
            fecha: DD_MM_YYYY. Si no se pasa, usa config o fecha actual.
            hoja: Nombre de la hoja. Si None, lee la primera.

        Returns:
            DataFrame con el contenido del archivo.
        """
        ruta = self._ruta_archivo("gestion_erestrad", fecha)
        if not ruta.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {ruta}")
        logger.info(f"Leyendo gestión erestrad: {ruta}")
        if hoja:
            df = pd.read_excel(ruta, sheet_name=hoja, engine="openpyxl")
        else:
            df = pd.read_excel(ruta, engine="openpyxl")
        logger.info(f"Filas: {len(df)}, columnas: {len(df.columns)}")
        return df

    def leer_consolidado_cajeros_cuadrados(self, fecha: Optional[str] = None, hoja: Optional[str] = None) -> pd.DataFrame:
        """
        Lee el archivo consolidado_cajeros_cuadrados_DD_MM_YYYY.xlsx.

        Args:
            fecha: DD_MM_YYYY. Si no se pasa, usa config o fecha actual.
            hoja: Nombre de la hoja. Si None, lee la primera.

        Returns:
            DataFrame con el contenido del archivo.
        """
        ruta = self._ruta_archivo("consolidado_cajeros_cuadrados", fecha)
        if not ruta.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {ruta}")
        logger.info(f"Leyendo consolidado cajeros cuadrados: {ruta}")
        if hoja:
            df = pd.read_excel(ruta, sheet_name=hoja, engine="openpyxl")
        else:
            df = pd.read_excel(ruta, engine="openpyxl")
        logger.info(f"Filas: {len(df)}, columnas: {len(df.columns)}")
        return df

    def leer_todos(self, fecha: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Lee ambos insumos y devuelve (gestion_erestrad, consolidado_cajeros_cuadrados).

        Args:
            fecha: DD_MM_YYYY. Si None, usa config o fecha actual.

        Returns:
            Tupla (df_gestion, df_consolidado).
        """
        df_gestion = self.leer_gestion_erestrad(fecha=fecha)
        df_consolidado = self.leer_consolidado_cajeros_cuadrados(fecha=fecha)
        return df_gestion, df_consolidado

    # -------------------------------------------------------------------------
    # Archivo mensual: MM- ARQUEOS MF MES AÑO.xlsx (ej: 02- ARQUEOS MF FEBRERO 2026.xlsx)
    # -------------------------------------------------------------------------

    def _ruta_arqueos_mf(self, mes: Optional[int] = None, anio: Optional[int] = None) -> Path:
        """Ruta del archivo de arqueos MF (patron_mes_anio desde config). Si mes/anio no se pasan, usa mes/año actual."""
        if mes is None or anio is None:
            hoy = datetime.now()
            mes = mes if mes is not None else hoy.month
            anio = anio if anio is not None else hoy.year
        if not (1 <= mes <= 12):
            raise ValueError(f"mes debe estar entre 1 y 12, recibido: {mes}")
        conf = self.config.cargar()
        patron = conf.get("insumos", {}).get("arqueos_mf", {}).get(
            "patron_mes_anio", "{mm:02d}- ARQUEOS MF {mes_nombre} {yyyy}.xlsx"
        )
        nombre = (
            patron.replace("{mm:02d}", f"{mes:02d}")
            .replace("{mes_nombre}", MESES_NOMBRE[mes - 1])
            .replace("{yyyy}", str(anio))
        )
        directorio = self._directorio_insumo("arqueos_mf")
        return directorio / nombre

    def leer_arqueos_mf(
        self,
        mes: Optional[int] = None,
        anio: Optional[int] = None,
        hoja: Optional[Union[str, int]] = None,
    ) -> pd.DataFrame:
        """
        Lee el archivo mensual de arqueos MF (ej: 02- ARQUEOS MF FEBRERO 2026.xlsx).

        Args:
            mes: Número de mes 1-12. Si None, mes actual.
            anio: Año (ej: 2026). Si None, año actual.
            hoja: Nombre o índice de hoja. Si None, primera hoja.

        Returns:
            DataFrame con el contenido de la hoja.
        """
        ruta = self._ruta_arqueos_mf(mes=mes, anio=anio)
        if not ruta.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {ruta}")
        logger.info("Leyendo arqueos MF: %s", ruta)
        if hoja is not None:
            df = pd.read_excel(ruta, sheet_name=hoja, engine="openpyxl")
        else:
            df = pd.read_excel(ruta, engine="openpyxl")
        logger.info("Filas: %d, columnas: %d", len(df), len(df.columns))
        return df

    def _ruta_historico_cuadre(self) -> Path:
        """Ruta del archivo de historico cuadre (patron desde config)."""
        conf = self.config.cargar()
        insumos = conf.get("insumos", {})
        patron = insumos.get("historico_cuadre_cajeros_sucursales", {}).get("patron", "HISTORICO_CUADRE_CAJEROS_SUCURSALES.xlsx")
        directorio = self._directorio_insumo("historico_cuadre_cajeros_sucursales")
        return directorio / patron

    def leer_historico_cuadre_cajeros_sucursales(self, hoja: Optional[Union[str, int]] = None) -> pd.DataFrame:
        """
        Lee HISTORICO_CUADRE_CAJEROS_SUCURSALES.xlsx (tipo_registro, fecha_arqueo, cajero, etc.).
        Se usa para obtener la fecha del ultimo arqueo cuando en ARQUEOS MF solo hay un registro del cajero.
        """
        ruta = self._ruta_historico_cuadre()
        if not ruta.exists():
            raise FileNotFoundError(f"No se encontro el archivo: {ruta}")
        logger.info("Leyendo historico cuadre cajeros sucursales: %s", ruta)
        if hoja is not None:
            df = pd.read_excel(ruta, sheet_name=hoja, engine="openpyxl")
        else:
            df = pd.read_excel(ruta, engine="openpyxl")
        logger.info("Historico: %d filas, %d columnas", len(df), len(df.columns))
        return df

    def guardar_arqueos_mf(
        self,
        df: pd.DataFrame,
        mes: Optional[int] = None,
        anio: Optional[int] = None,
        hoja: Union[str, int] = 0,
    ) -> Path:
        """
        Guarda (edita) el archivo mensual de arqueos MF. Si el archivo tiene varias hojas,
        se reemplaza solo la hoja indicada y se mantienen el resto.

        Args:
            df: DataFrame a escribir.
            mes: Número de mes 1-12. Si None, mes actual.
            anio: Año. Si None, año actual.
            hoja: Nombre o índice de la hoja a reemplazar (default 0).

        Returns:
            Path del archivo guardado.
        """
        ruta = self._ruta_arqueos_mf(mes=mes, anio=anio)
        if ruta.exists():
            # Leer todas las hojas, reemplazar la indicada y volver a escribir
            with pd.ExcelFile(ruta, engine="openpyxl") as xl:
                nombres_hojas = xl.sheet_names
            reemplazar = (lambda i, n: i == hoja) if isinstance(hoja, int) else (lambda i, n: n == hoja)
            dict_hojas = {}
            for i, nombre in enumerate(nombres_hojas):
                if reemplazar(i, nombre):
                    dict_hojas[nombre] = df
                else:
                    dict_hojas[nombre] = pd.read_excel(ruta, sheet_name=nombre, engine="openpyxl")
            with pd.ExcelWriter(ruta, engine="openpyxl") as writer:
                for nombre, frame in dict_hojas.items():
                    frame.to_excel(writer, sheet_name=nombre, index=False)
        else:
            df.to_excel(ruta, index=False, engine="openpyxl")
        logger.info("Guardado arqueos MF: %s", ruta)
        return ruta

    def _buscar_columna_arqueos(self, df: pd.DataFrame, nombres: tuple) -> Optional[str]:
        """Retorna la primera columna cuyo nombre normalizado está en nombres."""
        for c in df.columns:
            if (str(c).strip().lower() in [n.lower() for n in nombres]):
                return c
        return None

    def quitar_filas_por_fecha_descarga_arqueo(
        self,
        fecha_filtro: date,
        mes: Optional[int] = None,
        anio: Optional[int] = None,
        hoja: Union[str, int] = 0,
    ) -> int:
        """
        Borra del ARQUEOS MF todas las filas donde "Fecha descarga arqueo" = fecha_filtro.
        Útil para resetear lo del día y volver a pegar (flujo completo).

        Returns:
            Número de filas eliminadas.
        """
        df = self.leer_arqueos_mf(mes=mes, anio=anio, hoja=hoja)
        col = self._buscar_columna_arqueos(df, ("Fecha descarga arqueo", "Fecha Descarga Arqueo"))
        if not col:
            logger.warning("No se encontró columna 'Fecha descarga arqueo'; no se puede hacer reset.")
            return 0

        def valor_a_fecha(val):
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            if isinstance(val, date) and not isinstance(val, datetime):
                return val
            if isinstance(val, datetime):
                return val.date()
            try:
                dt = pd.to_datetime(val)
                return dt.date() if hasattr(dt, "date") else dt
            except Exception:
                return None

        mask = df[col].apply(lambda v: valor_a_fecha(v) == fecha_filtro)
        n_quitar = int(mask.sum())
        if n_quitar == 0:
            logger.info("Reset ARQUEOS MF: no hay filas con Fecha descarga arqueo = %s.", fecha_filtro)
            return 0
        df_nuevo = df[~mask].copy()
        self.guardar_arqueos_mf(df_nuevo, mes=mes, anio=anio, hoja=hoja)
        logger.info("Reset ARQUEOS MF: eliminadas %d fila(s) con Fecha descarga arqueo = %s.", n_quitar, fecha_filtro)
        return n_quitar
