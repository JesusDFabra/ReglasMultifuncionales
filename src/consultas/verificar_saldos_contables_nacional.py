"""
Verifica que el saldo contable de cada cajero (registros recientes en ARQUEOS MF)
coincida con el saldo en NACIONAL del día anterior al día del arqueo.
Consulta: GCOLIBRANL.GCOFFSD{MM} con columna SALD{d} (saldo del día d del mes MM).
"""
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Any, Union
import pandas as pd
import logging

logger = logging.getLogger(__name__)

CUENTA = 110505075
CODOFI_EXCLUIDO = 976


def _nombre_columna_saldo(dia: int) -> str:
    """Columna SALD1 .. SALD31 según el día."""
    return f"SALD{dia}"


def _tabla_mes(mes: int) -> str:
    """Nombre de tabla GCOFFSD01 .. GCOFFSD12."""
    return f"GCOFFSD{mes:02d}"


def consultar_saldo_contable_nacional(
    admin_bd,
    nit: int,
    anio: int,
    mes: int,
    dia: int,
) -> Optional[float]:
    """
    Consulta en NACIONAL el saldo contable del cajero (NIT) para el día dado.
    Tabla GCOLIBRANL.GCOFFSD{MM}, columna SALD{d}.

    Returns:
        Saldo (float) o None si no hay fila o hay error.
    """
    tabla = _tabla_mes(mes)
    col_saldo = _nombre_columna_saldo(dia)
    consulta = f"""
    SELECT CODOFI, NIT, {col_saldo} AS SALDO_CONTABLE
    FROM GCOLIBRANL.{tabla}
    WHERE CAST(CLASE * 100000000 AS INT)
        + CAST(GRUPO * 10000000 AS INT)
        + CAST(CUENTA * 100000 AS INT)
        + CAST(SUBCTA * 1000 AS INT)
        + CAST(AUXBIC AS INT) = {CUENTA}
      AND CODOFI <> {CODOFI_EXCLUIDO}
      AND NIT = {int(nit)}
    """
    try:
        df = admin_bd.consultar(consulta, mantener_conexion=True)
        if df.empty or "SALDO_CONTABLE" not in df.columns:
            return None
        val = df["SALDO_CONTABLE"].iloc[0]
        if pd.isna(val):
            return None
        return float(val)
    except Exception as e:
        logger.debug("Error consultando saldo NIT=%s %04d-%02d-%02d: %s", nit, anio, mes, dia, e)
        return None


def _parsear_fecha_arqueo(val) -> Optional[datetime]:
    """Convierte valor de celda (fecha) a datetime. Retorna None si no se puede."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    try:
        return pd.to_datetime(val)
    except Exception:
        return None


def _obtener_fecha_anterior(fecha_arqueo: datetime) -> tuple:
    """Retorna (anio, mes, dia) del día anterior a fecha_arqueo."""
    anterior = fecha_arqueo - timedelta(days=1)
    return anterior.year, anterior.month, anterior.day


def _buscar_columna(df: pd.DataFrame, nombres: List[str]) -> Optional[str]:
    """Retorna la primera columna cuyo nombre normalizado está en nombres."""
    for c in df.columns:
        if (str(c).strip().lower() in [n.lower() for n in nombres]):
            return c
    return None


def _valor_a_fecha(val) -> Optional[date]:
    """Convierte valor de celda a date para comparación. Retorna None si no se puede."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, pd.Timestamp):
        return val.date()
    try:
        dt = pd.to_datetime(val)
        return dt.date() if hasattr(dt, "date") else dt
    except Exception:
        return None


def verificar_saldos_contables(
    df_arqueos: pd.DataFrame,
    admin_bd,
    fecha_descarga_filtro: Optional[date] = None,
    tolerancia: float = 0.0,
) -> List[Dict[str, Any]]:
    """
    Compara Saldo contable del Excel con el saldo en NACIONAL del día anterior al Fecha Arqueo.
    Solo se verifican las filas donde "Fecha descarga arqueo" coincide con fecha_descarga_filtro
    (p. ej. fecha de hoy = los que recién se pegaron).

    Args:
        df_arqueos: DataFrame de la hoja ARQUEOS MF.
        admin_bd: AdminBDNacional ya creado (no se cierra aquí).
        fecha_descarga_filtro: Solo filas con esta fecha en "Fecha descarga arqueo". Si None, no se filtra (todas).
        tolerancia: Diferencia absoluta permitida para considerar igual (0 = debe ser igual).

    Returns:
        Lista de dicts con: cajero, fecha_arqueo, saldo_excel, saldo_nacional, diferencia.
    """
    col_cajero = _buscar_columna(df_arqueos, ["Cajero", "cajero"])
    col_fecha_arqueo = _buscar_columna(df_arqueos, ["Fecha Arqueo", "Fecha arqueo", "fecha_arqueo"])
    col_fecha_descarga = _buscar_columna(df_arqueos, ["Fecha descarga arqueo", "Fecha Descarga Arqueo"])
    col_saldo = _buscar_columna(df_arqueos, ["Saldo contable", "Saldo Contable", "saldo_contable"])

    if not col_cajero or not col_saldo:
        logger.warning("No se encontraron columnas Cajero y/o Saldo contable en el DataFrame.")
        return []

    if not col_fecha_arqueo:
        logger.warning("No se encontró columna Fecha Arqueo; no se puede calcular día anterior.")

    filas = df_arqueos
    if fecha_descarga_filtro is not None and col_fecha_descarga is not None:
        mask = df_arqueos[col_fecha_descarga].apply(lambda v: _valor_a_fecha(v) == fecha_descarga_filtro)
        filas = df_arqueos[mask]
        logger.info("Filtrando por Fecha descarga arqueo = %s: %d filas", fecha_descarga_filtro, len(filas))
    elif fecha_descarga_filtro is not None and col_fecha_descarga is None:
        logger.warning("No se encontró columna 'Fecha descarga arqueo'; no se puede filtrar por fecha.")

    discrepancias = []
    for idx, row in filas.iterrows():
        cajero = row.get(col_cajero)
        if cajero is None or (isinstance(cajero, float) and pd.isna(cajero)):
            continue
        try:
            nit = int(float(cajero))
        except (ValueError, TypeError):
            continue

        saldo_excel = row.get(col_saldo)
        if saldo_excel is None or (isinstance(saldo_excel, float) and pd.isna(saldo_excel)):
            continue
        try:
            saldo_excel_val = float(saldo_excel)
        except (ValueError, TypeError):
            continue

        if col_fecha_arqueo:
            fecha_arqueo = _parsear_fecha_arqueo(row.get(col_fecha_arqueo))
            if fecha_arqueo is None:
                discrepancias.append({
                    "cajero": nit,
                    "fecha_arqueo": None,
                    "saldo_excel": saldo_excel_val,
                    "saldo_nacional": None,
                    "diferencia": None,
                    "motivo": "Sin fecha de arqueo",
                })
                continue
            anio, mes, dia = _obtener_fecha_anterior(fecha_arqueo)
        else:
            discrepancias.append({
                "cajero": nit,
                "fecha_arqueo": None,
                "saldo_excel": saldo_excel_val,
                "saldo_nacional": None,
                "diferencia": None,
                "motivo": "Sin columna Fecha Arqueo",
            })
            continue

        saldo_nal = consultar_saldo_contable_nacional(admin_bd, nit, anio, mes, dia)
        if saldo_nal is None:
            discrepancias.append({
                "cajero": nit,
                "fecha_arqueo": fecha_arqueo.strftime("%Y-%m-%d"),
                "saldo_excel": saldo_excel_val,
                "saldo_nacional": None,
                "diferencia": None,
                "motivo": "Sin saldo en NACIONAL (día anterior)",
            })
            continue

        if abs(saldo_excel_val - saldo_nal) > tolerancia:
            discrepancias.append({
                "cajero": nit,
                "fecha_arqueo": fecha_arqueo.strftime("%Y-%m-%d"),
                "saldo_excel": saldo_excel_val,
                "saldo_nacional": saldo_nal,
                "diferencia": saldo_excel_val - saldo_nal,
                "motivo": "No coincide",
            })

    return discrepancias


MENSAJE_SALDO_NO_COINCIDE = "El saldo contable al día anterior del arqueo no coincide con NACIONAL."


def _fecha_discrepancia_a_date(fecha_arqueo_str) -> Optional[date]:
    """Convierte fecha_arqueo de un registro de discrepancia (str YYYY-MM-DD o None) a date."""
    if fecha_arqueo_str is None:
        return None
    try:
        return datetime.strptime(str(fecha_arqueo_str).strip()[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def marcar_discrepancias_gestion_a_realizar(
    lector,
    mes: int,
    anio: int,
    hoja: Union[str, int],
    discrepancias: List[Dict[str, Any]],
    mensaje: str = MENSAJE_SALDO_NO_COINCIDE,
) -> None:
    """
    Escribe en la columna "Gestión a Realizar" del ARQUEOS MF el mensaje indicado
    en las filas que corresponden a las discrepancias (mismo Cajero y Fecha Arqueo).
    """
    if not discrepancias:
        return
    df = lector.leer_arqueos_mf(mes=mes, anio=anio, hoja=hoja)
    col_cajero = _buscar_columna(df, ["Cajero", "cajero"])
    col_fecha = _buscar_columna(df, ["Fecha Arqueo", "Fecha arqueo", "fecha_arqueo"])
    nombres_gestion = [
        "Gestión a Realizar", "Gestion a Realizar", "Gestión a realizar",
        "Gestion a realizar", "GESTIÓN A REALIZAR", "GESTION A REALIZAR",
    ]
    col_gestion = _buscar_columna(df, nombres_gestion)
    if not col_gestion:
        # Crear la columna si no existe para poder marcar las discrepancias
        col_gestion = "Gestión a Realizar"
        df[col_gestion] = ""
        logger.info("Columna '%s' no existía; se creó para marcar discrepancias.", col_gestion)
    if not col_cajero:
        logger.warning("No se encontró columna Cajero en ARQUEOS MF; no se puede marcar discrepancias.")
        return

    # Conjunto (cajero, fecha_arqueo_date) para identificar filas a marcar
    claves = set()
    for r in discrepancias:
        c = r.get("cajero")
        if c is None:
            continue
        try:
            nit = int(c)
        except (ValueError, TypeError):
            continue
        fd = _fecha_discrepancia_a_date(r.get("fecha_arqueo"))
        claves.add((nit, fd))

    filas_marcadas = 0
    for idx, row in df.iterrows():
        if pd.isna(row.get(col_cajero)):
            continue
        try:
            cajero_val = int(float(row.get(col_cajero)))
        except (ValueError, TypeError):
            continue
        fecha_val = _valor_a_fecha(row.get(col_fecha)) if col_fecha else None
        if (cajero_val, fecha_val) in claves:
            df.at[idx, col_gestion] = mensaje
            filas_marcadas += 1

    lector.guardar_arqueos_mf(df, mes=mes, anio=anio, hoja=hoja)
    logger.info("Marcadas %d fila(s) en columna 'Gestión a Realizar' (saldo no coincide).", filas_marcadas)


def ejecutar_verificacion(
    lector,
    mes: Optional[int] = None,
    anio: Optional[int] = None,
    fecha_descarga_filtro: Optional[date] = None,
    hoja: Optional[int] = 0,
) -> List[Dict[str, Any]]:
    """
    Lee ARQUEOS MF, conecta a NACIONAL desde config, y retorna lista de cajeros
    cuyo saldo contable no coincide con NACIONAL (día anterior al arqueo).
    Solo se verifican filas con "Fecha descarga arqueo" = fecha_descarga_filtro (p. ej. hoy).
    """
    from src.config.cargador_config import CargadorConfig
    from src.consultas.admin_bd import crear_admin_nacional_desde_config

    config = CargadorConfig()
    admin = crear_admin_nacional_desde_config(config)
    if not admin:
        logger.warning("BD no configurada o usar_bd=false; no se puede verificar saldos en NACIONAL.")
        return []

    df = lector.leer_arqueos_mf(mes=mes, anio=anio, hoja=hoja)
    try:
        admin.conectar()
        discrepancias = verificar_saldos_contables(
            df, admin, fecha_descarga_filtro=fecha_descarga_filtro
        )
        return discrepancias
    finally:
        admin.desconectar()
