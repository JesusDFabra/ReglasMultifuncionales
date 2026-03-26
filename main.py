"""
Punto de entrada para reglas de cajeros multifuncionales.
Por defecto ejecuta el copiado y pegado a ARQUEOS MF (gestión + consolidado) con la fecha del día actual.
"""
import sys
from pathlib import Path
from datetime import datetime, date
from typing import Optional

# Asegurar que el proyecto esté en el path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import logging
from src.config.cargador_config import CargadorConfig
from src.insumos.lector_insumos import LectorInsumos
from src.procesamiento.pegar_gestion_a_arqueos_mf import pegar_gestion_a_arqueos_mf, pegar_consolidado_a_arqueos_mf
from src.consultas.verificar_saldos_contables_nacional import ejecutar_verificacion, marcar_discrepancias_gestion_a_realizar
from src.consultas.movimientos_remanente import procesar_cuadrados_fecha_descarga
from src.consultas.admin_bd import crear_admin_nacional_desde_config

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _fecha_hoy() -> str:
    """Fecha de hoy en formato DD_MM_YYYY."""
    hoy = datetime.now()
    return hoy.strftime("%d_%m_%Y")


def _parsear_fecha_dd_mm_yyyy(fecha: str):
    """Convierte DD_MM_YYYY a (dia, mes, anio)."""
    partes = fecha.strip().replace("-", "_").split("_")
    if len(partes) != 3:
        raise ValueError(f"Fecha debe ser DD_MM_YYYY: {fecha}")
    d, m, a = int(partes[0]), int(partes[1]), int(partes[2])
    return d, m, a


def _fecha_str_a_date(fecha: str) -> date:
    """Convierte DD_MM_YYYY a date."""
    d, m, a = _parsear_fecha_dd_mm_yyyy(fecha)
    return date(a, m, d)


def _existen_registros_fecha_descarga_hoy(lector: LectorInsumos, fecha: str, mes: int, anio: int, hoja) -> bool:
    """
    Verifica si ARQUEOS MF ya contiene filas con "Fecha descarga arqueo" igual a la fecha indicada.
    Si existen, se debe omitir el copiado de insumos (gestión/consolidado).
    """
    import pandas as pd

    fecha_obj = _fecha_str_a_date(fecha)
    df_arq = lector.leer_arqueos_mf(mes=mes, anio=anio, hoja=hoja)

    col_fecha_descarga = None
    for c in df_arq.columns:
        n = str(c).strip().lower()
        if n in ("fecha descarga arqueo", "fecha_descarga_arqueo", "fecha descarga arqueo "):
            col_fecha_descarga = c
            break
    if col_fecha_descarga is None:
        logger.warning("No se encontró columna 'Fecha descarga arqueo' en ARQUEOS MF; se continuará con copiado.")
        return False

    fechas = pd.to_datetime(df_arq[col_fecha_descarga], errors="coerce").dt.date
    existe = bool((fechas == fecha_obj).any())
    if existe:
        logger.info(
            "ARQUEOS MF ya tiene registros con Fecha descarga arqueo = %s. Se omite copiado de gestión y consolidado.",
            fecha,
        )
    return existe


def main(
    fecha: str = None,
    solo_leer: bool = False,
    pegar_gestion: bool = None,
    pegar_consolidado: bool = None,
    verificar_saldo: bool = False,
    reset_arqueos_hoy: bool = False,
    procesar_cuadrados: bool = False,
    mes: int = None,
    anio: int = None,
    hoja_arqueos: str = None,
):
    """
    Por defecto: ejecuta el copiado y pegado a ARQUEOS MF (gestión + consolidado) con la fecha del día.

    Args:
        fecha: DD_MM_YYYY. Si no se pasa, se usa la fecha del día actual.
        solo_leer: Si True, solo lee insumos y no pega en ARQUEOS MF.
        pegar_gestion: Si True, pega gestión; si False explícito, no. Por defecto True (si no es solo_leer).
        pegar_consolidado: Si True, pega consolidado; si False explícito, no. Por defecto True (si no es solo_leer).
        verificar_saldo: Si True, verifica contra NACIONAL solo las filas con "Fecha descarga arqueo" = fecha (las recién pegadas).
        reset_arqueos_hoy: Si True, borra del ARQUEOS MF todas las filas con "Fecha descarga arqueo" = fecha (luego se puede pegar de nuevo).
        procesar_cuadrados: Si True, procesa cajeros cuadrados (770500 día arqueo, sobrantes si faltante) y ratifica/marca Gestión a Realizar.
        mes: Mes 1-12 para ARQUEOS MF (por defecto desde fecha usada).
        anio: Año para ARQUEOS MF (por defecto desde fecha usada).
        hoja_arqueos: Nombre o índice de hoja en ARQUEOS MF (por defecto primera hoja).
    """
    config = CargadorConfig()
    lector = LectorInsumos(config)
    # Fecha del día actual si no se indica otra
    fecha_usar = fecha or _fecha_hoy()
    logger.info("Leyendo insumos para fecha: %s", fecha_usar)

    if hoja_arqueos is None:
        hoja = 0
    elif str(hoja_arqueos).strip().isdigit():
        hoja = int(hoja_arqueos)
    else:
        hoja = hoja_arqueos

    d, m, a = _parsear_fecha_dd_mm_yyyy(fecha_usar)
    mes_usar = mes if mes is not None else m
    anio_usar = anio if anio is not None else a

    # Reset: borrar del ARQUEOS MF todo lo de la fecha indicada (para ejecutar flujo completo de nuevo)
    if reset_arqueos_hoy:
        lector.quitar_filas_por_fecha_descarga_arqueo(
            fecha_filtro=_fecha_str_a_date(fecha_usar),
            mes=mes_usar,
            anio=anio_usar,
            hoja=hoja,
        )

    # Procesar cuadrados: revisar 770500 día arqueo, sobrantes si faltante, ratificar y marcar "Cajero cuadrado..."
    if procesar_cuadrados:
        admin = crear_admin_nacional_desde_config(config)
        if not admin:
            logger.warning("BD no configurada; no se puede procesar cuadrados.")
        else:
            try:
                admin.conectar()
                n = procesar_cuadrados_fecha_descarga(
                    lector, admin, mes_usar, anio_usar, hoja,
                    fecha_descarga=_fecha_str_a_date(fecha_usar),
                )
                logger.info("Procesamiento de cuadrados finalizado: %d registro(s) actualizados.", n)
            finally:
                admin.desconectar()
        return

    # Si solo se pide verificar saldo, ejecutar solo eso y salir (solo filas con Fecha descarga arqueo = fecha_usar)
    if verificar_saldo:
        fecha_filtro = _fecha_str_a_date(fecha_usar)
        discrepancias = ejecutar_verificacion(
            lector, mes=mes_usar, anio=anio_usar, fecha_descarga_filtro=fecha_filtro, hoja=hoja
        )
        if not discrepancias:
            logger.info("Verificación saldo contable vs NACIONAL: ninguna discrepancia (filas con Fecha descarga arqueo = %s).", fecha_usar)
        else:
            logger.info("Verificación saldo contable vs NACIONAL: %d registro(s) donde NO coincide el saldo:", len(discrepancias))
            for r in discrepancias:
                logger.info(
                    "  Cajero %s | Fecha arqueo %s | Saldo Excel %.2f | Saldo NACIONAL %s | Diferencia %s | %s",
                    r.get("cajero"),
                    r.get("fecha_arqueo"),
                    r.get("saldo_excel"),
                    r.get("saldo_nacional") if r.get("saldo_nacional") is not None else "N/A",
                    r.get("diferencia") if r.get("diferencia") is not None else "N/A",
                    r.get("motivo", ""),
                )
            marcar_discrepancias_gestion_a_realizar(lector, mes_usar, anio_usar, hoja, discrepancias)
        return

    # Por defecto ejecutar pegado (gestión y consolidado) salvo que sea solo_leer
    ejecutar_pegar = not solo_leer
    omitir_copiado_por_fecha_existente = False
    if ejecutar_pegar:
        if pegar_gestion is None and pegar_consolidado is None:
            pegar_gestion, pegar_consolidado = True, True
        else:
            if pegar_gestion is None:
                pegar_gestion = False
            if pegar_consolidado is None:
                pegar_consolidado = False

        # Nueva regla: si ya existen filas con Fecha descarga arqueo = fecha_usar, no volver a copiar.
        if _existen_registros_fecha_descarga_hoy(lector, fecha_usar, mes_usar, anio_usar, hoja):
            pegar_gestion = False
            pegar_consolidado = False
            omitir_copiado_por_fecha_existente = True
    else:
        pegar_gestion = pegar_consolidado = False

    if pegar_gestion or pegar_consolidado or omitir_copiado_por_fecha_existente:
        if pegar_gestion:
            df_gestion = lector.leer_gestion_erestrad(fecha=fecha_usar)
            pegar_gestion_a_arqueos_mf(
                df_gestion=df_gestion,
                lector=lector,
                mes=mes_usar,
                anio=anio_usar,
                hoja_arqueos_mf=hoja,
            )
            logger.info("Pegado de gestión a ARQUEOS MF completado (mes=%s, anio=%s).", mes_usar, anio_usar)
        if pegar_consolidado:
            df_consolidado = lector.leer_consolidado_cajeros_cuadrados(fecha=fecha_usar)
            pegar_consolidado_a_arqueos_mf(
                df_consolidado=df_consolidado,
                lector=lector,
                mes=mes_usar,
                anio=anio_usar,
                hoja_arqueos_mf=hoja,
                fecha_proceso=fecha_usar,
            )
            logger.info("Pegado de consolidado a ARQUEOS MF completado (mes=%s, anio=%s).", mes_usar, anio_usar)
        if omitir_copiado_por_fecha_existente:
            logger.info("Copiado omitido por regla de fecha ya existente; se continúa con el resto del proceso.")
        # Aplicar reglas: cuadrados (770500 día arqueo, sobrantes si faltante, ratificar, texto Gestión a Realizar)
        admin = crear_admin_nacional_desde_config(config)
        if admin:
            try:
                admin.conectar()
                n = procesar_cuadrados_fecha_descarga(
                    lector, admin, mes_usar, anio_usar, hoja,
                    fecha_descarga=_fecha_str_a_date(fecha_usar),
                )
                logger.info("Reglas aplicadas (cuadrados): %d registro(s) actualizados.", n)
            finally:
                admin.desconectar()
        else:
            logger.warning("BD no configurada; no se aplicaron reglas de cuadrados (770500/sobrantes).")

        # NUEVA REGLA: en gestión, grabar sobrante para cajeros cuyo ARQUEOS MF indique
        # contabilización centralizada en cuenta 279510020.
        try:
            n_grabar = lector.aplicar_regla_grabar_sobrante_desde_arqueos_mf(
                fecha=fecha_usar,
                hoja_gestion=None,
                hoja_arqueos_mf=0,
            )
            if n_grabar:
                logger.info("Regla grabar sobrante aplicada: %d registro(s) modificados en gestión.", n_grabar)
        except Exception as e:
            logger.warning("No se pudo aplicar regla grabar sobrante en gestión: %s", e)

        # NUEVA REGLA: sincronizar Gestión (ARQUEO) con "ACLARAR DIFERENCIA Y REPETIR EL ARQUEO"
        try:
            modificados = lector.aplicar_regla_arqueo_espera_aclarar_sucursal(fecha=fecha_usar, hoja_gestion=None, hoja_arqueos_mf=0)
            if modificados:
                logger.info("Regla ARQUEO aclarar/sucursal aplicada: %d registro(s) modificados en gestión.", modificados)
        except Exception as e:
            logger.warning("No se pudo aplicar la regla ARQUEO aclarar/sucursal en gestión: %s", e)

        # Prioridad: si ARQUEOS MF marca cajero cuadrado (fecha descarga = hoy), observaciones en gestión = CUADRADO EN ARQUEO.
        try:
            n_cuad = lector.aplicar_observaciones_cuadrado_desde_arqueos_mf(
                fecha=fecha_usar, hoja_gestion=None, hoja_arqueos_mf=0
            )
            if n_cuad:
                logger.info("Regla observaciones CUADRADO EN ARQUEO aplicada: %d fila(s) en gestión.", n_cuad)
        except Exception as e:
            logger.warning("No se pudo aplicar regla CUADRADO EN ARQUEO en gestión: %s", e)
        return

    try:
        df_gestion = lector.leer_gestion_erestrad(fecha=fecha_usar)
        df_consolidado = lector.leer_consolidado_cajeros_cuadrados(fecha=fecha_usar)
        logger.info("Gestion erestrad: %d filas, %d columnas", len(df_gestion), len(df_gestion.columns))
        logger.info("Consolidado cajeros cuadrados: %d filas, %d columnas", len(df_consolidado), len(df_consolidado.columns))

        # Cantidad de registros que se copiarían al pegar (mismos filtros que el pegado)
        if "tipo_registro" in df_gestion.columns:
            n_arqueo_gestion = (df_gestion["tipo_registro"].astype(str).str.strip().str.upper() == "ARQUEO").sum()
            logger.info("  -> Registros a copiar desde gestión (tipo_registro=ARQUEO): %d", n_arqueo_gestion)
        if "tipo_cajero" in df_consolidado.columns:
            tipo_str = df_consolidado["tipo_cajero"].astype(str).str.strip().str.upper()
            n_arqueo_consolidado = tipo_str.str.contains("MULTIFUNCIONAL", na=False).sum()
            logger.info("  -> Registros a copiar desde consolidado (tipo_cajero contiene 'multifuncional'): %d", n_arqueo_consolidado)

        return df_gestion, df_consolidado
    except FileNotFoundError as e:
        logger.error("%s", e)
        raise


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(
        description="Ejecuta copiado y pegado a ARQUEOS MF (gestión + consolidado) con fecha del día. Use --solo-leer para solo cargar insumos."
    )
    p.add_argument("--fecha", type=str, help="Fecha DD_MM_YYYY (por defecto: hoy)")
    p.add_argument("--solo-leer", action="store_true", help="Solo leer insumos y mostrar cuántos registros se copiarían desde cada archivo (no pega en ARQUEOS MF)")
    p.add_argument("--pegar-gestion", action="store_true", help="Pegar solo gestión (ignora consolidado)")
    p.add_argument("--pegar-consolidado", action="store_true", help="Pegar solo consolidado (ignora gestión)")
    p.add_argument("--mes", type=int, help="Mes 1-12 para ARQUEOS MF (por defecto desde --fecha)")
    p.add_argument("--anio", type=int, help="Año para ARQUEOS MF (por defecto desde --fecha)")
    p.add_argument("--hoja-arqueos", type=str, default=None, help="Hoja del archivo ARQUEOS MF (nombre o 0)")
    p.add_argument("--verificar-saldo", action="store_true",
                   help="Verifica saldo contable vs NACIONAL solo en filas con Fecha descarga arqueo = fecha de hoy (o --fecha)")
    p.add_argument("--reset", action="store_true",
                   help="Borra del ARQUEOS MF todas las filas con Fecha descarga arqueo = hoy (o --fecha); luego ejecuta el flujo normal (pegar)")
    p.add_argument("--procesar-cuadrados", action="store_true",
                   help="Procesa cajeros cuadrados: 770500 día arqueo, sobrantes si faltante, ratifica y marca Gestión a Realizar")
    args = p.parse_args()
    main(
        fecha=args.fecha,
        solo_leer=args.solo_leer,
        pegar_gestion=args.pegar_gestion if (args.pegar_gestion or args.pegar_consolidado) else None,
        pegar_consolidado=args.pegar_consolidado if (args.pegar_gestion or args.pegar_consolidado) else None,
        verificar_saldo=args.verificar_saldo,
        reset_arqueos_hoy=args.reset,
        procesar_cuadrados=args.procesar_cuadrados,
        mes=args.mes,
        anio=args.anio,
        hoja_arqueos=args.hoja_arqueos,
    )
