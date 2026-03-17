"""
Consulta movimientos en la cuenta del cajero (día del arqueo) con NROCMP 810291 o 770500
y actualiza la columna Remanente/Provisión/Ajustes en ARQUEOS MF.

Regla:
- NROCMP 770500 (banco): siempre se tiene en cuenta si se grabó el día del arqueo.
- NROCMP 810291 (sucursal): solo se tiene en cuenta si el movimiento puede ser justificado
  por la diferencia que queda después de ajustar con el comprobante del banco (770500).
"""
import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import date
import pandas as pd

logger = logging.getLogger(__name__)

CUENTA_CAJERO = 110505075
CUENTA_SOBRANTES = 279510020
CODOFI_EXCLUIDO = 976
NROCMP_BANCO = 770500      # Siempre se considera el día del arqueo
NROCMP_SUCURSAL = 810291  # Solo si justifica la diferencia tras ajustar 770500
NROCMP_PROVISION = (810291, 770500)
TOLERANCIA_JUSTIFICAR = 1.0  # 1 peso de tolerancia al comparar diferencia con 810291


def consultar_movimientos_dia_arqueo(
    admin_bd,
    nit: int,
    anio: int,
    mes: int,
    dia: int,
) -> Optional[List[Dict[str, Any]]]:
    """
    Movimientos en la cuenta del cajero (110505075) el día del arqueo,
    con NROCMP 810291 o 770500.

    Returns:
        Lista de dicts con VALOR, NROCMP, NUMDOC, etc. o None si error.
    """
    consulta = f"""
    SELECT (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, VALOR, NROCMP, NUMDOC,
           ANOELB, MESELB, DIAELB, NIT
    FROM gcolibranl.gcoffmvint
    WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) = {CUENTA_CAJERO}
      AND ANOELB = {anio}
      AND MESELB = {mes}
      AND DIAELB = {dia}
      AND NIT = {nit}
      AND (NROCMP = 810291 OR NROCMP = 770500)
    ORDER BY NROCMP, VALOR
    """
    try:
        df = admin_bd.consultar(consulta, mantener_conexion=True)
        if df is None or df.empty:
            return []
        return df.to_dict("records")
    except Exception as e:
        logger.error("Error al consultar movimientos día arqueo: %s", e)
        return None


def consultar_sobrantes_negativos_vigentes(
    admin_bd,
    nit: int,
    anio: int,
    mes: int,
    dia: int,
    cuenta: int = CUENTA_SOBRANTES,
    nrocmp: int = NROCMP_BANCO,
) -> Optional[List[Dict[str, Any]]]:
    """
    En cuenta de sobrantes (279510020): valores negativos vigentes.
    Busca desde la fecha del arqueo hacia atrás; se detiene al encontrar el primer valor positivo.
    Esos negativos son sobrantes vigentes (para justificar un faltante).
    """
    fecha_arqueo = anio * 10000 + mes * 100 + dia
    fecha_inicio = anio * 10000 + mes * 100 + 1  # día 1 del mes
    consulta = f"""
    SELECT (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, VALOR, NROCMP, NUMDOC
    FROM gcolibranl.gcoffmvint
    WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) = {cuenta}
      AND CODOFI <> {CODOFI_EXCLUIDO}
      AND NIT = {nit}
      AND NROCMP = {nrocmp}
      AND (ANOELB*10000+MESELB*100+DIAELB) BETWEEN {fecha_inicio} AND {fecha_arqueo}
    ORDER BY FECHA DESC
    """
    try:
        df = admin_bd.consultar(consulta, mantener_conexion=True)
        if df is None or df.empty:
            return []
        # Primer bloque de negativos desde el más reciente hasta encontrar un positivo
        vigentes = []
        for _, row in df.iterrows():
            v = float(row.get("VALOR", 0) or 0)
            if v < 0:
                vigentes.append(row.to_dict())
            else:
                break  # primer positivo, nos detenemos
        return vigentes
    except Exception as e:
        logger.error("Error al consultar sobrantes negativos vigentes: %s", e)
        return None


def calcular_remanente_segun_regla(
    movimientos: List[Dict[str, Any]],
    saldo_contable: float,
    efectivo_arqueado: float,
    dispensado_corte_arqueo: float,
    recibido_corte_arqueo: float,
    tolerancia: float = TOLERANCIA_JUSTIFICAR,
) -> Tuple[float, Dict[str, Any]]:
    """
    Calcula el valor a escribir en Remanente/Provisión/Ajustes según la regla:
    - 770500 (banco): siempre se suma (día del arqueo).
    - 810291 (sucursal): solo se suma si justifica la diferencia restante tras 770500.

    Args:
        movimientos: Lista de dicts con VALOR, NROCMP (de consultar_movimientos_dia_arqueo).
        saldo_contable, efectivo_arqueado, dispensado_corte_arqueo, recibido_corte_arqueo: valores del registro.
        tolerancia: margen para considerar que un 810291 "justifica" la diferencia.

    Returns:
        (remanente_final_a_escribir, detalle) con detalle para log (remanente_banco, remanente_810291, etc.).
    """
    detalle = {"remanente_banco": 0.0, "remanente_810291": 0.0, "diferencia_sin_remanente": None, "justificado_810291": False}
    movs_banco = [m for m in movimientos if m.get("NROCMP") == NROCMP_BANCO or int(float(m.get("NROCMP", 0))) == NROCMP_BANCO]
    movs_sucursal = [m for m in movimientos if m.get("NROCMP") == NROCMP_SUCURSAL or int(float(m.get("NROCMP", 0))) == NROCMP_SUCURSAL]

    # Siempre: remanente banco (770500) como positivo en la columna
    remanente_banco = sum(abs(float(m.get("VALOR", 0) or 0)) for m in movs_banco)
    detalle["remanente_banco"] = remanente_banco

    # Diferencia sin remanente: Saldo - (Efectivo + dispensado - recibido)
    d0 = saldo_contable - (efectivo_arqueado + dispensado_corte_arqueo - recibido_corte_arqueo)
    detalle["diferencia_sin_remanente"] = d0

    # Diferencia después de ajustar solo con el banco (770500)
    diferencia_after_banco = d0 - remanente_banco
    if abs(diferencia_after_banco) <= tolerancia:
        return (remanente_banco, detalle)

    # Buscar un movimiento 810291 que justifique la diferencia restante
    for m in movs_sucursal:
        valor_810291 = abs(float(m.get("VALOR", 0) or 0))
        if abs(valor_810291 - abs(diferencia_after_banco)) <= tolerancia:
            detalle["remanente_810291"] = valor_810291
            detalle["justificado_810291"] = True
            return (remanente_banco + valor_810291, detalle)

    return (remanente_banco, detalle)


def calcular_remanente_para_cajero_cuadrado(
    admin_bd,
    nit: int,
    anio: int,
    mes: int,
    dia: int,
    saldo_contable: float,
    efectivo_arqueado: float,
    dispensado_corte_arqueo: float,
    recibido_corte_arqueo: float,
    tolerancia: float = TOLERANCIA_JUSTIFICAR,
) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    Para un cajero que está CUADRADO (diferencia sin remanente ≈ 0):
    1) Revisa movimientos 770500 (banco) el día del arqueo en cuenta cajero (110505075).
       Si hay alguno, se agrega al ajuste (Remanente) con el signo contrario al de la BD.
    2) Si después de eso queda faltante: consulta cuenta sobrantes (279510020), valores
       negativos vigentes (desde fecha arqueo hacia atrás, hasta el primer positivo).
       Si algún valor justifica el faltante, se agrega al Remanente.
    3) Si tras 770500 sigue cuadrado → se ratifica (remanente = solo 770500).

    Returns:
        (remanente_final, detalle) con detalle.remanente_banco, .remanente_sobrantes, .ratificado_cuadrado, etc.
    """
    detalle = {
        "remanente_banco": 0.0,
        "remanente_sobrantes": 0.0,
        "diferencia_sin_remanente": None,
        "diferencia_after_banco": None,
        "ratificado_cuadrado": False,
        "justificado_sobrantes": False,
    }
    d0 = saldo_contable - (efectivo_arqueado + (dispensado_corte_arqueo or 0) - (recibido_corte_arqueo or 0))
    detalle["diferencia_sin_remanente"] = d0

    _trace = nit == 5200
    if _trace:
        logger.info("[SEGUIMIENTO 5200] Paso 1 - Datos del registro: Saldo=%.2f, Efectivo=%.2f, dispensado=%.2f, recibido=%.2f → d0 (diferencia sin remanente) = %.2f", saldo_contable, efectivo_arqueado, dispensado_corte_arqueo or 0, recibido_corte_arqueo or 0, d0)

    movs = consultar_movimientos_dia_arqueo(admin_bd, nit, anio, mes, dia)
    if movs is None:
        if _trace:
            logger.info("[SEGUIMIENTO 5200] Paso 2 - Consulta movimientos día arqueo: sin resultados o error.")
        return (None, detalle)
    movs_banco = [m for m in movs if int(float(m.get("NROCMP", 0))) == NROCMP_BANCO]
    # 770500 se agrega al ajuste (Remanente) con el signo contrario al de la BD: valor en BD = faltante a cruzar
    remanente_banco = -sum(float(m.get("VALOR", 0) or 0) for m in movs_banco)
    detalle["remanente_banco"] = remanente_banco

    if _trace:
        logger.info("[SEGUIMIENTO 5200] Paso 2 - Movimientos 770500 (banco) en cuenta cajero (día %04d-%02d-%02d): %d movimiento(s), VALOR en BD: %s → Remanente banco (signo contrario) = %.2f", anio, mes, dia, len(movs_banco), [m.get("VALOR") for m in movs_banco], remanente_banco)

    diferencia_after_banco = d0 - remanente_banco
    detalle["diferencia_after_banco"] = diferencia_after_banco

    if _trace:
        logger.info("[SEGUIMIENTO 5200] Paso 3 - Diferencia después de banco: d0 - remanente_banco = %.2f - (%.2f) = %.2f → %s", d0, remanente_banco, diferencia_after_banco, "FALTANTE (positivo)" if diferencia_after_banco > 0 else ("CUADRADO (≈0)" if abs(diferencia_after_banco) <= tolerancia else "SOBRANTE (negativo)"))

    if abs(diferencia_after_banco) <= tolerancia:
        detalle["ratificado_cuadrado"] = True
        if _trace:
            logger.info("[SEGUIMIENTO 5200] Paso 4 - Conclusión: RATIFICADO CUADRADO. Calificación: \"Cajero cuadrado, sin observación por parte de la sucursal\".")
        return (remanente_banco, detalle)

    if diferencia_after_banco <= 0:
        if _trace:
            logger.info("[SEGUIMIENTO 5200] Paso 4 - Conclusión: SOBRANTE (no se busca en sobrantes). Calificación: sin texto de cruce.")
        return (remanente_banco, detalle)

    # Faltante: buscar en sobrantes (279510020) un valor negativo que justifique el faltante
    faltante = diferencia_after_banco
    vigentes = consultar_sobrantes_negativos_vigentes(admin_bd, nit, anio, mes, dia)
    if _trace:
        logger.info("[SEGUIMIENTO 5200] Paso 4 - Faltante = %.2f. Consulta sobrantes negativos vigentes (279510020): %s", faltante, "ninguno" if not vigentes else f"{len(vigentes)} valor(es) = {[m.get('VALOR') for m in vigentes]}")
    if vigentes is None:
        return (remanente_banco, detalle)
    for m in vigentes:
        valor_neg = float(m.get("VALOR", 0) or 0)
        if valor_neg >= 0:
            continue
        if abs(abs(valor_neg) - faltante) <= tolerancia:
            detalle["remanente_sobrantes"] = abs(valor_neg)
            detalle["justificado_sobrantes"] = True
            detalle["valor_faltante"] = faltante
            # FECHA en BD: (ANOELB*10000+MESELB*100+DIAELB) ej. 20260312 → "12/03/2026"
            fec = m.get("FECHA")
            if fec is not None:
                try:
                    fec_int = int(float(fec))
                    d_sob, m_sob = fec_int % 100, (fec_int // 100) % 100
                    a_sob = fec_int // 10000
                    detalle["fecha_sobrante_str"] = f"{d_sob:02d}/{m_sob:02d}/{a_sob}"
                except (ValueError, TypeError):
                    detalle["fecha_sobrante_str"] = ""
            else:
                detalle["fecha_sobrante_str"] = ""
            if nit == 5200:
                logger.info("[SEGUIMIENTO 5200] Paso 5 - Coincidencia en sobrantes: valor negativo %.2f justifica faltante %.2f. Remanente final = remanente_banco + sobrante = %.2f + %.2f = %.2f. Fecha sobrante: %s", valor_neg, faltante, remanente_banco, abs(valor_neg), remanente_banco + abs(valor_neg), detalle.get("fecha_sobrante_str", ""))
                logger.info("[SEGUIMIENTO 5200] Conclusión: CRUCE FALTANTE-SOBRANTE. Calificación: \"El faltante $X es cruzado con sobrante contabilizado el día (fecha sobrante) el cual es reversado por la seccion el día (fecha descarga)\".")
            return (remanente_banco + abs(valor_neg), detalle)
    if nit == 5200:
        logger.info("[SEGUIMIENTO 5200] Paso 5 - Ningún sobrante vigente coincide con el faltante. Calificación: sin texto de cruce (remanente actualizado solo con banco).")
    return (remanente_banco, detalle)


def _float_val(row, col, default=0.0):
    if col is None or col not in row.index:
        return default
    v = row.get(col)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def procesar_cuadrados_fecha_descarga(
    lector,
    admin_bd,
    mes: int,
    anio: int,
    hoja,
    fecha_descarga: date,
    tolerancia: float = TOLERANCIA_JUSTIFICAR,
) -> int:
    """
    Procesa los cajeros que están CUADRADOS (Diferencia ≈ 0) con Fecha descarga arqueo = fecha_descarga:
    revisa movimientos 770500 el día del arqueo; si hay, ajusta Remanente (signo contrario).
    Si queda faltante, busca en cuenta sobrantes (279510020) negativos vigentes que lo justifiquen.
    Si sigue cuadrado, ratifica. Al final actualiza Excel, fórmulas y texto "Cajero cuadrado...".

    Returns:
        Número de registros actualizados (con Remanente o ratificados).
    """
    df = lector.leer_arqueos_mf(mes=mes, anio=anio, hoja=hoja)
    col_cajero = _buscar_columna(df, ["Cajero", "cajero"])
    col_fecha_arqueo = _buscar_columna(df, ["Fecha Arqueo", "Fecha arqueo"])
    col_fecha_descarga = _buscar_columna(df, ["Fecha descarga arqueo", "Fecha Descarga Arqueo"])
    col_saldo = _buscar_columna(df, ["Saldo Contable", "Saldo contable"])
    col_efectivo = _buscar_columna(df, ["Efectivo Arqueado /Arqueo fisico saldo contadores"])
    col_dispensado = _buscar_columna(df, ["dispensado_corte_arqueo"])
    col_recibido = _buscar_columna(df, ["recibido_corte_arqueo"])
    col_remanente = _buscar_columna(df, ["Remanente /Provisión /Ajustes", "Remanente/Provisión/Ajustes"])
    if not all([col_cajero, col_fecha_arqueo, col_fecha_descarga, col_saldo, col_efectivo]):
        logger.warning("Faltan columnas en ARQUEOS MF para procesar cuadrados.")
        return 0
    if col_remanente is None:
        df["Remanente /Provisión /Ajustes"] = 0.0
        col_remanente = "Remanente /Provisión /Ajustes"
    col_gestion = _buscar_columna(df, ["Gestión a Realizar", "Gestion a Realizar"])
    if col_gestion is None:
        col_gestion = "Gestión a Realizar"
        df[col_gestion] = ""

    from src.procesamiento.pegar_gestion_a_arqueos_mf import TEXTO_CAJERO_CUADRADO
    # Fecha de gestión (día de hoy o de gestión) para el texto de cruce faltante-sobrante
    fecha_gestion_str = f"{fecha_descarga.day:02d}/{fecha_descarga.month:02d}/{fecha_descarga.year}"

    indices_a_actualizar = []
    for idx, row in df.iterrows():
        if _valor_a_fecha(row.get(col_fecha_descarga)) != fecha_descarga:
            continue
        cajero = None
        try:
            cajero = int(float(row[col_cajero]))
        except (ValueError, TypeError):
            continue
        if pd.isna(row.get(col_fecha_arqueo)):
            continue
        fa = _valor_a_fecha(row.get(col_fecha_arqueo))
        if fa is None:
            continue
        saldo = _float_val(row, col_saldo)
        efectivo = _float_val(row, col_efectivo)
        dispensado = _float_val(row, col_dispensado)
        recibido = _float_val(row, col_recibido)
        remanente_actual = _float_val(row, col_remanente)
        d0 = saldo - (efectivo + dispensado - recibido)
        diferencia_actual = d0 - remanente_actual
        if abs(diferencia_actual) > tolerancia:
            continue  # no está cuadrado
        if cajero == 5200:
            logger.info("[SEGUIMIENTO 5200] --- Inicio seguimiento cajero 5200 --- Fecha arqueo: %s, Fecha descarga: %s. Registro está CUADRADO (diferencia_actual=%.2f). Se valida en BD.", fa, fecha_descarga, diferencia_actual)
        anio_a, mes_a, dia_a = fa.year, fa.month, fa.day
        remanente_final, detalle = calcular_remanente_para_cajero_cuadrado(
            admin_bd, cajero, anio_a, mes_a, dia_a,
            saldo, efectivo, dispensado, recibido, tolerancia,
        )
        if remanente_final is None:
            if cajero == 5200:
                logger.info("[SEGUIMIENTO 5200] Validación en BD devolvió None (error o sin movimientos). No se actualiza.")
            continue
        df.at[idx, col_remanente] = remanente_final
        indices_a_actualizar.append(idx)
        # No tocar Gestión a Realizar si ya tiene valor (no sobrescribir)
        valor_gestion_actual = df.at[idx, col_gestion]
        gestion_no_vacia = valor_gestion_actual is not None and not (isinstance(valor_gestion_actual, float) and pd.isna(valor_gestion_actual)) and str(valor_gestion_actual).strip() != ""
        if cajero == 5200:
            logger.info("[SEGUIMIENTO 5200] --- Escritura en Gestión a Realizar --- Rama: %s", "ratificado_cuadrado" if detalle.get("ratificado_cuadrado") else ("justificado_sobrantes (CRUCE)" if detalle.get("justificado_sobrantes") else "remanente actualizado (banco)"))
        if gestion_no_vacia:
            logger.info("Cajero %s (F. arqueo %s): Gestión a Realizar ya tiene valor; no se sobrescribe. Remanente actualizado a %.2f.", cajero, fa, remanente_final)
        elif detalle.get("ratificado_cuadrado"):
            df.at[idx, col_gestion] = TEXTO_CAJERO_CUADRADO
            logger.info("Cajero %s (F. arqueo %s): ratificado cuadrado, remanente banco %.2f.", cajero, fa, detalle.get("remanente_banco", 0))
        elif detalle.get("justificado_sobrantes"):
            # Texto: "El faltante $X es cruzado con sobrante contabilizado el día DD/MM/YYYY el cual es reversado por la seccion el día DD/MM/YYYY"
            valor_faltante = detalle.get("valor_faltante", 0)
            fecha_arqueo_str = f"{fa.day:02d}/{fa.month:02d}/{fa.year}"
            fecha_sob = detalle.get("fecha_sobrante_str") or fecha_arqueo_str
            valor_formato = f"{int(round(valor_faltante)):,}".replace(",", ".")
            texto_gestion = f"El faltante ${valor_formato} es cruzado con sobrante contabilizado el día {fecha_sob} el cual es reversado por la seccion el día {fecha_gestion_str}"
            df.at[idx, col_gestion] = texto_gestion
            logger.info("Cajero %s (F. arqueo %s): faltante justificado con sobrantes, remanente %.2f (banco %.2f + sobrantes %.2f). Gestión: %s",
                        cajero, fa, remanente_final, detalle.get("remanente_banco", 0), detalle.get("remanente_sobrantes", 0), texto_gestion[:60])
        else:
            logger.info("Cajero %s (F. arqueo %s): remanente actualizado %.2f (banco %.2f).", cajero, fa, remanente_final, detalle.get("remanente_banco", 0))

    if not indices_a_actualizar:
        logger.info("Ningún registro cuadrado con Fecha descarga arqueo = %s para actualizar.", fecha_descarga)
        return 0
    # No aplicar texto de forma masiva: Gestión a Realizar solo se actualiza en las filas ya validadas en BD (arriba)
    lector.guardar_arqueos_mf(df, mes=mes, anio=anio, hoja=hoja)
    ruta = lector._ruta_arqueos_mf(mes=mes, anio=anio)
    from src.procesamiento.pegar_gestion_a_arqueos_mf import _escribir_formula_diferencia_en_excel
    _escribir_formula_diferencia_en_excel(ruta)
    logger.info("Procesados %d cajero(s) cuadrado(s) con Fecha descarga arqueo = %s.", len(indices_a_actualizar), fecha_descarga)
    return len(indices_a_actualizar)


def suma_remanente_dia_arqueo(
    admin_bd,
    nit: int,
    anio: int,
    mes: int,
    dia: int,
) -> Optional[float]:
    """
    Suma de VALOR de los movimientos (NROCMP 810291/770500) del día del arqueo.
    Deprecated: use calcular_remanente_segun_regla con los datos del registro para aplicar la regla banco/sucursal.
    """
    movs = consultar_movimientos_dia_arqueo(admin_bd, nit, anio, mes, dia)
    if movs is None:
        return None
    if not movs:
        return 0.0
    total = sum(float(m.get("VALOR", 0) or 0) for m in movs)
    return total


def _buscar_columna(df: pd.DataFrame, nombres: list) -> Optional[str]:
    for c in df.columns:
        if (str(c).strip().lower() in [str(n).strip().lower() for n in nombres]):
            return c
    return None


def _valor_a_fecha(val) -> Optional[date]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if hasattr(val, "date"):
        return val.date()
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def calcular_remanente_para_registro(
    admin_bd,
    cajero: int,
    fecha_arqueo: date,
    saldo_contable: float,
    efectivo_arqueado: float,
    dispensado_corte_arqueo: float,
    recibido_corte_arqueo: float,
) -> Tuple[Optional[float], Optional[Dict[str, Any]]]:
    """
    Para un registro, consulta movimientos del día del arqueo y calcula Remanente
    según la regla (770500 siempre; 810291 solo si justifica la diferencia).

    Returns:
        (remanente_a_escribir, detalle) o (None, None) si error en consulta.
    """
    anio, mes, dia = fecha_arqueo.year, fecha_arqueo.month, fecha_arqueo.day
    movs = consultar_movimientos_dia_arqueo(admin_bd, cajero, anio, mes, dia)
    if movs is None:
        return (None, None)
    remanente, detalle = calcular_remanente_segun_regla(
        movs,
        saldo_contable,
        efectivo_arqueado,
        dispensado_corte_arqueo or 0.0,
        recibido_corte_arqueo or 0.0,
    )
    return (remanente, detalle)


def actualizar_remanente_registro(
    lector,
    mes: int,
    anio: int,
    hoja,
    cajero: int,
    fecha_arqueo: date,
    valor_remanente: float,
) -> bool:
    """
    Actualiza la columna Remanente/Provisión/Ajustes para el registro
    que coincida con Cajero y Fecha Arqueo. Escribe el valor como positivo.
    """
    df = lector.leer_arqueos_mf(mes=mes, anio=anio, hoja=hoja)
    col_cajero = _buscar_columna(df, ["Cajero", "cajero"])
    col_fecha = _buscar_columna(df, ["Fecha Arqueo", "Fecha arqueo"])
    col_remanente = _buscar_columna(
        df,
        ["Remanente /Provisión /Ajustes", "Remanente /Provisión /Ajustes", "Remanente/Provisión/Ajustes"],
    )
    if not col_cajero or not col_remanente:
        logger.warning("No se encontraron columnas Cajero o Remanente en ARQUEOS MF.")
        return False
    encontrado = False
    for idx, row in df.iterrows():
        try:
            c = int(float(row[col_cajero]))
        except (ValueError, TypeError):
            continue
        if pd.isna(row.get(col_cajero)):
            continue
        if c != cajero:
            continue
        f = _valor_a_fecha(row.get(col_fecha)) if col_fecha else None
        if f != fecha_arqueo:
            continue
        # Escribir valor como positivo en Remanente
        df.at[idx, col_remanente] = abs(float(valor_remanente))
        encontrado = True
        # Solo actualizar Gestión a Realizar en esta fila si queda cuadrado (validación ya hecha por quien llama)
        col_saldo = _buscar_columna(df, ["Saldo Contable", "Saldo contable"])
        col_efectivo = _buscar_columna(df, ["Efectivo Arqueado /Arqueo fisico saldo contadores"])
        col_dispensado = _buscar_columna(df, ["dispensado_corte_arqueo"])
        col_recibido = _buscar_columna(df, ["recibido_corte_arqueo"])
        col_gestion = _buscar_columna(df, ["Gestión a Realizar", "Gestion a Realizar"])
        if col_gestion and col_saldo and col_efectivo and col_dispensado is not None and col_recibido is not None:
            saldo = _float_val(row, col_saldo)
            efectivo = _float_val(row, col_efectivo)
            dispensado = _float_val(row, col_dispensado)
            recibido = _float_val(row, col_recibido)
            remanente = abs(float(valor_remanente))
            diferencia = saldo - (efectivo + (dispensado or 0) - (recibido or 0)) - remanente
            if abs(diferencia) <= TOLERANCIA_JUSTIFICAR:
                valor_actual = df.at[idx, col_gestion]
                if not (isinstance(valor_actual, str) and "es cruzado con sobrante" in valor_actual):
                    from src.procesamiento.pegar_gestion_a_arqueos_mf import TEXTO_CAJERO_CUADRADO
                    df.at[idx, col_gestion] = TEXTO_CAJERO_CUADRADO
        break
    if not encontrado:
        logger.warning("No se encontró registro Cajero=%s Fecha Arqueo=%s en ARQUEOS MF.", cajero, fecha_arqueo)
        return False
    lector.guardar_arqueos_mf(df, mes=mes, anio=anio, hoja=hoja)
    # Reescribir fórmulas Diferencia y Naturaleza para que recalculen con el nuevo Remanente
    ruta = lector._ruta_arqueos_mf(mes=mes, anio=anio)
    from src.procesamiento.pegar_gestion_a_arqueos_mf import _escribir_formula_diferencia_en_excel
    _escribir_formula_diferencia_en_excel(ruta)
    logger.info("Remanente actualizado: Cajero %s, Fecha Arqueo %s, valor %.2f.", cajero, fecha_arqueo, valor_remanente)
    return True
