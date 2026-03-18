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
from datetime import date, timedelta
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
           ANOELB, MESELB, DIAELB, NIT, CLVMOV
    FROM gcolibranl.gcoffmvint
    WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) = {CUENTA_CAJERO}
      AND ANOELB = {anio}
      AND MESELB = {mes}
      AND DIAELB = {dia}
      AND NIT = {nit}
      AND (NROCMP = 810291 OR NROCMP = 770500)
    ORDER BY NROCMP, CLVMOV
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
    fecha_desde: Optional[date] = None,
) -> Optional[List[Dict[str, Any]]]:
    """
    En cuenta de sobrantes (279510020): valores negativos vigentes.

    Significado en la cuenta 279510020:
    - VALOR negativo: sobrante contabilizado (hubo exceso en cajero, se registra como sobrante vigente
      para poder "cruzar" luego con un faltante del mismo monto).
    - VALOR positivo: reversión o cierre de sobrante; al encontrar el primer positivo hacia atrás
      se deja de considerar sobrantes vigentes (ese bloque de negativos ya fue revertido).

    Rango de busqueda:
    - Hasta: siempre un dia antes del arqueo (nunca incluye el dia del arqueo).
    - Desde: si se pasa fecha_desde (fecha del ultimo arqueo del cajero), se usa esa fecha;
      si no, fallback: dia 1 del mes de (arqueo - 1 dia).
    Devuelve el primer bloque de movimientos negativos hasta encontrar un positivo.
    """
    fecha_arqueo_date = date(anio, mes, dia)
    fecha_antes = fecha_arqueo_date - timedelta(days=1)  # un dia antes del arqueo
    fecha_fin = fecha_antes.year * 10000 + fecha_antes.month * 100 + fecha_antes.day
    if fecha_desde is not None:
        fecha_inicio = fecha_desde.year * 10000 + fecha_desde.month * 100 + fecha_desde.day
    else:
        fecha_inicio = fecha_antes.year * 10000 + fecha_antes.month * 100 + 1  # fallback: dia 1 del mes
    consulta = f"""
    SELECT (ANOELB*10000+MESELB*100+DIAELB) AS FECHA, VALOR, NROCMP, NUMDOC
    FROM gcolibranl.gcoffmvint
    WHERE (CLASE*100000000+GRUPO*10000000+CUENTA*100000+SUBCTA*1000+AUXBIC) = {cuenta}
      AND CODOFI <> {CODOFI_EXCLUIDO}
      AND NIT = {nit}
      AND NROCMP = {nrocmp}
      AND (ANOELB*10000+MESELB*100+DIAELB) BETWEEN {fecha_inicio} AND {fecha_fin}
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
    [Legacy] Calcula remanente a partir de una lista de movimientos ya cargada (no consulta BD).
    No usa cuenta sobrantes. La lógica unificada del proyecto está en calcular_remanente_para_cajero_cuadrado.

    Regla aquí: 770500 (banco) en positivo; 810291 (sucursal) solo si justifica la diferencia.

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


def _term_formula(valor: float) -> str:
    """Devuelve un término para la fórmula Excel: '+150132000' o '-710000'."""
    v = valor
    if v >= 0:
        return "+" + str(int(round(v)))
    return str(int(round(v)))  # ya lleva el menos


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
    umbral_faltante_810291: Optional[float] = None,
    umbral_faltante_sobrantes: Optional[float] = None,
    fecha_desde_sobrantes: Optional[date] = None,
) -> Tuple[Optional[float], Dict[str, Any]]:
    """
    1) Ajuste 770500 (signo contrario al de la BD). Construye términos de fórmula.
    2) Si faltante > umbral_810291 (50M): aplicar 810291 en orden CLVMOV (uno a uno) con signo contrario.
    3) Si faltante restante <= umbral_sobrantes (20M): buscar en cuenta sobrantes; si coincide, agregar.
    4) Si faltante > 20M y no hay más 810291: gestion_manual.
    Remanente se escribe como fórmula Excel "=valor1+valor2-valor3..."
    """
    try:
        from src.config.cargador_config import CargadorConfig
        config = CargadorConfig()
        umbrales = config.obtener_umbrales_remanente()
    except Exception:
        umbrales = {"faltante_minimo_para_810291": 50_000_000, "faltante_limite_sobrantes": 20_000_000}
    lim_810291 = umbral_faltante_810291 if umbral_faltante_810291 is not None else umbrales["faltante_minimo_para_810291"]
    lim_sobrantes = umbral_faltante_sobrantes if umbral_faltante_sobrantes is not None else umbrales["faltante_limite_sobrantes"]

    detalle = {
        "remanente_banco": 0.0,
        "remanente_sobrantes": 0.0,
        "remanente_810291": 0.0,
        "diferencia_sin_remanente": None,
        "diferencia_after_banco": None,
        "ratificado_cuadrado": False,
        "justificado_sobrantes": False,
        "gestion_manual": False,
        "formula_remanente": None,
    }
    d0 = saldo_contable - (efectivo_arqueado + (dispensado_corte_arqueo or 0) - (recibido_corte_arqueo or 0))
    detalle["diferencia_sin_remanente"] = d0
    terminos = []  # términos para "=t1+t2-t3..."

    _trace = nit == 5200
    if _trace:
        logger.info("[SEGUIMIENTO 5200] Paso 1 - d0=%.2f, umbral_810291=%.0f, umbral_sobrantes=%.0f", d0, lim_810291, lim_sobrantes)

    movs = consultar_movimientos_dia_arqueo(admin_bd, nit, anio, mes, dia)
    if movs is None:
        return (None, detalle)
    movs_banco = [m for m in movs if int(float(m.get("NROCMP", 0))) == NROCMP_BANCO]
    movs_810291 = [m for m in movs if int(float(m.get("NROCMP", 0))) == NROCMP_SUCURSAL]
    # Ordenar 810291 por CLVMOV (menor primero)
    def _clvmov_key(m):
        c = m.get("CLVMOV")
        if c is None or (isinstance(c, float) and pd.isna(c)):
            return (1, "")
        try:
            return (0, str(c).strip())
        except Exception:
            return (1, "")

    # Separar positivos y negativos (cada grupo ordenado por CLVMOV). No aplicar 2 positivos o 2 negativos seguidos.
    positivos_810291 = sorted([m for m in movs_810291 if float(m.get("VALOR", 0) or 0) > 0], key=_clvmov_key)
    negativos_810291 = sorted([m for m in movs_810291 if float(m.get("VALOR", 0) or 0) < 0], key=_clvmov_key)

    # 770500: signo contrario al de la BD
    remanente = -sum(float(m.get("VALOR", 0) or 0) for m in movs_banco)
    detalle["remanente_banco"] = remanente
    for m in movs_banco:
        v_bd = float(m.get("VALOR", 0) or 0)
        terminos.append(_term_formula(-v_bd))

    diferencia = d0 - remanente
    detalle["diferencia_after_banco"] = diferencia

    # Aplicar 810291 en alternancia: preferir positivo luego negativo; si no hay del turno, aplicar del otro (así se permiten solo negativos o solo positivos).
    aplicados_810291 = []
    ip, in_ = 0, 0
    apply_positive_next = True  # preferir empezar por un positivo
    while diferencia > lim_810291:  # solo entrar si faltante > 50M
        aplicado = False
        if apply_positive_next:
            if ip < len(positivos_810291):
                m = positivos_810291[ip]
                ip += 1
                v_bd = float(m.get("VALOR", 0) or 0)
                aporte = -v_bd
                remanente += aporte
                terminos.append(_term_formula(aporte))
                aplicados_810291.append(aporte)
                apply_positive_next = False
                aplicado = True
            elif in_ < len(negativos_810291):
                m = negativos_810291[in_]
                in_ += 1
                v_bd = float(m.get("VALOR", 0) or 0)
                aporte = -v_bd
                remanente += aporte
                terminos.append(_term_formula(aporte))
                aplicados_810291.append(aporte)
                apply_positive_next = True
                aplicado = True
        else:
            if in_ < len(negativos_810291):
                m = negativos_810291[in_]
                in_ += 1
                v_bd = float(m.get("VALOR", 0) or 0)
                aporte = -v_bd
                remanente += aporte
                terminos.append(_term_formula(aporte))
                aplicados_810291.append(aporte)
                apply_positive_next = True
                aplicado = True
            elif ip < len(positivos_810291):
                m = positivos_810291[ip]
                ip += 1
                v_bd = float(m.get("VALOR", 0) or 0)
                aporte = -v_bd
                remanente += aporte
                terminos.append(_term_formula(aporte))
                aplicados_810291.append(aporte)
                apply_positive_next = False
                aplicado = True
        if not aplicado:
            break
        diferencia = d0 - remanente
        if diferencia <= lim_sobrantes:
            break
    detalle["remanente_810291"] = sum(aplicados_810291)

    if abs(diferencia) <= tolerancia:
        detalle["ratificado_cuadrado"] = True
        detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
        return (remanente, detalle)

    if diferencia <= 0:
        detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
        return (remanente, detalle)

    # Faltante restante: cruce cuando un sobrante o la suma de sobrantes >= faltante
    faltante = diferencia
    if faltante <= lim_sobrantes:
        vigentes = consultar_sobrantes_negativos_vigentes(
            admin_bd, nit, anio, mes, dia, fecha_desde=fecha_desde_sobrantes
        )
        if vigentes is not None:
            running_sum = 0.0
            sobrantes_utilizados = []
            for m in vigentes:
                valor_neg = float(m.get("VALOR", 0) or 0)
                if valor_neg >= 0:
                    continue
                amount = abs(valor_neg)
                need = faltante - running_sum
                if need <= tolerancia:
                    break
                use_amount = min(amount, need)
                running_sum += use_amount
                fec = m.get("FECHA")
                fecha_str = ""
                if fec is not None:
                    try:
                        fec_int = int(float(fec))
                        d_sob, m_sob = fec_int % 100, (fec_int // 100) % 100
                        a_sob = fec_int // 10000
                        fecha_str = f"{d_sob:02d}/{m_sob:02d}/{a_sob}"
                    except (ValueError, TypeError):
                        pass
                sobrantes_utilizados.append({"fecha_str": fecha_str, "valor": use_amount})
                if running_sum >= faltante - tolerancia:
                    break
            if running_sum >= faltante - tolerancia and sobrantes_utilizados:
                detalle["remanente_sobrantes"] = running_sum
                detalle["justificado_sobrantes"] = True
                detalle["valor_faltante"] = faltante
                detalle["sobrantes_utilizados"] = sobrantes_utilizados
                for u in sobrantes_utilizados:
                    terminos.append(_term_formula(u["valor"]))
                    remanente += u["valor"]
                if sobrantes_utilizados:
                    detalle["fecha_sobrante_str"] = sobrantes_utilizados[-1].get("fecha_str", "")
                detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
                return (remanente, detalle)
    else:
        # Faltante > 20M y no se pudo seguir alternando 810291 (falta positivo o negativo): gestión manual
        if diferencia > lim_sobrantes:
            detalle["gestion_manual"] = True

    detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
    return (remanente, detalle)


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
    Procesa todos los cajeros con Fecha descarga arqueo = fecha_descarga (cuadrados y descuadrados).
    Para cada uno: revisa movimientos 770500 el día del arqueo y ajusta Remanente (signo contrario al de la BD).
    Si queda faltante, busca en cuenta sobrantes (279510020) negativos vigentes que lo justifiquen (cruce).
    Si queda cuadrado, ratifica y puede escribir "Cajero cuadrado...". Actualiza Excel, fórmulas y Gestión a Realizar.

    Returns:
        Número de registros actualizados (con Remanente y/o Gestión).
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
        # Procesar tanto cuadrados como descuadrados: la misma lógica (770500 opuesto + sobrantes) aplica a todos
        if cajero == 5200:
            logger.info("[SEGUIMIENTO 5200] --- Inicio seguimiento cajero 5200 --- Fecha arqueo: %s, Fecha descarga: %s. Diferencia actual=%.2f. Se valida en BD (770500 + sobrantes).", fa, fecha_descarga, diferencia_actual)
        anio_a, mes_a, dia_a = fa.year, fa.month, fa.day
        fecha_desde_sob = obtener_fecha_ultimo_arqueo_para_sobrantes(
            cajero, fa, df, col_cajero, col_fecha_arqueo, lector
        )
        remanente_final, detalle = calcular_remanente_para_cajero_cuadrado(
            admin_bd, cajero, anio_a, mes_a, dia_a,
            saldo, efectivo, dispensado, recibido, tolerancia,
            fecha_desde_sobrantes=fecha_desde_sob,
        )
        if remanente_final is None:
            if cajero == 5200:
                logger.info("[SEGUIMIENTO 5200] Validación en BD devolvió None (error o sin movimientos). No se actualiza.")
            continue
        formula_remanente = detalle.get("formula_remanente")
        if formula_remanente:
            df.at[idx, col_remanente] = formula_remanente
            indices_a_actualizar.append((idx, formula_remanente))
        else:
            df.at[idx, col_remanente] = remanente_final
            indices_a_actualizar.append((idx, None))
        # No tocar Gestión a Realizar si ya tiene valor (no sobrescribir)
        valor_gestion_actual = df.at[idx, col_gestion]
        gestion_no_vacia = valor_gestion_actual is not None and not (isinstance(valor_gestion_actual, float) and pd.isna(valor_gestion_actual)) and str(valor_gestion_actual).strip() != ""
        if cajero == 5200:
            logger.info("[SEGUIMIENTO 5200] --- Escritura en Gestión a Realizar --- Rama: %s", "ratificado_cuadrado" if detalle.get("ratificado_cuadrado") else ("justificado_sobrantes (CRUCE)" if detalle.get("justificado_sobrantes") else ("gestion_manual" if detalle.get("gestion_manual") else "remanente actualizado (banco)")))
        if gestion_no_vacia:
            logger.info("Cajero %s (F. arqueo %s): Gestión a Realizar ya tiene valor; no se sobrescribe. Remanente actualizado.", cajero, fa)
        elif detalle.get("gestion_manual"):
            df.at[idx, col_gestion] = "Gestión manual - Revisar por el personal encargado"
            logger.info("Cajero %s (F. arqueo %s): faltante > umbral sin más 810291; Gestión manual.", cajero, fa)
        elif detalle.get("ratificado_cuadrado"):
            df.at[idx, col_gestion] = TEXTO_CAJERO_CUADRADO
            logger.info("Cajero %s (F. arqueo %s): ratificado cuadrado, remanente banco %.2f.", cajero, fa, detalle.get("remanente_banco", 0))
        elif detalle.get("justificado_sobrantes"):
            valor_faltante = detalle.get("valor_faltante", 0)
            valor_formato = f"{int(round(valor_faltante)):,}".replace(",", ".")
            sobrantes_utilizados = detalle.get("sobrantes_utilizados") or []
            if sobrantes_utilizados:
                partes = []
                for u in sobrantes_utilizados:
                    v = u.get("valor", 0)
                    v_fmt = f"{int(round(v)):,}".replace(",", ".")
                    partes.append(f"el día {u['fecha_str']} (${v_fmt})")
                texto_gestion = f"El faltante ${valor_formato} es cruzado con sobrante contabilizado " + ", ".join(partes) + f" el cual es reversado por la seccion el día {fecha_gestion_str}"
            else:
                fecha_sob = detalle.get("fecha_sobrante_str") or f"{fa.day:02d}/{fa.month:02d}/{fa.year}"
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
    from src.procesamiento.pegar_gestion_a_arqueos_mf import _escribir_formula_diferencia_en_excel, _escribir_formulas_remanente_en_excel
    _escribir_formula_diferencia_en_excel(ruta)
    filas_formula = [(2 + df.index.get_loc(idx), formula) for (idx, formula) in indices_a_actualizar if formula]
    _escribir_formulas_remanente_en_excel(ruta, filas_formula)
    logger.info("Procesados %d registro(s) con Fecha descarga arqueo = %s (770500 + 810291 + sobrantes).", len(indices_a_actualizar), fecha_descarga)
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


def obtener_fecha_ultimo_arqueo_para_sobrantes(
    cajero: int,
    fecha_arqueo_actual: date,
    df_arqueos_mf: pd.DataFrame,
    col_cajero: str,
    col_fecha_arqueo: str,
    lector=None,
) -> Optional[date]:
    """
    Fecha del ultimo arqueo (anterior al que estamos procesando) para acotar el "Desde" en sobrantes.
    - Primero: en ARQUEOS MF, registro anterior al actual (mismo cajero, fecha_arqueo < actual); se toma la fecha_arqueo mas reciente de esos.
    - Si en ARQUEOS MF solo hay un registro con este cajero: se busca en HISTORICO_CUADRE_CAJEROS_SUCURSALES.xlsx,
      filtrando tipo_registro = "ARQUEO" y el cajero, y se toma la fecha_arqueo mas reciente.
    Returns:
        Fecha a usar como "Desde" en la consulta de sobrantes, o None para usar fallback (dia 1 del mes).
    """
    # En ARQUEOS MF: registros del mismo cajero con fecha_arqueo < fecha_arqueo_actual
    fechas_anteriores = []
    for _, row in df_arqueos_mf.iterrows():
        try:
            c = int(float(row[col_cajero]))
        except (ValueError, TypeError):
            continue
        if c != cajero:
            continue
        fa = _valor_a_fecha(row.get(col_fecha_arqueo))
        if fa is not None and fa < fecha_arqueo_actual:
            fechas_anteriores.append(fa)
    if fechas_anteriores:
        return max(fechas_anteriores)

    # Solo hay un registro (o ninguno anterior) en ARQUEOS MF: buscar en historico
    if lector is None:
        return None
    try:
        df_hist = lector.leer_historico_cuadre_cajeros_sucursales()
    except FileNotFoundError:
        logger.debug("Historico cuadre no encontrado; se usara fallback para fecha desde sobrantes.")
        return None
    col_tipo = _buscar_columna(df_hist, ["tipo_registro", "tipo registro"])
    col_fa_hist = _buscar_columna(df_hist, ["fecha_arqueo", "Fecha Arqueo"])
    col_cajero_hist = _buscar_columna(df_hist, ["codigo_cajero", "Cajero", "cajero"])
    if not col_tipo or not col_fa_hist or not col_cajero_hist:
        return None
    fechas_hist = []
    for _, row in df_hist.iterrows():
        if str(row.get(col_tipo) or "").strip().upper() != "ARQUEO":
            continue
        try:
            c = int(float(row[col_cajero_hist]))
        except (ValueError, TypeError):
            continue
        if c != cajero:
            continue
        fa = _valor_a_fecha(row.get(col_fa_hist))
        if fa is not None:
            fechas_hist.append(fa)
    if fechas_hist:
        return max(fechas_hist)
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
    con la misma regla que el flujo principal: 770500 con signo contrario al de la BD,
    y si queda faltante, consulta cuenta sobrantes (279510020).

    Usa calcular_remanente_para_cajero_cuadrado para una sola lógica en todo el proyecto.

    Returns:
        (remanente_a_escribir, detalle) o (None, None) si error en consulta.
    """
    anio, mes, dia = fecha_arqueo.year, fecha_arqueo.month, fecha_arqueo.day
    remanente, detalle = calcular_remanente_para_cajero_cuadrado(
        admin_bd,
        cajero,
        anio,
        mes,
        dia,
        saldo_contable,
        efectivo_arqueado,
        dispensado_corte_arqueo or 0.0,
        recibido_corte_arqueo or 0.0,
        TOLERANCIA_JUSTIFICAR,
    )
    if remanente is None:
        return (None, None)
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
