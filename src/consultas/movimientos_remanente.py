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

from src.insumos.arqueos_mf_calendario import meses_libro_candidatos_fecha_descarga

logger = logging.getLogger(__name__)

CUENTA_CAJERO = 110505075
CUENTA_SOBRANTES = 279510020
CODOFI_EXCLUIDO = 976
NROCMP_BANCO = 770500      # Siempre se considera el día del arqueo
NROCMP_SUCURSAL = 810291  # Solo si justifica la diferencia tras ajustar 770500
NROCMP_PROVISION = (810291, 770500)
TOLERANCIA_JUSTIFICAR = 1.0  # 1 peso de tolerancia al comparar diferencia con 810291

# Columna en ARQUEOS MF con seguimiento de reglas (procesar_cuadrados_fecha_descarga).
COL_PASO_A_PASO_ARQUEOS_MF = "paso_a_paso_regla"


def _fmt_traza_num(v: float) -> str:
    try:
        vi = int(round(float(v)))
        return f"${vi:,}".replace(",", ".")
    except (TypeError, ValueError):
        return str(v)


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
    incluir_dia_arqueo: bool = True,
) -> Optional[List[Dict[str, Any]]]:
    """
    En cuenta de sobrantes (279510020): movimientos de sobrante "vigente" para cruzar.

    Regla por `NUMDOC` (según ajuste pedido):
    - Se consultan todos los movimientos de la cuenta en el rango [fecha_inicio, fecha_fin].
    - Para cada `NUMDOC`, se calcula el neto: `sum(VALOR)` de todos los registros con ese `NUMDOC`.
    - Si el neto es negativo (< 0), ese monto neto es el "sobrante vigente" disponible para cruces.
    - Si el neto es positivo o 0, ese `NUMDOC` NO se considera para ningún cruce.

    Además:
    - La función devuelve 1 registro sintético por cada `NUMDOC` vigente, con:
      - `VALOR`: neto negativo calculado
      - `FECHA`: la FECHA del movimiento negativo más reciente dentro de ese `NUMDOC`
        (se usa para ordenar y, por tanto, para el criterio de "desde más reciente").
    - Se mantiene el retorno ordenado por `FECHA DESC` (más reciente primero).
    """
    fecha_arqueo_date = date(anio, mes, dia)
    fecha_hasta = fecha_arqueo_date if incluir_dia_arqueo else (fecha_arqueo_date - timedelta(days=1))
    fecha_fin = fecha_hasta.year * 10000 + fecha_hasta.month * 100 + fecha_hasta.day
    if fecha_desde is not None:
        fecha_inicio = fecha_desde.year * 10000 + fecha_desde.month * 100 + fecha_desde.day
    else:
        fecha_inicio = fecha_arqueo_date.year * 10000 + fecha_arqueo_date.month * 100 + 1  # fallback: dia 1 del mes
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
    def _norm_numdoc(v):
        if v is None:
            return None
        try:
            return int(float(v))
        except Exception:
            return v

    try:
        df = admin_bd.consultar(consulta, mantener_conexion=True)
        if df is None or df.empty:
            return []
        if "VALOR" not in df.columns or "NUMDOC" not in df.columns or "FECHA" not in df.columns:
            return []

        df = df.copy()
        df["VALOR"] = df["VALOR"].apply(lambda x: float(x or 0))
        df["NUMDOC_NORM"] = df["NUMDOC"].apply(_norm_numdoc)
        df["FECHA_INT"] = df["FECHA"].apply(lambda x: int(float(x or 0)))

        # Nueva regla de vigencia solicitada:
        # - Se suma VALOR por NUMDOC
        # - Si el neto es negativo (< 0), el sobrante sigue "activo"
        # - Si el neto es >= 0, se considera cancelado/no vigente
        #
        # Para la FECHA de salida se conserva la del movimiento NEGATIVO más reciente
        # dentro del NUMDOC (si no existe, se usa la más reciente general como fallback).
        vigentes: List[Dict[str, Any]] = []
        for numdoc, sub in df.groupby("NUMDOC_NORM", dropna=False):
            saldo_neto = float(sub["VALOR"].sum())
            if saldo_neto >= 0:
                continue

            sub_neg = sub[sub["VALOR"] < 0]
            if not sub_neg.empty:
                idx_ult_neg = sub_neg["FECHA_INT"].idxmax()
                fila_fecha = sub_neg.loc[idx_ult_neg]
            else:
                idx_ult = sub["FECHA_INT"].idxmax()
                fila_fecha = sub.loc[idx_ult]

            nroc = None
            if "NROCMP" in fila_fecha.index:
                try:
                    nroc = float(fila_fecha.get("NROCMP", 0) or 0)
                except Exception:
                    nroc = fila_fecha.get("NROCMP")

            vigentes.append(
                {
                    "FECHA": int(fila_fecha["FECHA_INT"]),
                    "VALOR": saldo_neto,  # neto por NUMDOC (negativo = vigente)
                    "NROCMP": nroc,
                    "NUMDOC": numdoc,
                }
            )

        # Orden salida: más reciente -> más antiguo
        vigentes.sort(key=lambda x: int(float(x.get("FECHA") or 0)), reverse=True)
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


def _fecha_sobrante_str(m: Dict[str, Any]) -> str:
    """Convierte FECHA (AAAAMMDD) del movimiento a 'DD/MM/YYYY'."""
    fec = m.get("FECHA")
    if fec is None:
        return ""
    try:
        fec_int = int(float(fec))
        d_sob, m_sob = fec_int % 100, (fec_int // 100) % 100
        a_sob = fec_int // 10000
        return f"{d_sob:02d}/{m_sob:02d}/{a_sob}"
    except (ValueError, TypeError):
        return ""


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
    2) Tras 770500, evaluar 810291 por naturaleza de la diferencia:
       - Sobrante (diferencia<0): considerar 810291 positivos
       - Faltante (diferencia>0): considerar 810291 negativos
       Se toma el movimiento con menor CLVMOV y solo aplica si su cobertura sobre la diferencia
       está en el rango [90%, 110%] (signo aplicado contrario al de la BD).
    3) Si faltante restante <= umbral_sobrantes (20M): buscar en cuenta sobrantes; si coincide, agregar.
    4) Si faltante > 20M y no hay más 810291: gestion_manual.
    5) Si es sobrante y su magnitud supera el tope parametrizable para 279510020, no contabilizar:
       marcar ACLARAR DIFERENCIA Y REPETIR EL ARQUEO.
    Remanente se escribe como fórmula Excel "=valor1+valor2-valor3..."
    """
    try:
        from src.config.cargador_config import CargadorConfig
        config = CargadorConfig()
        umbrales = config.obtener_umbrales_remanente()
    except Exception:
        umbrales = {
            "faltante_limite_sobrantes": 20_000_000,
            "sobrante_maximo_contabilizar_279510020": 50_000_000,
            "sobrantes_incluir_dia_arqueo": True,
        }
    lim_sobrantes = umbral_faltante_sobrantes if umbral_faltante_sobrantes is not None else umbrales.get("faltante_limite_sobrantes", 20_000_000)
    lim_sobrante_contabilizar = float(umbrales.get("sobrante_maximo_contabilizar_279510020", 50_000_000))
    incluir_dia_arqueo_sob = umbrales.get("sobrantes_incluir_dia_arqueo", True)

    detalle = {
        "remanente_banco": 0.0,
        "remanente_sobrantes": 0.0,
        "remanente_810291": 0.0,
        "diferencia_sin_remanente": None,
        "diferencia_after_banco": None,
        "ratificado_cuadrado": False,
        "justificado_sobrantes": False,
        "gestion_manual": False,
        "sobrante_excede_tope_repetir": False,
        "aclarar_diferencia": False,  # Faltante <= umbral sobrantes pero no se logró justificar con sobrantes
        "sobrante_contabilizado_gerencia": False,  # Tras 770500+810291 queda sobrante; se graba texto cuenta 279510020
        "formula_remanente": None,
        "traza_pasos": [],
    }
    d0 = saldo_contable - (efectivo_arqueado + (dispensado_corte_arqueo or 0) - (recibido_corte_arqueo or 0))
    detalle["diferencia_sin_remanente"] = d0
    terminos = []  # términos para "=t1+t2-t3..."
    detalle["traza_pasos"].append(
        f"Inicio_cajero={nit} | fecha_arqueo={dia:02d}/{mes:02d}/{anio} | "
        f"d0(Saldo-(Efectivo+Dispensado-Recibido))={_fmt_traza_num(d0)}"
    )

    movs = consultar_movimientos_dia_arqueo(admin_bd, nit, anio, mes, dia)
    if movs is None:
        detalle["traza_pasos"].append("ERROR no se pudieron leer movimientos 770500/810291 en NACIONAL para ese día")
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
    detalle["traza_pasos"].append(
        f"Tras_770500 movs={len(movs_banco)} | remanente_770500(aplicado_con_signo_inv_BD)={_fmt_traza_num(remanente)} | "
        f"diferencia_d0_menos_remanente={_fmt_traza_num(diferencia)}"
    )

    # 810291 iterativo por signo de diferencia (sin umbral fijo), usando CLVMOV asc.
    aplicados_810291 = []
    idx_pos, idx_neg = 0, 0
    rango_min_base, rango_max_base = 0.80, 1.20
    hubo_intento_810 = False
    while abs(float(diferencia)) > tolerancia:
        diferencia_before_810 = float(diferencia)
        rango_min, rango_max = rango_min_base, rango_max_base
        candidato = None
        signo_objetivo = ""
        if diferencia_before_810 < -tolerancia:
            # Sobrante: buscar 810291 positivos (al invertir signo, restan remanente).
            if idx_pos < len(positivos_810291):
                candidato = positivos_810291[idx_pos]
                idx_pos += 1
            signo_objetivo = "positivo"
        elif diferencia_before_810 > tolerancia:
            # Faltante: buscar 810291 negativos (al invertir signo, suman remanente).
            if idx_neg < len(negativos_810291):
                candidato = negativos_810291[idx_neg]
                idx_neg += 1
            signo_objetivo = "negativo"
        else:
            break

        if candidato is None:
            if not hubo_intento_810:
                detalle["traza_pasos"].append(
                    f"810291 no_aplica_por_signo | diferencia={_fmt_traza_num(diferencia)} | "
                    f"signo_requerido={signo_objetivo or 'ninguno'}"
                )
            else:
                detalle["traza_pasos"].append(
                    f"810291 sin_mas_candidatos_signo={signo_objetivo} | diferencia_actual={_fmt_traza_num(diferencia)}"
                )
            break

        hubo_intento_810 = True
        v_bd = float(candidato.get("VALOR", 0) or 0)
        aporte = -v_bd
        cobertura = (abs(aporte) / abs(diferencia_before_810)) if abs(diferencia_before_810) > 0 else 0.0
        cobertura_pct = cobertura * 100.0
        if rango_min <= cobertura <= rango_max:
            remanente += aporte
            terminos.append(_term_formula(aporte))
            aplicados_810291.append(aporte)
            diferencia = d0 - remanente
            detalle["traza_pasos"].append(
                f"810291 aplica_por_signo={signo_objetivo} | clvmov={candidato.get('CLVMOV')} | "
                f"valor_bd={_fmt_traza_num(v_bd)} | aporte_inv_signo={_fmt_traza_num(aporte)} | "
                f"cobertura={cobertura_pct:.2f}% (rango {rango_min*100:.0f}%-{rango_max*100:.0f}%) | diferencia_actual={_fmt_traza_num(diferencia)}"
            )
            continue

        detalle["traza_pasos"].append(
            f"810291 no_aplica_por_cobertura | signo_objetivo={signo_objetivo} | clvmov={candidato.get('CLVMOV')} | "
            f"valor_bd={_fmt_traza_num(v_bd)} | cobertura={cobertura_pct:.2f}% fuera de {rango_min*100:.0f}%-{rango_max*100:.0f}% | "
            f"diferencia_se_mantiene={_fmt_traza_num(diferencia)}"
        )
        # Si el mejor candidato por CLVMOV no cumple cobertura, no seguimos con más del mismo signo.
        break

    detalle["remanente_810291"] = sum(aplicados_810291)

    if abs(diferencia) <= tolerancia:
        detalle["ratificado_cuadrado"] = True
        detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
        detalle["traza_pasos"].append(
            f"RESULTADO cuadrado | |diferencia|<=tolerancia({_fmt_traza_num(tolerancia)}) | remanente={_fmt_traza_num(remanente)}"
        )
        return (remanente, detalle)

    if detalle.get("gestion_manual"):
        detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
        detalle["traza_pasos"].append(
            "RESULTADO gestión manual | faltante sin cierre según reglas vigentes"
        )
        return (remanente, detalle)

    if diferencia <= 0:
        if abs(float(diferencia)) > lim_sobrante_contabilizar:
            detalle["sobrante_excede_tope_repetir"] = True
            detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
            detalle["traza_pasos"].append(
                f"RESULTADO sobrante_excede_tope | abs(diferencia)={_fmt_traza_num(abs(float(diferencia)))} > "
                f"tope_contabilizar_sobrante({_fmt_traza_num(lim_sobrante_contabilizar)}) | "
                "marcar ACLARAR DIFERENCIA Y REPETIR EL ARQUEO"
            )
            return (remanente, detalle)
        detalle["sobrante_contabilizado_gerencia"] = True
        detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
        detalle["traza_pasos"].append(
            f"RESULTADO sobrante tras banco | diferencia<={_fmt_traza_num(0)} -> sugiere texto Gestión MF cuenta 279510020 (si celda vacía)"
        )
        return (remanente, detalle)

    # Faltante restante: cruce con sobrantes según regla (exacto desde más reciente o desde más antiguo; orden salida: más antiguo a más reciente)
    faltante = diferencia
    if faltante <= lim_sobrantes:
        detalle["aclarar_diferencia"] = True  # Por defecto; se pondrá False si se logra justificar con sobrantes
        detalle["traza_pasos"].append(
            f"Faltante restante={_fmt_traza_num(faltante)} <= umbral_sobrantes({_fmt_traza_num(lim_sobrantes)}) | "
            "buscar cruce en cuenta 279510020 (NUMDOC suma neta negativa = sobrante activo)"
        )
        vigentes = consultar_sobrantes_negativos_vigentes(
            admin_bd, nit, anio, mes, dia, fecha_desde=fecha_desde_sobrantes, incluir_dia_arqueo=incluir_dia_arqueo_sob
        )
        if vigentes is None:
            detalle["traza_pasos"].append("Cuenta 279510020: consulta de sobrantes vigentes devolvió None (revisar conexión o consulta)")
        if vigentes is not None:
            vigentes = [m for m in vigentes if float(m.get("VALOR", 0) or 0) < 0]
            fecha_arqueo_int = anio * 10000 + mes * 100 + dia
            # "DIARIO" se decide por NUMDOC (YYYYMMDD), no por la fecha de grabación (FECHA/ANOELB/MESELB/DIAELB)
            en_dia_arqueo = sum(
                1
                for m in vigentes
                if m.get("NUMDOC") is not None and int(float(m.get("NUMDOC"))) == fecha_arqueo_int
            )
            if en_dia_arqueo > 1:
                detalle["gestion_manual"] = True
                detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
                detalle["traza_pasos"].append(
                    f"RESULTADO gestión manual | >1 movimiento sobrante con NUMDOC=día arqueo ({en_dia_arqueo})"
                )
                return (remanente, detalle)
            # Etapa 1/2 y orden de consumo dependen del "DIARIO" (NUMDOC = YYYYMMDD)
            # Para desempate dentro de un mismo NUMDOC, consumimos por fecha de grabación más antigua a más reciente.
            vigentes_asc = sorted(
                vigentes,
                key=lambda x: (
                    int(float(x.get("NUMDOC") or 0)),
                    int(float(x.get("FECHA") or 0)),
                ),
            )

            penultimo_int: Optional[int] = None
            if fecha_desde_sobrantes is not None:
                penultimo_int = (
                    fecha_desde_sobrantes.year * 10000
                    + fecha_desde_sobrantes.month * 100
                    + fecha_desde_sobrantes.day
                )

            if penultimo_int is None:
                vigentes_after_penultimo = vigentes_asc
                vigentes_penultimo = []
            else:
                vigentes_after_penultimo = [m for m in vigentes_asc if int(float(m.get("NUMDOC") or 0)) > penultimo_int]
                vigentes_penultimo = [m for m in vigentes_asc if int(float(m.get("NUMDOC") or 0)) == penultimo_int]

            sum_after = sum(abs(float(m.get("VALOR", 0) or 0)) for m in vigentes_after_penultimo)

            def _intentar_cruce(subset, objetivo: float):
                """
                Consume en orden (más antiguo -> más reciente) hasta llegar al objetivo.
                Permite consumo parcial del último "chunk" si no alcanza entero.
                """
                running_sum = 0.0
                utilizados = []
                for m in subset:
                    amount = abs(float(m.get("VALOR", 0) or 0))
                    if amount <= 0:
                        continue
                    need = objetivo - running_sum
                    if need <= tolerancia:
                        break
                    use_amount = min(amount, need)
                    running_sum += use_amount
                    utilizados.append({"fecha_str": _fecha_sobrante_str(m), "valor": use_amount})
                    if abs(running_sum - objetivo) <= tolerancia:
                        return running_sum, utilizados
                return running_sum, utilizados

            # Etapa 1: si faltante <= suma disponible (por DIARIO) después del penúltimo arqueo,
            # cruzar únicamente con los sobrantes con FECHA > penúltimo.
            if faltante <= sum_after + tolerancia:
                running_sum, utilizados = _intentar_cruce(vigentes_after_penultimo, faltante)
                if abs(running_sum - faltante) <= tolerancia and utilizados:
                    detalle["remanente_sobrantes"] = running_sum
                    detalle["justificado_sobrantes"] = True
                    detalle["aclarar_diferencia"] = False
                    detalle["valor_faltante"] = faltante
                    detalle["sobrantes_utilizados"] = utilizados
                    for u in utilizados:
                        terminos.append(_term_formula(u["valor"]))
                        remanente += u["valor"]
                    if utilizados:
                        detalle["fecha_sobrante_str"] = utilizados[-1].get("fecha_str", "")
                    detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
                    detalle["traza_pasos"].append(
                        "RESULTADO justificado_sobrantes | etapa1 NUMDOC posteriores al penúltimo arqueo | "
                        f"cant_chunks={len(utilizados)}"
                    )
                    return (remanente, detalle)

            # Etapa 2: si faltante > suma disponible después del penúltimo,
            # cruzar con todo lo posterior y luego con FECHA == penúltimo (solo para el saldo restante).
            after_utilizados = []
            sum_after_utilizada = 0.0
            for m in vigentes_after_penultimo:
                amount = abs(float(m.get("VALOR", 0) or 0))
                if amount <= 0:
                    continue
                sum_after_utilizada += amount
                after_utilizados.append({"fecha_str": _fecha_sobrante_str(m), "valor": amount})

            remaining = faltante - sum_after_utilizada
            if remaining < -tolerancia:
                remaining = 0.0

            if remaining <= tolerancia:
                total_utilizados = after_utilizados
                if total_utilizados:
                    detalle["remanente_sobrantes"] = sum_after_utilizada
                    detalle["justificado_sobrantes"] = True
                    detalle["aclarar_diferencia"] = False
                    detalle["valor_faltante"] = faltante
                    detalle["sobrantes_utilizados"] = total_utilizados
                    for u in total_utilizados:
                        terminos.append(_term_formula(u["valor"]))
                        remanente += u["valor"]
                    detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
                    detalle["traza_pasos"].append(
                        "RESULTADO justificado_sobrantes | etapa2 solo posteriores al penúltimo cubren faltante"
                    )
                    return (remanente, detalle)

            running_sum_pen, utilizados_pen = _intentar_cruce(vigentes_penultimo, remaining)
            total_sum = sum_after_utilizada + running_sum_pen
            if abs(total_sum - faltante) <= tolerancia and after_utilizados is not None and utilizados_pen:
                total_utilizados = after_utilizados + utilizados_pen
                detalle["remanente_sobrantes"] = total_sum
                detalle["justificado_sobrantes"] = True
                detalle["aclarar_diferencia"] = False
                detalle["valor_faltante"] = faltante
                detalle["sobrantes_utilizados"] = total_utilizados
                for u in total_utilizados:
                    terminos.append(_term_formula(u["valor"]))
                    remanente += u["valor"]
                if total_utilizados:
                    detalle["fecha_sobrante_str"] = total_utilizados[-1].get("fecha_str", "")
                detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
                detalle["traza_pasos"].append(
                    "RESULTADO justificado_sobrantes | etapa2 posteriores + penúltimo día (cruce parcial)"
                )
                return (remanente, detalle)
            detalle["traza_pasos"].append(
                "Cruce sobrantes | no alcanzó faltante exacto | queda aclarar_diferencia o gestión manual"
            )
    else:
        # Faltante > 20M y no se pudo seguir alternando 810291 (falta positivo o negativo): gestión manual
        detalle["traza_pasos"].append(
            f"Faltante {_fmt_traza_num(diferencia)} > umbral_sobrantes({_fmt_traza_num(lim_sobrantes)}) | "
            "no entra cruce por cuenta sobrantes en este bloque"
        )
        if diferencia > lim_sobrantes:
            detalle["gestion_manual"] = True

    detalle["formula_remanente"] = "=" + "".join(terminos).lstrip("+") if terminos else "=0"
    if detalle.get("gestion_manual"):
        detalle["traza_pasos"].append(
            "RESULTADO gestión manual | faltante sin cerrar con 810291/sobrantes según reglas"
        )
    elif detalle.get("aclarar_diferencia") and not detalle.get("justificado_sobrantes"):
        detalle["traza_pasos"].append(
            "RESULTADO pendiente aclarar/repetir o faltante pequeño 168710093 (según paso ARQUEOS MF)"
        )
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


def _contar_filas_por_cajero(df: pd.DataFrame, col_cajero: str, cajero: int) -> int:
    """Cantidad de filas en ARQUEOS MF (hoja actual) para el mismo código de cajero."""
    n = 0
    for _, row in df.iterrows():
        if pd.isna(row.get(col_cajero)):
            continue
        try:
            c = int(float(row[col_cajero]))
        except (ValueError, TypeError):
            continue
        if c == cajero:
            n += 1
    return n


TEXTO_ACLARAR_REPETIR_ARQUEO = "ACLARAR DIFERENCIA Y REPETIR EL ARQUEO"


def texto_gestion_faltante_pequeno_centralizado(fecha_gestion_dd_mm_yyyy: str) -> str:
    """
    Texto estándar para faltante residual muy bajo: contabilización centralizada en cuenta de faltantes.
    fecha_gestion_dd_mm_yyyy: día de la ejecución (fecha descarga / proceso), formato DD/MM/YYYY.
    """
    return (
        "La diferencia es contabilizada por la Gerencia De Autoservicios y Efectivo de manera centralizada "
        f"a la cuenta de faltantes 168710093 el {fecha_gestion_dd_mm_yyyy}, esta diferencia ingresa a proceso "
        "de investigación en el área para identificar a que corresponde."
    )


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
    Recorre los libros mensuales candidatos (mes de la descarga y mes anterior): cada fila vive en el
    ARQUEOS MF del **mes de su Fecha Arqueo**. Parámetros mes/anio del proceso se ignoran (compatibilidad).

    Returns:
        Número total de registros actualizados (con Remanente y/o Gestión).
    """
    total = 0
    for mes_libro, anio_libro in meses_libro_candidatos_fecha_descarga(fecha_descarga):
        try:
            lector.asegurar_archivo_arqueos_mf(mes_libro, anio_libro)
        except FileNotFoundError:
            logger.warning(
                "No se pudo crear u obtener ARQUEOS MF para libro mes=%s anio=%s (sin plantilla en el directorio).",
                mes_libro,
                anio_libro,
            )
            continue
        df_lookup = _concat_df_lookup_remanente(lector, mes_libro, anio_libro, hoja)
        total += _procesar_cuadrados_fecha_descarga_un_libro(
            lector,
            admin_bd,
            mes_libro,
            anio_libro,
            hoja,
            fecha_descarga,
            tolerancia,
            df_lookup,
        )
    return total


def _procesar_cuadrados_fecha_descarga_un_libro(
    lector,
    admin_bd,
    mes: int,
    anio: int,
    hoja,
    fecha_descarga: date,
    tolerancia: float,
    df_lookup: pd.DataFrame,
) -> int:
    """
    Procesa solo el archivo ARQUEOS MF del mes `mes`/`anio`, usando `df_lookup` (este libro + anterior)
    para penúltimo arqueo y comparación de gestión previa.
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
    # Excel/pandas suelen cargar Remanente como float; las fórmulas (=t1+t2...) son texto.
    df[col_remanente] = df[col_remanente].astype(object)
    col_gestion = _buscar_columna(df, ["Gestión a Realizar", "Gestion a Realizar"])
    if col_gestion is None:
        col_gestion = "Gestión a Realizar"
        df[col_gestion] = ""
    # Hoja ARQUEOS MF a veces carga esta columna como float (celdas vacías); los textos de gestión son string.
    df[col_gestion] = df[col_gestion].astype(object)

    from src.procesamiento.pegar_gestion_a_arqueos_mf import TEXTO_CAJERO_CUADRADO
    # Fecha de gestión (día de hoy o de gestión) para el texto de cruce faltante-sobrante
    fecha_gestion_str = f"{fecha_descarga.day:02d}/{fecha_descarga.month:02d}/{fecha_descarga.year}"

    col_traza = COL_PASO_A_PASO_ARQUEOS_MF
    try:
        from src.config.cargador_config import CargadorConfig
        _cfg = CargadorConfig()
        _umb = _cfg.obtener_umbrales_remanente()
        escribir_traza_arqueos_mf = _cfg.columna_paso_a_paso_arqueos_mf_activa()
    except Exception:
        _umb = {}
        escribir_traza_arqueos_mf = True
    lim_faltante_grabar = float(_umb.get("faltante_maximo_grabar_cuenta_faltantes", 20_000))

    if escribir_traza_arqueos_mf:
        if col_traza not in df.columns:
            df[col_traza] = ""
        df[col_traza] = df[col_traza].apply(lambda x: "" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x))

    indices_a_actualizar = []
    filas_con_traza = 0
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
        # Procesar tanto cuadrados como descuadrados: la misma lógica (770500 opuesto + sobrantes) aplica a todos
        anio_a, mes_a, dia_a = fa.year, fa.month, fa.day
        # El rango de sobrantes va desde el penúltimo arqueo del cajero hasta el último arqueo (incluyendo el día del arqueo actual).
        fecha_desde_sob = obtener_fecha_penultimo_arqueo(
            cajero, fa, df_lookup, col_cajero, col_fecha_arqueo, lector
        )
        remanente_final, detalle = calcular_remanente_para_cajero_cuadrado(
            admin_bd, cajero, anio_a, mes_a, dia_a,
            saldo, efectivo, dispensado, recibido, tolerancia,
            fecha_desde_sobrantes=fecha_desde_sob,
        )
        if remanente_final is None:
            if escribir_traza_arqueos_mf:
                pasos_err = list(detalle.get("traza_pasos") or [])
                pasos_err.append("ARQUEOS_MF: no se pudo calcular remanente | columna Remanente sin cambios en esta fila")
                df.at[idx, col_traza] = " | ".join(pasos_err)
                filas_con_traza += 1
            continue
        # Faltante que queda después de aplicar el remanente calculado (770500 + 810291 + cruces sobrantes en fórmula/valor).
        faltante_residual = max(0.0, float(d0) - float(remanente_final))
        # Solo si queda faltante real (no ruido de redondeo) y es menor o igual al umbral operativo.
        faltante_pequeno_grabar = (faltante_residual > tolerancia) and (
            faltante_residual <= lim_faltante_grabar + tolerancia
        )
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
        if gestion_no_vacia:
            logger.info("Cajero %s (F. arqueo %s): Gestión a Realizar ya tiene valor; no se sobrescribe. Remanente actualizado.", cajero, fa)
        elif detalle.get("sobrante_excede_tope_repetir"):
            df.at[idx, col_gestion] = TEXTO_ACLARAR_REPETIR_ARQUEO
            logger.info(
                "Cajero %s (F. arqueo %s): sobrante excede tope para 279510020; %s.",
                cajero, fa, TEXTO_ACLARAR_REPETIR_ARQUEO,
            )
        elif detalle.get("gestion_manual"):
            # Faltante residual muy bajo: grabar en cuenta faltantes (no repetir arqueo por montos triviales).
            if faltante_pequeno_grabar:
                df.at[idx, col_gestion] = texto_gestion_faltante_pequeno_centralizado(fecha_gestion_str)
                logger.info(
                    "Cajero %s (F. arqueo %s): gestión manual evitada; faltante residual %.2f <= %.0f; texto cuenta 168710093.",
                    cajero, fa, faltante_residual, lim_faltante_grabar,
                )
            # Si tras todas las reglas sigue faltante (gestión manual) pero es la única fila de ese cajero
            # en ARQUEOS MF, pedir aclarar y repetir arqueo en lugar de escalar a gestión manual genérica.
            elif _contar_filas_por_cajero(df, col_cajero, cajero) == 1:
                df.at[idx, col_gestion] = TEXTO_ACLARAR_REPETIR_ARQUEO
                logger.info(
                    "Cajero %s (F. arqueo %s): persiste faltante tras reglas; único arqueo del cajero en ARQUEOS MF; %s.",
                    cajero, fa, TEXTO_ACLARAR_REPETIR_ARQUEO,
                )
            else:
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
        elif detalle.get("aclarar_diferencia"):
            if faltante_pequeno_grabar:
                df.at[idx, col_gestion] = texto_gestion_faltante_pequeno_centralizado(fecha_gestion_str)
                logger.info(
                    "Cajero %s (F. arqueo %s): faltante no cruzado con sobrantes; residual %.2f <= %.0f; texto cuenta 168710093.",
                    cajero, fa, faltante_residual, lim_faltante_grabar,
                )
            else:
                # Evitar repetir arqueo 2 veces seguidas: revisar si el arqueo anterior del mismo cajero tuvo la misma calificación.
                gestion_prev = ""
                fa_prev = None
                for _, rprev in df_lookup.iterrows():
                    if pd.isna(rprev.get(col_cajero)):
                        continue
                    try:
                        c_prev = int(float(rprev.get(col_cajero)))
                    except (ValueError, TypeError):
                        continue
                    if c_prev != cajero:
                        continue
                    fa_candidate = _valor_a_fecha(rprev.get(col_fecha_arqueo))
                    if fa_candidate is None or fa_candidate >= fa:
                        continue
                    if fa_prev is None or fa_candidate > fa_prev:
                        fa_prev = fa_candidate
                        gestion_prev = str(rprev.get(col_gestion) or "").strip()

                gestion_prev_norm = gestion_prev.strip().upper()
                repetir_prev = any(gestion_prev_norm == g.strip().upper() for g in GESTION_REPETIR_ARQUEO_EXACTAS)
                if repetir_prev:
                    df.at[idx, col_gestion] = texto_gestion_faltante_pequeno_centralizado(fecha_gestion_str)
                    logger.info("Cajero %s (F. arqueo %s): 2da repetición consecutiva; gestionar como faltantes.", cajero, fa)
                else:
                    df.at[idx, col_gestion] = "ACLARAR DIFERENCIA Y REPETIR EL ARQUEO"
                    logger.info("Cajero %s (F. arqueo %s): faltante no justificado con sobrantes; aclarar diferencia y repetir arqueo.", cajero, fa)
        elif detalle.get("sobrante_contabilizado_gerencia"):
            texto_sobrante = f"La diferencia es contabilizada por la Gerencia De Autoservicios y Efectivo de manera centralizada a la cuenta de sobrantes 279510020 el {fecha_gestion_str}"
            df.at[idx, col_gestion] = texto_sobrante
            logger.info("Cajero %s (F. arqueo %s): sobrante tras validación 770500+810291; Gestión: cuenta sobrantes %s.", cajero, fa, fecha_gestion_str)
        else:
            logger.info("Cajero %s (F. arqueo %s): remanente actualizado %.2f (banco %.2f).", cajero, fa, remanente_final, detalle.get("remanente_banco", 0))

        if escribir_traza_arqueos_mf:
            pasos_fin = list(detalle.get("traza_pasos") or [])
            pasos_fin.append(
                f"ARQUEOS_MF_post | faltante_residual_d0_menos_remanente_calc={_fmt_traza_num(faltante_residual)} | "
                f"umbral_grabar_faltantes={_fmt_traza_num(lim_faltante_grabar)}"
            )
            if gestion_no_vacia:
                pasos_fin.append("Escritura_Gestion_MF: omitida | celda ya tenía texto")
            elif detalle.get("gestion_manual"):
                if faltante_pequeno_grabar:
                    pasos_fin.append("Escritura_Gestion_MF: contabilización faltantes 168710093 (residual pequeño tras reglas)")
                elif _contar_filas_por_cajero(df, col_cajero, cajero) == 1:
                    pasos_fin.append(f"Escritura_Gestion_MF: {TEXTO_ACLARAR_REPETIR_ARQUEO}")
                else:
                    pasos_fin.append("Escritura_Gestion_MF: revisión manual personal encargado")
            elif detalle.get("ratificado_cuadrado"):
                pasos_fin.append("Escritura_Gestion_MF: texto cajero cuadrado")
            elif detalle.get("justificado_sobrantes"):
                pasos_fin.append("Escritura_Gestion_MF: cruce faltante con sobrante(s) 279510020 (texto reverso sección)")
            elif detalle.get("aclarar_diferencia"):
                if faltante_pequeno_grabar:
                    pasos_fin.append("Escritura_Gestion_MF: 168710093 por residual pequeño (sin cruce sobrantes)")
                else:
                    gestion_prev_a = ""
                    fa_prev_a = None
                    for _, rprev in df_lookup.iterrows():
                        if pd.isna(rprev.get(col_cajero)):
                            continue
                        try:
                            c_prev = int(float(rprev.get(col_cajero)))
                        except (ValueError, TypeError):
                            continue
                        if c_prev != cajero:
                            continue
                        fa_candidate = _valor_a_fecha(rprev.get(col_fecha_arqueo))
                        if fa_candidate is None or fa_candidate >= fa:
                            continue
                        if fa_prev_a is None or fa_candidate > fa_prev_a:
                            fa_prev_a = fa_candidate
                            gestion_prev_a = str(rprev.get(col_gestion) or "").strip()
                    gestion_prev_norm = gestion_prev_a.strip().upper()
                    repetir_prev = any(gestion_prev_norm == g.strip().upper() for g in GESTION_REPETIR_ARQUEO_EXACTAS)
                    if repetir_prev:
                        pasos_fin.append("Escritura_Gestion_MF: 168710093 (2da repetición arqueo previo con repetir)")
                    else:
                        pasos_fin.append("Escritura_Gestion_MF: ACLARAR DIFERENCIA Y REPETIR EL ARQUEO")
            elif detalle.get("sobrante_contabilizado_gerencia"):
                pasos_fin.append(
                    "Escritura_Gestion_MF: contabilización sobrante cuenta 279510020 (tras 770500/810291 queda lado sobrante)"
                )
            else:
                pasos_fin.append("Escritura_Gestion_MF: sin texto nuevo | solo ajuste de Remanente/Diferencia")

            df.at[idx, col_traza] = " | ".join(pasos_fin)
            filas_con_traza += 1

    if not indices_a_actualizar and filas_con_traza == 0:
        logger.info("Ningún registro cuadrado con Fecha descarga arqueo = %s para actualizar.", fecha_descarga)
        return 0
    # No aplicar texto de forma masiva: Gestión a Realizar solo se actualiza en las filas ya validadas en BD (arriba)
    lector.guardar_arqueos_mf(df, mes=mes, anio=anio, hoja=hoja)
    ruta = lector._ruta_arqueos_mf(mes=mes, anio=anio)
    from src.procesamiento.pegar_gestion_a_arqueos_mf import _escribir_formula_diferencia_en_excel, _escribir_formulas_remanente_en_excel
    _escribir_formula_diferencia_en_excel(ruta)
    filas_formula = [(2 + df.index.get_loc(idx), formula) for (idx, formula) in indices_a_actualizar if formula]
    _escribir_formulas_remanente_en_excel(ruta, filas_formula)
    if escribir_traza_arqueos_mf:
        logger.info(
            "Procesados %d registro(s) remanente y %d con columna %s (Fecha descarga arqueo = %s).",
            len(indices_a_actualizar),
            filas_con_traza,
            col_traza,
            fecha_descarga,
        )
    else:
        logger.info(
            "Procesados %d registro(s) remanente (columna %s desactivada en config remanente.columna_paso_a_paso_arqueos_mf; Fecha descarga = %s).",
            len(indices_a_actualizar),
            col_traza,
            fecha_descarga,
        )
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


def _concat_df_lookup_remanente(lector, mes_libro: int, anio_libro: int, hoja) -> pd.DataFrame:
    """
    Libro del mes del arqueo + mes calendario anterior (mismo archivo o el previo) para resolver
    penúltimo arqueo y gestión previa al cruzar fin de mes.
    """
    pares = [
        (mes_libro, anio_libro),
        (12, anio_libro - 1) if mes_libro == 1 else (mes_libro - 1, anio_libro),
    ]
    dfs = []
    for mm, aa in pares:
        ruta = lector._ruta_arqueos_mf(mm, aa)
        if ruta.exists():
            dfs.append(lector.leer_arqueos_mf(mm, aa, hoja=hoja, crear_si_falta=False))
    if not dfs:
        return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True)
    col_caj = _buscar_columna(out, ["Cajero", "cajero"])
    col_fa = _buscar_columna(out, ["Fecha Arqueo", "Fecha arqueo"])
    if col_caj and col_fa:
        out = out.drop_duplicates(subset=[col_caj, col_fa], keep="last")
    return out


def _valor_a_fecha(val) -> Optional[date]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if hasattr(val, "date"):
        return val.date()
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


# Textos en "Gestión a Realizar" que indican que el "Desde" de sobrantes debe retroceder un arqueo más.
GESTION_REPETIR_ARQUEO_EXACTAS = ("REPITIERON ARQUEO", "ACLARAR DIFERENCIA Y REPETIR EL ARQUEO")


def obtener_fecha_penultimo_arqueo(
    cajero: int,
    fecha_arqueo_actual: date,
    df_arqueos_mf: pd.DataFrame,
    col_cajero: str,
    col_fecha_arqueo: str,
    lector=None,
) -> Optional[date]:
    """
    Fecha del arqueo inmediatamente anterior (penúltimo) para un cajero.

    Se basa en ARQUEOS MF: buscar el máximo `fecha_arqueo` < `fecha_arqueo_actual`.
    Si no existe en ARQUEOS MF, se usa historico cuadre (si lector está disponible).
    """
    candidatos: list[date] = []
    for _, row in df_arqueos_mf.iterrows():
        try:
            c = int(float(row[col_cajero]))
        except (ValueError, TypeError):
            continue
        if c != cajero:
            continue
        fa = _valor_a_fecha(row.get(col_fecha_arqueo))
        if fa is not None and fa < fecha_arqueo_actual:
            candidatos.append(fa)

    if candidatos:
        return max(candidatos)

    if lector is None:
        return None

    return _fecha_ultimo_arqueo_desde_historico(lector, cajero, fecha_limite=fecha_arqueo_actual)


def obtener_fecha_ultimo_arqueo_para_sobrantes(
    cajero: int,
    fecha_arqueo_actual: date,
    df_arqueos_mf: pd.DataFrame,
    col_cajero: str,
    col_fecha_arqueo: str,
    lector=None,
    col_gestion: Optional[str] = None,
) -> Optional[date]:
    """
    Fecha del ultimo arqueo (anterior al que estamos procesando) para acotar el "Desde" en sobrantes.
    - Primero: en ARQUEOS MF, registro anterior al actual (mismo cajero, fecha_arqueo < actual); se toma la fecha_arqueo mas reciente de esos.
    - Si ese arqueo tiene en "Gestión a Realizar" exactamente "REPITIERON ARQUEO" o "ACLARAR DIFERENCIA Y REPETIR EL ARQUEO",
      se usa como "Desde" el arqueo anterior a ese (un paso más atrás).
    - Si en ARQUEOS MF no hay anterior: se busca en HISTORICO_CUADRE_CAJEROS_SUCURSALES.xlsx.
    Returns:
        Fecha a usar como "Desde" en la consulta de sobrantes, o None para usar fallback (dia 1 del mes).
    """
    # En ARQUEOS MF: registros del mismo cajero con fecha_arqueo < fecha_arqueo_actual (guardamos fecha y row para revisar Gestión)
    candidatos = []  # (fecha_arqueo, row)
    for _, row in df_arqueos_mf.iterrows():
        try:
            c = int(float(row[col_cajero]))
        except (ValueError, TypeError):
            continue
        if c != cajero:
            continue
        fa = _valor_a_fecha(row.get(col_fecha_arqueo))
        if fa is not None and fa < fecha_arqueo_actual:
            candidatos.append((fa, row))
    if not candidatos:
        pass  # ir a historico
    else:
        # Ordenar por fecha desc para tener el más reciente primero
        candidatos.sort(key=lambda x: x[0], reverse=True)
        fecha_desde_candidata, row_candidata = candidatos[0]
        # Si tiene Gestión "REPITIERON ARQUEO" o "ACLARAR DIFERENCIA Y REPETIR EL ARQUEO", usar el arqueo anterior a este
        if col_gestion and col_gestion in row_candidata.index:
            gestion = str(row_candidata.get(col_gestion) or "").strip()
            if gestion in GESTION_REPETIR_ARQUEO_EXACTAS:
                # Buscar arqueo anterior a fecha_desde_candidata
                fechas_anteriores_a_esta = [fa for fa, _ in candidatos if fa < fecha_desde_candidata]
                if fechas_anteriores_a_esta:
                    return max(fechas_anteriores_a_esta)
                # No hay más en ARQUEOS MF: buscar en historico con límite < fecha_desde_candidata
                if lector is not None:
                    fecha_hist = _fecha_ultimo_arqueo_desde_historico(lector, cajero, fecha_limite=fecha_desde_candidata)
                    if fecha_hist is not None:
                        return fecha_hist
                return None
        return fecha_desde_candidata

    # No hay anterior en ARQUEOS MF: buscar en historico (sin límite superior)
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


def _fecha_ultimo_arqueo_desde_historico(lector, cajero: int, fecha_limite: date) -> Optional[date]:
    """Mayor fecha_arqueo en historico para cajero con fecha_arqueo < fecha_limite."""
    try:
        df_hist = lector.leer_historico_cuadre_cajeros_sucursales()
    except FileNotFoundError:
        return None
    col_tipo = _buscar_columna(df_hist, ["tipo_registro", "tipo registro"])
    col_fa_hist = _buscar_columna(df_hist, ["fecha_arqueo", "Fecha Arqueo"])
    col_cajero_hist = _buscar_columna(df_hist, ["codigo_cajero", "Cajero", "cajero"])
    if not col_tipo or not col_fa_hist or not col_cajero_hist:
        return None
    fechas = []
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
        if fa is not None and fa < fecha_limite:
            fechas.append(fa)
    return max(fechas) if fechas else None


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
