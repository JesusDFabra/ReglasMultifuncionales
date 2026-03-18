"""
Script para analizar un cajero: movimientos del día del arqueo (770500, 810291),
d0, aplicación de reglas y fórmula de Remanente.
Uso: python analizar_cajero.py 5045
     python analizar_cajero.py 5045 --fecha-arqueo 2026-03-13
"""
import sys
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent))

import argparse
from src.config.cargador_config import CargadorConfig
from src.insumos.lector_insumos import LectorInsumos
from src.consultas.admin_bd import crear_admin_nacional_desde_config
from src.consultas.movimientos_remanente import (
    consultar_movimientos_dia_arqueo,
    consultar_sobrantes_negativos_vigentes,
    calcular_remanente_para_cajero_cuadrado,
    obtener_fecha_ultimo_arqueo_para_sobrantes,
    _buscar_columna,
    _valor_a_fecha,
    _float_val,
)


def _main():
    p = argparse.ArgumentParser(description="Analizar cajero: movimientos BD y cálculo Remanente")
    p.add_argument("cajero", type=int, help="Número de cajero (NIT)")
    p.add_argument("--fecha-arqueo", type=str, default="2026-03-13", help="Fecha arqueo YYYY-MM-DD")
    p.add_argument("--mes", type=int, default=3, help="Mes ARQUEOS MF (1-12)")
    p.add_argument("--anio", type=int, default=2026, help="Año ARQUEOS MF")
    args = p.parse_args()

    # Parsear fecha arqueo
    partes = args.fecha_arqueo.split("-")
    if len(partes) != 3:
        print("fecha-arqueo debe ser YYYY-MM-DD")
        return 1
    anio_a, mes_a, dia_a = int(partes[0]), int(partes[1]), int(partes[2])
    fecha_arqueo = date(anio_a, mes_a, dia_a)

    config = CargadorConfig()
    lector = LectorInsumos(config)
    df = lector.leer_arqueos_mf(mes=args.mes, anio=args.anio, hoja=0)
    col_cajero = _buscar_columna(df, ["Cajero", "cajero"])
    col_fa = _buscar_columna(df, ["Fecha Arqueo", "Fecha arqueo"])
    col_saldo = _buscar_columna(df, ["Saldo Contable", "Saldo contable"])
    col_efectivo = _buscar_columna(df, ["Efectivo Arqueado /Arqueo fisico saldo contadores"])
    col_dispensado = _buscar_columna(df, ["dispensado_corte_arqueo"])
    col_recibido = _buscar_columna(df, ["recibido_corte_arqueo"])
    col_remanente = _buscar_columna(df, ["Remanente /Provisión /Ajustes", "Remanente/Provisión/Ajustes"])

    # Buscar fila del cajero con esa fecha arqueo
    row = None
    for _, r in df.iterrows():
        try:
            if int(float(r[col_cajero])) != args.cajero:
                continue
        except (ValueError, TypeError):
            continue
        fa = _valor_a_fecha(r.get(col_fa))
        if fa == fecha_arqueo:
            row = r
            break
    if row is None:
        print(f"No se encontró en ARQUEOS MF: Cajero {args.cajero}, Fecha arqueo {fecha_arqueo}")
        return 1

    saldo = _float_val(row, col_saldo)
    efectivo = _float_val(row, col_efectivo)
    dispensado = _float_val(row, col_dispensado)
    recibido = _float_val(row, col_recibido)
    remanente_actual = _float_val(row, col_remanente) if col_remanente else 0.0

    d0 = saldo - (efectivo + (dispensado or 0) - (recibido or 0))
    print("=" * 60)
    print(f"CAJERO {args.cajero} - Fecha arqueo: {fecha_arqueo}")
    print("=" * 60)
    print(f"Saldo contable:     {saldo:,.0f}")
    print(f"Efectivo arqueado:  {efectivo:,.0f}")
    print(f"Dispensado corte:   {dispensado or 0:,.0f}")
    print(f"Recibido corte:     {recibido or 0:,.0f}")
    print(f"Diferencia sin remanente (d0): {d0:,.0f}")
    print(f"Remanente actual en Excel:     {remanente_actual:,.0f}")
    print()

    admin = crear_admin_nacional_desde_config(config)
    if not admin:
        print("BD no configurada.")
        return 1
    admin.conectar()
    try:
        movs = consultar_movimientos_dia_arqueo(admin, args.cajero, anio_a, mes_a, dia_a)
        if movs is None:
            print("Error al consultar movimientos.")
            return 1
        movs_banco = [m for m in movs if int(float(m.get("NROCMP", 0))) == 770500]
        movs_810291 = [m for m in movs if int(float(m.get("NROCMP", 0))) == 810291]
        movs_810291.sort(key=lambda m: (0, str(m.get("CLVMOV") or "").strip()) if m.get("CLVMOV") is not None else (1, ""))

        print("MOVIMIENTOS DÍA ARQUEO (770500 - banco)")
        print("-" * 40)
        if not movs_banco:
            print("  (ninguno)")
        for m in movs_banco:
            v = float(m.get("VALOR", 0) or 0)
            print(f"  VALOR={v:,.0f}  NROCMP={m.get('NROCMP')}  NUMDOC={m.get('NUMDOC')}  CLVMOV={m.get('CLVMOV')}")
        total_770500_bd = sum(float(m.get("VALOR", 0) or 0) for m in movs_banco)
        print(f"  -> Suma VALOR en BD: {total_770500_bd:,.0f}  (aplicamos signo contrario: {-total_770500_bd:,.0f})")
        print()

        print("MOVIMIENTOS DÍA ARQUEO (810291 - sucursal)")
        print("-" * 40)
        if not movs_810291:
            print("  (ninguno)")
        for m in movs_810291:
            v = float(m.get("VALOR", 0) or 0)
            print(f"  VALOR={v:,.0f}  NROCMP={m.get('NROCMP')}  CLVMOV={m.get('CLVMOV')}  NUMDOC={m.get('NUMDOC')}")
        total_810291_bd = sum(float(m.get("VALOR", 0) or 0) for m in movs_810291)
        print(f"  -> Suma VALOR en BD: {total_810291_bd:,.0f}  (aplicamos signo contrario)")
        print()

        fecha_desde_sob = obtener_fecha_ultimo_arqueo_para_sobrantes(
            args.cajero, fecha_arqueo, df, col_cajero, col_fa, lector
        )
        fecha_hasta_sob = fecha_arqueo - timedelta(days=1)
        if fecha_desde_sob is not None:
            print("RANGO CONSULTA SOBRANTES (desde arqueo anterior hasta arqueo actual - 1):")
            print("-" * 40)
            print(f"  Desde: {fecha_desde_sob} (arqueo anterior)")
            print(f"  Hasta: {fecha_hasta_sob} (dia antes del arqueo actual)")
            print()
        else:
            print("RANGO SOBRANTES: fallback (dia 1 del mes hasta dia antes del arqueo)")
            print()

        remanente_final, detalle = calcular_remanente_para_cajero_cuadrado(
            admin, args.cajero, anio_a, mes_a, dia_a,
            saldo, efectivo, dispensado, recibido,
            fecha_desde_sobrantes=fecha_desde_sob,
        )
        print("RESULTADO DEL CALCULO")
        print("-" * 40)
        print(f"Remanente final:        {remanente_final:,.0f}" if remanente_final is not None else "Remanente final: None")
        print(f"Remanente banco (770500+810291 aplicados): {detalle.get('remanente_banco', 0):,.0f}")
        print(f"Remanente 810291 (aporte):                 {detalle.get('remanente_810291', 0):,.0f}")
        print(f"Remanente sobrantes:     {detalle.get('remanente_sobrantes', 0):,.0f}")
        print(f"d0 (diferencia sin remanente): {detalle.get('diferencia_sin_remanente'):,.0f}")
        print(f"Diferencia tras banco:   {detalle.get('diferencia_after_banco'):,.0f}")
        print(f"Ratificado cuadrado:    {detalle.get('ratificado_cuadrado')}")
        print(f"Justificado sobrantes:  {detalle.get('justificado_sobrantes')}")
        print(f"Gestion manual:         {detalle.get('gestion_manual')}")
        if detalle.get("justificado_sobrantes") and detalle.get("sobrantes_utilizados"):
            print()
            print("SOBRANTES UTILIZADOS EN EL CRUCE (valor y fecha de cada registro):")
            print("-" * 40)
            for i, u in enumerate(detalle["sobrantes_utilizados"], 1):
                print(f"  {i}. dia {u.get('fecha_str', 'N/A')}  valor cruzado: {u.get('valor', 0):,.0f}")
            print(f"  -> Faltante cubierto: {detalle.get('valor_faltante', 0):,.0f}")
        # Si hubo faltante <= 20M y no se justifico con sobrantes, mostrar que se consulto sobrantes y que devolvio
        faltante = (detalle.get("diferencia_sin_remanente") or 0) - (remanente_final or 0)
        if remanente_final is not None and faltante > 0 and faltante <= 20_000_000 and not detalle.get("justificado_sobrantes"):
            vigentes = consultar_sobrantes_negativos_vigentes(
                admin, args.cajero, anio_a, mes_a, dia_a, fecha_desde=fecha_desde_sob
            )
            print()
            print("CONSULTA SOBRANTES (cuenta 279510020):")
            print("-" * 40)
            print(f"  Rango: desde {fecha_desde_sob or 'dia 1 mes (fallback)'} hasta {fecha_hasta_sob}")
            if vigentes is None:
                print("  Error al consultar.")
            elif not vigentes:
                print("  (sin movimientos negativos vigentes para este NIT)")
            else:
                for m in vigentes:
                    v = float(m.get("VALOR", 0) or 0)
                    print(f"  VALOR={v:,.0f}  FECHA={m.get('FECHA')}  (faltante a cruzar= {faltante:,.0f})")
                print("  -> Ninguno coincidio en monto (tolerancia 1 peso).")
        print()
        print("FORMULA Remanente / Provision / Ajustes:")
        print(f"  {detalle.get('formula_remanente', 'N/A')}")
        print("=" * 60)
    finally:
        admin.desconectar()
    return 0


if __name__ == "__main__":
    sys.exit(_main())
