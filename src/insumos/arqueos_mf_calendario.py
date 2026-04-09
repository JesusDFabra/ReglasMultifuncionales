"""
Utilidad para decidir qué libro mensual ARQUEOS MF usar según fechas de arqueo / descarga.

Los arqueos cargados el 1.º u primeros días del mes pueden tener fecha de arqueo del mes anterior
mientras la «Fecha descarga arqueo» es del mes actual (o viceversa). Cada fila debe ir al archivo
MM- ARQUEOS MF cuyo mes calendario coincide con el mes de **Fecha Arqueo** de ese registro.
"""
from datetime import date, datetime
from typing import List, Optional, Tuple

import pandas as pd


def periodo_libro_desde_fecha_arqueo(fa: date) -> Tuple[int, int]:
    """Mes y año del nombre de archivo ARQUEOS MF para una fecha de arqueo."""
    return fa.month, fa.year


def valor_a_fecha_celda(val) -> Optional[date]:
    """Parsea valores típicos de Excel/celda a date (misma semántica que en movimientos_remanente)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    if hasattr(val, "date"):
        try:
            return val.date()
        except Exception:
            return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def meses_libro_candidatos_fecha_descarga(fecha_descarga: date) -> List[Tuple[int, int]]:
    """
    Libros mensuales que pueden contener filas con esta «Fecha descarga arqueo»
    (pegadas el mismo día de gestión con arqueos de fin/inicio de mes).

    Incluye el mes calendario de la descarga y el mes anterior.
    """
    m, y = fecha_descarga.month, fecha_descarga.year
    if m == 1:
        return [(12, y - 1), (1, y)]
    return [(m - 1, y), (m, y)]
