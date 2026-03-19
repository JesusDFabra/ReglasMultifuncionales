"""
Carga la configuracion de insumos para reglas multifuncionales.
"""
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Raíz del proyecto (reglas_multifuncionales): src/config/cargador_config.py -> parent.parent.parent
PROYECTO_ROOT = Path(__file__).resolve().parent.parent.parent


class CargadorConfig:
    """Carga y expone la configuracion desde config/insumos.yaml."""

    def __init__(self, ruta_config: Optional[Path] = None):
        self.ruta_config = ruta_config or (PROYECTO_ROOT / "config" / "insumos.yaml")
        self._config: Optional[Dict[str, Any]] = None

    def cargar(self) -> Dict[str, Any]:
        if self._config is None:
            if not self.ruta_config.exists():
                raise FileNotFoundError(f"No se encontro la configuracion: {self.ruta_config}")
            with open(self.ruta_config, "r", encoding="utf-8") as f:
                self._config = yaml.safe_load(f)
            logger.info(f"Configuracion cargada: {self.ruta_config}")
        return self._config

    def obtener_directorio_insumos(self) -> Path:
        """Ruta absoluta a la carpeta de insumos. Si en config es ruta absoluta, se usa tal cual."""
        config = self.cargar()
        nombre_dir = config.get("directorios", {}).get("insumos", "insumos_excel")
        path = Path(nombre_dir)
        if path.is_absolute():
            return path
        return PROYECTO_ROOT / nombre_dir

    def obtener_fecha_proceso(self) -> str:
        """Fecha de proceso DD_MM_YYYY. Si no esta definida, usa fecha actual."""
        config = self.cargar()
        fecha = config.get("proceso", {}).get("fecha", "").strip()
        if fecha:
            return fecha
        from datetime import datetime
        return datetime.now().strftime("%d_%m_%Y")

    def obtener_insumos_activos(self) -> Dict[str, Any]:
        """Solo insumos activos."""
        config = self.cargar()
        insumos = config.get("insumos", {})
        return {k: v for k, v in insumos.items() if v.get("activo", True)}

    def obtener_umbrales_remanente(self) -> Dict[str, Any]:
        """Umbrales y opciones para reglas de Remanente (faltante_minimo_para_810291, faltante_limite_sobrantes, sobrantes_incluir_dia_arqueo)."""
        config = self.cargar()
        r = config.get("remanente", {})
        return {
            "faltante_minimo_para_810291": float(r.get("faltante_minimo_para_810291", 50_000_000)),
            "faltante_limite_sobrantes": float(r.get("faltante_limite_sobrantes", 20_000_000)),
            "sobrantes_incluir_dia_arqueo": bool(r.get("sobrantes_incluir_dia_arqueo", True)),
        }

    def obtener_config_bd(self) -> Dict[str, Any]:
        """Configuración de base de datos (NACIONAL). usuario/clave desde usuario_nal y clave_nal."""
        config = self.cargar()
        bd = config.get("base_datos", {})
        usuario = bd.get("usuario_nal") or bd.get("usuario", "")
        clave = bd.get("clave_nal") or bd.get("clave", "")
        return {
            "usar_bd": bd.get("usar_bd", False),
            "servidor": bd.get("servidor", "NACIONAL"),
            "usuario": usuario,
            "clave": clave,
        }
