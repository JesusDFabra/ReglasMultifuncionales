# Por qué había dos formas de calcular el remanente

## 1. `calcular_remanente_para_cajero_cuadrado` (la que sí usas)

- **Qué hace:** Consulta la BD (cuenta cajero, día del arqueo), toma el 770500 con **valor opuesto** al de la BD, y si queda faltante consulta la **cuenta de sobrantes** (279510020) para el cruce.
- **Dónde se usa:** En el flujo de “gestión del 13” (y cualquier fecha): `procesar_cuadrados_fecha_descarga` → esta función.
- **Resumen:** Es la regla completa (770500 opuesto + sobrantes). Es la que se ejecuta cuando corres la gestión.

## 2. `calcular_remanente_segun_regla` (la otra)

- **Qué hace:** No consulta la BD; recibe la lista de movimientos ya cargada. Usa el 770500 en **positivo** (`abs`) y el 810291 si justifica la diferencia. **No** mira la cuenta de sobrantes.
- **Dónde se usa:** Solo dentro de `calcular_remanente_para_registro`, y esa función **no la llama** el `main.py` (no se usa en “gestión del 13” ni en el flujo normal).
- **Resumen:** Es una lógica antigua/distinta (770500 en positivo, sin sobrantes). Quedó como código alternativo o para otro uso.

## Conclusión

- La **única** “consulta de remanente” que realmente se usa cuando ejecutas la gestión es la de **calcular_remanente_para_cajero_cuadrado** (770500 con signo opuesto + sobrantes).
- La otra existe porque en su momento se implementó otra forma de calcular (sin BD en la función, sin sobrantes) y quedó solo usada por `calcular_remanente_para_registro`, que hoy no se llama desde el flujo principal.

Si quieres, el siguiente paso es **unificar**: que `calcular_remanente_para_registro` use `calcular_remanente_para_cajero_cuadrado` y así quede **una sola** lógica de remanente en todo el proyecto.
