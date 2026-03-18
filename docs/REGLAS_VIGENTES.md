# Reglas vigentes – reglas_multifuncionales

## ¿Hay alguna regla que se active según si el registro llega por faltante, sobrante o cuadrado?

**No.** Hoy **no** existe una regla que se active solo cuando el registro “llega” como faltante, sobrante o cuadrado.

- Se procesan **todos** los registros con la misma **Fecha descarga arqueo** (cuadrados y descuadrados).
- A todos se les aplica la **misma lógica**: consulta 770500 día del arqueo → Remanente con signo contrario → si después de eso **queda** faltante, se busca en sobrantes; si **queda** cuadrado, se ratifica.
- Lo que cambia es el **resultado después del 770500** (no cómo llegó el registro):
  - **Después queda cuadrado** → ratificado, texto "Cajero cuadrado...".
  - **Después queda faltante** → se busca en cuenta sobrantes (desde un día antes del arqueo); si hay coincidencia → cruce y texto de cruce.
  - **Después queda sobrante** → solo se actualiza Remanente con el 770500; no se busca en sobrantes ni se escribe texto de cruce.

---

## Reglas que hay hasta el momento

### 1. Ajuste 770500 (cuenta cajero, día del arqueo)

- **Cuándo:** Para cada registro con la Fecha descarga arqueo indicada.
- **Qué hace:** Consulta movimientos con comprobante **770500** en la cuenta del cajero (110505075) el **día del arqueo**. El valor se lleva a la columna **Remanente / Provisión / Ajustes** con el **signo contrario** al de la base de datos.
- **Dónde:** `calcular_remanente_para_cajero_cuadrado` → usada en `procesar_cuadrados_fecha_descarga`.

---

### 2. Cruce faltante–sobrante (cuenta sobrantes 279510020)

- **Cuándo:** Solo si **después** de aplicar el 770500 **queda faltante** (diferencia > 0).
- **Qué hace:** Consulta la cuenta de sobrantes (279510020) desde **un día antes del arqueo** hacia atrás, toma negativos vigentes (hasta el primer positivo). Si algún negativo coincide en monto con el faltante, se hace el **cruce**: Remanente = remanente_banco + valor del sobrante, y se escribe en Gestión a Realizar el texto de cruce (si la celda está vacía).
- **Dónde:** `consultar_sobrantes_negativos_vigentes` + rama `justificado_sobrantes` en `calcular_remanente_para_cajero_cuadrado` y en `procesar_cuadrados_fecha_descarga`.

---

### 3. Ratificado cuadrado

- **Cuándo:** Si **después** de aplicar el 770500 la diferencia queda **≈ 0** (cuadrado).
- **Qué hace:** Se considera ratificado; se puede escribir en Gestión a Realizar "Cajero cuadrado, sin observación por parte de la sucursal" (solo si la celda está vacía).
- **Dónde:** Rama `ratificado_cuadrado` en `calcular_remanente_para_cajero_cuadrado` y en `procesar_cuadrados_fecha_descarga`.

---

### 4. No sobrescribir Gestión a Realizar

- **Cuándo:** Siempre que se vaya a escribir algo en "Gestión a Realizar".
- **Qué hace:** Si la celda ya tiene un valor (no está vacía), **no se sobrescribe**; solo se actualiza Remanente si aplica.
- **Dónde:** Comprobación `gestion_no_vacia` en `procesar_cuadrados_fecha_descarga`.

---

### 5. Verificación saldo contable vs NACIONAL (opcional)

- **Cuándo:** Si se ejecuta con `--verificar-saldo` (o la opción equivalente).
- **Qué hace:** Compara el Saldo contable del Excel con el saldo en BD NACIONAL para las filas con la Fecha descarga arqueo indicada. Donde no coincida, escribe en Gestión a Realizar un mensaje de discrepancia.
- **Dónde:** `verificar_saldos_contables_nacional` + `marcar_discrepancias_gestion_a_realizar`.

---

### 6. Pegado sin huecos

- **Cuándo:** Al pegar gestión o consolidado en ARQUEOS MF.
- **Qué hace:** Antes de concatenar, se eliminan filas vacías al final del archivo (por columnas clave), para que los registros nuevos queden seguidos, sin espacios en blanco.
- **Dónde:** `_pegar_filas_a_arqueos_mf` en `pegar_gestion_a_arqueos_mf.py`.

---

### 7. Fórmulas sin tocar Gestión

- **Cuándo:** Al escribir fórmulas de Diferencia y Naturaleza en el Excel.
- **Qué hace:** Solo se escriben las columnas Diferencia y Naturaleza; se evita escribir en la columna "Gestión a Realizar" para no sobrescribir textos de cruce o ratificado.
- **Dónde:** `_escribir_formula_diferencia_en_excel` en `pegar_gestion_a_arqueos_mf.py`.

---

## Resumen

| Regla                         | Se activa por “cómo llega” (faltante/sobrante/cuadrado) | Se activa por resultado tras 770500 |
|------------------------------|---------------------------------------------------------|-------------------------------------|
| Ajuste 770500                | No (a todos)                                            | —                                   |
| Cruce faltante–sobrante      | No                                                      | Sí (si queda faltante)              |
| Ratificado cuadrado          | No                                                      | Sí (si queda cuadrado)              |
| No sobrescribir Gestión      | Siempre                                                 | —                                   |
| Verificación saldo           | No (por opción y fecha descarga)                        | —                                   |
| Pegado sin huecos / Fórmulas | No (por flujo de pegado)                                | —                                   |

Hasta el momento **no** hay reglas que se activen específicamente porque el registro “llegue” por faltante, sobrante o cuadrado; la única distinción relevante es **cómo queda la diferencia después del ajuste 770500** (faltante, sobrante o cuadrado).
