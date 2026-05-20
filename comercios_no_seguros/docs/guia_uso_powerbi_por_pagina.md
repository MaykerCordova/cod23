# Guía de Uso en Power BI — Por Página del Dashboard
## CPF · Reporte de Fraude E-commerce No 3DS

> Este documento está organizado por **página del dashboard**.
> Para cada página se indica:
>   - Qué variables del parquet se usan
>   - En qué tipo de visual se colocan
>   - Con qué medidas DAX se combinan
>   - Cómo interpretar el resultado

---

# PÁGINA 1 — Resumen Ejecutivo

**Objetivo:** Vista general para el director. Responde:
¿cuánto, dónde y cómo está el fraude en total?

---

## Bloque A · Tarjetas KPI (parte superior)

| Variable / Medida | Visual | Configuración | Interpretación |
|---|---|---|---|
| `[# Fraudes]` | Tarjeta KPI | Sin eje, solo valor | Total de transacciones fraudulentas |
| `[Monto Fraude]` | Tarjeta KPI | Formato S/ #,##0 | Monto total defraudado |
| `[Tarjetas Únicas]` | Tarjeta KPI | Sin decimales | Personas afectadas (aproximado) |
| `[Comercios Únicos]` | Tarjeta KPI | Sin decimales | Comercios comprometidos |
| `[Ticket Promedio]` | Tarjeta KPI | Formato S/ #,##0.00 | Monto promedio por fraude |
| `[Monto Promedio por Tarjeta]` | Tarjeta KPI | Formato S/ #,##0.00 | Pérdida promedio por tarjeta afectada |

---

## Bloque B · Evolutivo Mensual

**Variables del parquet:** `MES_NOM`, `ANIO`, `DATETIME_TRX`
**Medidas DAX:** `[# Fraudes]`, `[Monto Fraude]`, `[Tarjetas Únicas]`

| Configuración | Detalle |
|---|---|
| Visual | Gráfico de líneas + columnas combinado |
| Eje X | `MES_NOM` ordenado por `MES` |
| Eje Y izquierdo | `[Monto Fraude]` (barras) |
| Eje Y derecho | `[# Fraudes]` y `[Tarjetas Únicas]` (líneas) |
| Filtro de página | `ANIO` para comparar años |

**Interpretación:** Una barra alta con línea baja = fraude de monto alto pero pocas transacciones (ticket grande). Una línea alta con barra baja = muchas transacciones pero de poco monto (card testing).

---

## Bloque C · Top Comercios y Top BIN

**Variables del parquet:** `COMERCIO_NOMBRE`, `BIN`, `RANKING_COMERCIO`
**Medidas DAX:** `[Monto Fraude]`, `[% Participación sobre Total]`

| Configuración | Detalle |
|---|---|
| Visual | Barras horizontales |
| Eje Y | `COMERCIO_NOMBRE` (ordenar por `[Monto Fraude]` DESC) |
| Valor | `[Monto Fraude]` |
| Etiqueta de datos | `[% Participación sobre Total]` |
| Top N | Filtro visual → Top 10 por `[Monto Fraude]` |

**Interpretación:** El % de participación dice cuánto concentra ese comercio del total. Si APPLE.COM tiene 14% → actuar sobre ese comercio reduce el fraude en un 14%.

---

## Bloque D · Evolutivo por % Alertamiento y CVV

**Variable del parquet:** `CVV_DINAMICO`, `FLAG_CVV_ESTATICO`, `MES_NOM`
**Medidas DAX:** `[% CVV Estático]`, `[Monto Fraude]`

| Configuración | Detalle |
|---|---|
| Visual | Barras apiladas al 100% |
| Eje X | `MES_NOM` |
| Leyenda | `CVV_DINAMICO` (Dinámico / Estático) |
| Valor | `[Monto Fraude]` |

**Interpretación:** Si la franja roja (CVV estático) crece mes a mes → el fraude está migrando hacia tarjetas más vulnerables.

---

## Bloque E · Top Segmento

**Variable del parquet:** `SEGMENTO`
**Medidas DAX:** `[Monto Fraude]`, `[# Fraudes]`, `[% Participación sobre Total]`

| Configuración | Detalle |
|---|---|
| Visual | Barras horizontales |
| Eje Y | `SEGMENTO` ordenado por `[Monto Fraude]` DESC |
| Valor | `[Monto Fraude]` |
| Etiqueta | `[% Participación sobre Total]` |

---

## Slicers recomendados — Página 1

| Slicer | Campo | Tipo |
|---|---|---|
| Año Fiscal | `dim_Calendario[Año Fiscal]` | Desplegable |
| BIN | `BIN` | Desplegable |
| Comercio | `COMERCIO_NOMBRE` | Desplegable |
| Organización | `ORGANIZACION` | Botones (SVP / CSF) |

---

---

# PÁGINA 2 — Análisis Temporal

**Objetivo:** Identificar **cuándo** ocurre el fraude. Patrones de hora, día y franja.

---

## Bloque A · KPIs temporales

| Variable / Medida | Visual | Interpretación |
|---|---|---|
| `[Hora Pico]` | Tarjeta KPI | La hora con más fraudes — ¿coincide con bajo monitoreo? |
| `[% Madrugada]` | Tarjeta KPI | % de fraudes 00-05h — benchmark normal < 5% |
| `[% Fin de Semana]` | Tarjeta KPI | % en Sáb/Dom — más fraude cuando hay menos vigilancia |
| `[% Horario Laboral]` | Tarjeta KPI | % en horario 8-18h Lun-Vie |
| `[Día Pico]` | Tarjeta KPI | Día de la semana con más fraudes |

---

## Bloque B · Fraude por Día de Semana

**Variable del parquet:** `DIA_SEMANA_NOM`, `Orden Día` (columna calculada)
**Medidas DAX:** `[Monto Fraude]`, `[# Fraudes]`, `[Color Día]`

| Configuración | Detalle |
|---|---|
| Visual | Barras verticales agrupadas |
| Eje X | `DIA_SEMANA_NOM` ordenado por `Orden Día` |
| Valor | `[Monto Fraude]` |
| Color | Aplicar `[Color Día]` → rojo Sáb/Dom, azul Lun-Vie |

**Interpretación:** Si Sáb y Dom tienen barras altas → el fraude aprovecha el fin de semana. Refuerzo de alertas automáticas los fines de semana.

---

## Bloque C · Fraude por Franja Horaria

**Variable del parquet:** `FRANJA_HORARIA`, `Orden Franja` (columna calculada)
**Medidas DAX:** `[Monto Fraude]`, `[# Fraudes]`

| Configuración | Detalle |
|---|---|
| Visual | Barras horizontales |
| Eje Y | `FRANJA_HORARIA` ordenado por `Orden Franja` |
| Valor | `[Monto Fraude]` y `[# Fraudes]` |
| Orden | MADRUGADA → MAÑANA → TARDE → NOCHE |

**Interpretación:** MADRUGADA con alto % → fraude automatizado fuera del horario de vigilancia.

---

## Bloque D · Mapa de Calor Hora × Día

**Variables del parquet:** `HORA_DIA` (0-23), `DIA_SEMANA_NOM`, `Orden Día`
**Medidas DAX:** `[# Fraudes]`

| Configuración | Detalle |
|---|---|
| Visual | **Matriz** (no tabla) |
| Filas | `HORA_DIA` |
| Columnas | `DIA_SEMANA_NOM` ordenado por `Orden Día` |
| Valores | `[# Fraudes]` |
| Formato condicional | Valores de celda → Color de fondo → Escala: blanco → rojo `#C00000` |

**Interpretación:** Las celdas más oscuras indican los momentos de mayor concentración de fraude. El cruce hora-día con más oscuro es la ventana de mayor riesgo.

---

## Bloque E · Evolutivo Mensual por Franja

**Variables del parquet:** `MES_NOM`, `ANIO`, `FRANJA_HORARIA`
**Medidas DAX:** `[Monto Fraude]`

| Configuración | Detalle |
|---|---|
| Visual | Barras apiladas |
| Eje X | `MES_NOM` ordenado por `MES` |
| Leyenda | `FRANJA_HORARIA` |
| Valor | `[Monto Fraude]` |

**Interpretación:** Ver si alguna franja crece desproporcionadamente mes a mes → señal de nuevo patrón de fraude.

---

## Bloque F · Ventanas Temporales *(variables nuevas)*

**Variables del parquet:** `TXN_CARD_2M`, `TXN_CARD_5M`, `TXN_CARD_10M`, `TXN_CARD_1H`, `TXN_CARD_24H`, `AMT_CARD_1H`, `AMT_CARD_24H`, `FLAG_VEL_ALTA_1H`, `FLAG_VEL_ALTA_10M`

| Variable | Qué mide | Uso en Power BI |
|---|---|---|
| `TXN_CARD_2M` | Txn previas de esa tarjeta en los 2 min anteriores | Histograma — distribución de velocidad |
| `TXN_CARD_5M` | Txn previas en los 5 min anteriores | Mismo |
| `TXN_CARD_10M` | Txn previas en los 10 min anteriores | Mismo |
| `TXN_CARD_1H` | Txn previas en la última hora | KPI o slicer de umbral |
| `TXN_CARD_24H` | Txn previas en las últimas 24h | Tabla de detalle |
| `AMT_CARD_1H` | Monto acumulado previo en 1 hora | Comparar con monto actual |
| `AMT_CARD_24H` | Monto acumulado previo en 24h | Tabla de detalle |
| `FLAG_VEL_ALTA_1H` | 1 si tuvo 2+ fraudes en la hora previa | KPI: % velocidad alta |
| `FLAG_VEL_ALTA_10M` | 1 si tuvo 2+ fraudes en los 10 min previos | KPI: ráfaga rápida |

**Medidas DAX recomendadas para estas variables:**

```dax
% Velocidad Alta 1H =
DIVIDE(
    CALCULATE(
        COUNTROWS('fraudes_comercios_no_seguros_features'),
        'fraudes_comercios_no_seguros_features'[FLAG_VEL_ALTA_1H] = 1
    ),
    [# Fraudes]
)
-- Formato: 0.0%
-- Interpretación: 15% → el 15% de los fraudes ocurrieron cuando
--   esa tarjeta ya había tenido otro fraude en la hora previa
```

```dax
% Velocidad Alta 10 Min =
DIVIDE(
    CALCULATE(
        COUNTROWS('fraudes_comercios_no_seguros_features'),
        'fraudes_comercios_no_seguros_features'[FLAG_VEL_ALTA_10M] = 1
    ),
    [# Fraudes]
)
-- Formato: 0.0%
-- Interpretación: % de fraudes que ocurrieron con otro fraude
--   previo de la misma tarjeta en los últimos 10 minutos
--   → señal de fraude automatizado / bot
```

```dax
Promedio TXN en 1H previa =
AVERAGE('fraudes_comercios_no_seguros_features'[TXN_CARD_1H])
-- Formato: 0.0
```

```dax
Promedio Monto Acumulado 24H =
AVERAGE('fraudes_comercios_no_seguros_features'[AMT_CARD_24H])
-- Formato: S/ #,##0.00
```

**Cómo usarlas en la tabla de detalle:**
Agrega las columnas `TXN_CARD_1H`, `AMT_CARD_1H`, `TXN_CARD_24H`, `AMT_CARD_24H` a la tabla detalle de cualquier página. Con formato condicional: a mayor valor → más rojo.

---

## Slicers recomendados — Página 2

| Slicer | Campo |
|---|---|
| Mes | `MES_NOM` ordenado por `MES` |
| Canal | `CANAL` |
| Organización | `ORGANIZACION` |
| Tipo Tarjeta | `TIPO_TARJETA` |

---

---

# PÁGINA 3 — Comercios & MCC

**Objetivo:** Identificar **dónde** se concentra el fraude y en qué rubros.

---

## Bloque A · KPIs de comercio

| Medida | Visual | Interpretación |
|---|---|---|
| `[Top Comercio]` | Tarjeta KPI texto | Comercio más golpeado actualmente |
| `[Top MCC]` | Tarjeta KPI texto | Rubro más golpeado |
| `[Comercios Únicos]` | Tarjeta KPI | Cuántos comercios distintos tienen fraude |
| `[% Top 10 Comercios]` | Tarjeta KPI % | Concentración — si es > 60% el riesgo está muy focalizado |
| `[Monto Promedio por Comercio]` | Tarjeta KPI | Monto promedio de fraude por comercio |

---

## Bloque B · Top Comercios

**Variables del parquet:** `COMERCIO_NOMBRE`, `RANKING_COMERCIO`, `TOTAL_FRAUDES_COMERCIO`, `MONTO_TOTAL_FRAUDE_COM`
**Medidas DAX:** `[Monto Fraude]`, `[# Fraudes]`, `[% Participación sobre Total]`, `[% Ráfaga en Comercio]`

| Configuración | Detalle |
|---|---|
| Visual | Barras horizontales |
| Eje Y | `COMERCIO_NOMBRE` ordenado por `[Monto Fraude]` DESC |
| Valor | `[Monto Fraude]` |
| Etiqueta | `[% Participación sobre Total]` |
| Filtro | Top 20 por `[Monto Fraude]` |
| Tooltip adicional | `[% Ráfaga en Comercio]`, `[Días con Fraude]`, `[Tarjetas en Comercio]` |

**Interpretación:** Ordenar por monto para priorizar impacto financiero. Ver `% Ráfaga` en tooltip para saber si el comercio tiene patrón de velocidad.

---

## Bloque C · Top MCC

**Variables del parquet:** `MCC`, `RANKING_MCC`, `TOTAL_FRAUDES_MCC`
**Medidas DAX:** `[Monto Fraude]`, `[# Fraudes]`, `[Comercios Únicos]`

| Configuración | Detalle |
|---|---|
| Visual | Barras horizontales |
| Eje Y | `MCC` ordenado por `[# Fraudes]` DESC |
| Valor | `[Monto Fraude]` |
| Etiqueta | `[Comercios Únicos]` (cuántos comercios dentro de ese MCC) |
| Filtro | Top 10 por `[Monto Fraude]` |

**Interpretación:** Un MCC con muchos comercios distintos → el fraude está disperso en ese rubro. Un MCC con 1-2 comercios → fraude concentrado en un actor específico.

---

## Bloque D · Scatter de Comercios

**Variables del parquet:** `COMERCIO_NOMBRE`, `TOTAL_FRAUDES_COMERCIO`, `MONTO_TOTAL_FRAUDE_COM`, `TARJETAS_DISTINTAS_COM`, `MCC`
**Medidas DAX:** `[# Fraudes]`, `[Monto Fraude]`, `[Tarjetas Distintas Comercio]`

| Configuración | Detalle |
|---|---|
| Visual | Gráfico de dispersión (scatter) |
| Eje X | `[# Fraudes]` |
| Eje Y | `[Monto Fraude]` |
| Tamaño burbuja | `[Tarjetas Distintas Comercio]` |
| Leyenda (color) | `MCC` |
| Detalles | `COMERCIO_NOMBRE` |

**Interpretación de cuadrantes:**

| Posición | Significado | Acción |
|---|---|---|
| Arriba derecha (mucho fraude + mucho monto) | Comercio crítico | Intervención urgente |
| Arriba izquierda (poco fraude + mucho monto) | Fraudes de ticket alto | Investigar transacciones grandes |
| Abajo derecha (mucho fraude + poco monto) | Card testing masivo | Revisar patrones de velocidad |
| Abajo izquierda | Impacto bajo | Monitoreo estándar |

---

## Bloque E · Tabla Detalle por Comercio

**Variables del parquet:** `COMERCIO_NOMBRE`, `MCC`, `TARJETAS_DISTINTAS_COM`, `DIAS_CON_FRAUDE_COM`, `RANKING_COMERCIO`
**Medidas DAX:** `[# Fraudes]`, `[Monto Fraude]`, `[Ticket Promedio]`, `[Ticket Mínimo]`, `[Ticket Máximo]`, `[% Ráfaga en Comercio]`, `[% Monto Redondo en Comercio]`, `[Fraudes por Tarjeta en Comercio]`, `[Días con Fraude]`

**Formato condicional:**
- `[# Fraudes]` → escala blanco → rojo
- `[% Ráfaga en Comercio]` → escala blanco → rojo
- `[% Monto Redondo]` → si > 20% pintar celda naranja

---

## Slicers recomendados — Página 3

| Slicer | Campo |
|---|---|
| MCC | `MCC` |
| Canal | `CANAL` |
| Organización | `ORGANIZACION` |
| Tipo Tarjeta | `TIPO_TARJETA` |
| Rango monto | `RANGO_MONTO` |

---

---

# PÁGINA 4 — Perfil de Tarjetas

**Objetivo:** Entender **a quién** le ocurre el fraude y qué perfil tienen las tarjetas afectadas.

---

## Bloque A · KPIs de tarjeta

| Medida | Visual | Interpretación |
|---|---|---|
| `[% Tarjetas Reincidentes]` | Tarjeta KPI % | % que no fue bloqueada a tiempo |
| `[% Ráfaga (3+ fraudes/día)]` | Tarjeta KPI % | % en operaciones masivas |
| `[% Multi Comercio en un Día]` | Tarjeta KPI % | % atacando múltiples comercios |
| `[Monto Promedio por Tarjeta]` | Tarjeta KPI S/ | Pérdida promedio por tarjeta |
| `[% Tarjetas MUY ALTO Riesgo]` | Tarjeta KPI % | % de tarjetas con 3+ señales |

---

## Bloque B · Crédito vs Débito

**Variable del parquet:** `TIPO_TARJETA`
**Medidas DAX:** `[# Fraudes]`, `[Monto Fraude]`, `[% Crédito]`, `[% Débito]`, `[Ticket Promedio]`, `[Color Tipo Tarjeta]`

| Configuración | Detalle |
|---|---|
| Visual | Gráfico de anillo (donut) |
| Leyenda | `TIPO_TARJETA` |
| Valores | `[Monto Fraude]` |
| Colores | Aplicar `[Color Tipo Tarjeta]` |
| Tooltip | `[# Fraudes]`, `[Ticket Promedio]` |

**Interpretación:** Si crédito domina en monto pero débito en cantidad → el fraude de débito es de menor ticket pero más frecuente.

---

## Bloque C · Por Nivel de Tarjeta

**Variable del parquet:** `NIVEL_TARJETA`
**Medidas DAX:** `[# Fraudes]`, `[Monto Fraude]`, `[Ticket Promedio]`, `[Color Nivel Tarjeta]`

| Configuración | Detalle |
|---|---|
| Visual | Barras verticales agrupadas |
| Eje X | `NIVEL_TARJETA` |
| Valor | `[Monto Fraude]` y `[# Fraudes]` |
| Colores | Aplicar `[Color Nivel Tarjeta]` |

**Interpretación:** Tarjetas PLATINUM o BLACK con alto monto → fraude de alto valor. Tarjetas CLASSIC con muchos fraudes → volumen masivo de menor ticket.

---

## Bloque D · Distribución PERFIL_RIESGO

**Variable del parquet:** `PERFIL_RIESGO`, `Orden Perfil` (columna calculada), `SCORE_RIESGO_TRJ`
**Medidas DAX:** `[# Fraudes]`, `[Monto Fraude]`, `[Ticket Promedio]`

| Configuración | Detalle |
|---|---|
| Visual | Barras horizontales |
| Eje Y | `PERFIL_RIESGO` ordenado por `Orden Perfil` |
| Valor | `[# Fraudes]` (barras) + `[Monto Fraude]` (línea o segunda barra) |
| Orden | BAJO → MEDIO → ALTO → MUY_ALTO |

**Interpretación clave:**
- `MUY_ALTO` con muchas txn pero poco monto → fraude de velocidad (card testing, bots)
- `BAJO` con poco volumen pero mucho monto → fraude sofisticado de alto impacto
- Ambos necesitan atención pero estrategias distintas

---

## Bloque E · Por Segmento

**Variable del parquet:** `SEGMENTO`
**Medidas DAX:** `[Monto Fraude]`, `[# Fraudes]`, `[Tarjetas Únicas]`

| Configuración | Detalle |
|---|---|
| Visual | Barras horizontales |
| Eje Y | `SEGMENTO` ordenado por `[Monto Fraude]` DESC |
| Valor | `[Monto Fraude]` |
| Etiqueta | `[Tarjetas Únicas]` |

---

## Bloque F · Tabla Top Tarjetas

**Variables del parquet:** `TARJETA`, `TIPO_TARJETA`, `NIVEL_TARJETA`, `SEGMENTO`, `PERFIL_RIESGO`, `SCORE_RIESGO_TRJ`, `TOTAL_FRAUDES_TARJETA`, `COMERCIOS_DISTINTOS_TRJ`, `DIAS_ACTIVA_TRJ`, `TXN_CARD_24H`, `AMT_CARD_24H`
**Medidas DAX:** `[Monto Fraude]`, `[# Fraudes]`

| Columna en tabla | Formato condicional |
|---|---|
| `SCORE_RIESGO_TRJ` | 0-1 verde / 2-3 amarillo / 4-6 rojo |
| `TOTAL_FRAUDES_TARJETA` | Escala blanco → rojo |
| `TXN_CARD_24H` | Escala blanco → naranja |
| `AMT_CARD_24H` | Escala blanco → naranja |
| `PERFIL_RIESGO` | Texto: BAJO=verde / MEDIO=amarillo / ALTO=naranja / MUY_ALTO=rojo |

**Tip:** Filtrar tabla para mostrar solo tarjetas con `TOTAL_FRAUDES_TARJETA > 1` (reincidentes) para ver el patrón más claro.

---

## Slicers recomendados — Página 4

| Slicer | Campo |
|---|---|
| Perfil Riesgo | `PERFIL_RIESGO` ordenado por `Orden Perfil` |
| Tipo Tarjeta | `TIPO_TARJETA` |
| Nivel Tarjeta | `NIVEL_TARJETA` |
| Segmento | `SEGMENTO` |
| Mes | `MES_NOM` |

---

---

# REFERENCIA RÁPIDA — Variables nuevas de ventanas temporales

| Variable | Ventana | Qué mide | Señal de alerta |
|---|---|---|---|
| `TXN_CARD_2M` | 2 min | Txn previas de la misma tarjeta | ≥ 1 → sospechoso |
| `TXN_CARD_5M` | 5 min | Txn previas de la misma tarjeta | ≥ 2 → alerta |
| `TXN_CARD_10M` | 10 min | Txn previas de la misma tarjeta | ≥ 2 → alerta |
| `TXN_CARD_1H` | 1 hora | Txn previas de la misma tarjeta | ≥ 3 → crítico |
| `TXN_CARD_24H` | 24 horas | Txn previas de la misma tarjeta | ≥ 5 → crítico |
| `AMT_CARD_1H` | 1 hora | Monto acumulado previo | Comparar con límite del cliente |
| `AMT_CARD_24H` | 24 horas | Monto acumulado previo | Comparar con límite del cliente |
| `FLAG_VEL_ALTA_1H` | 1 hora | 1 si hubo 2+ fraudes previos | Directo a accionable |
| `FLAG_VEL_ALTA_10M` | 10 min | 1 si hubo 2+ fraudes previos en 10 min | Señal de bot/automatización |

**Nota:** Estas variables miden cuántos fraudes ya había cometido esa tarjeta ANTES de la transacción actual. Son retrospectivas — muestran el contexto de velocidad en que ocurrió cada fraude.
