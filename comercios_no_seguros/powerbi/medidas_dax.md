# Medidas DAX — Comercios No Seguros (E-commerce No 3DS)
# CPF · Centro de Prevención del Fraude

> Pegar cada bloque en Power BI: Modelado → Nueva medida

---

## BASE — Medidas fundamentales (todas las páginas las usan)

```dax
# Fraudes =
COUNTROWS('fraudes_comercios_no_seguros_features')
```

```dax
Monto Fraude =
SUM('fraudes_comercios_no_seguros_features'[IMPORTE])
```

```dax
Monto Fraude USD =
SUM('fraudes_comercios_no_seguros_features'[IMPORTE_USD])
```

```dax
Ticket Promedio =
DIVIDE(
    [Monto Fraude],
    [# Fraudes]
)
-- Formato: #,##0.00
```

```dax
Tarjetas Únicas =
DISTINCTCOUNT('fraudes_comercios_no_seguros_features'[TARJETA])
```

```dax
Comercios Únicos =
DISTINCTCOUNT('fraudes_comercios_no_seguros_features'[COMERCIO_ID])
```

```dax
MCC Únicos =
DISTINCTCOUNT('fraudes_comercios_no_seguros_features'[MCC])
```

```dax
Monto Promedio por Tarjeta =
DIVIDE(
    [Monto Fraude],
    [Tarjetas Únicas]
)
-- Formato: #,##0.00
```

---

## PÁGINA 1 — Resumen Ejecutivo

```dax
-- % participación del segmento / comercio / BIN seleccionado
-- sobre el total general (ignora filtros del visual para el denominador)
% Participación Monto =
DIVIDE(
    [Monto Fraude],
    CALCULATE([Monto Fraude], ALL('fraudes_comercios_no_seguros_features'))
)
-- Formato: 0.00%
```

```dax
-- Variación mes a mes del monto
Variación Monto MoM =
VAR mes_actual =
    CALCULATE(
        [Monto Fraude],
        DATESMTD(dim_Calendario[Date])
    )
VAR mes_anterior =
    CALCULATE(
        [Monto Fraude],
        DATEADD(DATESMTD(dim_Calendario[Date]), -1, MONTH)
    )
RETURN
    DIVIDE(mes_actual - mes_anterior, mes_anterior)
-- Formato: +0.0%;-0.0%
```

```dax
-- % Monto Alertado vs No Alertado
-- (ajusta el nombre de columna si tu campo se llama diferente)
% Monto Alertado =
DIVIDE(
    CALCULATE(
        [Monto Fraude],
        'fraudes_comercios_no_seguros_features'[CVV_DINAMICO] = "S"
    ),
    [Monto Fraude]
)
-- Formato: 0.0%
```

---

## PÁGINA 2 — Análisis Temporal

```dax
Hora Pico =
VAR resumen =
    ADDCOLUMNS(
        VALUES('fraudes_comercios_no_seguros_features'[HORA_DIA]),
        "conteo", CALCULATE(COUNTROWS('fraudes_comercios_no_seguros_features'))
    )
VAR hora_max =
    MAXX(TOPN(1, resumen, [conteo], DESC), [HORA_DIA])
RETURN
    hora_max & ":00 hrs"
```

```dax
% Madrugada =
DIVIDE(
    CALCULATE(
        COUNTROWS('fraudes_comercios_no_seguros_features'),
        'fraudes_comercios_no_seguros_features'[FRANJA_HORARIA] = "MADRUGADA"
    ),
    [# Fraudes]
)
-- Formato: 0.0%
```

```dax
% Fin de Semana =
DIVIDE(
    CALCULATE(
        COUNTROWS('fraudes_comercios_no_seguros_features'),
        'fraudes_comercios_no_seguros_features'[ES_FIN_SEMANA] = 1
    ),
    [# Fraudes]
)
-- Formato: 0.0%
```

```dax
% Horario Laboral =
DIVIDE(
    CALCULATE(
        COUNTROWS('fraudes_comercios_no_seguros_features'),
        'fraudes_comercios_no_seguros_features'[ES_HORARIO_LAB] = 1
    ),
    [# Fraudes]
)
-- Formato: 0.0%
```

```dax
Día Pico =
VAR resumen =
    ADDCOLUMNS(
        VALUES('fraudes_comercios_no_seguros_features'[DIA_SEMANA_NOM]),
        "conteo", CALCULATE(COUNTROWS('fraudes_comercios_no_seguros_features'))
    )
RETURN
    MAXX(TOPN(1, resumen, [conteo], DESC), [DIA_SEMANA_NOM])
```

```dax
-- Columna calculada para ordenar días correctamente en los visuales
-- Modelado → Nueva columna (no medida)
Orden Día =
SWITCH('fraudes_comercios_no_seguros_features'[DIA_SEMANA_NOM],
    "LUN", 1,
    "MAR", 2,
    "MIE", 3,
    "JUE", 4,
    "VIE", 5,
    "SAB", 6,
    "DOM", 7,
    7
)
-- Luego: clic en DIA_SEMANA_NOM → Ordenar por columna → Orden Día
```

```dax
-- Columna calculada para ordenar franjas horarias
-- Modelado → Nueva columna (no medida)
Orden Franja =
SWITCH('fraudes_comercios_no_seguros_features'[FRANJA_HORARIA],
    "MADRUGADA", 1,
    "MAÑANA",    2,
    "TARDE",     3,
    "NOCHE",     4,
    5
)
-- Luego: clic en FRANJA_HORARIA → Ordenar por columna → Orden Franja
```

```dax
-- Color condicional para barras de días (fin de semana en rojo)
-- Usar en: Formato del visual → Color de datos → fx → Valor de campo
Color Día =
IF(
    MAX('fraudes_comercios_no_seguros_features'[ES_FIN_SEMANA]) = 1,
    "#E63946",
    "#1D3557"
)
```

---

## PÁGINA 3 — Comercios & MCC

```dax
% Participación Comercio =
DIVIDE(
    [Monto Fraude],
    CALCULATE([Monto Fraude], ALLEXCEPT(
        'fraudes_comercios_no_seguros_features',
        'fraudes_comercios_no_seguros_features'[MES]
    ))
)
-- Formato: 0.00%
```

```dax
Tarjetas por Comercio =
DIVIDE(
    [Tarjetas Únicas],
    [Comercios Únicos]
)
-- Promedio de tarjetas distintas por comercio
```

```dax
Monto por MCC =
CALCULATE(
    [Monto Fraude],
    ALLEXCEPT(
        'fraudes_comercios_no_seguros_features',
        'fraudes_comercios_no_seguros_features'[MCC]
    )
)
```

---

## PÁGINA 4 — Perfil de Tarjetas

```dax
% Tarjetas Reincidentes =
DIVIDE(
    CALCULATE(
        [Tarjetas Únicas],
        'fraudes_comercios_no_seguros_features'[FLAG_TARJETA_REINCIDENTE] = 1
    ),
    [Tarjetas Únicas]
)
-- Formato: 0.0%
```

```dax
% Ráfaga (3+ fraudes/día) =
DIVIDE(
    CALCULATE(
        COUNTROWS('fraudes_comercios_no_seguros_features'),
        'fraudes_comercios_no_seguros_features'[FLAG_RAFAGA_DIA] = 1
    ),
    [# Fraudes]
)
-- Formato: 0.0%
```

```dax
% Multi Comercio en un Día =
DIVIDE(
    CALCULATE(
        COUNTROWS('fraudes_comercios_no_seguros_features'),
        'fraudes_comercios_no_seguros_features'[FLAG_MULTI_COMERCIO_DIA] = 1
    ),
    [# Fraudes]
)
-- Formato: 0.0%
```

```dax
Monto Promedio por Nivel =
DIVIDE(
    [Monto Fraude],
    [Tarjetas Únicas]
)
```

---

## PÁGINA 5 — Dashboard Accionable

```dax
Tarjetas MUY ALTO Riesgo =
CALCULATE(
    [Tarjetas Únicas],
    'fraudes_comercios_no_seguros_features'[PERFIL_RIESGO] = "MUY_ALTO"
)
```

```dax
% CVV Estático =
DIVIDE(
    CALCULATE(
        COUNTROWS('fraudes_comercios_no_seguros_features'),
        'fraudes_comercios_no_seguros_features'[FLAG_CVV_ESTATICO] = 1
    ),
    [# Fraudes]
)
-- Formato: 0.0%
```

```dax
% Monto Redondo =
DIVIDE(
    CALCULATE(
        [Monto Fraude],
        'fraudes_comercios_no_seguros_features'[FLAG_MONTO_REDONDO] = 1
    ),
    [Monto Fraude]
)
-- Formato: 0.0%
```

```dax
Score Riesgo Promedio =
AVERAGE('fraudes_comercios_no_seguros_features'[SCORE_RIESGO_TRJ])
-- Formato: 0.00
```

```dax
% Saldo Agotado =
DIVIDE(
    CALCULATE(
        COUNTROWS('fraudes_comercios_no_seguros_features'),
        'fraudes_comercios_no_seguros_features'[FLAG_SALDO_AGOTADO] = 1
    ),
    [# Fraudes]
)
-- Formato: 0.0%
```

---

## PÁGINA 6 — Gestión de Casos

```dax
Días Promedio Cierre =
AVERAGE('fraudes_comercios_no_seguros_features'[DIAS_PARA_CIERRE])
-- Formato: 0.0
```

```dax
Casos Cerrados en 1 Día =
CALCULATE(
    COUNTROWS('fraudes_comercios_no_seguros_features'),
    'fraudes_comercios_no_seguros_features'[RANGO_DIAS_CIERRE] = "1_DIA"
)
```

```dax
% Casos Cerrados en 1 Semana =
DIVIDE(
    CALCULATE(
        COUNTROWS('fraudes_comercios_no_seguros_features'),
        'fraudes_comercios_no_seguros_features'[RANGO_DIAS_CIERRE] IN {"1_DIA", "1_SEMANA"}
    ),
    [# Fraudes]
)
-- Formato: 0.0%
```

```dax
Monto Pendiente Cierre =
CALCULATE(
    [Monto Fraude],
    'fraudes_comercios_no_seguros_features'[RANGO_DIAS_CIERRE] = "SIN_CIERRE"
)
```

---

## DIM_CALENDARIO — Tabla de fechas
-- Modelado → Nueva tabla (pegar todo el bloque)

```dax
dim_Calendario =
VAR FechaMin = MIN('fraudes_comercios_no_seguros_features'[DATETIME_TRX])
VAR FechaMax = MAX('fraudes_comercios_no_seguros_features'[DATETIME_TRX])
RETURN
ADDCOLUMNS(
    CALENDAR(FechaMin, FechaMax),
    "Año",           YEAR([Date]),
    "Mes Num",       MONTH([Date]),
    "Mes Nombre",    FORMAT([Date], "MMM", "es-PE"),
    "Día Semana",    WEEKDAY([Date], 2),
    "Día Nombre",    FORMAT([Date], "ddd", "es-PE"),
    "Semana ISO",    WEEKNUM([Date], 2),
    "Es Fin Semana", IF(WEEKDAY([Date], 2) >= 6, 1, 0),
    "Quincena",      IF(DAY([Date]) <= 15, "Q1", "Q2"),
    "Año-Mes",       FORMAT([Date], "YYYY-MM"),
    "Año Fiscal",    IF(MONTH([Date]) >= 11,
                        "FY" & YEAR([Date]) + 1,
                        "FY" & YEAR([Date]))
)
-- Después: relacionar dim_Calendario[Date] → tabla[DATETIME_TRX]
```

---

## NOTAS DE IMPLEMENTACIÓN

1. Las medidas que dicen "columna calculada" van en
   **Modelado → Nueva columna** (no en Nueva medida)

2. Para aplicar color condicional en barras:
   Visual → Formato → Colores de datos → fx → Valor de campo
   y seleccionar la medida Color Día

3. Para el mapa de calor (Matriz):
   - Visual: Matriz
   - Filas: HORA_DIA
   - Columnas: DIA_SEMANA_NOM (ordenado por Orden Día)
   - Valores: [# Fraudes]
   - Formato → Valores de celda → activar escala de colores
     Mínimo: #FFFFFF  |  Máximo: #C00000

4. Reemplaza 'fraudes_comercios_no_seguros_features' con el nombre
   exacto que tiene la tabla en tu modelo de Power BI

5. Ajusta los nombres de columna (IMPORTE, IMPORTE_USD, etc.)
   si en tu config.py pusiste nombres diferentes
