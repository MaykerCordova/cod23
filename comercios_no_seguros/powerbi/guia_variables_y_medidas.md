# Guía de Variables y Medidas DAX
## CPF · Reporte de Fraude E-commerce No 3DS (Comercios No Seguros)

> Este documento explica cada variable de ingeniería y cada medida DAX
> del dashboard: qué es, qué mide, cómo interpretarla y un ejemplo real.

---

# PARTE 1 — VARIABLES DE INGENIERÍA DE DATOS
> Generadas por `feature_engineering.py` sobre el parquet de fraudes

---

## BLOQUE A/B — Variables Temporales

Estas variables se extraen de la fecha y hora de la transacción.
Sirven para entender **cuándo ocurre el fraude**.

---

### DATETIME_TRX
- **Qué es:** Fecha y hora exacta de la transacción en formato estándar.
- **Cómo se construye:** Se combina la columna de fecha (`YYYY-MM-DD HH:MM:SS`) original.
- **Para qué sirve:** Base de todas las demás variables temporales. Permite ordenar y filtrar por tiempo.
- **Ejemplo:** `2025-07-02 17:18:49`

---

### DATETIME_CIERRE
- **Qué es:** Fecha en que el equipo de investigación cerró el caso de fraude.
- **Cómo se construye:** Se parsea directamente la columna `FECHA_CIERRE` (`YYYY-MM-DD`).
- **Para qué sirve:** Calcular cuánto tiempo tardó el equipo en resolver el caso.
- **Ejemplo:** `2025-08-10` → el caso se cerró 39 días después de la transacción.

---

### HORA_DIA
- **Qué es:** Número entero que representa la hora del día (0 a 23).
- **Cómo se construye:** Se extrae del DATETIME_TRX.
- **Para qué sirve:** Identificar en qué hora del día ocurren más fraudes.
- **Ejemplo:** `17` → la transacción ocurrió entre las 17:00 y 17:59.
- **Interpretación:** Horas pico de fraude suelen ser tarde (16-20h) y madrugada (0-5h).

---

### DIA_SEMANA
- **Qué es:** Número del día de la semana (0=Lunes, 6=Domingo).
- **Para qué sirve:** Detectar si hay más fraude en ciertos días. En Power BI se usa para ordenar `DIA_SEMANA_NOM` correctamente.
- **Ejemplo:** `5` → Sábado.

---

### DIA_SEMANA_NOM
- **Qué es:** Nombre abreviado del día de la semana.
- **Para qué sirve:** Etiqueta legible para gráficos en Power BI.
- **Ejemplo:** `SAB`
- **Nota:** Para que aparezca ordenado (Lun→Dom) en Power BI, usar la columna calculada `Orden Día`.

---

### MES / MES_NOM / ANIO
- **Qué es:** Mes en número, mes en texto y año de la transacción.
- **Para qué sirve:** Filtros y agrupaciones temporales en el dashboard.
- **Ejemplo:** `MES=7`, `MES_NOM=JUL`, `ANIO=2025`

---

### FECHA_DIA
- **Qué es:** Solo la fecha sin hora (YYYY-MM-DD).
- **Para qué sirve:** Agrupar transacciones por día. Se usa internamente para calcular velocidades diarias.
- **Ejemplo:** `2025-07-02`

---

### ES_FIN_SEMANA
- **Qué es:** Flag binario (1/0).
- **Valor 1:** La transacción ocurrió sábado o domingo.
- **Valor 0:** La transacción ocurrió de lunes a viernes.
- **Por qué importa en No 3DS:** El fin de semana hay menos monitoreo activo → los defraudadores lo aprovechan.
- **Ejemplo de uso:** "El 27.6% de los fraudes ocurren en fin de semana."

---

### FRANJA_HORARIA
- **Qué es:** Categoría del período del día en que ocurrió el fraude.
- **Valores posibles:**

| Valor | Horario | Interpretación |
|---|---|---|
| `MADRUGADA` | 00:00 – 05:59 | Horario de bajo control, alto riesgo |
| `MAÑANA` | 06:00 – 11:59 | Horario laboral temprano |
| `TARDE` | 12:00 – 18:59 | Horario de mayor actividad comercial |
| `NOCHE` | 19:00 – 23:59 | Horario de transacciones post-laboral |

- **Nota:** En Power BI usar la columna `Orden Franja` para que aparezcan en este orden y no alfabético.

---

### ES_MADRUGADA
- **Qué es:** Flag binario (1/0). Vale 1 si FRANJA_HORARIA = MADRUGADA.
- **Por qué importa:** Transacciones de madrugada en e-commerce son inusuales para clientes legítimos. Es una señal fuerte de fraude automatizado.
- **Ejemplo:** "El 13.7% de los fraudes ocurren en madrugada."

---

### ES_HORARIO_LAB
- **Qué es:** Flag binario (1/0).
- **Valor 1:** Lunes a viernes entre 08:00 y 17:59.
- **Valor 0:** Fuera de horario laboral.
- **Por qué importa:** Un fraude fuera de horario laboral tiene menos chance de ser detectado por el equipo en tiempo real.

---

### QUINCENA
- **Qué es:** Indica si la transacción ocurrió en la primera o segunda quincena del mes.
- **Valores:** `Q1` (días 1-15) / `Q2` (días 16-31).
- **Por qué importa:** En Perú los pagos de sueldo son quincenales → más actividad de compra (y fraude) justo después de quincena.

---

## BLOQUE C — Días de Investigación

---

### DIAS_PARA_CIERRE
- **Qué es:** Número de días que tardó el equipo en cerrar el caso desde que ocurrió la transacción.
- **Cómo se calcula:** `DATETIME_CIERRE - DATETIME_TRX` en días.
- **Interpretación:**

| Valor | Significado |
|---|---|
| `0` o `1` | Caso resuelto muy rápido (mismo día o al día siguiente) |
| `2 a 7` | Cierre en una semana → tiempo normal |
| `8 a 30` | Cierre en el mes → puede indicar backlog |
| `> 30` | Caso tardó más de un mes → alerta de gestión |
| Negativo | Error en datos (fecha cierre antes de la txn) |

- **Ejemplo:** `DIAS_PARA_CIERRE = 45` → ese caso tardó 45 días en cerrarse, lo cual es alto.

---

### RANGO_DIAS_CIERRE
- **Qué es:** Categorización del DIAS_PARA_CIERRE en grupos legibles.
- **Valores:** `1_DIA` / `1_SEMANA` / `1_MES` / `MAS_1_MES` / `SIN_CIERRE`
- **Para qué sirve:** Gráficos de distribución de tiempos de gestión en Power BI.
- **Ejemplo de uso:** "El 62% de los casos se cierran en menos de 1 semana."

---

## BLOQUE D — Velocidad y Comportamiento de Tarjeta

Estas variables describen el **comportamiento histórico de cada tarjeta** dentro del dataset de fraudes. Responden a: ¿esta tarjeta es un caso aislado o un patrón repetido?

---

### TOTAL_FRAUDES_TARJETA
- **Qué es:** Cuántas veces aparece esa tarjeta en el dataset de fraudes (total histórico).
- **Cómo se calcula:** Conteo de filas agrupado por tarjeta en todo el dataset.
- **Interpretación:**

| Valor | Significado |
|---|---|
| `1` | Fraude aislado — tarjeta comprometida una sola vez |
| `2 a 5` | Tarjeta reincidente — fue usada varias veces para fraude |
| `> 5` | Tarjeta altamente comprometida o cuenta de prueba masiva |

- **Ejemplo:** `TOTAL_FRAUDES_TARJETA = 12` → esa tarjeta aparece en 12 transacciones fraudulentas.

---

### MONTO_TOTAL_FRAUDE_TRJ
- **Qué es:** Monto total defraudado con esa tarjeta en todo el dataset.
- **Ejemplo:** Si la tarjeta aparece 5 veces con montos 100, 200, 150, 300, 250 → `MONTO_TOTAL = 1,000`

---

### COMERCIOS_DISTINTOS_TRJ
- **Qué es:** En cuántos comercios distintos tuvo fraude esa tarjeta (en todo el dataset).
- **Interpretación:**

| Valor | Significado |
|---|---|
| `1` | Todos los fraudes de esa tarjeta fueron en un solo comercio |
| `2 a 5` | La tarjeta fue usada para fraude en varios comercios |
| `> 5` | Tarjeta usada en muchos comercios → fraude masivo o datos comprometidos |

---

### DIAS_ACTIVA_TRJ
- **Qué es:** Cuántos días distintos tuvo fraude esa tarjeta.
- **Interpretación:** Una tarjeta con fraudes en 15 días distintos fue explotada durante medio mes sin ser bloqueada.
- **Ejemplo:** `DIAS_ACTIVA_TRJ = 1` → todos los fraudes ocurrieron en un solo día (ráfaga).

---

### FRAUDES_TRJ_DIA
- **Qué es:** Cuántos fraudes tuvo esa tarjeta **ese día específico** (en cualquier comercio).
- **Importante:** Cambia por día. La misma tarjeta puede tener `FRAUDES_TRJ_DIA=5` el lunes y `FRAUDES_TRJ_DIA=1` el miércoles.
- **Ejemplo:** El 15 de marzo, la tarjeta `4556****` tuvo 5 transacciones fraudulentas.

---

### COMERCIOS_DISTINTOS_DIA
- **Qué es:** En cuántos comercios distintos tuvo fraude esa tarjeta **ese día específico**.
- **Ejemplo:** `COMERCIOS_DISTINTOS_DIA = 3` → ese día la tarjeta hizo fraude en Apple, Amazon y eBay.

---

### FLAG_TARJETA_REINCIDENTE
- **Qué es:** Flag binario (1/0).
- **Valor 1:** La tarjeta tiene más de 1 fraude en el dataset (TOTAL_FRAUDES_TARJETA > 1).
- **Valor 0:** Solo aparece una vez.
- **Por qué importa:** Una tarjeta reincidente no fue bloqueada a tiempo → falla en el proceso de detección.
- **Ejemplo de uso:** "El 38% de las tarjetas afectadas son reincidentes."

---

### FLAG_MULTI_COMERCIO_DIA
- **Qué es:** Flag binario (1/0).
- **Valor 1:** Esa tarjeta tuvo fraude en 2 o más comercios distintos **ese mismo día**.
- **Valor 0:** Solo en un comercio ese día.
- **Por qué importa:** Indica que la tarjeta estaba siendo explotada activamente en múltiples lugares simultáneamente.
- **Ejemplo:** Tarjeta usada en APPLE.COM, AMAZON y NETFLIX el mismo día → FLAG = 1.

---

### FLAG_RAFAGA_DIA
- **Qué es:** Flag binario (1/0).
- **Valor 1:** Esa tarjeta tuvo 3 o más fraudes **ese día** (en cualquier comercio).
- **Valor 0:** Menos de 3 fraudes ese día.
- **Por qué importa:** Indica operación masiva o automatizada. Los defraudadores prueban tarjetas rápidamente antes de que sean bloqueadas.
- **Ejemplo:** Tarjeta con 7 fraudes el mismo día → FLAG_RAFAGA = 1.
- **⚠️ Importante:** El conteo es en TODOS los comercios del dataset, no solo en uno.

---

### RATIO_MONTO_VS_SALDO
- **Qué es:** Qué proporción del saldo disponible representa el monto del fraude.
- **Cómo se calcula:** `MONTO / SALDO_DISPONIBLE`
- **Interpretación:**

| Valor | Significado |
|---|---|
| `0.10` (10%) | El fraude consumió el 10% del saldo |
| `0.90` (90%) | El fraude casi agotó el saldo disponible |
| `> 1.0` | El fraude superó el saldo (puede ser error de datos) |

---

### FLAG_SALDO_AGOTADO
- **Qué es:** Flag binario (1/0).
- **Valor 1:** El monto del fraude representa el 90% o más del saldo disponible.
- **Por qué importa:** Indica que el defraudador conocía el saldo de la tarjeta o ejecutó el máximo posible.

---

## BLOQUE E — Perfil del Comercio y MCC

---

### TOTAL_FRAUDES_COMERCIO
- **Qué es:** Cuántos fraudes tiene ese comercio en todo el dataset.
- **Ejemplo:** `TOTAL_FRAUDES_COMERCIO = 2,590` → APPLE.COM tiene 2,590 transacciones fraudulentas.

---

### MONTO_PROM_FRAUDE_COM
- **Qué es:** Ticket promedio de fraude en ese comercio.
- **Para qué sirve:** Detectar comercios donde el fraude es de montos atípicamente altos o bajos.
- **Ejemplo:** Comercio con `MONTO_PROM = 15` → fraudes pequeños → probable card testing.

---

### TARJETAS_DISTINTAS_COM
- **Qué es:** Cuántas tarjetas distintas tuvieron fraude en ese comercio.
- **Interpretación:** Si un comercio tiene 2,590 fraudes pero solo 500 tarjetas distintas → en promedio cada tarjeta fue usada 5 veces ahí.

---

### DIAS_CON_FRAUDE_COM
- **Qué es:** Cuántos días distintos tuvo al menos un fraude ese comercio.
- **Ejemplo:** `DIAS_CON_FRAUDE_COM = 180` → ese comercio tuvo fraude en 180 días distintos → fraude sistemático, no puntual.

---

### RANKING_COMERCIO
- **Qué es:** Posición del comercio ordenado por número de fraudes (1 = el más golpeado).
- **Ejemplo:** `RANKING_COMERCIO = 1` → es el comercio con más fraudes en el dataset.

---

### TOTAL_FRAUDES_MCC / RANKING_MCC
- **Qué es:** Total de fraudes y ranking del rubro comercial (MCC = Merchant Category Code).
- **Por qué importa:** Algunos rubros son más vulnerables al fraude sin 3DS.
- **Ejemplo:** `MCC = 5045` (computadoras y software) con `RANKING_MCC = 1` → es el rubro más golpeado.

---

## BLOQUE F — Señales de Monto

---

### FLAG_MONTO_REDONDO
- **Qué es:** Flag binario (1/0).
- **Valor 1:** El monto es un número entero exacto (sin decimales): 10.00, 50.00, 100.00.
- **Por qué importa:** Los defraudadores que prueban tarjetas clonadas usan montos redondos y pequeños para verificar si la tarjeta está activa antes de hacer el fraude grande.
- **Ejemplo de uso:** "El 23% de los fraudes en APPLE.COM son de monto redondo → señal de card testing."

---

### DESVIO_MONTO_VS_COM
- **Qué es:** Diferencia entre el monto de la transacción y el ticket promedio del comercio.
- **Cómo se calcula:** `MONTO - MONTO_PROM_FRAUDE_COM`
- **Interpretación:**

| Valor | Significado |
|---|---|
| Positivo alto | Transacción muy por encima del promedio del comercio → monto atípico |
| Cercano a 0 | Monto similar al promedio del comercio |
| Negativo | Transacción por debajo del promedio → posible card testing |

---

### RATIO_MONTO_VS_COM
- **Qué es:** El monto de la transacción dividido entre el promedio del comercio.
- **Ejemplo:** `RATIO = 5.0` → esta transacción es 5 veces más cara que el fraude promedio de ese comercio → outlier a investigar.

---

### RANGO_MONTO
- **Qué es:** Categorización del monto en cuartiles del dataset completo.
- **Valores:**

| Valor | Significado |
|---|---|
| `BAJO` | Monto en el 25% inferior de todos los fraudes |
| `MEDIO_BAJO` | Entre el percentil 25 y 50 |
| `MEDIO_ALTO` | Entre el percentil 50 y 75 |
| `ALTO` | Monto en el 25% superior de todos los fraudes |

---

## BLOQUE G — Score y Perfil de Riesgo Compuesto

---

### SCORE_RIESGO_TRJ
- **Qué es:** Puntuación de riesgo de la transacción de 0 a 6.
- **Cómo se calcula:** Suma de 6 flags binarios:

| Flag sumado | Qué detecta |
|---|---|
| FLAG_TARJETA_REINCIDENTE | Tarjeta con historial de fraude |
| FLAG_MULTI_COMERCIO_DIA | Tarjeta usada en múltiples comercios ese día |
| FLAG_RAFAGA_DIA | Tarjeta con 3+ fraudes ese día |
| FLAG_MONTO_REDONDO | Monto sospechoso de card testing |
| ES_MADRUGADA | Horario inusual de alta exposición |
| FLAG_CVV_ESTATICO | CVV fijo, más fácil de clonar |

- **Ejemplo:** `SCORE = 4` → esa transacción activó 4 de las 6 señales de alerta.

---

### PERFIL_RIESGO
- **Qué es:** Etiqueta de riesgo basada en el SCORE_RIESGO_TRJ.
- **Escala:**

| SCORE | PERFIL | Interpretación |
|---|---|---|
| 0 | `BAJO` | Sin señales de alerta — fraude sofisticado o aislado |
| 1 | `MEDIO` | Una señal activada — monitorear |
| 2 | `ALTO` | Dos señales — investigar |
| 3 a 6 | `MUY_ALTO` | Tres o más señales — acción inmediata |

- **⚠️ Nota importante:** `MUY_ALTO` no siempre significa el mayor monto. Los fraudes de ticket alto suelen tener perfil `BAJO` o `MEDIO` porque el defraudador evita levantar señales. `MUY_ALTO` indica **velocidad y volumen**, no necesariamente monto.

---

### FLAG_CVV_ESTATICO
- **Qué es:** Flag binario (1/0).
- **Valor 1:** La transacción usó CVV estático (no dinámico).
- **Por qué importa en No 3DS:** Sin 3DS + CVV estático = la combinación de menor seguridad posible. El CVV estático es más fácil de clonar o robar.

---

### FLAG_HORARIO_RIESGO
- **Qué es:** Flag binario (1/0).
- **Valor 1:** La transacción ocurrió en madrugada (00-05h) O en fin de semana.
- **Valor 0:** Horario laboral normal entre semana.
- **Por qué importa:** Ambos horarios tienen menor capacidad de respuesta del equipo de fraude.

---

---

# PARTE 2 — MEDIDAS DAX (Power BI)

---

## BASE — Medidas fundamentales

---

### # Fraudes
- **Qué mide:** Total de transacciones fraudulentas visibles con los filtros actuales.
- **Cambia con:** Cualquier slicer (fecha, canal, comercio, etc.)
- **Ejemplo:** Si filtras por CANAL = "WEB" → muestra solo fraudes del canal web.

---

### Monto Fraude
- **Qué mide:** Suma del monto de todas las transacciones fraudulentas visibles.
- **Ejemplo:** S/ 7,025,387 = monto total defraudado en el período seleccionado.

---

### Ticket Promedio
- **Qué mide:** Monto promedio por transacción fraudulenta.
- **Cómo se interpreta:** `Ticket = 131.32` → en promedio cada fraude fue de S/ 131.32.
- **Utilidad:** Comparar entre canales, comercios o períodos. Un ticket muy bajo puede indicar card testing.

---

### Tarjetas Únicas
- **Qué mide:** Cantidad de tarjetas distintas afectadas.
- **Diferencia con # Fraudes:** Una misma tarjeta puede tener 10 fraudes → cuenta como 1 tarjeta única pero 10 en # Fraudes.

---

### Comercios Únicos
- **Qué mide:** Cantidad de comercios distintos donde ocurrió fraude.

---

### Monto Promedio por Tarjeta
- **Qué mide:** Cuánto se defraudó en promedio por cada tarjeta afectada.
- **Ejemplo:** `348.15` → cada tarjeta afectada perdió en promedio S/ 348.15.

---

## PÁGINA 1 — Resumen Ejecutivo

---

### % Participación sobre Total
- **Qué mide:** Qué % del monto total representa el elemento seleccionado (comercio, MCC, segmento).
- **Ejemplo:** APPLE.COM aparece con 14.2% → concentra el 14.2% de todo el monto defraudado.
- **Importante:** El denominador es el total general, ignorando filtros del visual (no cambia cuando filtras dentro del gráfico).

---

### Variación Monto MoM
- **Qué mide:** Variación porcentual del monto versus el mes anterior.
- **Ejemplo:** `+23.5%` → este mes se defraudó 23.5% más que el mes anterior.
- **Interpretación:** Valor positivo = el fraude creció. Valor negativo = disminuyó.

---

## PÁGINA 2 — Análisis Temporal

---

### Hora Pico
- **Qué mide:** La hora del día con más transacciones fraudulentas.
- **Ejemplo:** `19:00 hrs` → la hora con más fraudes es entre 19:00 y 19:59.
- **Cambia con filtros:** Si filtras por CANAL=WEB puede cambiar la hora pico.

---

### % Madrugada
- **Qué mide:** Porcentaje de fraudes que ocurrieron entre las 00:00 y las 05:59.
- **Ejemplo:** `13.7%` → casi 1 de cada 7 fraudes ocurre de madrugada.
- **Benchmark:** En transacciones legítimas de e-commerce, este % suele ser menor al 5%.

---

### % Fin de Semana
- **Qué mide:** Porcentaje de fraudes en sábado o domingo.
- **Ejemplo:** `27.6%` → más de 1 de cada 4 fraudes ocurre en fin de semana.

---

### % Horario Laboral
- **Qué mide:** Porcentaje de fraudes en horario laboral (Lun-Vie 08:00-17:59).
- **Ejemplo:** `36.8%` → solo el 36.8% ocurre en horario donde el equipo está activo para responder.

---

### Día Pico
- **Qué mide:** El día de la semana con más transacciones fraudulentas.
- **Ejemplo:** `MON` → los lunes tienen más fraudes que cualquier otro día.

---

### Color Día
- **Qué hace:** Devuelve un color para aplicar en los gráficos de barras por día.
- `#E63946` (rojo) → Sábado y Domingo
- `#1D3557` (azul oscuro) → Lunes a Viernes
- **Para qué sirve:** Destacar visualmente que el fin de semana tiene un comportamiento diferente.

---

## PÁGINA 3 — Comercios & MCC

---

### Top Comercio
- **Qué muestra:** El nombre del comercio con mayor monto defraudado en el contexto actual.
- **Cambia con filtros:** Si filtras por MCC o canal, mostrará el top dentro de ese contexto.

---

### % Top 10 Comercios
- **Qué mide:** Qué porcentaje del monto total concentran los 10 comercios más golpeados.
- **Ejemplo:** `65%` → los 10 comercios principales concentran el 65% de todo el fraude.
- **Cómo interpretarlo:** Un % muy alto indica **concentración de riesgo** — pocos comercios explican la mayoría del fraude. Actuar sobre esos 10 tiene alto impacto.

---

### Días con Fraude
- **Qué mide:** Cuántos días distintos tuvo al menos una transacción fraudulenta el comercio seleccionado.
- **Ejemplo:** `180 días` → el comercio tuvo fraude en 180 días distintos → es un problema sistemático, no un evento puntual.

---

### % Ráfaga en Comercio
- **Qué mide:** Del total de fraudes de ese comercio, qué % vino de tarjetas que ese día tuvieron 3+ fraudes en cualquier comercio.
- **Ejemplo:** `40%` → 4 de cada 10 fraudes en ese comercio vienen de tarjetas en operación masiva.
- **Interpretación:**

| % Ráfaga | Señal |
|---|---|
| < 10% | Fraudes dispersos, sin patrón de ráfaga |
| 10% - 25% | Atención — revisar |
| > 25% | Comercio involucrado en operaciones de fraude masivo 🚨 |

---

### % Monto Redondo en Comercio
- **Qué mide:** Qué % de los fraudes de ese comercio tienen monto exacto (sin decimales).
- **Ejemplo:** `35%` → 35% de los fraudes en ese comercio son de montos redondos → alto indicio de card testing.
- **Contexto:** Los defraudadores prueban tarjetas con montos como S/ 1.00, S/ 5.00, S/ 10.00 para verificar si están activas.

---

### Fraudes por Tarjeta en Comercio
- **Qué mide:** En promedio, cuántos fraudes tiene cada tarjeta en ese comercio.
- **Ejemplo:** `5.0` → cada tarjeta afectada tiene en promedio 5 fraudes en ese comercio.
- **Interpretación:** Valor alto = pocas tarjetas hacen muchos fraudes = ataque concentrado y repetido.

---

### Color Comercio Riesgo
- **Qué hace:** Asigna un color según cuánto está por encima del promedio de fraudes.
- `#C00000` (rojo) → más del doble del promedio → crítico
- `#FF8C00` (naranja) → sobre el promedio → atención
- `#1D6F42` (verde) → bajo el promedio → controlado

---

## PÁGINA 4 — Perfil de Tarjetas

---

### % Tarjetas Reincidentes
- **Qué mide:** Qué % de las tarjetas afectadas aparecen más de una vez en el dataset.
- **Ejemplo:** `38%` → 38 de cada 100 tarjetas afectadas tuvieron fraude más de una vez.
- **Por qué importa:** Una tarjeta reincidente no fue bloqueada a tiempo.

---

### % Ráfaga (3+ fraudes/día)
- **Qué mide:** Qué % de las transacciones vienen de tarjetas que tuvieron 3+ fraudes ese día.
- **Ejemplo:** `22%` → 1 de cada 5 fraudes viene de tarjetas en operación masiva ese día.

---

### % Multi Comercio en un Día
- **Qué mide:** Qué % de las transacciones vienen de tarjetas que ese mismo día tuvieron fraude en 2+ comercios distintos.
- **Ejemplo:** `31%` → 31% de los fraudes vienen de tarjetas que ese día atacaron múltiples comercios.

---

### Comercios Prom por Tarjeta
- **Qué mide:** En promedio, en cuántos comercios distintos tuvo fraude cada tarjeta afectada.
- **Ejemplo:** `2.5` → cada tarjeta afectada cometió fraude en 2.5 comercios distintos en promedio.

---

### Score Riesgo Promedio Tarjetas
- **Qué mide:** El score promedio de las transacciones de las tarjetas en el contexto seleccionado.
- **Escala:** 0 (sin señales) a 6 (todas las señales activas).
- **Ejemplo:** `2.3` → en promedio, las transacciones activan poco más de 2 señales de riesgo.

---

### % Tarjetas MUY ALTO Riesgo
- **Qué mide:** Qué % de las tarjetas afectadas tienen al menos una transacción con perfil MUY_ALTO.
- **Ejemplo:** `28%` → 28% de las tarjetas tienen transacciones con 3+ señales de riesgo.

---

### % Crédito / % Débito
- **Qué mide:** Distribución del fraude entre tarjetas de crédito y débito.
- **Por qué importa en No 3DS:** Ambos tipos están expuestos, pero el ticket promedio de crédito suele ser mayor.

---

### Orden Perfil *(columna calculada)*
- **Qué hace:** Asigna un número para ordenar PERFIL_RIESGO correctamente en Power BI.
- `BAJO=1`, `MEDIO=2`, `ALTO=3`, `MUY_ALTO=4`
- **Por qué es necesaria:** Sin esta columna, Power BI ordena alfabético: ALTO → BAJO → MEDIO → MUY_ALTO.

---

### Color Tipo Tarjeta / Color Nivel Tarjeta
- **Qué hacen:** Devuelven colores para aplicar en visuales según tipo o nivel de tarjeta.
- Crédito → rojo `#C00000`
- Débito → verde `#1D6F42`
- Black → negro `#1A1A1A`
- Platinum → gris `#A0A0A0`
- Gold → dorado `#C9A84C`
- Classic → azul `#1D3557`

---

---

# RESUMEN RÁPIDO — Tabla de variables para defender en reunión

| Variable | En una frase |
|---|---|
| FRANJA_HORARIA | En qué momento del día ocurrió el fraude |
| ES_MADRUGADA | Flag: ¿fue entre medianoche y las 6am? |
| ES_FIN_SEMANA | Flag: ¿fue sábado o domingo? |
| DIAS_PARA_CIERRE | Cuántos días tardó el equipo en resolver el caso |
| TOTAL_FRAUDES_TARJETA | Cuántas veces esa tarjeta aparece en fraudes |
| FLAG_TARJETA_REINCIDENTE | Flag: ¿la tarjeta tiene más de 1 fraude? |
| FLAG_RAFAGA_DIA | Flag: ¿la tarjeta tuvo 3+ fraudes en un mismo día? |
| FLAG_MULTI_COMERCIO_DIA | Flag: ¿la tarjeta atacó 2+ comercios el mismo día? |
| FLAG_MONTO_REDONDO | Flag: ¿el monto es número exacto (señal de card testing)? |
| FLAG_SALDO_AGOTADO | Flag: ¿el fraude consumió el 90%+ del saldo disponible? |
| FLAG_CVV_ESTATICO | Flag: ¿usó CVV fijo (menos seguro)? |
| SCORE_RIESGO_TRJ | Suma de señales de riesgo activas (0 a 6) |
| PERFIL_RIESGO | Etiqueta: BAJO / MEDIO / ALTO / MUY_ALTO según score |
| RANKING_COMERCIO | Posición del comercio por cantidad de fraudes (1=el peor) |
| RANKING_MCC | Posición del rubro comercial por cantidad de fraudes |
| % Top 10 Comercios | % del monto total que concentran los 10 comercios más golpeados |
| % Ráfaga en Comercio | % de fraudes de ese comercio que vienen de tarjetas en operación masiva |
| % Monto Redondo | % de fraudes con montos exactos (señal de card testing) |
| Fraudes por Tarjeta | Cuántos fraudes tiene en promedio cada tarjeta en ese comercio |
