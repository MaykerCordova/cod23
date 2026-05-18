# =============================================================================
# MÓDULO DE CORRECCIONES Y NUEVAS SECCIONES — MCC 7995
# Agrega al final del script analisis_mcc7995_FINAL.py
# O reemplaza las secciones correspondientes
# =============================================================================

# =============================================================================
# CORRECCIÓN 1 — DECIMALES EN TABLA DIARIA
# Reemplaza el bloque de formateo de tabla_diaria_display
# =============================================================================

tabla_diaria_display = tabla_diaria.copy()
tabla_diaria_display["Impacto_fraude_pct"]    = tabla_diaria_display["Impacto_fraude_pct"].map("{:.4f}%".format)  # 4 decimales
tabla_diaria_display["Monto_Genuina"]         = tabla_diaria_display["Monto_Genuina"].map("S/ {:,.2f}".format)
tabla_diaria_display["Monto_Fraude_Total"]    = tabla_diaria_display["Monto_Fraude_Total"].map("S/ {:,.2f}".format)
tabla_diaria_display["Monto_Fraude_Aprobado"] = tabla_diaria_display["Monto_Fraude_Aprobado"].map("S/ {:,.2f}".format)
tabla_diaria_display["Ticket_Fraude_Prom"]    = tabla_diaria_display["Ticket_Fraude_Prom"].map("S/ {:,.2f}".format)
tabla_diaria_display["Ticket_Genuina_Prom"]   = tabla_diaria_display["Ticket_Genuina_Prom"].map("S/ {:,.2f}".format)
tabla_diaria_display.columns = [
    "Intervalo","Genuina","Fraude Aprobado","Total Clientes",
    "Trx Genuina","Monto Genuina",
    "Trx Fraude Total","Trx Fraude Aprobado",
    "Monto Fraude Total","Monto Fraude Aprobado",
    "Impacto Fraude %","Ticket Fraude Prom","Ticket Genuina Prom"
]

# =============================================================================
# CORRECCIÓN 2 — DECIMALES EN DECILES
# Reemplaza la función construir_tabla_deciles en la sección de Fraude/Trx Aprobadas
# =============================================================================
# En la función construir_tabla_deciles, cambia estas dos líneas:
#   "Fraude/Trx Aprobadas"    : f"{n_f_ap_d/n_ap_d*100:.4f}%" if n_ap_d > 0 else "0.0000%",
#   "No Fraude/Trx Aprobadas" : f"{(1-n_f_ap_d/n_ap_d)*100:.4f}%" if n_ap_d > 0 else "100.0000%",

# =============================================================================
# CORRECCIÓN 3 — APERTURA D10 CON CORTES FIJOS
# Reemplaza el bloque de apertura del D10
# =============================================================================
print("\nApertura del Decil 10 (top monto) — cortes fijos:")
monto_d10_min = df["MONTO"].quantile(0.90)
df_d10 = df[df["MONTO"] >= monto_d10_min].copy()

# Usar cortes por percentiles para forzar 10 grupos aunque haya repetidos
percentiles_d10 = np.linspace(0, 100, 11)
bins_d10 = np.unique([df_d10["MONTO"].quantile(p/100) for p in percentiles_d10])

if len(bins_d10) < 3:
    print("  ⚠️  Muy pocos valores únicos para abrir el D10 en 10 grupos")
    tabla_d10 = df_d10.groupby(
        pd.cut(df_d10["MONTO"], bins=5, include_lowest=True)
    ).agg(
        Trx=("MONTO","count"),
        Fraude_Total=("ES_FRAUDE","sum"),
        Fraude_Aprobado=("ES_FRAUDE_APROBADO","sum"),
        Monto_total=("MONTO","sum"),
        Ticket=("MONTO","mean")
    )
else:
    df_d10["DECIL_D10"] = pd.cut(df_d10["MONTO"], bins=bins_d10, include_lowest=True,
                                  labels=[f"SD{str(i+1).zfill(2)}" for i in range(len(bins_d10)-1)])
    tabla_d10 = (
        df_d10.groupby("DECIL_D10", observed=True)
        .agg(
            Trx_Aprobadas    =("MONTO",               lambda x: (df_d10.loc[x.index,"ESTADO"]=="APROBADA").sum()),
            Monto_Aprobado   =("MONTO",               lambda x: x[df_d10.loc[x.index,"ESTADO"]=="APROBADA"].sum()),
            Trx_Denegadas    =("MONTO",               lambda x: (df_d10.loc[x.index,"ESTADO"]=="DENEGADA").sum()),
            Total_Trx        =("MONTO",               "count"),
            Ticket_Promedio  =("MONTO",               "mean"),
            Monto_Total      =("MONTO",               "sum"),
            Fraude_Trx_Total =("ES_FRAUDE",           "sum"),
            Fraude_Monto_Tot =("MONTO",               lambda x: x[df_d10.loc[x.index,"ES_FRAUDE"]==1].sum()),
            Fraude_Trx_Ap    =("ES_FRAUDE_APROBADO",  "sum"),
            Fraude_Monto_Ap  =("MONTO",               lambda x: x[df_d10.loc[x.index,"ES_FRAUDE_APROBADO"]==1].sum()),
        )
        .assign(
            FR_Aprobado_pct=lambda x: (x["Fraude_Trx_Ap"]/x["Trx_Aprobadas"]*100).map("{:.4f}%".format),
            Rango=lambda x: [
                f"{df_d10[df_d10['DECIL_D10']==d]['MONTO'].min():.0f}–{df_d10[df_d10['DECIL_D10']==d]['MONTO'].max():.0f}"
                for d in x.index
            ]
        )
    )
print(tabla_d10.to_string())

# =============================================================================
# CORRECCIÓN 4 — TOP COMERCIOS CON FRAUDE TOTAL DESDE df COMPLETO
# =============================================================================
print("\n— Top 10 comercios por fraude — TOTAL (ap+den) vs APROBADO:")
df_top_comercios = (
    # Fraude total desde df completo
    df.groupby("COMERCIO")
    .agg(
        Fraude_Total_g     =("ES_FRAUDE",           "sum"),
        Monto_Fraude_Tot_g =("MONTO",               lambda x: x[df.loc[x.index,"ES_FRAUDE"]==1].sum()),
    )
    .join(
        # Fraude aprobado y métricas de aprobadas desde df_ap
        df_ap.groupby("COMERCIO")
        .agg(
            Tarjetas        =("ID_CLIENTE",           "nunique"),
            Trx             =("MONTO",                "count"),
            Fraude_Aprobado =("ES_FRAUDE_APROBADO",   "sum"),
            Monto_Fraude_Ap =("MONTO",               lambda x: x[df_ap.loc[x.index,"ES_FRAUDE_APROBADO"]==1].sum()),
            Monto_total     =("MONTO",                "sum"),
            FR_Ap_pct       =("ES_FRAUDE_APROBADO",   lambda x: x.mean()*100),
        )
    )
    .reset_index()
    .sort_values("Monto_Fraude_Ap", ascending=False)
    .head(10)
    .rename(columns={
        "COMERCIO"         : "Comercio",
        "Tarjetas"         : "# Tarjetas",
        "Trx"              : "# Trx Aprobadas",
        "Fraude_Total_g"   : "# Fraude Total (ap+den)",
        "Fraude_Aprobado"  : "# Fraude Aprobado",
        "Monto_Fraude_Tot_g":"Monto Fraude Total S/",
        "Monto_Fraude_Ap"  : "Monto Fraude Aprobado S/",
        "Monto_total"      : "Monto Total S/",
        "FR_Ap_pct"        : "Fraud Rate % (ap)",
    })
)
df_top_comercios["Fraud Rate % (ap)"] = df_top_comercios["Fraud Rate % (ap)"].map("{:.4f}%".format)
print(df_top_comercios.to_string(index=False))

# =============================================================================
# CORRECCIÓN 5 — DIMENSIONES POR SEG_NOMBRE (individual, no grupo)
# =============================================================================
print("\n— Por Segmento INDIVIDUAL (nombre de cada código):")
df_por_seg_nombre = resumen_por(df, "SEG_NOMBRE")
print(df_por_seg_nombre.to_string())

# =============================================================================
# CORRECCIÓN 6 — ESTADÍSTICAS CON INTERPRETACIONES AUTOMÁTICAS DETALLADAS
# =============================================================================
def interpretar_stats(serie_f_ap, serie_nof, nombre_col="Fraude Aprobado"):
    """Genera interpretaciones automáticas comparando fraude aprobado vs no fraude."""
    media_f  = serie_f_ap.mean()  if len(serie_f_ap)  > 0 else 0
    median_f = serie_f_ap.median() if len(serie_f_ap) > 0 else 0
    std_f    = serie_f_ap.std()   if len(serie_f_ap)  > 0 else 0
    media_n  = serie_nof.mean()
    median_n = serie_nof.median()
    std_n    = serie_nof.std()
    p75_f    = serie_f_ap.quantile(0.75) if len(serie_f_ap) > 0 else 0
    p90_f    = serie_f_ap.quantile(0.90) if len(serie_f_ap) > 0 else 0
    p95_f    = serie_f_ap.quantile(0.95) if len(serie_f_ap) > 0 else 0
    p75_n    = serie_nof.quantile(0.75)
    p90_n    = serie_nof.quantile(0.90)
    p95_n    = serie_nof.quantile(0.95)

    interpretaciones = []

    # Media vs Mediana → detectar sesgo por valores extremos
    if media_f > 0 and median_f > 0:
        ratio_mm = media_f / median_f
        if ratio_mm > 2:
            interpretaciones.append(
                f"📌 SESGO ALTO: La media de fraude aprobado (S/ {media_f:,.2f}) es {ratio_mm:.1f}x "
                f"mayor que la mediana (S/ {median_f:,.2f}). Hay transacciones de monto muy alto "
                f"que jalan la media hacia arriba — el fraude típico es de S/ {median_f:,.2f} "
                f"pero hay casos extremos."
            )
        elif ratio_mm > 1.5:
            interpretaciones.append(
                f"📌 SESGO MODERADO: Media S/ {media_f:,.2f} vs mediana S/ {median_f:,.2f} "
                f"(ratio {ratio_mm:.1f}x). Existen algunos montos altos que distorsionan el promedio."
            )
        else:
            interpretaciones.append(
                f"📌 DISTRIBUCIÓN SIMÉTRICA: Media S/ {media_f:,.2f} ≈ mediana S/ {median_f:,.2f}. "
                f"El fraude es homogéneo en monto, sin valores extremos dominantes."
            )

    # Desviación estándar → dispersión
    if std_f > 0 and media_f > 0:
        cv = std_f / media_f
        if cv > 1:
            interpretaciones.append(
                f"📌 ALTA VARIABILIDAD: Desv. estándar S/ {std_f:,.2f} (coef. variación {cv:.1f}x la media). "
                f"Los montos de fraude son muy heterogéneos — no hay un patrón único de monto."
            )
        else:
            interpretaciones.append(
                f"📌 VARIABILIDAD MODERADA: Desv. estándar S/ {std_f:,.2f}. "
                f"Los montos de fraude tienen cierta consistencia."
            )

    # Percentiles → dónde calibrar una regla
    if p75_f > p75_n:
        interpretaciones.append(
            f"📌 UMBRAL SUGERIDO P75: A partir de S/ {p75_f:,.2f} el fraude supera al legítimo "
            f"(P75 legítimo: S/ {p75_n:,.2f}). Una regla con umbral en este rango captaría "
            f"el 25% superior de montos fraudulentos con menor impacto en legítimas."
        )
    if p90_f > p90_n:
        interpretaciones.append(
            f"📌 UMBRAL SUGERIDO P90: S/ {p90_f:,.2f} (P90 legítimo: S/ {p90_n:,.2f}). "
            f"Umbral más conservador — captura el 10% de fraudes de mayor monto "
            f"con mínimo impacto en clientes legítimos."
        )
    if p95_f > p95_n * 1.5:
        interpretaciones.append(
            f"📌 COLA EXTREMA P95: El fraude tiene una cola muy alta S/ {p95_f:,.2f} "
            f"vs legítimo S/ {p95_n:,.2f} ({p95_f/p95_n:.1f}x). "
            f"Los ataques de mayor impacto económico son montos que normalmente no ocurren en legítimas."
        )

    return interpretaciones

interps_stats = interpretar_stats(df_f_ap["MONTO"], df_nofraude["MONTO"])
print("\nInterpretaciones automáticas de estadísticas de monto:")
for i in interps_stats:
    print(f"  {i}")

# =============================================================================
# NUEVA SECCIÓN 8 — TABLAS CRUZADAS (PIVOT) DE DOS DIMENSIONES
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 8 — TABLAS CRUZADAS DE DIMENSIONES")
print("="*65)

from scipy.stats import chi2_contingency

def tabla_cruzada(df_base, dim_filas, dim_cols, titulo):
    """
    Genera tabla cruzada con fraude aprobado y monto,
    más prueba chi-cuadrado para significancia estadística.
    """
    print(f"\n{'─'*55}")
    print(f"  {titulo}")
    print(f"  Filas: {dim_filas} | Columnas: {dim_cols}")
    print(f"{'─'*55}")

    # Tabla de fraude aprobado (conteo)
    pivot_fraude = df_base.pivot_table(
        values="ES_FRAUDE_APROBADO",
        index=dim_filas, columns=dim_cols,
        aggfunc="sum", fill_value=0
    )
    print("\nFraudes Aprobados (cantidad):")
    print(pivot_fraude.to_string())

    # Tabla de monto fraude aprobado
    pivot_monto = df_base[df_base["ES_FRAUDE_APROBADO"]==1].pivot_table(
        values="MONTO",
        index=dim_filas, columns=dim_cols,
        aggfunc="sum", fill_value=0
    ).applymap(lambda v: f"S/ {v:,.2f}")
    print("\nMonto Fraude Aprobado (S/):")
    print(pivot_monto.to_string())

    # Tabla de fraud rate % por celda
    pivot_fr = df_base.pivot_table(
        values="ES_FRAUDE_APROBADO",
        index=dim_filas, columns=dim_cols,
        aggfunc="mean", fill_value=0
    ).applymap(lambda v: f"{v*100:.4f}%")
    print("\nFraud Rate % Aprobado por celda:")
    print(pivot_fr.to_string())

    # Chi-cuadrado sobre la tabla de conteos
    try:
        contingencia = df_base.pivot_table(
            values="ES_FRAUDE_APROBADO",
            index=dim_filas, columns=dim_cols,
            aggfunc="sum", fill_value=0
        ).values
        chi2, p_val, dof, expected = chi2_contingency(contingencia)
        sig = "✅ SIGNIFICATIVA" if p_val < 0.05 else "❌ No significativa"
        print(f"\nPrueba Chi-cuadrado:")
        print(f"  Chi2 = {chi2:.4f} | p-valor = {p_val:.6f} | gl = {dof}")
        print(f"  Resultado: {sig} (alpha=0.05)")
        if p_val < 0.05:
            print(f"  → La combinación {dim_filas} × {dim_cols} SÍ tiene asociación")
            print(f"    estadística con el fraude — útil para segmentar la regla.")
        else:
            print(f"  → No hay asociación estadística significativa entre")
            print(f"    {dim_filas} y {dim_cols} para el fraude.")
    except Exception as e:
        print(f"  Chi-cuadrado no calculable: {e}")

    return pivot_fraude, pivot_monto

# Cruces definidos
cruces = [
    ("TIPO_PRODUCTO", "SEGURO",       "Tipo Producto × Seguro/No Seguro"),
    ("SEG_NOMBRE",    "SEGURO",       "Segmento × Seguro/No Seguro"),
    ("SEG_NOMBRE",    "TIPO_PRODUCTO","Segmento × Tipo Producto"),
    ("COD_RED_LABEL", "SEGURO",       "CVV/Red × Seguro/No Seguro"),
    ("CANAL",         "SEGURO",       "Canal × Seguro/No Seguro"),
    ("TIPO_PRODUCTO", "COD_RED_LABEL","Tipo Producto × CVV/Red"),
]

tablas_cruzadas = {}
for dim_f, dim_c, titulo in cruces:
    piv_f, piv_m = tabla_cruzada(df_ap, dim_f, dim_c, titulo)
    tablas_cruzadas[titulo] = (piv_f, piv_m)

# =============================================================================
# NUEVA SECCIÓN 9 — CLIENTES ÚNICOS Y RECURRENTES CON FRAUDE
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 9 — CLIENTES ÚNICOS Y RECURRENCIA DE FRAUDE")
print("="*65)

# Nota sobre clientes únicos por mes
print("""
NOTA IMPORTANTE — Por qué clientes únicos por mes no suma al total:
  Un cliente con trxs en marzo Y abril se cuenta en ambos meses por separado,
  pero solo una vez en el total global. Esto es correcto — no es un error.
""")

clientes_por_mes = df_ap.groupby("MES")["ID_CLIENTE"].nunique()
total_clientes_globales = df_ap["ID_CLIENTE"].nunique()
suma_clientes_meses = clientes_por_mes.sum()

print("Clientes únicos por mes:")
print(clientes_por_mes.to_string())
print(f"\nSuma de clientes por mes:     {suma_clientes_meses:,}")
print(f"Clientes únicos globales:     {total_clientes_globales:,}")
print(f"Diferencia (solapamiento):    {suma_clientes_meses - total_clientes_globales:,}")
print(f"→ Hay {suma_clientes_meses - total_clientes_globales:,} apariciones de clientes")
print(f"  que compraron en más de un mes")

# Clientes con fraude por mes
clientes_fraude_por_mes = (
    df_ap[df_ap["ES_FRAUDE_APROBADO"]==1]
    .groupby("MES")["ID_CLIENTE"]
    .apply(set)
)

print("\nClientes con fraude aprobado por mes:")
for mes, clientes in clientes_fraude_por_mes.items():
    print(f"  {mes}: {len(clientes):,} clientes")

# Clientes recurrentes (con fraude en más de un mes)
meses_list = sorted(df_ap["MES"].unique())

if len(meses_list) >= 2:
    print("\nAnálisis de recurrencia de fraude entre meses:")

    # Para cada par de meses consecutivos
    for i in range(len(meses_list)-1):
        mes_a = meses_list[i]
        mes_b = meses_list[i+1]

        cli_f_a = set(df_ap[(df_ap["MES"]==mes_a) & (df_ap["ES_FRAUDE_APROBADO"]==1)]["ID_CLIENTE"])
        cli_f_b = set(df_ap[(df_ap["MES"]==mes_b) & (df_ap["ES_FRAUDE_APROBADO"]==1)]["ID_CLIENTE"])
        cli_tot_b = set(df_ap[df_ap["MES"]==mes_b]["ID_CLIENTE"])

        recurrentes = cli_f_a & cli_f_b
        nuevos_b    = cli_f_b - cli_f_a
        pct_recurrentes = len(recurrentes) / len(cli_f_b) * 100 if len(cli_f_b) > 0 else 0

        print(f"\n  {mes_a} → {mes_b}:")
        print(f"    Clientes con fraude en {mes_a}:        {len(cli_f_a):,}")
        print(f"    Clientes con fraude en {mes_b}:        {len(cli_f_b):,}")
        print(f"    Recurrentes (fraude en ambos meses): {len(recurrentes):,} ({pct_recurrentes:.2f}%)")
        print(f"    Nuevos casos en {mes_b}:              {len(nuevos_b):,}")

        if len(recurrentes) > 0:
            print(f"    → Hay {len(recurrentes):,} clientes que tuvieron fraude aprobado")
            print(f"      en {mes_a} Y {mes_b} — posible fraude recurrente o no gestionado.")
        else:
            print(f"    → No hay clientes con fraude en ambos meses consecutivos.")

    # Resumen global de recurrencia (fraude en los 3 meses)
    if len(meses_list) >= 3:
        sets_fraude = [
            set(df_ap[(df_ap["MES"]==m) & (df_ap["ES_FRAUDE_APROBADO"]==1)]["ID_CLIENTE"])
            for m in meses_list
        ]
        recurrentes_todos = sets_fraude[0]
        for s in sets_fraude[1:]:
            recurrentes_todos = recurrentes_todos & s

        print(f"\n  Clientes con fraude en TODOS los meses ({', '.join(meses_list)}): {len(recurrentes_todos):,}")
        if len(recurrentes_todos) > 0:
            print(f"  → Estos {len(recurrentes_todos):,} clientes son ALTAMENTE recurrentes.")
            print(f"  IDs: {list(recurrentes_todos)[:10]}")  # muestra hasta 10

# =============================================================================
# EXPORTACIÓN — Nuevas hojas al Excel
# Agrega dentro del bloque with pd.ExcelWriter(...)
# =============================================================================

# ── HOJA 8: TABLAS CRUZADAS ───────────────────────────────────────────────────
# Agrega esto dentro del with pd.ExcelWriter(...) del script principal:
"""
    sheet = "8_Tablas_Cruzadas"
    ws8 = writer.book.create_sheet(sheet)
    writer.sheets[sheet] = ws8
    fila_actual = 1

    from scipy.stats import chi2_contingency
    cruces_excel = [
        ("TIPO_PRODUCTO", "SEGURO",       "Tipo Producto × Seguro/No Seguro"),
        ("SEG_NOMBRE",    "SEGURO",       "Segmento × Seguro/No Seguro"),
        ("COD_RED_LABEL", "SEGURO",       "CVV/Red × Seguro/No Seguro"),
        ("TIPO_PRODUCTO", "COD_RED_LABEL","Tipo Producto × CVV/Red"),
    ]
    for dim_f, dim_c, titulo in cruces_excel:
        # Header
        estilizar_header(ws8, fila_actual, 10, f"MCC 7995 — {titulo.upper()}")
        fila_actual += 1

        # Pivot fraude
        piv = df_ap.pivot_table(
            values="ES_FRAUDE_APROBADO",
            index=dim_f, columns=dim_c,
            aggfunc="sum", fill_value=0
        ).reset_index()
        piv.to_excel(writer, sheet_name=sheet, index=False, startrow=fila_actual-1)
        fila_actual += len(piv) + 3

        # Chi-cuadrado
        try:
            contingencia = df_ap.pivot_table(
                values="ES_FRAUDE_APROBADO",
                index=dim_f, columns=dim_c,
                aggfunc="sum", fill_value=0
            ).values
            chi2, p_val, dof, _ = chi2_contingency(contingencia)
            sig = "SIGNIFICATIVA ✅" if p_val < 0.05 else "No significativa ❌"
            ws8.cell(row=fila_actual, column=1,
                     value=f"Chi2={chi2:.4f} | p={p_val:.6f} | {sig}")
            fila_actual += 3
        except:
            fila_actual += 2

    ajustar_columnas(ws8)

    # ── HOJA 9: CLIENTES RECURRENTES ─────────────────────────────────────────
    sheet = "9_Clientes_Recurrentes"
    # Resumen por mes
    df_recurrencia = pd.DataFrame({
        "Mes": meses_list,
        "Clientes únicos": [df_ap[df_ap["MES"]==m]["ID_CLIENTE"].nunique() for m in meses_list],
        "Clientes con fraude": [
            df_ap[(df_ap["MES"]==m) & (df_ap["ES_FRAUDE_APROBADO"]==1)]["ID_CLIENTE"].nunique()
            for m in meses_list
        ],
    })
    df_recurrencia["% con fraude"] = (
        df_recurrencia["Clientes con fraude"] / df_recurrencia["Clientes únicos"] * 100
    ).map("{:.4f}%".format)

    df_recurrencia.to_excel(writer, sheet_name=sheet, index=False, startrow=3)
    ws = writer.sheets[sheet]
    estilizar_header(ws, 1, len(df_recurrencia.columns),
        "MCC 7995 — CLIENTES ÚNICOS Y RECURRENCIA DE FRAUDE")
    estilizar_encabezados_df(ws, 4)
    estilizar_datos(ws, 5, ws.max_row, fraude_keywords=["fraude"])
    fi = ws.max_row + 2
    agregar_interpretacion(ws, fi, len(df_recurrencia.columns),
        f"Total clientes únicos globales: {total_clientes_globales:,}. "
        "La suma por mes es mayor porque un mismo cliente puede aparecer en varios meses.")
    ajustar_columnas(ws)
"""

print("\n✅ Correcciones y nuevas secciones procesadas")
print("   Para exportar al Excel: agrega las hojas 8 y 9 dentro del bloque with pd.ExcelWriter")
