# =============================================================================
# ANÁLISIS INTEGRAL MCC 7995 — Juegos de Azar
# Fuente: Parquet consolidado desde Monitor
# =============================================================================

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings("ignore")

# =============================================================================
# ██████  AJUSTA SOLO ESTE BLOQUE — nombres reales de tus columnas en Monitor
# =============================================================================
COLS = {
    "FECHA"           : "acf_fecha_transaccion",           # fecha transacción
    "HORA"            : "hf_hora_transaccion",             # hora transacción
    "COMERCIO"        : "acf_nombre_comercio",             # nombre comercio
    "INDICADOR"       : "acf_indicador_fraude",            # F/N/G/B/P
    "COD_RESPUESTA"   : "acf_codigo_respuesta",            # 00 / 0000 = aprobada
    "CONDICION_RT"    : "acf_condicion_rt",                # LIKE 088
    "CANAL"           : "acf_canal",                       # POS/ATM/ECOM
    "ENTRY_MODE"      : "acf_entry_mode",                  # entry mode
    "SALDO"           : "hf_saldo_disponible",             # saldo disponible
    "ID_CLIENTE"      : "acf_id_cliente",                  # ID cliente
    "ESI_UCAP"        : "acf_esi_ucap",                    # 5/2=seguro, else=no seguro
    "PAIS"            : "acf_pais_origen",                 # país origen
    "COD_RED_COMERCIO": "acf_codigo_red_comercio",         # S/D/E/N
    "MONTO"           : "acf_monto_original_transaccion",  # monto original
    "SEGMENTO"        : "acf_evento_compromiso_otra_fuente", # segmento cliente
    "TIPO_PRODUCTO"   : "acf_tipo_producto",               # TD / TC
}

RUTA_PARQUET = r"C:\ruta\al\consolidado_7995.parquet"     # ← AJUSTAR
# =============================================================================

# ── Diccionarios de decodificación ────────────────────────────────────────────
SEG_NOMBRE = {
    "30": "Polo Dirección",   "99": "Polo Dirección",
    "31": "Premium",          "32": "Preferente",
    "33": "Personal",         "34": "Estándar",
    "5" : "Inst. Financieras","21": "Corporativo",
    "2" : "Mediano Empresas", "15": "Sector Gobierno",
    "16": "Otras Instituciones",
    "3" : "Pequeñas Empresas","4" : "Negocios 2",
    "7" : "Negocios 3",       "8" : "Negocios 1",
    "13": "Microempresas",
}
SEG_GRUPO = {
    "30": "Affluent",         "99": "Affluent",
    "31": "Emerging Affluent","32": "Emerging Affluent",
    "33": "Top of Mass",      "34": "Mass",
    "5" : "Corporate",        "21": "Corporate",
    "2" : "Commercial",       "15": "Commercial",
    "16": "Commercial",
    "3" : "Small Business",   "4" : "Small Business",
    "7" : "Small Business",   "8" : "Small Business",
    "13": "Small Business",
}
COD_RED_LABEL = {
    "S": "Estático (TD)",
    "D": "Dinámico (TD/TC)",
    "E": "Estático (TC)",
    "N": "No Match / Sin CVV",
}

C_FRAUDE = "#D9534F"
C_OK     = "#5B9BD5"
C_088    = "#F0AD4E"
C_NEU    = "#8EA9C1"

# =============================================================================
# CARGA Y PREPARACIÓN
# =============================================================================
print("Cargando parquet...")
df_raw = pd.read_parquet(RUTA_PARQUET)

# Renombrar a alias internos
col_map = {v: k for k, v in COLS.items()}
df = df_raw.rename(columns=col_map).copy()

# Verificar columnas disponibles
faltantes = [a for a in COLS if a not in df.columns]
if faltantes:
    print(f"  ⚠️  Columnas no encontradas (revisa el diccionario COLS): {faltantes}")

# Tipos
df["FECHA"]  = pd.to_datetime(df["FECHA"], errors="coerce")
df["MONTO"]  = pd.to_numeric(df["MONTO"],  errors="coerce")
df["SALDO"]  = pd.to_numeric(df["SALDO"],  errors="coerce")

for c in ["INDICADOR","COD_RESPUESTA","CONDICION_RT","CANAL",
          "ESI_UCAP","COD_RED_COMERCIO","SEGMENTO","TIPO_PRODUCTO","ENTRY_MODE"]:
    if c in df.columns:
        df[c] = df[c].astype(str).str.strip().str.upper()

# HORA: combinar fecha + hora para datetime completo
if "HORA" in df.columns:
    df["HORA"] = df["HORA"].astype(str).str.strip()
    df["DATETIME"] = pd.to_datetime(
        df["FECHA"].astype(str) + " " + df["HORA"], errors="coerce"
    )
else:
    df["DATETIME"] = df["FECHA"]

# ── Variables derivadas ────────────────────────────────────────────────────────
# Estado aprobada / denegada desde código de respuesta
df["ESTADO"] = df["COD_RESPUESTA"].apply(
    lambda x: "APROBADA" if str(x).strip() in ["00", "0000", "000"] else "DENEGADA"
)

# Fraude binario
df["ES_FRAUDE"] = (df["INDICADOR"] == "F").astype(int)

# Condición 088
df["COND_088"] = df["CONDICION_RT"].str.contains("088", na=False)

# Seguro / No seguro
df["SEGURO"] = df["ESI_UCAP"].apply(
    lambda x: "Seguro" if str(x).strip() in ["2", "5"] else "No Seguro"
)

# Segmento decodificado
df["SEG_NOMBRE"] = df["SEGMENTO"].map(SEG_NOMBRE).fillna("Otro")
df["SEG_GRUPO"]  = df["SEGMENTO"].map(SEG_GRUPO).fillna("Otro")

# Código red decodificado
df["COD_RED_LABEL"] = df["COD_RED_COMERCIO"].map(COD_RED_LABEL).fillna("Otro")

# Mes
df["MES"] = df["FECHA"].dt.to_period("M").astype(str)

print(f"  Registros totales:  {len(df):,}")
print(f"  Rango fechas:       {df['FECHA'].min().date()} → {df['FECHA'].max().date()}")
print(f"  Aprobadas:          {(df['ESTADO']=='APROBADA').sum():,}")
print(f"  Denegadas:          {(df['ESTADO']=='DENEGADA').sum():,}")
print(f"  Fraudes (F):        {df['ES_FRAUDE'].sum():,}")
print(f"  Con condición 088:  {df['COND_088'].sum():,}")

# Sub-datasets frecuentes
df_ap  = df[df["ESTADO"] == "APROBADA"].copy()
df_den = df[df["ESTADO"] == "DENEGADA"].copy()
df_f   = df[df["ES_FRAUDE"] == 1].copy()

# =============================================================================
# SECCIÓN 1 — RESUMEN GENERAL (tabla ejecutiva)
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 1 — RESUMEN GENERAL MCC 7995")
print("="*65)

mask_labeled = df["INDICADOR"].isin(["F","G"])
fr_global = df.loc[mask_labeled,"ES_FRAUDE"].mean() if mask_labeled.sum() > 0 else 0

resumen = pd.DataFrame({
    "Métrica": [
        "Total transacciones",
        "Aprobadas",
        "Denegadas",
        "Fraudes confirmados (F)",
        "Monto total aprobado (S/)",
        "Ticket promedio aprobadas (S/)",
        "Ticket promedio fraude (S/)",
        "Fraud Rate (F / F+G)",
        "Con condición 088 disparada",
        "Comercios únicos",
        "Clientes únicos",
    ],
    "Valor": [
        f"{len(df):,}",
        f"{(df['ESTADO']=='APROBADA').sum():,}",
        f"{(df['ESTADO']=='DENEGADA').sum():,}",
        f"{df['ES_FRAUDE'].sum():,}",
        f"S/ {df_ap['MONTO'].sum():,.2f}",
        f"S/ {df_ap['MONTO'].mean():,.2f}",
        f"S/ {df_f['MONTO'].mean():,.2f}" if len(df_f) > 0 else "N/A",
        f"{fr_global*100:.2f}%",
        f"{df['COND_088'].sum():,}  ({df['COND_088'].mean()*100:.1f}%)",
        f"{df['COMERCIO'].nunique():,}",
        f"{df['ID_CLIENTE'].nunique():,}" if "ID_CLIENTE" in df.columns else "N/A",
    ]
}).set_index("Métrica")
print(resumen.to_string())

# =============================================================================
# SECCIÓN 2 — TABLA DE DECILES (replica análisis de referencia)
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 2 — TABLA DE DECILES POR MONTO")
print("="*65)

def construir_tabla_deciles(df_base, n_deciles=10, label="D"):
    """Construye la tabla de deciles estilo análisis de referencia."""
    df_base = df_base.copy().dropna(subset=["MONTO"])

    # Crear deciles por frecuencia igual
    df_base["DECIL"] = pd.qcut(
        df_base["MONTO"], q=n_deciles,
        labels=[f"{label}{str(i+1).zfill(2)}" for i in range(n_deciles)],
        duplicates="drop"
    )

    rows = []
    fraude_acum = 0
    trx_ap_acum = 0

    for decil in df_base["DECIL"].cat.categories:
        sub = df_base[df_base["DECIL"] == decil]
        sub_ap  = sub[sub["ESTADO"] == "APROBADA"]
        sub_den = sub[sub["ESTADO"] == "DENEGADA"]
        sub_f   = sub[(sub["ESTADO"] == "APROBADA") & (sub["ES_FRAUDE"] == 1)]

        rango_min = sub["MONTO"].min()
        rango_max = sub["MONTO"].max()

        n_ap    = len(sub_ap)
        m_ap    = sub_ap["MONTO"].sum()
        freq_ap = n_ap / len(df_base[df_base["ESTADO"]=="APROBADA"]) if len(df_base[df_base["ESTADO"]=="APROBADA"]) > 0 else 0
        trx_ap_acum += n_ap

        n_den   = len(sub_den)
        m_den   = sub_den["MONTO"].sum()

        n_tot   = len(sub)
        ticket  = sub["MONTO"].mean()
        m_tot   = sub["MONTO"].sum()

        n_f     = len(sub_f)
        m_f     = sub_f["MONTO"].sum()
        fraude_acum += n_f
        freq_f  = n_f / len(df_base[df_base["ES_FRAUDE"]==1]) if df_base["ES_FRAUDE"].sum() > 0 else 0
        freq_f_acum = fraude_acum / df_base["ES_FRAUDE"].sum() if df_base["ES_FRAUDE"].sum() > 0 else 0

        pct_f_ap   = n_f / n_ap if n_ap > 0 else 0
        pct_nof_ap = 1 - pct_f_ap

        rows.append({
            "Decil"               : decil,
            "Rango S/"            : f"{rango_min:.0f}–{rango_max:.0f}",
            "Trx Aprobadas"       : n_ap,
            "Monto Aprobadas"     : round(m_ap, 1),
            "Freq Aprobadas"      : f"{freq_ap*100:.0f}%",
            "Freq Acum Aprobadas" : f"{trx_ap_acum/len(df_base[df_base['ESTADO']=='APROBADA'])*100:.0f}%" if len(df_base[df_base['ESTADO']=='APROBADA'])>0 else "0%",
            "Trx Denegadas"       : n_den,
            "Monto Denegadas"     : round(m_den, 1),
            "Total Trx"           : n_tot,
            "Ticket Promedio"     : round(ticket, 2),
            "Monto Total S/"      : round(m_tot, 1),
            "Fraude Trx Aprobado" : n_f,
            "Fraude Monto Aprobado": round(m_f, 1),
            "Fraude Frecuencia"   : f"{freq_f*100:.0f}%",
            "Fraude Freq Acumulada": f"{freq_f_acum*100:.0f}%",
            "Fraude/Trx Aprobadas": f"{pct_f_ap*100:.0f}%",
            "No Fraude/Trx Aprobadas": f"{pct_nof_ap*100:.0f}%",
        })

    return pd.DataFrame(rows).set_index("Decil")

tabla_deciles = construir_tabla_deciles(df, n_deciles=10)
print("\nTabla de deciles (D01=menor monto, D10=mayor monto):")
print(tabla_deciles.to_string())

# Apertura del D10
print("\nApertura del Decil 10 (top monto):")
monto_d10_min = df["MONTO"].quantile(0.90)
df_d10 = df[df["MONTO"] >= monto_d10_min].copy()
tabla_d10 = construir_tabla_deciles(df_d10, n_deciles=10, label="D")
print(tabla_d10.to_string())

# Gráfico Plotly: Fraude por decil
fig = make_subplots(rows=1, cols=2,
    subplot_titles=["Fraudes por decil de monto", "Fraud Rate % por decil"])

deciles_idx = tabla_deciles.index.tolist()
n_fraudes_d = tabla_deciles["Fraude Trx Aprobado"].tolist()
fr_d        = [
    (tabla_deciles.loc[d,"Fraude Trx Aprobado"] / tabla_deciles.loc[d,"Trx Aprobadas"] * 100)
    if tabla_deciles.loc[d,"Trx Aprobadas"] > 0 else 0
    for d in deciles_idx
]

fig.add_trace(go.Bar(
    x=deciles_idx, y=n_fraudes_d, name="Fraudes",
    marker_color=C_FRAUDE,
    text=[f"{v:,}" for v in n_fraudes_d], textposition="outside",
), row=1, col=1)

fig.add_trace(go.Bar(
    x=deciles_idx, y=fr_d, name="FR%",
    marker_color=C_088,
    text=[f"{v:.1f}%" for v in fr_d], textposition="outside",
), row=1, col=2)

fig.update_layout(title_text="MCC 7995 — Concentración del fraude por decil de monto",
                  showlegend=False, height=450, plot_bgcolor="white")
fig.write_html("g1_deciles.html")
fig.show()
print("  → Guardado: g1_deciles.html")

# =============================================================================
# SECCIÓN 3 — VELOCIDAD POR INTERVALO DE TIEMPO (entre transacciones del cliente)
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 3 — VELOCIDAD: INTERVALO ENTRE TRANSACCIONES DEL MISMO CLIENTE")
print("="*65)

# Ordenar por cliente y datetime
df_vel = df.sort_values(["ID_CLIENTE","DATETIME"]).copy()

# Tiempo desde la transacción anterior del mismo cliente (en minutos)
df_vel["PREV_DT"] = df_vel.groupby("ID_CLIENTE")["DATETIME"].shift(1)
df_vel["MIN_DESDE_PREV"] = (
    df_vel["DATETIME"] - df_vel["PREV_DT"]
).dt.total_seconds() / 60

# Solo filas con transacción previa del mismo cliente
df_vel2 = df_vel.dropna(subset=["MIN_DESDE_PREV"]).copy()
df_vel2 = df_vel2[df_vel2["MIN_DESDE_PREV"] >= 0]  # eliminar negativos por error de datos

# Bucket de intervalo
def bucket_intervalo(m):
    if   m <=  2: return "≤2 min"
    elif m <=  5: return "≤5 min"
    elif m <= 10: return "≤10 min"
    elif m <= 15: return "≤15 min"
    elif m <= 20: return "≤20 min"
    elif m <= 60: return "≤1 hora"
    else:         return ">1 hora"

ORDEN_BUCKETS = ["≤2 min","≤5 min","≤10 min","≤15 min","≤20 min","≤1 hora",">1 hora"]

df_vel2["BUCKET"] = df_vel2["MIN_DESDE_PREV"].apply(bucket_intervalo)
df_vel2["BUCKET"] = pd.Categorical(df_vel2["BUCKET"], categories=ORDEN_BUCKETS, ordered=True)

tabla_vel = (
    df_vel2
    .groupby("BUCKET", observed=True)
    .agg(
        Transacciones       =("MONTO",      "count"),
        Genuinas            =("ES_FRAUDE",  lambda x: (x==0).sum()),
        Fraudes             =("ES_FRAUDE",  "sum"),
        FR_pct              =("ES_FRAUDE",  lambda x: x.mean()*100),
        Monto_Genuino       =("MONTO",      lambda x: x[df_vel2.loc[x.index,"ES_FRAUDE"]==0].sum()),
        Monto_Fraude        =("MONTO",      lambda x: x[df_vel2.loc[x.index,"ES_FRAUDE"]==1].sum()),
        Ticket_Genuino_Prom =("MONTO",      lambda x: x[df_vel2.loc[x.index,"ES_FRAUDE"]==0].mean()),
        Ticket_Fraude_Prom  =("MONTO",      lambda x: x[df_vel2.loc[x.index,"ES_FRAUDE"]==1].mean()),
    )
    .assign(
        Pct_fraudes_total=lambda x: x["Fraudes"] / x["Fraudes"].sum() * 100,
        Impacto_fraude_pct=lambda x: x["Fraudes"] / x["Transacciones"] * 100,
    )
)

print("\nTabla de velocidad por intervalo entre transacciones del mismo cliente:")
print(tabla_vel.to_string())

# Gráfico Plotly: Velocidad
fig2 = make_subplots(rows=1, cols=2,
    subplot_titles=["Genuinas vs Fraudes por intervalo",
                    "Fraud Rate % por intervalo"])

bkts = tabla_vel.index.tolist()

fig2.add_trace(go.Bar(
    x=bkts, y=tabla_vel["Genuinas"].tolist(), name="Genuinas",
    marker_color=C_OK,
    text=[f"{v:,}" for v in tabla_vel["Genuinas"]], textposition="outside",
), row=1, col=1)
fig2.add_trace(go.Bar(
    x=bkts, y=tabla_vel["Fraudes"].tolist(), name="Fraudes",
    marker_color=C_FRAUDE,
    text=[f"{v:,}" for v in tabla_vel["Fraudes"]], textposition="outside",
), row=1, col=1)

fig2.add_trace(go.Bar(
    x=bkts, y=tabla_vel["FR_pct"].round(2).tolist(), name="FR%",
    marker_color=C_088,
    text=[f"{v:.1f}%" for v in tabla_vel["FR_pct"]], textposition="outside",
), row=1, col=2)

fig2.update_layout(
    title_text="MCC 7995 — Fraude según intervalo entre transacciones del mismo cliente",
    barmode="group", height=450, plot_bgcolor="white"
)
fig2.write_html("g2_velocidad_intervalo.html")
fig2.show()
print("  → Guardado: g2_velocidad_intervalo.html")

# =============================================================================
# SECCIÓN 4 — CONDICIÓN 088 vs FRAUDE REAL
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 4 — CONDICIÓN 088: ¿CAPTURA EL FRAUDE REAL?")
print("="*65)

total_fraudes = df["ES_FRAUDE"].sum()
fraudes_en_088    = (df["COND_088"] & (df["ES_FRAUDE"]==1)).sum()
fraudes_fuera_088 = (~df["COND_088"] & (df["ES_FRAUDE"]==1)).sum()

print(f"  Fraudes DENTRO de txns con condición 088:  {fraudes_en_088:,}  ({fraudes_en_088/total_fraudes*100:.1f}%)" if total_fraudes > 0 else "  Sin fraudes.")
print(f"  Fraudes FUERA  de txns con condición 088:  {fraudes_fuera_088:,}  ({fraudes_fuera_088/total_fraudes*100:.1f}%)" if total_fraudes > 0 else "")

comp_088 = pd.DataFrame({
    "Con 088": {
        "Transacciones"    : df["COND_088"].sum(),
        "Fraudes"          : fraudes_en_088,
        "FR%"              : df.loc[df["COND_088"] & df["INDICADOR"].isin(["F","G"]), "ES_FRAUDE"].mean()*100 if (df["COND_088"] & df["INDICADOR"].isin(["F","G"])).sum() > 0 else 0,
        "Monto prom (S/)"  : df.loc[df["COND_088"],"MONTO"].mean(),
        "Monto mediano (S/)": df.loc[df["COND_088"],"MONTO"].median(),
    },
    "Sin 088": {
        "Transacciones"    : (~df["COND_088"]).sum(),
        "Fraudes"          : fraudes_fuera_088,
        "FR%"              : df.loc[~df["COND_088"] & df["INDICADOR"].isin(["F","G"]), "ES_FRAUDE"].mean()*100 if (~df["COND_088"] & df["INDICADOR"].isin(["F","G"])).sum() > 0 else 0,
        "Monto prom (S/)"  : df.loc[~df["COND_088"],"MONTO"].mean(),
        "Monto mediano (S/)": df.loc[~df["COND_088"],"MONTO"].median(),
    },
}).T
print("\nComparativo Con 088 vs Sin 088:")
print(comp_088.to_string())

# =============================================================================
# SECCIÓN 5 — ANÁLISIS POR DIMENSIONES (Segmento, ESI_UCAP, Canal, Tipo Producto)
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 5 — DISTRIBUCIÓN DEL FRAUDE POR DIMENSIONES")
print("="*65)

def resumen_por(df_base, col, top_n=10):
    """Tabla resumen de fraude por cualquier dimensión."""
    return (
        df_base
        .groupby(col)
        .agg(
            Txns        =(col,         "count"),
            Fraudes     =("ES_FRAUDE", "sum"),
            FR_pct      =("ES_FRAUDE", lambda x: x.mean()*100),
            Monto_total =("MONTO",     "sum"),
            Monto_fraude=("MONTO",     lambda x: x[df_base.loc[x.index,"ES_FRAUDE"]==1].sum()),
            Ticket_prom =("MONTO",     "mean"),
        )
        .sort_values("Fraudes", ascending=False)
        .head(top_n)
        .rename(columns={"FR_pct":"FR%"})
    )

# Por segmento
print("\n— Por segmento de cliente:")
print(resumen_por(df, "SEG_NOMBRE").to_string())

# Por ESI_UCAP (seguro/no seguro)
print("\n— Por seguridad del comercio (ESI/UCAP):")
print(resumen_por(df, "SEGURO").to_string())

# Por canal
print("\n— Por canal:")
print(resumen_por(df, "CANAL").to_string())

# Por tipo de producto
print("\n— Por tipo de producto (TD/TC):")
print(resumen_por(df, "TIPO_PRODUCTO").to_string())

# Por código red comercio
print("\n— Por código red comercio (CVV estático/dinámico):")
print(resumen_por(df, "COD_RED_LABEL").to_string())

# Por país origen (top 10)
print("\n— Por país origen (top 10 por fraudes):")
print(resumen_por(df, "PAIS").to_string())

# Por comercio (top 10)
print("\n— Top 10 comercios por fraudes:")
print(resumen_por(df, "COMERCIO").to_string())

# Gráfico Plotly: Fraude por dimensiones (4 paneles)
dims = [
    ("SEG_NOMBRE",    "Segmento"),
    ("SEGURO",        "Comercio seguro/no seguro"),
    ("CANAL",         "Canal"),
    ("COD_RED_LABEL", "Código red (CVV)"),
]

fig3 = make_subplots(rows=2, cols=2, subplot_titles=[d[1] for d in dims])

for i, (col, titulo) in enumerate(dims):
    r, c = divmod(i, 2)
    sub = resumen_por(df, col, top_n=8).reset_index()
    fig3.add_trace(go.Bar(
        x=sub[col], y=sub["Fraudes"],
        text=sub["Fraudes"].apply(lambda v: f"{v:,}"),
        textposition="outside",
        marker_color=C_FRAUDE, showlegend=False,
    ), row=r+1, col=c+1)

fig3.update_layout(
    title_text="MCC 7995 — Distribución del fraude por dimensión",
    height=700, plot_bgcolor="white"
)
fig3.write_html("g3_fraude_por_dimension.html")
fig3.show()
print("  → Guardado: g3_fraude_por_dimension.html")

# =============================================================================
# SECCIÓN 6 — EVOLUCIÓN MENSUAL (barras separadas, no apiladas)
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 6 — EVOLUCIÓN MENSUAL")
print("="*65)

evol = (
    df.groupby("MES")
    .agg(
        Aprobadas   =("ESTADO",     lambda x: (x=="APROBADA").sum()),
        Denegadas   =("ESTADO",     lambda x: (x=="DENEGADA").sum()),
        Fraudes     =("ES_FRAUDE",  "sum"),
        Con_088     =("COND_088",   "sum"),
        Monto_total =("MONTO",      "sum"),
    )
    .reset_index()
)

fig4 = make_subplots(rows=1, cols=2,
    subplot_titles=["Aprobadas / Denegadas / Fraudes por mes",
                    "Fraudes y condición 088 por mes"])

for serie, color, nombre in [
    ("Aprobadas", C_OK,     "Aprobadas"),
    ("Denegadas", C_NEU,    "Denegadas"),
    ("Fraudes",   C_FRAUDE, "Fraudes"),
]:
    fig4.add_trace(go.Bar(
        x=evol["MES"], y=evol[serie], name=nombre,
        marker_color=color,
        text=evol[serie].apply(lambda v: f"{v:,}"),
        textposition="outside",
    ), row=1, col=1)

for serie, color, nombre in [
    ("Fraudes",  C_FRAUDE, "Fraudes"),
    ("Con_088",  C_088,    "Con condición 088"),
]:
    fig4.add_trace(go.Bar(
        x=evol["MES"], y=evol[serie], name=nombre,
        marker_color=color,
        text=evol[serie].apply(lambda v: f"{v:,}"),
        textposition="outside",
    ), row=1, col=2)

fig4.update_layout(
    title_text="MCC 7995 — Evolución mensual",
    barmode="group", height=450, plot_bgcolor="white"
)
fig4.write_html("g4_evolucion_mensual.html")
fig4.show()
print("  → Guardado: g4_evolucion_mensual.html")

# =============================================================================
# RESUMEN FINAL
# =============================================================================
print("\n" + "="*65)
print("RESUMEN EJECUTIVO — MCC 7995")
print("="*65)
print(f"""
  Fraud Rate global (F/F+G):       {fr_global*100:.2f}%
  Fraudes con condición 088:       {fraudes_en_088:,}  ({fraudes_en_088/total_fraudes*100:.1f}% del total) {"" if total_fraudes > 0 else "— sin fraudes"}
  Fraudes sin condición 088:       {fraudes_fuera_088:,}  ({fraudes_fuera_088/total_fraudes*100:.1f}% del total) {"" if total_fraudes > 0 else ""}
  Txns con 088 que NO son fraude:  {(df["COND_088"] & (df["ES_FRAUDE"]==0)).sum():,}

  Archivos generados:
  g1_deciles.html           → Tabla de deciles visualizada
  g2_velocidad_intervalo.html → Fraude por intervalo entre transacciones
  g3_fraude_por_dimension.html → Fraude por segmento, canal, seguridad, CVV
  g4_evolucion_mensual.html → Evolución mensual barras separadas
""")
