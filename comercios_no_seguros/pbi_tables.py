"""
Generador de tablas para Power BI — Comercios No Seguros
Lee el parquet enriquecido (salida de feature_engineering.py)
y exporta CSVs optimizados para cada página del dashboard.

Tablas generadas:
  tab_kpi              → KPIs ejecutivos (una sola fila de métricas)
  tab_tendencia        → Fraudes por semana y mes (línea de tendencia)
  tab_temporal         → Mapa de calor hora × día semana
  tab_franja           → Fraudes por franja horaria y quincena
  tab_comercios        → Ranking de comercios
  tab_mcc              → Ranking de MCC
  tab_tarjetas         → Perfil de tarjetas afectadas
  tab_modalidad        → Distribución por modalidad de fraude
  tab_canal_org        → Fraudes por canal y organización
  tab_cierre           → Tiempos de cierre de casos
  tab_detalle          → Tabla de detalle para drillthrough (todas las filas)
  dim_calendario       → Dimensión de fechas para el modelo Power BI
"""

import pandas as pd
import numpy as np
import os
import warnings

warnings.filterwarnings("ignore")

from config import COLS, PARQUET_OUTPUT

C = COLS
PARQUET_INPUT = PARQUET_OUTPUT       # salida del feature_engineering.py
OUTPUT_DIR    = "pbi_tables"         # carpeta donde se guardan los CSVs

os.makedirs(OUTPUT_DIR, exist_ok=True)

def guardar(df_out, nombre):
    ruta = os.path.join(OUTPUT_DIR, f"{nombre}.csv")
    df_out.to_csv(ruta, index=False, encoding="utf-8-sig")
    print(f"  ✅ {nombre}.csv  —  {len(df_out):,} filas")

# ═══════════════════════════════════════════════════════════════════════════════
#  CARGA
# ═══════════════════════════════════════════════════════════════════════════════
print("─" * 60)
print(f"Leyendo {PARQUET_INPUT}...")
df = pd.read_parquet(PARQUET_INPUT)
df["DATETIME_TRX"] = pd.to_datetime(df["DATETIME_TRX"])
print(f"  {len(df):,} filas  |  {df.shape[1]} columnas")
print("\nGenerando tablas Power BI...\n")


# ═══════════════════════════════════════════════════════════════════════════════
#  tab_kpi  —  una fila con todos los KPIs ejecutivos
# ═══════════════════════════════════════════════════════════════════════════════
kpi = {
    "TOTAL_FRAUDES"          : len(df),
    "MONTO_TOTAL_LOCAL"      : df[C["monto"]].sum().round(2),
    "TARJETAS_AFECTADAS"     : df[C["tarjeta"]].nunique(),
    "COMERCIOS_AFECTADOS"    : df[C["comercio_id"]].nunique(),
    "MCC_AFECTADOS"          : df[C["mcc"]].nunique(),
    "MONTO_PROMEDIO"         : df[C["monto"]].mean().round(2),
    "MONTO_MEDIANA"          : df[C["monto"]].median().round(2),
    "PCT_TARJETAS_REINCID"   : round(df["FLAG_TARJETA_REINCIDENTE"].mean() * 100, 1),
    "PCT_RAFAGA_DIA"         : round(df["FLAG_RAFAGA_DIA"].mean() * 100, 1),
    "PCT_MADRUGADA"          : round(df["ES_MADRUGADA"].mean() * 100, 1),
    "PCT_FIN_SEMANA"         : round(df["ES_FIN_SEMANA"].mean() * 100, 1),
    "PCT_MONTO_REDONDO"      : round(df["FLAG_MONTO_REDONDO"].mean() * 100, 1),
    "DIAS_CIERRE_PROMEDIO"   : round(df["DIAS_PARA_CIERRE"].mean(), 1) if "DIAS_PARA_CIERRE" in df.columns else None,
}
if C["monto_dolar"] in df.columns:
    kpi["MONTO_TOTAL_USD"] = df[C["monto_dolar"]].sum().round(2)

guardar(pd.DataFrame([kpi]), "tab_kpi")


# ═══════════════════════════════════════════════════════════════════════════════
#  tab_tendencia  —  fraudes y monto por semana / mes
# ═══════════════════════════════════════════════════════════════════════════════
tend = (
    df.assign(
        PERIODO_MES     = df["DATETIME_TRX"].dt.to_period("M").astype(str),
        PERIODO_SEMANA  = df["DATETIME_TRX"].dt.to_period("W").astype(str),
    )
    .groupby(["PERIODO_MES", "PERIODO_SEMANA", "MES_NOM", "ANIO"])
    .agg(
        N_FRAUDES        = (C["monto"], "count"),
        MONTO_TOTAL      = (C["monto"], "sum"),
        TARJETAS_UNICAS  = (C["tarjeta"], "nunique"),
        COMERCIOS_UNICOS = (C["comercio_id"], "nunique"),
    )
    .reset_index()
    .sort_values("PERIODO_SEMANA")
)
tend["MONTO_TOTAL"] = tend["MONTO_TOTAL"].round(2)
guardar(tend, "tab_tendencia")


# ═══════════════════════════════════════════════════════════════════════════════
#  tab_temporal  —  mapa de calor hora × día semana
# ═══════════════════════════════════════════════════════════════════════════════
heatmap = (
    df.groupby(["HORA_DIA", "DIA_SEMANA", "DIA_SEMANA_NOM"])
    .agg(
        N_FRAUDES   = (C["monto"], "count"),
        MONTO_TOTAL = (C["monto"], "sum"),
    )
    .reset_index()
)
heatmap["MONTO_TOTAL"] = heatmap["MONTO_TOTAL"].round(2)
heatmap["ETIQUETA_DIA"] = heatmap["DIA_SEMANA"].map({
    0:"1-Lun", 1:"2-Mar", 2:"3-Mié", 3:"4-Jue",
    4:"5-Vie", 5:"6-Sáb", 6:"7-Dom"
})
guardar(heatmap, "tab_temporal")


# ═══════════════════════════════════════════════════════════════════════════════
#  tab_franja  —  fraudes por franja horaria, quincena, mes
# ═══════════════════════════════════════════════════════════════════════════════
franja = (
    df.groupby(["FRANJA_HORARIA", "QUINCENA", "MES_NOM", "ANIO", C["canal"]])
    .agg(
        N_FRAUDES   = (C["monto"], "count"),
        MONTO_TOTAL = (C["monto"], "sum"),
    )
    .reset_index()
    .rename(columns={C["canal"]: "CANAL"})
)
franja["MONTO_TOTAL"] = franja["MONTO_TOTAL"].round(2)
guardar(franja, "tab_franja")


# ═══════════════════════════════════════════════════════════════════════════════
#  tab_comercios  —  ranking de comercios
# ═══════════════════════════════════════════════════════════════════════════════
col_nom = C["comercio_nom"] if C["comercio_nom"] in df.columns else C["comercio_id"]

com = (
    df.groupby([C["comercio_id"], col_nom, C["mcc"]])
    .agg(
        N_FRAUDES           = (C["monto"], "count"),
        MONTO_TOTAL         = (C["monto"], "sum"),
        MONTO_PROMEDIO      = (C["monto"], "mean"),
        TARJETAS_DISTINTAS  = (C["tarjeta"], "nunique"),
        DIAS_CON_FRAUDE     = ("FECHA_DIA", "nunique"),
    )
    .reset_index()
    .sort_values("N_FRAUDES", ascending=False)
    .reset_index(drop=True)
)
com["RANKING"]      = com.index + 1
com["MONTO_TOTAL"]  = com["MONTO_TOTAL"].round(2)
com["MONTO_PROMEDIO"] = com["MONTO_PROMEDIO"].round(2)
com.rename(columns={C["comercio_id"]: "COMERCIO_ID", col_nom: "COMERCIO_NOMBRE", C["mcc"]: "MCC"}, inplace=True)
guardar(com, "tab_comercios")


# ═══════════════════════════════════════════════════════════════════════════════
#  tab_mcc  —  ranking de MCC
# ═══════════════════════════════════════════════════════════════════════════════
mcc_df = (
    df.groupby(C["mcc"])
    .agg(
        N_FRAUDES          = (C["monto"], "count"),
        MONTO_TOTAL        = (C["monto"], "sum"),
        COMERCIOS_DISTINTOS = (C["comercio_id"], "nunique"),
        TARJETAS_DISTINTAS  = (C["tarjeta"], "nunique"),
    )
    .reset_index()
    .sort_values("N_FRAUDES", ascending=False)
    .reset_index(drop=True)
)
mcc_df["RANKING"]     = mcc_df.index + 1
mcc_df["MONTO_TOTAL"] = mcc_df["MONTO_TOTAL"].round(2)
mcc_df.rename(columns={C["mcc"]: "MCC"}, inplace=True)
guardar(mcc_df, "tab_mcc")


# ═══════════════════════════════════════════════════════════════════════════════
#  tab_tarjetas  —  perfil de tarjetas afectadas
# ═══════════════════════════════════════════════════════════════════════════════
cols_tarjeta = [C["tipo_tarjeta"], C["nivel_tarjeta"], C["segmento"]]
cols_tarjeta = [c for c in cols_tarjeta if c in df.columns]

tarj = (
    df.groupby(cols_tarjeta + ["PERFIL_RIESGO"])
    .agg(
        N_FRAUDES          = (C["monto"], "count"),
        MONTO_TOTAL        = (C["monto"], "sum"),
        TARJETAS_DISTINTAS = (C["tarjeta"], "nunique"),
        PCT_REINCIDENTE    = ("FLAG_TARJETA_REINCIDENTE", "mean"),
        PCT_RAFAGA         = ("FLAG_RAFAGA_DIA", "mean"),
        PCT_MULTI_COMERCIO = ("FLAG_MULTI_COMERCIO_DIA", "mean"),
    )
    .reset_index()
    .sort_values("N_FRAUDES", ascending=False)
)
tarj["MONTO_TOTAL"]        = tarj["MONTO_TOTAL"].round(2)
tarj["PCT_REINCIDENTE"]    = (tarj["PCT_REINCIDENTE"] * 100).round(1)
tarj["PCT_RAFAGA"]         = (tarj["PCT_RAFAGA"] * 100).round(1)
tarj["PCT_MULTI_COMERCIO"] = (tarj["PCT_MULTI_COMERCIO"] * 100).round(1)
rename_map = {C["tipo_tarjeta"]: "TIPO_TARJETA", C["nivel_tarjeta"]: "NIVEL_TARJETA", C["segmento"]: "SEGMENTO"}
tarj.rename(columns={k: v for k, v in rename_map.items() if k in tarj.columns}, inplace=True)
guardar(tarj, "tab_tarjetas")


# ═══════════════════════════════════════════════════════════════════════════════
#  tab_modalidad  —  distribución por modalidad de fraude
# ═══════════════════════════════════════════════════════════════════════════════
if C["modalidad_fraude"] in df.columns:
    modal = (
        df.groupby([C["modalidad_fraude"], C["canal"]] +
                   ([C["organizacion"]] if C["organizacion"] in df.columns else []))
        .agg(
            N_FRAUDES       = (C["monto"], "count"),
            MONTO_TOTAL     = (C["monto"], "sum"),
            MONTO_PROMEDIO  = (C["monto"], "mean"),
        )
        .reset_index()
        .sort_values("N_FRAUDES", ascending=False)
    )
    modal["MONTO_TOTAL"]    = modal["MONTO_TOTAL"].round(2)
    modal["MONTO_PROMEDIO"] = modal["MONTO_PROMEDIO"].round(2)
    rename_modal = {C["modalidad_fraude"]: "MODALIDAD", C["canal"]: "CANAL", C["organizacion"]: "ORGANIZACION"}
    modal.rename(columns={k: v for k, v in rename_modal.items() if k in modal.columns}, inplace=True)
    guardar(modal, "tab_modalidad")
else:
    print("  — tab_modalidad omitida (columna no disponible)")


# ═══════════════════════════════════════════════════════════════════════════════
#  tab_canal_org  —  fraudes por canal y organización
# ═══════════════════════════════════════════════════════════════════════════════
group_cols = [C["canal"]]
if C["organizacion"] in df.columns:
    group_cols.append(C["organizacion"])

canal_org = (
    df.groupby(group_cols + ["MES_NOM", "ANIO"])
    .agg(
        N_FRAUDES       = (C["monto"], "count"),
        MONTO_TOTAL     = (C["monto"], "sum"),
        MONTO_PROMEDIO  = (C["monto"], "mean"),
    )
    .reset_index()
    .sort_values(["ANIO", "N_FRAUDES"], ascending=[True, False])
)
canal_org["MONTO_TOTAL"]    = canal_org["MONTO_TOTAL"].round(2)
canal_org["MONTO_PROMEDIO"] = canal_org["MONTO_PROMEDIO"].round(2)
rename_co = {C["canal"]: "CANAL", C["organizacion"]: "ORGANIZACION"}
canal_org.rename(columns={k: v for k, v in rename_co.items() if k in canal_org.columns}, inplace=True)
guardar(canal_org, "tab_canal_org")


# ═══════════════════════════════════════════════════════════════════════════════
#  tab_cierre  —  tiempos de gestión / cierre de casos
# ═══════════════════════════════════════════════════════════════════════════════
if "DIAS_PARA_CIERRE" in df.columns:
    cierre_cols = ["RANGO_DIAS_CIERRE", "MES_NOM", "ANIO"]
    if C["organizacion"] in df.columns:
        cierre_cols.insert(1, C["organizacion"])

    cierre = (
        df.groupby(cierre_cols)
        .agg(
            N_CASOS          = (C["monto"], "count"),
            MONTO_TOTAL      = (C["monto"], "sum"),
            DIAS_PROM        = ("DIAS_PARA_CIERRE", "mean"),
            DIAS_MAX         = ("DIAS_PARA_CIERRE", "max"),
        )
        .reset_index()
    )
    cierre["MONTO_TOTAL"] = cierre["MONTO_TOTAL"].round(2)
    cierre["DIAS_PROM"]   = cierre["DIAS_PROM"].round(1)
    if C["organizacion"] in cierre.columns:
        cierre.rename(columns={C["organizacion"]: "ORGANIZACION"}, inplace=True)
    guardar(cierre, "tab_cierre")
else:
    print("  — tab_cierre omitida (DIAS_PARA_CIERRE no disponible)")


# ═══════════════════════════════════════════════════════════════════════════════
#  tab_detalle  —  tabla completa para drillthrough en Power BI
# ═══════════════════════════════════════════════════════════════════════════════
cols_detalle = [
    "DATETIME_TRX", C["tarjeta"], C["bin"], C["comercio_id"],
    C["mcc"], C["monto"],
]
# Agregar columnas opcionales si existen
for col_key in ["monto_dolar", "canal", "tipo_tarjeta", "nivel_tarjeta",
                "segmento", "organizacion", "modalidad_fraude",
                "cvv_dinamico", "saldo_disponible"]:
    col_val = C.get(col_key)
    if col_val and col_val in df.columns:
        cols_detalle.append(col_val)

cols_detalle += [
    "HORA_DIA", "FRANJA_HORARIA", "DIA_SEMANA_NOM", "ES_FIN_SEMANA",
    "ES_MADRUGADA", "MES_NOM", "ANIO", "QUINCENA",
    "TOTAL_FRAUDES_TARJETA", "FRAUDES_TRJ_DIA", "COMERCIOS_DISTINTOS_DIA",
    "FLAG_TARJETA_REINCIDENTE", "FLAG_RAFAGA_DIA", "FLAG_MULTI_COMERCIO_DIA",
    "RANKING_COMERCIO", "RANKING_MCC",
    "FLAG_MONTO_REDONDO", "RANGO_MONTO",
    "SCORE_RIESGO_TRJ", "PERFIL_RIESGO", "FLAG_HORARIO_RIESGO",
    "DIAS_PARA_CIERRE", "RANGO_DIAS_CIERRE",
]
cols_detalle = [c for c in cols_detalle if c in df.columns]
cols_detalle = list(dict.fromkeys(cols_detalle))   # quitar duplicados

detalle = df[cols_detalle].copy()
# Redondear montos
for mc in [C["monto"], C.get("monto_dolar", "")]:
    if mc and mc in detalle.columns:
        detalle[mc] = detalle[mc].round(2)

guardar(detalle, "tab_detalle")


# ═══════════════════════════════════════════════════════════════════════════════
#  dim_calendario  —  dimensión de fechas para el modelo estrella
# ═══════════════════════════════════════════════════════════════════════════════
fecha_min = df["DATETIME_TRX"].dt.date.min()
fecha_max = df["DATETIME_TRX"].dt.date.max()
rango = pd.date_range(start=fecha_min, end=fecha_max, freq="D")

cal = pd.DataFrame({"FECHA": rango})
cal["ANIO"]          = cal["FECHA"].dt.year
cal["MES"]           = cal["FECHA"].dt.month
cal["MES_NOM"]       = cal["FECHA"].dt.strftime("%b").str.upper()
cal["DIA"]           = cal["FECHA"].dt.day
cal["DIA_SEMANA"]    = cal["FECHA"].dt.dayofweek
cal["DIA_SEM_NOM"]   = cal["FECHA"].dt.strftime("%a").str.upper()
cal["SEMANA_ISO"]    = cal["FECHA"].dt.isocalendar().week.astype(int)
cal["QUINCENA"]      = np.where(cal["DIA"] <= 15, "Q1", "Q2")
cal["ES_FIN_SEMANA"] = (cal["DIA_SEMANA"] >= 5).astype(int)
cal["PERIODO_MES"]   = cal["FECHA"].dt.to_period("M").astype(str)
cal["FECHA"]         = cal["FECHA"].dt.date.astype(str)
guardar(cal, "dim_calendario")


# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "─" * 60)
print(f"✅ Todas las tablas guardadas en: ./{OUTPUT_DIR}/")
print("\nSiguiente paso en Power BI:")
print("  1. Abrir Power BI Desktop")
print("  2. Inicio → Obtener datos → Texto/CSV")
print("  3. Importar cada CSV de la carpeta pbi_tables/")
print("  4. En el modelo: relacionar dim_calendario.FECHA con")
print("     tab_tendencia.PERIODO_MES y tab_detalle.DATETIME_TRX")
print("─" * 60)
