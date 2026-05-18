"""
Feature Engineering — Comercios No Seguros (sin 3DS/TDS)
Dataset: fraudes confirmados (calificación F) extraídos de la 8850.

Bloques:
  A  Parseo de fecha + hora  → DATETIME
  B  Variables temporales    → hora, día, franja, quincena
  C  Velocidad por tarjeta   → conteo/monto de fraudes de esa tarjeta
  D  Perfil del comercio     → cuántos fraudes concentra cada comercio
  E  Señales de monto        → monto redondo, desviación, rango
  F  Combinación de riesgo   → flags compuestos para el dashboard
  G  Guardar parquet enriquecido
"""

import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore")

from config import COLS, PARQUET_INPUT, PARQUET_OUTPUT

# ── Alias cortos para no repetir COLS["..."] en todo el script ────────────────
C = COLS


# ═══════════════════════════════════════════════════════════════════════════════
#  CARGA
# ═══════════════════════════════════════════════════════════════════════════════
print("─" * 60)
print("Cargando parquet...")
df = pd.read_parquet(PARQUET_INPUT)

# Limpiar espacios en columnas de texto
for col in [C["tarjeta"], C["comercio_id"], C["canal"], C["tipo_tarjeta"], C["segmento"]]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.upper()

# Filtro de seguridad: solo fraudes confirmados
if C["calificacion"] in df.columns:
    df = df[df[C["calificacion"]].astype(str).str.upper() == "F"].copy()

print(f"  Filas (fraudes F)  : {len(df):,}")
print(f"  Tarjetas únicas    : {df[C['tarjeta']].nunique():,}")
print(f"  Comercios únicos   : {df[C['comercio_id']].nunique():,}")
print(f"  Monto total        : {df[C['monto']].sum():,.2f}")


# ═══════════════════════════════════════════════════════════════════════════════
#  BLOQUE A — Parseo de FECHA (YYYYMMDD) + HORA (HH:MM:SS)  →  DATETIME
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[A] Construcción de DATETIME...")

fecha_str = df[C["fecha"]].astype(str).str.zfill(8)   # garantiza 8 dígitos
hora_str  = df[C["hora"]].astype(str).str.strip()

df["DATETIME"] = pd.to_datetime(
    fecha_str + " " + hora_str,
    format="%Y%m%d %H:%M:%S",
    errors="coerce"
)

nulos_dt = df["DATETIME"].isna().sum()
if nulos_dt > 0:
    print(f"  ⚠️  {nulos_dt:,} filas con DATETIME no parseado — revisa el formato de fecha/hora")
else:
    print("  DATETIME OK ✅")

df = df.sort_values(["DATETIME"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  BLOQUE B — Variables temporales
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[B] Variables temporales...")

df["HORA_DIA"]     = df["DATETIME"].dt.hour
df["DIA_SEMANA"]   = df["DATETIME"].dt.dayofweek          # 0=Lunes … 6=Domingo
df["DIA_SEMANA_NOM"] = df["DATETIME"].dt.day_name(locale=None).str[:3].str.upper()
df["MES"]          = df["DATETIME"].dt.month
df["MES_NOM"]      = df["DATETIME"].dt.strftime("%b").str.upper()
df["FECHA_DIA"]    = df["DATETIME"].dt.date               # para agrupaciones diarias
df["SEMANA_ISO"]   = df["DATETIME"].dt.isocalendar().week.astype(int)

# Flags de día
df["ES_FIN_SEMANA"] = (df["DIA_SEMANA"] >= 5).astype(int)  # Sab=5, Dom=6

# Franja horaria
def franja(h):
    if   0  <= h <  6: return "MADRUGADA"   # 00-05
    elif 6  <= h < 12: return "MAÑANA"      # 06-11
    elif 12 <= h < 19: return "TARDE"       # 12-18
    else:              return "NOCHE"       # 19-23

df["FRANJA_HORARIA"] = df["HORA_DIA"].map(franja)
df["ES_MADRUGADA"]   = (df["FRANJA_HORARIA"] == "MADRUGADA").astype(int)
df["ES_HORARIO_LAB"] = (
    (df["DIA_SEMANA"] < 5) & df["HORA_DIA"].between(8, 17)
).astype(int)

# Quincena del mes
df["QUINCENA"] = np.where(df["DATETIME"].dt.day <= 15, "Q1", "Q2")

print("  Variables temporales creadas ✅")
print(f"    Distribución por franja:\n{df['FRANJA_HORARIA'].value_counts().to_string()}")


# ═══════════════════════════════════════════════════════════════════════════════
#  BLOQUE C — Velocidad / comportamiento de la TARJETA en el dataset de fraudes
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[C] Velocidad por tarjeta...")

# ── C1: Totales por tarjeta en todo el dataset ────────────────────────────────
totales_tarjeta = (
    df.groupby(C["tarjeta"])
    .agg(
        TOTAL_FRAUDES_TARJETA    = (C["monto"], "count"),
        MONTO_TOTAL_FRAUDE_TRJ   = (C["monto"], "sum"),
        COMERCIOS_DISTINTOS_TRJ  = (C["comercio_id"], "nunique"),
        CANALES_DISTINTOS_TRJ    = (C["canal"], "nunique"),
    )
    .reset_index()
)
df = df.merge(totales_tarjeta, on=C["tarjeta"], how="left")

# ── C2: Fraudes de la tarjeta en el MISMO DÍA ────────────────────────────────
fraudes_dia_trj = (
    df.groupby([C["tarjeta"], "FECHA_DIA"])
    .agg(
        FRAUDES_TRJ_DIA         = (C["monto"], "count"),
        MONTO_FRAUDE_TRJ_DIA    = (C["monto"], "sum"),
        COMERCIOS_DISTINTOS_DIA = (C["comercio_id"], "nunique"),
    )
    .reset_index()
)
df = df.merge(fraudes_dia_trj, on=[C["tarjeta"], "FECHA_DIA"], how="left")

# Flags de velocidad
df["FLAG_TARJETA_REINCIDENTE"] = (df["TOTAL_FRAUDES_TARJETA"] > 1).astype(int)
df["FLAG_MULTI_COMERCIO_DIA"]  = (df["COMERCIOS_DISTINTOS_DIA"] > 1).astype(int)
df["FLAG_RAFAGA_DIA"]          = (df["FRAUDES_TRJ_DIA"] >= 3).astype(int)  # 3+ fraudes en un día

print("  Variables por tarjeta creadas ✅")
print(f"    Tarjetas reincidentes    : {df.loc[df['FLAG_TARJETA_REINCIDENTE']==1, C['tarjeta']].nunique():,}")
print(f"    Ráfagas (≥3 fraudes/día) : {df['FLAG_RAFAGA_DIA'].sum():,} transacciones")


# ═══════════════════════════════════════════════════════════════════════════════
#  BLOQUE D — Perfil del COMERCIO
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[D] Perfil del comercio...")

totales_comercio = (
    df.groupby(C["comercio_id"])
    .agg(
        TOTAL_FRAUDES_COMERCIO   = (C["monto"], "count"),
        MONTO_TOTAL_FRAUDE_COM   = (C["monto"], "sum"),
        MONTO_PROM_FRAUDE_COM    = (C["monto"], "mean"),
        TARJETAS_DISTINTAS_COM   = (C["tarjeta"], "nunique"),
        CANALES_DISTINTOS_COM    = (C["canal"], "nunique"),
    )
    .reset_index()
)
df = df.merge(totales_comercio, on=C["comercio_id"], how="left")

# Fraudes por comercio por día
fraudes_dia_com = (
    df.groupby([C["comercio_id"], "FECHA_DIA"])
    .agg(FRAUDES_COM_DIA = (C["monto"], "count"))
    .reset_index()
)
df = df.merge(fraudes_dia_com, on=[C["comercio_id"], "FECHA_DIA"], how="left")

# Ranking de comercios más golpeados
rank_com = (
    totales_comercio
    .sort_values("TOTAL_FRAUDES_COMERCIO", ascending=False)
    .reset_index(drop=True)
)
rank_com["RANKING_COMERCIO"] = rank_com.index + 1
df = df.merge(rank_com[[C["comercio_id"], "RANKING_COMERCIO"]], on=C["comercio_id"], how="left")

print("  Variables por comercio creadas ✅")
print(f"    Top 5 comercios más golpeados:")
print(rank_com[[C["comercio_id"], "TOTAL_FRAUDES_COMERCIO", "MONTO_TOTAL_FRAUDE_COM"]].head(5).to_string(index=False))


# ═══════════════════════════════════════════════════════════════════════════════
#  BLOQUE E — Señales de MONTO
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[E] Señales de monto...")

# Monto redondo: sin decimales o terminado en .00
df["FLAG_MONTO_REDONDO"] = (df[C["monto"]] % 1 == 0).astype(int)

# Desviación del monto respecto al promedio del comercio
df["DESVIO_MONTO_VS_COM"] = df[C["monto"]] - df["MONTO_PROM_FRAUDE_COM"]
df["RATIO_MONTO_VS_COM"]  = (df[C["monto"]] / df["MONTO_PROM_FRAUDE_COM"].replace(0, np.nan)).round(2)

# Rango de monto (cuartiles del dataset)
q25, q50, q75 = df[C["monto"]].quantile([0.25, 0.50, 0.75])

def rango_monto(m):
    if   m <= q25: return "BAJO"
    elif m <= q50: return "MEDIO_BAJO"
    elif m <= q75: return "MEDIO_ALTO"
    else:          return "ALTO"

df["RANGO_MONTO"] = df[C["monto"]].map(rango_monto)

print(f"  Cuartiles de monto: Q25={q25:.2f} | Q50={q50:.2f} | Q75={q75:.2f}")
print(f"  Montos redondos    : {df['FLAG_MONTO_REDONDO'].sum():,} ({df['FLAG_MONTO_REDONDO'].mean()*100:.1f}%)")


# ═══════════════════════════════════════════════════════════════════════════════
#  BLOQUE F — Flags compuestos de RIESGO (para slicers en Power BI)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[F] Flags compuestos de riesgo...")

# Perfil de riesgo de la tarjeta (cuántas señales activa)
df["SCORE_RIESGO_TRJ"] = (
    df["FLAG_TARJETA_REINCIDENTE"] +
    df["FLAG_MULTI_COMERCIO_DIA"]  +
    df["FLAG_RAFAGA_DIA"]          +
    df["FLAG_MONTO_REDONDO"]       +
    df["ES_MADRUGADA"]
)

df["PERFIL_RIESGO"] = pd.cut(
    df["SCORE_RIESGO_TRJ"],
    bins=[-1, 0, 1, 2, 99],
    labels=["BAJO", "MEDIO", "ALTO", "MUY_ALTO"]
)

# Flag madrugada + fin de semana (patrón frecuente en fraude CNP)
df["FLAG_HORARIO_RIESGO"] = (
    (df["ES_MADRUGADA"] == 1) | (df["ES_FIN_SEMANA"] == 1)
).astype(int)

# CVV dinámico vs estático (si la columna existe)
if C["cvv_dinamico"] and C["cvv_dinamico"] in df.columns:
    df["FLAG_CVV_ESTATICO"] = (
        df[C["cvv_dinamico"]].astype(str).str.upper().isin(["N", "NO", "0", "FALSE"])
    ).astype(int)
    print(f"  FLAG_CVV_ESTATICO  : {df['FLAG_CVV_ESTATICO'].sum():,} txns con CVV estático")

print(f"  Distribución PERFIL_RIESGO:\n{df['PERFIL_RIESGO'].value_counts().to_string()}")


# ═══════════════════════════════════════════════════════════════════════════════
#  RESUMEN DE VARIABLES CONSTRUIDAS
# ═══════════════════════════════════════════════════════════════════════════════
vars_nuevas = [
    # Bloque A/B — temporales
    "DATETIME", "HORA_DIA", "DIA_SEMANA", "DIA_SEMANA_NOM", "MES", "MES_NOM",
    "FECHA_DIA", "SEMANA_ISO", "ES_FIN_SEMANA", "FRANJA_HORARIA",
    "ES_MADRUGADA", "ES_HORARIO_LAB", "QUINCENA",
    # Bloque C — tarjeta
    "TOTAL_FRAUDES_TARJETA", "MONTO_TOTAL_FRAUDE_TRJ", "COMERCIOS_DISTINTOS_TRJ",
    "CANALES_DISTINTOS_TRJ", "FRAUDES_TRJ_DIA", "MONTO_FRAUDE_TRJ_DIA",
    "COMERCIOS_DISTINTOS_DIA",
    "FLAG_TARJETA_REINCIDENTE", "FLAG_MULTI_COMERCIO_DIA", "FLAG_RAFAGA_DIA",
    # Bloque D — comercio
    "TOTAL_FRAUDES_COMERCIO", "MONTO_TOTAL_FRAUDE_COM", "MONTO_PROM_FRAUDE_COM",
    "TARJETAS_DISTINTAS_COM", "CANALES_DISTINTOS_COM", "FRAUDES_COM_DIA",
    "RANKING_COMERCIO",
    # Bloque E — monto
    "FLAG_MONTO_REDONDO", "DESVIO_MONTO_VS_COM", "RATIO_MONTO_VS_COM", "RANGO_MONTO",
    # Bloque F — riesgo compuesto
    "SCORE_RIESGO_TRJ", "PERFIL_RIESGO", "FLAG_HORARIO_RIESGO",
]

print("\n─" * 30)
print("VARIABLES NUEVAS AGREGADAS:")
for v in vars_nuevas:
    existe = "✅" if v in df.columns else "❌"
    print(f"  {existe} {v}")

print(f"\nColumnas totales en el dataset enriquecido: {df.shape[1]}")


# ═══════════════════════════════════════════════════════════════════════════════
#  BLOQUE G — Guardar parquet enriquecido
# ═══════════════════════════════════════════════════════════════════════════════
print(f"\n[G] Guardando en: {PARQUET_OUTPUT}")
df.to_parquet(PARQUET_OUTPUT, index=False)
print(f"  ✅ Listo — {len(df):,} filas × {df.shape[1]} columnas")
print(f"  Archivo: {PARQUET_OUTPUT}")
print("─" * 60)
