# =============================================================================
# ANÁLISIS MCC 7995 — Juegos de Azar / Casas de Apuestas
# Objetivo: Evaluar si la regla 088 está calibrada correctamente
#           o está declinando transacciones legítimas en este segmento
# Fuente: Export del Monitor (journal filtrado por MCC = 7995)
# =============================================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from pathlib import Path

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
# Ajusta estos nombres de columna según el export real del Monitor
COL_FECHA       = "FECHA_TXN"       # Fecha de la transacción
COL_COMERCIO    = "COMERCIO"        # Nombre del comercio
COL_MONTO       = "MONTO"           # Monto en soles
COL_INDICADOR   = "INDICADOR"       # F / N / G / D / P
COL_CONDICION   = "CONDICION"       # Regla que disparó (ej: "088", "057")
COL_ESTADO      = "ESTADO"          # "APROBADA" / "DECLINADA"  (o derivar de condición)
COL_CANAL       = "CANAL"           # POS / ATM / E-COMMERCE
COL_BIN         = "BIN"             # BIN de la tarjeta (string)
COL_ENTRY_MODE  = "ENTRY_MODE"      # Entry mode (string)
COL_MCC         = "MCC"             # Para confirmar filtro

REGLA_FOCO      = "088"             # Regla que estamos evaluando
RUTA_ARCHIVO    = r"C:\ruta\al\export_monitor_7995.xlsx"  # ← AJUSTAR

# Paleta institucional
COLOR_FRAUDE    = "#D9534F"
COLOR_OK        = "#5B9BD5"
COLOR_DECLINA   = "#F0AD4E"
COLOR_NEUTRO    = "#8EA9C1"

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor":   "white",
    "axes.spines.top":  False,
    "axes.spines.right":False,
    "font.family":      "Segoe UI",
    "axes.titlesize":   12,
    "axes.labelsize":   10,
})

# ─── CARGA DE DATOS ────────────────────────────────────────────────────────────
print("Cargando datos...")
df_raw = pd.read_excel(RUTA_ARCHIVO, dtype={COL_BIN: str, COL_ENTRY_MODE: str, COL_MCC: str})

# Normalizar
df = (
    df_raw
    .copy()
    .assign(**{
        COL_FECHA:     lambda x: pd.to_datetime(x[COL_FECHA], errors="coerce"),
        COL_MONTO:     lambda x: pd.to_numeric(x[COL_MONTO], errors="coerce"),
        COL_CONDICION: lambda x: x[COL_CONDICION].astype(str).str.strip().str.upper(),
        COL_INDICADOR: lambda x: x[COL_INDICADOR].astype(str).str.strip().str.upper(),
        COL_ESTADO:    lambda x: x[COL_ESTADO].astype(str).str.strip().str.upper(),
        COL_COMERCIO:  lambda x: x[COL_COMERCIO].astype(str).str.strip().str.upper(),
    })
)

print(f"  Registros cargados: {len(df):,}")
print(f"  Rango fechas: {df[COL_FECHA].min().date()} → {df[COL_FECHA].max().date()}")
print(f"  MCC únicos presentes: {df[COL_MCC].unique()}")

# ─── SEGMENTACIONES BASE ──────────────────────────────────────────────────────
mask_declinadas  = df[COL_ESTADO]   == "DECLINADA"
mask_aprobadas   = df[COL_ESTADO]   == "APROBADA"
mask_regla088    = df[COL_CONDICION] == REGLA_FOCO
mask_fraude      = df[COL_INDICADOR] == "F"
mask_labeled     = df[COL_INDICADOR].isin(["F", "G"])  # para fraud rate limpio

df_aprobadas     = df[mask_aprobadas]
df_declinadas    = df[mask_declinadas]
df_088           = df[mask_declinadas & mask_regla088]

# =============================================================================
# SECCIÓN 1 — UNIVERSO MCC 7995
# =============================================================================
print("\n" + "="*60)
print("SECCIÓN 1 — UNIVERSO GENERAL MCC 7995")
print("="*60)

resumen_universo = pd.DataFrame({
    "Métrica": [
        "Total transacciones",
        "Aprobadas",
        "Declinadas (todas las reglas)",
        "Declinadas regla 088",
        "Tasa de declinación global (%)",
        "Tasa de declinación regla 088 (% sobre total)",
    ],
    "Valor": [
        f"{len(df):,}",
        f"{mask_aprobadas.sum():,}",
        f"{mask_declinadas.sum():,}",
        f"{(mask_declinadas & mask_regla088).sum():,}",
        f"{mask_declinadas.mean()*100:.1f}%",
        f"{(mask_declinadas & mask_regla088).mean()*100:.1f}%",
    ]
})
print(resumen_universo.to_string(index=False))

# Top comercios por declinaciones
print("\nTop 10 comercios por volumen de declinaciones:")
top_comercios_dec = (
    df_declinadas
    .groupby(COL_COMERCIO)
    .agg(
        Declinaciones=(COL_MONTO, "count"),
        Monto_declinado=(COL_MONTO, "sum"),
        Pct_por_088=(COL_CONDICION, lambda x: (x == REGLA_FOCO).mean() * 100)
    )
    .sort_values("Declinaciones", ascending=False)
    .head(10)
    .assign(Monto_declinado=lambda x: x["Monto_declinado"].map("S/ {:,.0f}".format))
)
print(top_comercios_dec.to_string())

# Evolución mensual
print("\nEvolución mensual — Aprobadas vs Declinadas:")
evolucion = (
    df
    .assign(Mes=lambda x: x[COL_FECHA].dt.to_period("M").astype(str))
    .groupby(["Mes", COL_ESTADO])
    .size()
    .unstack(fill_value=0)
    .assign(Tasa_dec=lambda x: x.get("DECLINADA", 0) / (x.get("APROBADA", 0) + x.get("DECLINADA", 0)) * 100)
)
print(evolucion.to_string())

# ── Gráfico 1: Evolución mensual ─────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("MCC 7995 — Evolución mensual de transacciones", fontsize=13, fontweight="bold")

meses = evolucion.index.tolist()
x = np.arange(len(meses))
w = 0.35

ax = axes[0]
ax.bar(x - w/2, evolucion.get("APROBADA", 0), width=w, color=COLOR_OK,    label="Aprobadas")
ax.bar(x + w/2, evolucion.get("DECLINADA", 0), width=w, color=COLOR_DECLINA, label="Declinadas")
ax.set_xticks(x); ax.set_xticklabels(meses, rotation=30)
ax.set_title("Volumen por estado"); ax.legend()
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))

ax2 = axes[1]
ax2.plot(meses, evolucion["Tasa_dec"], marker="o", color=COLOR_FRAUDE, linewidth=2)
ax2.set_title("Tasa de declinación mensual (%)")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.1f}%"))
ax2.set_xticks(range(len(meses))); ax2.set_xticklabels(meses, rotation=30)

plt.tight_layout()
plt.savefig("g1_evolucion_mensual.png", dpi=150, bbox_inches="tight")
plt.show()
print("  → Guardado: g1_evolucion_mensual.png")

# =============================================================================
# SECCIÓN 2 — FRAUDE REAL EN APROBADAS
# =============================================================================
print("\n" + "="*60)
print("SECCIÓN 2 — FRAUDE EN TRANSACCIONES APROBADAS")
print("="*60)

df_aprobadas_labeled = df_aprobadas[df_aprobadas[COL_INDICADOR].isin(["F", "G"])]
n_fraudes_aprobadas  = (df_aprobadas[COL_INDICADOR] == "F").sum()
monto_fraude         = df_aprobadas.loc[df_aprobadas[COL_INDICADOR] == "F", COL_MONTO].sum()
fraud_rate           = n_fraudes_aprobadas / len(df_aprobadas_labeled) if len(df_aprobadas_labeled) > 0 else 0
severidad            = monto_fraude / n_fraudes_aprobadas if n_fraudes_aprobadas > 0 else 0

print(f"  Aprobadas totales:           {len(df_aprobadas):,}")
print(f"  Con indicador F (fraude):    {n_fraudes_aprobadas:,}")
print(f"  Monto total fraudulento:     S/ {monto_fraude:,.2f}")
print(f"  Fraud Rate (F / F+G):        {fraud_rate*100:.2f}%")
print(f"  Severidad (monto prom. fraude): S/ {severidad:,.2f}")

# Fraude por comercio
print("\nFraude por comercio (en aprobadas):")
fraude_comercio = (
    df_aprobadas
    .assign(es_fraude=lambda x: (x[COL_INDICADOR] == "F").astype(int))
    .groupby(COL_COMERCIO)
    .agg(
        Txns        =(COL_MONTO,    "count"),
        Fraudes     =("es_fraude",  "sum"),
        Monto_fraude=(COL_MONTO,    lambda x: x[df_aprobadas.loc[x.index, COL_INDICADOR] == "F"].sum()),
    )
    .assign(Fraud_Rate=lambda x: x["Fraudes"] / x["Txns"].clip(lower=1) * 100)
    .sort_values("Fraudes", ascending=False)
    .head(10)
)
print(fraude_comercio.to_string())

# Distribución de montos — fraude vs no fraude
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("MCC 7995 — Perfil de monto en aprobadas", fontsize=13, fontweight="bold")

montos_fraude   = df_aprobadas.loc[df_aprobadas[COL_INDICADOR] == "F",  COL_MONTO].dropna()
montos_normales = df_aprobadas.loc[df_aprobadas[COL_INDICADOR] != "F",  COL_MONTO].dropna()

bins = np.percentile(df_aprobadas[COL_MONTO].dropna(), np.linspace(0, 99, 30))

ax = axes[0]
ax.hist(montos_normales.clip(upper=np.percentile(montos_normales, 99)), bins=bins,
        alpha=0.6, color=COLOR_OK,     label="No fraude", density=True)
ax.hist(montos_fraude.clip(upper=np.percentile(montos_fraude, 99)),    bins=bins,
        alpha=0.7, color=COLOR_FRAUDE, label="Fraude",    density=True)
ax.set_title("Distribución de montos (densidad)"); ax.legend()
ax.set_xlabel("Monto (S/)")

ax2 = axes[1]
percentiles = [10, 25, 50, 75, 90, 95]
p_fraude    = np.percentile(montos_fraude,   percentiles) if len(montos_fraude)   > 0 else [0]*6
p_normal    = np.percentile(montos_normales, percentiles) if len(montos_normales) > 0 else [0]*6
x_p = np.arange(len(percentiles))
ax2.bar(x_p - 0.2, p_normal, 0.35, color=COLOR_OK,     label="No fraude")
ax2.bar(x_p + 0.2, p_fraude, 0.35, color=COLOR_FRAUDE, label="Fraude")
ax2.set_xticks(x_p); ax2.set_xticklabels([f"P{p}" for p in percentiles])
ax2.set_title("Percentiles de monto: fraude vs legítimo")
ax2.legend(); ax2.set_ylabel("S/")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"S/ {v:,.0f}"))

plt.tight_layout()
plt.savefig("g2_montos_fraude_vs_normal.png", dpi=150, bbox_inches="tight")
plt.show()
print("  → Guardado: g2_montos_fraude_vs_normal.png")

# =============================================================================
# SECCIÓN 3 — PERFIL DE DECLINACIONES REGLA 088
# =============================================================================
print("\n" + "="*60)
print("SECCIÓN 3 — DECLINACIONES REGLA 088")
print("="*60)

print(f"  Total declinadas por regla 088:  {len(df_088):,}")
print(f"  Monto total bloqueado:           S/ {df_088[COL_MONTO].sum():,.2f}")
print(f"  Monto promedio bloqueado:        S/ {df_088[COL_MONTO].mean():,.2f}")
print(f"  Monto mediano bloqueado:         S/ {df_088[COL_MONTO].median():,.2f}")

print("\nTop comercios declinados por 088:")
print(
    df_088.groupby(COL_COMERCIO)
    .agg(Declinaciones=(COL_MONTO, "count"), Monto_bloqueado=(COL_MONTO, "sum"))
    .sort_values("Declinaciones", ascending=False)
    .head(10)
    .assign(Monto_bloqueado=lambda x: x["Monto_bloqueado"].map("S/ {:,.0f}".format))
    .to_string()
)

if COL_CANAL in df.columns:
    print("\nDeclinaciones 088 por canal:")
    print(df_088[COL_CANAL].value_counts().to_string())

# Perfil de monto: declinadas 088 vs fraudes confirmados
fig, ax = plt.subplots(figsize=(10, 5))
fig.suptitle("MCC 7995 — Perfil de monto: Regla 088 vs Fraude real", fontsize=13, fontweight="bold")

percentiles = [10, 25, 50, 75, 90, 95]
p_088    = np.percentile(df_088[COL_MONTO].dropna(),         percentiles)
p_fraude = np.percentile(montos_fraude,                      percentiles) if len(montos_fraude) > 0 else [0]*6

x_p = np.arange(len(percentiles))
ax.bar(x_p - 0.2, p_fraude, 0.35, color=COLOR_FRAUDE, label="Fraudes confirmados (aprobadas)")
ax.bar(x_p + 0.2, p_088,    0.35, color=COLOR_DECLINA, label="Declinadas por regla 088")
ax.set_xticks(x_p); ax.set_xticklabels([f"P{p}" for p in percentiles])
ax.set_title("¿Se parecen los montos que bloquea la 088 a los montos de fraude real?")
ax.legend(); ax.set_ylabel("S/")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"S/ {v:,.0f}"))

plt.tight_layout()
plt.savefig("g3_perfil_088_vs_fraude.png", dpi=150, bbox_inches="tight")
plt.show()
print("  → Guardado: g3_perfil_088_vs_fraude.png")

# =============================================================================
# SECCIÓN 4 — TABLA CONTRASTE: 088 vs FRAUDE REAL
# =============================================================================
print("\n" + "="*60)
print("SECCIÓN 4 — TABLA CONTRASTE FINAL")
print("="*60)

metricas_fraude = {
    "Segmento":          "Fraudes confirmados (aprobadas)",
    "N":                 n_fraudes_aprobadas,
    "Monto promedio":    f"S/ {montos_fraude.mean():,.2f}" if len(montos_fraude) > 0 else "N/A",
    "Monto mediano":     f"S/ {montos_fraude.median():,.2f}" if len(montos_fraude) > 0 else "N/A",
    "Canal más frecuente": df_aprobadas.loc[df_aprobadas[COL_INDICADOR] == "F", COL_CANAL].mode()[0]
                           if COL_CANAL in df.columns and n_fraudes_aprobadas > 0 else "N/A",
}
metricas_088 = {
    "Segmento":          "Declinadas por regla 088",
    "N":                 len(df_088),
    "Monto promedio":    f"S/ {df_088[COL_MONTO].mean():,.2f}",
    "Monto mediano":     f"S/ {df_088[COL_MONTO].median():,.2f}",
    "Canal más frecuente": df_088[COL_CANAL].mode()[0] if COL_CANAL in df.columns and len(df_088) > 0 else "N/A",
}

contraste = pd.DataFrame([metricas_fraude, metricas_088]).set_index("Segmento")
print(contraste.to_string())

# =============================================================================
# SECCIÓN 5 — PERFIL TÍPICO MCC 7995 (¿cómo es el cliente legítimo?)
# =============================================================================
print("\n" + "="*60)
print("SECCIÓN 5 — PERFIL TÍPICO DE TRANSACCIÓN LEGÍTIMA EN MCC 7995")
print("="*60)

df_legitimas = df_aprobadas[df_aprobadas[COL_INDICADOR].isin(["N", "G"])]

print(f"  Transacciones legítimas:      {len(df_legitimas):,}")
print(f"  Monto promedio:               S/ {df_legitimas[COL_MONTO].mean():,.2f}")
print(f"  Monto mediano:                S/ {df_legitimas[COL_MONTO].median():,.2f}")
print(f"  Monto p25 – p75:              S/ {df_legitimas[COL_MONTO].quantile(0.25):,.2f} – "
      f"S/ {df_legitimas[COL_MONTO].quantile(0.75):,.2f}")

if COL_CANAL in df.columns:
    print("\n  Distribución por canal (legítimas):")
    print(df_legitimas[COL_CANAL].value_counts(normalize=True).mul(100).map("{:.1f}%".format).to_string())

if COL_ENTRY_MODE in df.columns:
    print("\n  Entry mode más frecuente (legítimas):")
    print(df_legitimas[COL_ENTRY_MODE].value_counts().head(5).to_string())

# ── Gráfico final: Resumen de los 3 perfiles ─────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(16, 5))
fig.suptitle("MCC 7995 — Boxplot de montos: 3 perfiles clave", fontsize=13, fontweight="bold")

datos_box = {
    "Legítimas\n(N/G aprobadas)":  df_legitimas[COL_MONTO].dropna().clip(
                                        upper=df_legitimas[COL_MONTO].quantile(0.99)),
    "Fraudes\nconfirmados":         montos_fraude.clip(
                                        upper=montos_fraude.quantile(0.99)) if len(montos_fraude) > 0
                                        else pd.Series(dtype=float),
    "Declinadas\nregla 088":        df_088[COL_MONTO].dropna().clip(
                                        upper=df_088[COL_MONTO].quantile(0.99)),
}
colores_box = [COLOR_OK, COLOR_FRAUDE, COLOR_DECLINA]

for ax, (label, serie), color in zip(axes, datos_box.items(), colores_box):
    ax.boxplot(serie, patch_artist=True,
               boxprops=dict(facecolor=color, alpha=0.6),
               medianprops=dict(color="black", linewidth=2))
    ax.set_title(label)
    ax.set_ylabel("Monto (S/)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"S/ {v:,.0f}"))

plt.tight_layout()
plt.savefig("g4_boxplot_tres_perfiles.png", dpi=150, bbox_inches="tight")
plt.show()
print("  → Guardado: g4_boxplot_tres_perfiles.png")

print("\n✅ Análisis completado. Revisa las 4 imágenes generadas.")
print("   Las preguntas clave a responder con estos resultados:")
print("   1. ¿El fraud rate del segmento justifica el nivel de declinación?")
print("   2. ¿Los montos que bloquea la 088 se parecen al perfil de fraude real?")
print("   3. ¿Existe un perfil legítimo claro que la 088 podría estar afectando?")
