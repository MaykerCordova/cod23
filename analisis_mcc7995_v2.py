# =============================================================================
# ANÁLISIS MCC 7995 — Juegos de Azar / Casas de Apuestas
# Fuente: Parquet consolidado desde Monitor (ya filtrado: MCC=7995, E-commerce, Aprobadas)
# Objetivo: Evaluar comportamiento del fraude y calibración de condición 088
# =============================================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import warnings
warnings.filterwarnings("ignore")

# ─── AJUSTA ESTOS NOMBRES DE COLUMNA SEGÚN TU PARQUET ─────────────────────────
COL_FECHA       = "FECHA_TXN"          # Fecha de transacción
COL_TARJETA     = "NROTARJETA"         # Nro tarjeta / identificador cliente ← AJUSTAR
COL_COMERCIO    = "NOMBRE_COMERCIO"    # Nombre del comercio
COL_MONTO       = "MONTO"             # Monto de la transacción
COL_SALDO       = "MONTO_RESTANTE"    # Saldo restante / monto restante ← AJUSTAR nombre exacto
COL_INDICADOR   = "INDICADOR"         # F / N / G / B / P
COL_CONDICION   = "CONDICION_RT"      # Condición Real Time (088, 0088, 088, etc.) ← AJUSTAR nombre exacto
COL_CANAL       = "CANAL"             # Canal (ya filtrado a E-COMMERCE)
COL_BIN         = "BIN"               # BIN de la tarjeta
COL_ENTRY_MODE  = "ENTRY_MODE"        # Entry mode

RUTA_PARQUET    = r"C:\ruta\al\consolidado_7995.parquet"   # ← AJUSTAR

# Paleta
C_FRAUDE  = "#D9534F"
C_OK      = "#5B9BD5"
C_088     = "#F0AD4E"
C_NEUTRO  = "#8EA9C1"

plt.rcParams.update({
    "figure.facecolor": "white", "axes.facecolor": "white",
    "axes.spines.top": False, "axes.spines.right": False,
    "font.family": "Segoe UI", "axes.titlesize": 11, "axes.labelsize": 9,
})

# ─── CARGA ─────────────────────────────────────────────────────────────────────
print("Cargando parquet...")
df = pd.read_parquet(RUTA_PARQUET)

# Forzar tipos críticos
for col in [COL_BIN, COL_ENTRY_MODE, COL_CONDICION, COL_INDICADOR, COL_CANAL]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.upper()

df[COL_FECHA]  = pd.to_datetime(df[COL_FECHA], errors="coerce")
df[COL_MONTO]  = pd.to_numeric(df[COL_MONTO],  errors="coerce")
df[COL_SALDO]  = pd.to_numeric(df[COL_SALDO],  errors="coerce")

# Columna auxiliar: es_fraude
df["es_fraude"] = (df[COL_INDICADOR] == "F").astype(int)

# Máscara condición 088 (captura 088 / 0088 / "088," / etc.)
mask_088 = df[COL_CONDICION].str.contains("088", na=False)

print(f"  Registros totales:  {len(df):,}")
print(f"  Rango de fechas:    {df[COL_FECHA].min().date()} → {df[COL_FECHA].max().date()}")
print(f"  Con condición 088:  {mask_088.sum():,}  ({mask_088.mean()*100:.1f}% del total)")
print(f"  Fraudes (F):        {df['es_fraude'].sum():,}  ({df['es_fraude'].mean()*100:.2f}%)")
print(f"  Columnas disponibles: {list(df.columns)}")


# =============================================================================
# SECCIÓN 1 — RESUMEN GENERAL DEL UNIVERSO
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 1 — UNIVERSO MCC 7995 (todo aprobado, E-commerce)")
print("="*65)

# Todos son aprobados → el split es: con condición 088 disparada vs sin ella
df_sin088 = df[~mask_088]
df_con088 = df[mask_088]

# Fraud rate limpio solo sobre F y G (excluye N sin etiquetar)
mask_labeled = df[COL_INDICADOR].isin(["F", "G"])
fraud_rate_global = df.loc[mask_labeled, "es_fraude"].mean()

resumen = {
    "Total transacciones":                f"{len(df):,}",
    "Con condición 088 disparada":        f"{len(df_con088):,}  ({len(df_con088)/len(df)*100:.1f}%)",
    "Sin condición 088":                  f"{len(df_sin088):,}  ({len(df_sin088)/len(df)*100:.1f}%)",
    "Fraudes totales (F)":                f"{df['es_fraude'].sum():,}",
    "Fraud Rate (F/F+G)":                 f"{fraud_rate_global*100:.2f}%",
    "Monto total transaccionado":         f"S/ {df[COL_MONTO].sum():,.2f}",
    "Monto total en txns con 088":        f"S/ {df_con088[COL_MONTO].sum():,.2f}",
    "Monto promedio global":              f"S/ {df[COL_MONTO].mean():,.2f}",
    "Saldo restante promedio (global)":   f"S/ {df[COL_SALDO].mean():,.2f}",
}
for k, v in resumen.items():
    print(f"  {k:<45} {v}")

# Top 10 comercios
print("\nTop 10 comercios por volumen:")
print(
    df.groupby(COL_COMERCIO)
    .agg(
        Txns        =(COL_MONTO,   "count"),
        Fraudes     =("es_fraude", "sum"),
        FR_pct      =("es_fraude", lambda x: x.mean()*100),
        Monto_total =(COL_MONTO,   "sum"),
        Pct_con_088 =(COL_CONDICION, lambda x: x.str.contains("088", na=False).mean()*100),
    )
    .sort_values("Txns", ascending=False)
    .head(10)
    .rename(columns={"FR_pct": "Fraud_Rate%", "Pct_con_088": "%_con_088"})
    .assign(Monto_total=lambda x: x["Monto_total"].map("S/ {:,.0f}".format))
    .to_string()
)

# Evolución mensual
print("\nEvolución mensual:")
evol = (
    df
    .assign(Mes=lambda x: x[COL_FECHA].dt.to_period("M").astype(str))
    .groupby("Mes")
    .agg(
        Txns       =(COL_MONTO,    "count"),
        Fraudes    =("es_fraude",  "sum"),
        FR_pct     =("es_fraude",  lambda x: x.mean()*100),
        Con_088    =(COL_CONDICION,lambda x: x.str.contains("088", na=False).sum()),
        Monto_total=(COL_MONTO,    "sum"),
    )
)
print(evol.to_string())

# Gráfico 1: Evolución
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("MCC 7995 — Evolución mensual (Aprobadas)", fontweight="bold")
meses = evol.index.tolist()
x = np.arange(len(meses))

ax = axes[0]
ax.bar(x, evol["Txns"],    color=C_OK,    alpha=0.7, label="Total txns")
ax.bar(x, evol["Con_088"], color=C_088,   alpha=0.9, label="Con condición 088")
ax.bar(x, evol["Fraudes"], color=C_FRAUDE,alpha=0.9, label="Fraudes (F)")
ax.set_xticks(x); ax.set_xticklabels(meses, rotation=30)
ax.set_title("Volumen mensual"); ax.legend()
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"{v:,.0f}"))

ax2 = axes[1]
ax2.plot(meses, evol["FR_pct"], marker="o", color=C_FRAUDE, linewidth=2, label="Fraud Rate%")
ax2.set_title("Fraud Rate mensual (%)")
ax2.set_xticks(range(len(meses))); ax2.set_xticklabels(meses, rotation=30)
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"{v:.2f}%"))
plt.tight_layout(); plt.savefig("g1_evolucion.png", dpi=150, bbox_inches="tight"); plt.show()


# =============================================================================
# SECCIÓN 2 — DISTRIBUCIÓN DEL FRAUDE
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 2 — CÓMO SE DISTRIBUYE EL FRAUDE EN MCC 7995")
print("="*65)

df_fraude   = df[df[COL_INDICADOR] == "F"]
df_nofraude = df[df[COL_INDICADOR] != "F"]

# Por canal
if COL_CANAL in df.columns:
    print("\nFraude por canal:")
    print(
        df.groupby(COL_CANAL)
        .agg(Txns=(COL_MONTO,"count"), Fraudes=("es_fraude","sum"),
             FR=("es_fraude", lambda x: x.mean()*100),
             Monto_fraude=(COL_MONTO, lambda x: x[df.loc[x.index,"es_fraude"]==1].sum()))
        .rename(columns={"FR":"FR%"})
        .to_string()
    )

# Estadísticas de monto: fraude vs no fraude
print("\nEstadísticas de monto — Fraude vs No fraude:")
stats_monto = pd.DataFrame({
    "No fraude": df_nofraude[COL_MONTO].describe(),
    "Fraude":    df_fraude[COL_MONTO].describe(),
}).loc[["count","mean","std","min","25%","50%","75%","max"]]
print(stats_monto.map(lambda v: f"S/ {v:,.2f}" if not pd.isna(v) else "N/A").to_string())

# Estadísticas de saldo restante
print("\nEstadísticas de saldo restante — Fraude vs No fraude:")
stats_saldo = pd.DataFrame({
    "No fraude": df_nofraude[COL_SALDO].describe(),
    "Fraude":    df_fraude[COL_SALDO].describe(),
}).loc[["count","mean","50%","min","max"]]
print(stats_saldo.map(lambda v: f"S/ {v:,.2f}" if not pd.isna(v) else "N/A").to_string())

# Gráfico 2: Distribución de montos
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("MCC 7995 — Monto: Fraude vs No Fraude", fontweight="bold")
p99 = df[COL_MONTO].quantile(0.99)

ax = axes[0]
ax.hist(df_nofraude[COL_MONTO].clip(upper=p99), bins=40, alpha=0.6, color=C_OK,    density=True, label="No fraude")
ax.hist(df_fraude[COL_MONTO].clip(upper=p99),   bins=40, alpha=0.7, color=C_FRAUDE,density=True, label="Fraude")
ax.set_title("Distribución de montos (densidad)"); ax.legend(); ax.set_xlabel("Monto (S/)")

ax2 = axes[1]
pcts = [10, 25, 50, 75, 90, 95]
p_nof = [df_nofraude[COL_MONTO].quantile(p/100) for p in pcts]
p_fra = [df_fraude[COL_MONTO].quantile(p/100)   for p in pcts] if len(df_fraude) > 0 else [0]*6
xp = np.arange(len(pcts))
ax2.bar(xp-0.2, p_nof, 0.35, color=C_OK,     label="No fraude")
ax2.bar(xp+0.2, p_fra, 0.35, color=C_FRAUDE, label="Fraude")
ax2.set_xticks(xp); ax2.set_xticklabels([f"P{p}" for p in pcts])
ax2.set_title("Percentiles de monto"); ax2.legend()
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"S/ {v:,.0f}"))
plt.tight_layout(); plt.savefig("g2_distribucion_monto.png", dpi=150, bbox_inches="tight"); plt.show()


# =============================================================================
# SECCIÓN 3 — INGENIERÍA DE VARIABLES: VELOCIDAD DIARIA POR CLIENTE
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 3 — VELOCIDAD DIARIA POR CLIENTE (BEHAVIORAL FEATURES)")
print("="*65)
print("  (Requiere columna de identificador de tarjeta/cliente)")

# Agrupación cliente-día
df["FECHA_DIA"] = df[COL_FECHA].dt.date

vel = (
    df
    .groupby([COL_TARJETA, "FECHA_DIA"])
    .agg(
        Txns_dia        =(COL_MONTO,    "count"),
        Fraudes_dia     =("es_fraude",  "sum"),
        Monto_acumulado =(COL_MONTO,    "sum"),
        Monto_prom      =(COL_MONTO,    "mean"),
        Saldo_min       =(COL_SALDO,    "min"),   # saldo más bajo del día
        Con_088         =(COL_CONDICION,lambda x: x.str.contains("088",na=False).sum()),
    )
    .reset_index()
    .assign(
        tiene_fraude = lambda x: (x["Fraudes_dia"] > 0).astype(int),
        bucket_vel   = lambda x: pd.cut(
            x["Txns_dia"],
            bins=[0, 1, 2, 3, 4, np.inf],
            labels=["1 txn", "2 txns", "3 txns", "4 txns", "5+ txns"],
            right=True
        )
    )
)

# Tabla de velocidad: dimensión clave del análisis
print("\nTabla de velocidad diaria:")
tabla_vel = (
    vel
    .groupby("bucket_vel", observed=True)
    .agg(
        Clientes_dia       =("Txns_dia",        "count"),     # pares cliente-día
        Clientes_sin_fraude=("tiene_fraude",    lambda x: (x==0).sum()),
        Clientes_con_fraude=("tiene_fraude",    "sum"),
        FR_pct             =("tiene_fraude",    lambda x: x.mean()*100),
        Monto_acum_prom    =("Monto_acumulado", "mean"),
        Monto_acum_max     =("Monto_acumulado", "max"),
        Saldo_min_prom     =("Saldo_min",       "mean"),
        Con_088_total      =("Con_088",         "sum"),
    )
    .assign(
        Pct_total = lambda x: x["Clientes_dia"] / x["Clientes_dia"].sum() * 100,
        Pct_fraude_sobre_total = lambda x: x["Clientes_con_fraude"] / x["Clientes_dia"].sum() * 100,
    )
)
print(tabla_vel.to_string())

# Tabla extra: distribución de fraude por bucket de velocidad
print("\nConcentración del fraude por velocidad diaria:")
total_fraudes_vel = tabla_vel["Clientes_con_fraude"].sum()
print(
    tabla_vel[["Clientes_con_fraude","FR_pct","Monto_acum_prom"]]
    .assign(
        Pct_fraudes_del_total=lambda x: x["Clientes_con_fraude"] / total_fraudes_vel * 100,
        Monto_acum_prom=lambda x: x["Monto_acum_prom"].map("S/ {:,.2f}".format)
    )
    .rename(columns={"FR_pct":"FR%","Pct_fraudes_del_total":"% fraudes totales"})
    .to_string()
)

# Gráfico 3: Velocidad vs Fraude
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("MCC 7995 — Velocidad diaria por cliente", fontweight="bold")

buckets = tabla_vel.index.tolist()
xb = np.arange(len(buckets))

ax = axes[0]
ax.bar(xb, tabla_vel["Clientes_sin_fraude"], color=C_OK,    label="Sin fraude")
ax.bar(xb, tabla_vel["Clientes_con_fraude"], color=C_FRAUDE, label="Con fraude",
       bottom=tabla_vel["Clientes_sin_fraude"])
ax.set_xticks(xb); ax.set_xticklabels(buckets)
ax.set_title("Clientes-día por bucket de velocidad"); ax.legend()
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"{v:,.0f}"))

ax2 = axes[1]
ax2.bar(xb, tabla_vel["FR_pct"], color=C_FRAUDE, alpha=0.8)
ax2.set_xticks(xb); ax2.set_xticklabels(buckets)
ax2.set_title("Fraud Rate (%) por bucket de velocidad")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"{v:.1f}%"))
plt.tight_layout(); plt.savefig("g3_velocidad_diaria.png", dpi=150, bbox_inches="tight"); plt.show()

# Gráfico 4: Monto acumulado vs velocidad
fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(xb-0.2, tabla_vel["Monto_acum_prom"], 0.35, color=C_NEUTRO, label="Monto acum. promedio")
ax.bar(xb+0.2, tabla_vel["Monto_acum_max"],  0.35, color=C_088,    label="Monto acum. máximo")
ax.set_xticks(xb); ax.set_xticklabels(buckets)
ax.set_title("MCC 7995 — Monto acumulado del día por velocidad"); ax.legend()
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"S/ {v:,.0f}"))
plt.tight_layout(); plt.savefig("g4_monto_acumulado_velocidad.png", dpi=150, bbox_inches="tight"); plt.show()


# =============================================================================
# SECCIÓN 4 — ANÁLISIS DE LA CONDICIÓN 088
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 4 — CONDICIÓN 088: ¿CAPTURA EL FRAUDE REAL?")
print("="*65)

df_con088 = df[mask_088]
df_sin088 = df[~mask_088]

print("\nResumen: Con 088 vs Sin 088")
comp = pd.DataFrame({
    "Con condición 088": {
        "Transacciones":        len(df_con088),
        "Fraudes (F)":          df_con088["es_fraude"].sum(),
        "Fraud Rate (F/F+G)%":  df_con088.loc[df_con088[COL_INDICADOR].isin(["F","G"]), "es_fraude"].mean()*100,
        "Monto promedio":       df_con088[COL_MONTO].mean(),
        "Monto mediano":        df_con088[COL_MONTO].median(),
        "Saldo restante prom":  df_con088[COL_SALDO].mean(),
    },
    "Sin condición 088": {
        "Transacciones":        len(df_sin088),
        "Fraudes (F)":          df_sin088["es_fraude"].sum(),
        "Fraud Rate (F/F+G)%":  df_sin088.loc[df_sin088[COL_INDICADOR].isin(["F","G"]), "es_fraude"].mean()*100,
        "Monto promedio":       df_sin088[COL_MONTO].mean(),
        "Monto mediano":        df_sin088[COL_MONTO].median(),
        "Saldo restante prom":  df_sin088[COL_SALDO].mean(),
    },
}).T
print(comp.to_string())

# Distribución de fraude dentro vs fuera del 088
fraudes_con    = (mask_088 & (df["es_fraude"]==1)).sum()
fraudes_sin    = (~mask_088 & (df["es_fraude"]==1)).sum()
total_fraudes  = df["es_fraude"].sum()
print(f"\n  Fraudes DENTRO de txns con condición 088:  {fraudes_con:,}  ({fraudes_con/total_fraudes*100:.1f}% del total de fraudes)")
print(f"  Fraudes FUERA  de txns con condición 088:  {fraudes_sin:,}  ({fraudes_sin/total_fraudes*100:.1f}% del total de fraudes)")
print(f"\n  → Si la mayoría del fraude está FUERA de 088: la regla no está capturando bien.")
print(f"  → Si la mayoría está DENTRO: la regla sí está correlacionada con fraude.")

# Top comercios con condición 088
print("\nTop comercios con condición 088 disparada:")
print(
    df_con088.groupby(COL_COMERCIO)
    .agg(
        Txns_088    =(COL_MONTO,    "count"),
        Fraudes     =("es_fraude",  "sum"),
        FR_pct      =("es_fraude",  lambda x: x.mean()*100),
        Monto_total =(COL_MONTO,    "sum"),
    )
    .sort_values("Txns_088", ascending=False)
    .head(10)
    .rename(columns={"FR_pct":"FR%"})
    .assign(Monto_total=lambda x: x["Monto_total"].map("S/ {:,.0f}".format))
    .to_string()
)

# Gráfico 5: Boxplot tres perfiles
fig, axes = plt.subplots(1, 3, figsize=(16, 5))
fig.suptitle("MCC 7995 — Perfil de monto: 3 segmentos clave", fontweight="bold")

perfiles = {
    "Legítimas\n(no fraude)": (df[df["es_fraude"]==0][COL_MONTO], C_OK),
    "Fraudes\nconfirmados":   (df_fraude[COL_MONTO],              C_FRAUDE),
    "Con condición\n088":     (df_con088[COL_MONTO],              C_088),
}
for ax, (label, (serie, color)) in zip(axes, perfiles.items()):
    serie_clean = serie.dropna().clip(upper=serie.quantile(0.99))
    ax.boxplot(serie_clean, patch_artist=True,
               boxprops=dict(facecolor=color, alpha=0.6),
               medianprops=dict(color="black", linewidth=2))
    ax.set_title(label)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f"S/ {v:,.0f}"))
plt.tight_layout(); plt.savefig("g5_boxplot_perfiles.png", dpi=150, bbox_inches="tight"); plt.show()


# =============================================================================
# SECCIÓN 5 — PERFIL TÍPICO LEGÍTIMO MCC 7995
# =============================================================================
print("\n" + "="*65)
print("SECCIÓN 5 — PERFIL TÍPICO DE CLIENTE LEGÍTIMO EN MCC 7995")
print("="*65)

df_legit = df[df[COL_INDICADOR].isin(["N","G"])]
print(f"  Transacciones legítimas (N/G):  {len(df_legit):,}")
print(f"  Monto promedio:                 S/ {df_legit[COL_MONTO].mean():,.2f}")
print(f"  Monto mediano:                  S/ {df_legit[COL_MONTO].median():,.2f}")
print(f"  Monto P25 – P75:                S/ {df_legit[COL_MONTO].quantile(0.25):,.2f} – S/ {df_legit[COL_MONTO].quantile(0.75):,.2f}")
print(f"  Saldo restante promedio:        S/ {df_legit[COL_SALDO].mean():,.2f}")
print(f"  Saldo restante mediano:         S/ {df_legit[COL_SALDO].median():,.2f}")

if COL_CANAL in df.columns:
    print("\n  Por canal:")
    print(df_legit[COL_CANAL].value_counts(normalize=True).mul(100).map("{:.1f}%".format).to_string())

if COL_ENTRY_MODE in df.columns:
    print("\n  Por entry mode:")
    print(df_legit[COL_ENTRY_MODE].value_counts(normalize=True).mul(100).map("{:.1f}%".format).head(5).to_string())

print("\n  Velocidad típica del cliente legítimo:")
vel_legit = vel[vel["tiene_fraude"] == 0]
print(f"    Txns/día promedio:    {vel_legit['Txns_dia'].mean():.2f}")
print(f"    Txns/día mediana:     {vel_legit['Txns_dia'].median():.1f}")
print(f"    Monto acum. promedio: S/ {vel_legit['Monto_acumulado'].mean():,.2f}")
print(f"    Monto acum. mediano:  S/ {vel_legit['Monto_acumulado'].median():,.2f}")


# =============================================================================
# RESUMEN FINAL — LAS 3 PREGUNTAS RESPONDIDAS
# =============================================================================
print("\n" + "="*65)
print("RESUMEN FINAL")
print("="*65)
print(f"""
  1. ¿Cuál es el fraud rate real del segmento MCC 7995?
     → FR = {fraud_rate_global*100:.2f}%  (base: solo F y G)

  2. ¿La condición 088 está correlacionada con el fraude?
     → Fraudes dentro de txns con 088:   {fraudes_con:,}  ({fraudes_con/total_fraudes*100:.1f}%)
     → Fraudes fuera de txns con 088:    {fraudes_sin:,}  ({fraudes_sin/total_fraudes*100:.1f}%)

  3. ¿Hay un segmento legítimo afectado?
     → Txns con 088 que NO son fraude:   {(mask_088 & (df['es_fraude']==0)).sum():,}
     → Monto bloqueado en no-fraudes:    S/ {df.loc[mask_088 & (df['es_fraude']==0), COL_MONTO].sum():,.2f}

  Gráficos generados:
  g1_evolucion.png | g2_distribucion_monto.png | g3_velocidad_diaria.png
  g4_monto_acumulado_velocidad.png | g5_boxplot_perfiles.png
""")
