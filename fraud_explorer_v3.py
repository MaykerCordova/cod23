"""
fraud_explorer_v3.py
────────────────────
Explorador Interactivo de Fraude — Transferencias a Terceros
Scotiabank Perú · Prevención de Fraude

Todo en un solo archivo:
  · Carga el parquet base (transferencias_consolidado.parquet)
  · Calcula Canal Joy, Tipo Validación, todos los flags
  · Visualiza y construye reglas interactivamente

Uso:
    streamlit run fraud_explorer_v3.py

Requisitos:
    pip install streamlit pandas numpy matplotlib seaborn pyarrow
"""

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════
#  SECCIÓN 1 — CONFIG
#  Ajusta solo esta sección con los nombres reales de tus columnas
# ══════════════════════════════════════════════════════════════

# Columnas base
COL_MONTO    = "MONTO"
COL_IND      = "INDICADOR"
COL_SEGMENTO = "SEGMENTO"
COL_FECHA    = "FECHA_HORA"
COL_ORIGEN   = "ACF_CUENTA_ORIGEN"       # <-- reemplaza
COL_DESTINO  = "ACF_CUENTA_DESTINO"      # <-- reemplaza
COL_CLIENTE  = "ACFYD_CLIENTE"           # <-- reemplaza

# Columnas para Canal Joy
COL_CANAL        = "CANAL"                           # llega: MOT, ATM
COL_TIPO_PROD    = "ACF-TIPO PROD TC"                # llega: TC, TD
COL_COD_SERVICIO = "ACF-CODIGO DE SERVICIO"          # llega: 1, 2, 3
COL_NOMBRE_COM   = "ACF-NOMBRE/LOCALIZACION COMERCIO"

# Parquet de entrada — puede ser el consolidado base o el de features
PARQUET_PATH = "transferencias_consolidado.parquet"

# Flags de comportamiento — si ya vienen del NB3 se usan,
# si no, se calculan aquí con las columnas disponibles
FLAGS_CONFIG = [
    ("FLAG_VEL_ALTA",           "Vel. Alta ≥2 txn/día",         True),
    ("FLAG_VEL_CRITICA",        "Vel. Crítica ≥3 txn/día",      True),
    ("FLAG_BENEF_NUEVO",        "Beneficiario nuevo",            True),
    ("FLAG_CUENTA_MULA",        "Cuenta mula destino",           False),
    ("FLAG_CLIENTE_REINCIDENTE","Cliente reincidente",           True),
    ("FLAG_VACIADO",            "Vaciado cuenta ≥80%",           False),
]

# ── Diccionarios de segmento ──────────────────────────────────
SEG_NOMBRE = {
    "30":"Polo Dirección","99":"Polo Dirección",
    "31":"Premium",       "32":"Preferente",
    "33":"Personal",      "34":"Estándar",
    "5" :"Inst. Financieras","21":"Corporativo",
    "2" :"Mediano Empresas", "15":"Sector Gobierno",
    "16":"Otras Instituciones",
    "3" :"Pequeñas Empresas","4":"Negocios 2",
    "7" :"Negocios 3",       "8":"Negocios 1",
    "13":"Microempresas",
}
SEG_GRUPO = {
    "30":"Affluent",         "99":"Affluent",
    "31":"Emerging Affluent","32":"Emerging Affluent",
    "33":"Top of Mass",      "34":"Mass",
    "5" :"Corporate",        "21":"Corporate",
    "2" :"Commercial",       "15":"Commercial","16":"Commercial",
    "3" :"Small Business",   "4":"Small Business",
    "7" :"Small Business",   "8":"Small Business","13":"Small Business",
}
COLORES   = {"F":"#E63946","N":"#457B9D","G":"#2A9D8F","D":"#E9C46A","P":"#A8DADC"}
MESES_ES  = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",
             5:"Mayo",6:"Junio",7:"Julio",8:"Agosto"}
ORDEN_DIAS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
DIAS_ES    = {"Monday":"Lun","Tuesday":"Mar","Wednesday":"Mié",
              "Thursday":"Jue","Friday":"Vie","Saturday":"Sáb","Sunday":"Dom"}


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 2 — INGENIERÍA DE VARIABLES
# ══════════════════════════════════════════════════════════════

def construir_canal_joy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construye CANAL_JOY a partir de las 3 columnas fuente de Monitor.
    Canal llega como MOT (= Homebanking) o ATM.
    Cod servicio llega como 1, 2, 3 (sin ceros adelante).
    """
    cols_needed = [COL_CANAL, COL_TIPO_PROD, COL_COD_SERVICIO, COL_NOMBRE_COM]
    if not all(c in df.columns for c in cols_needed):
        faltantes = [c for c in cols_needed if c not in df.columns]
        st.warning(f"Canal Joy: columnas no encontradas → {faltantes}. "
                   "Ajusta los nombres en la config.")
        df["CANAL_JOY"] = "Sin canal"
        return df

    canal     = df[COL_CANAL].astype(str).str.strip().str.upper()
    tipo_prod = df[COL_TIPO_PROD].astype(str).str.strip().str.upper()
    cod_serv  = df[COL_COD_SERVICIO].astype(str).str.strip()
    nombre    = df[COL_NOMBRE_COM].astype(str).str.strip()

    es_hb = (canal == "MOT")   # MOT = Homebanking

    cond_web_td1  = es_hb & (tipo_prod == "TD") & (cod_serv == "1")
    cond_app_td2  = es_hb & (tipo_prod == "TD") & (cod_serv == "2")
    cond_ios_td3  = es_hb & (tipo_prod == "TD") & (cod_serv == "3")
    cond_web_tc_w = es_hb & (tipo_prod == "TC") & (nombre.str[-1:].str.upper() == "W")
    cond_app_misc = es_hb & (cod_serv.isin(["0", "201", ""]))

    df["CANAL_JOY"] = np.select(
        [cond_web_td1, cond_app_td2, cond_ios_td3,
         cond_web_tc_w, cond_app_misc],
        ["JOY WEB", "JOY APP ANDROID", "JOY APP IOS",
         "JOY WEB", "JOY APP"],
        default=canal   # ATM u otros: valor original
    )
    return df


def construir_validacion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tipo de validación: últimos 2 caracteres del nombre de comercio.
    PN=Push | TI=Touch ID | FI=Face ID | SM=SMS | EM=Email
    """
    if COL_NOMBRE_COM not in df.columns:
        df["TIPO_VALIDACION"] = "Sin validación"
        return df

    cod = (df[COL_NOMBRE_COM].astype(str)
           .str.strip().str[-2:].str.upper())

    df["TIPO_VALIDACION"] = cod.map({
        "PN": "Push Notification",
        "TI": "Touch ID",
        "FI": "Face ID",
        "SM": "SMS",
        "EM": "Email",
    }).fillna("Sin validación")
    return df


def construir_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula los flags de comportamiento si no vienen ya en el parquet.
    Solo calcula los que dependen de columnas disponibles.
    """
    # Necesita fecha ordenada
    if COL_FECHA in df.columns:
        df = df.sort_values([COL_ORIGEN, COL_FECHA]).reset_index(drop=True)
        df["_FECHA_DIA"] = df[COL_FECHA].dt.date
        df["_SEMANA"]    = df[COL_FECHA].dt.to_period("W")

    # FLAG_VEL_ALTA y FLAG_VEL_CRITICA
    if "FLAG_VEL_ALTA" not in df.columns and COL_ORIGEN in df.columns:
        vel = (df.groupby([COL_ORIGEN, "_FECHA_DIA"])
               .size().reset_index(name="VEL_TRX_DIA"))
        df = df.merge(vel, on=[COL_ORIGEN, "_FECHA_DIA"], how="left")
        df["FLAG_VEL_ALTA"]    = (df["VEL_TRX_DIA"] >= 2).astype(int)
        df["FLAG_VEL_CRITICA"] = (df["VEL_TRX_DIA"] >= 3).astype(int)

    # FLAG_BENEF_NUEVO
    if "FLAG_BENEF_NUEVO" not in df.columns and \
       COL_ORIGEN in df.columns and COL_DESTINO in df.columns:
        df = df.sort_values([COL_ORIGEN, COL_DESTINO, COL_FECHA]).reset_index(drop=True)
        df["FREC_PAR_ACUM"] = df.groupby([COL_ORIGEN, COL_DESTINO]).cumcount()
        df["FLAG_BENEF_NUEVO"] = (df["FREC_PAR_ACUM"] == 0).astype(int)

    # FLAG_CUENTA_MULA
    if "FLAG_CUENTA_MULA" not in df.columns and \
       COL_DESTINO in df.columns and COL_ORIGEN in df.columns:
        mula = (df.groupby([COL_DESTINO, "_FECHA_DIA"])[COL_ORIGEN]
                .nunique().reset_index(name="ORIGENES_DIA"))
        df = df.merge(mula, on=[COL_DESTINO, "_FECHA_DIA"], how="left")
        df["FLAG_CUENTA_MULA"] = (df["ORIGENES_DIA"] >= 3).astype(int)

    # FLAG_CLIENTE_REINCIDENTE
    if "FLAG_CLIENTE_REINCIDENTE" not in df.columns and COL_CLIENTE in df.columns:
        df = df.sort_values([COL_CLIENTE, COL_FECHA]).reset_index(drop=True)
        df["FRAUDES_PREV"] = (
            df.groupby(COL_CLIENTE)[COL_IND]
            .transform(lambda x: (x == "F").shift(1).fillna(0).cumsum())
            .astype(int)
        )
        df["FLAG_CLIENTE_REINCIDENTE"] = (df["FRAUDES_PREV"] >= 1).astype(int)

    # Limpiar columnas auxiliares
    aux = ["_FECHA_DIA", "_SEMANA", "FREC_PAR_ACUM", "ORIGENES_DIA", "FRAUDES_PREV"]
    df.drop(columns=[c for c in aux if c in df.columns], inplace=True)

    return df


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 3 — CARGA
# ══════════════════════════════════════════════════════════════
@st.cache_data(show_spinner="Cargando y procesando datos…")
def cargar_datos():
    df = pd.read_parquet(PARQUET_PATH)

    # Normalizar tipos básicos
    df[COL_IND]      = df[COL_IND].astype(str).str.strip().str.upper()
    df[COL_SEGMENTO] = df[COL_SEGMENTO].astype(str).str.strip()
    df[COL_MONTO]    = pd.to_numeric(df[COL_MONTO], errors="coerce")

    if COL_FECHA in df.columns:
        df[COL_FECHA] = pd.to_datetime(df[COL_FECHA], errors="coerce")
        if "HORA"       not in df.columns: df["HORA"]       = df[COL_FECHA].dt.hour
        if "DIA_SEMANA" not in df.columns: df["DIA_SEMANA"] = df[COL_FECHA].dt.day_name()
        if "MES_NUM"    not in df.columns: df["MES_NUM"]    = df[COL_FECHA].dt.month

    # Ingeniería de variables
    df = construir_canal_joy(df)
    df = construir_validacion(df)
    df = construir_flags(df)

    return df


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 4 — SETUP PÁGINA
# ══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Fraud Rule Builder · Scotiabank",
    page_icon="🛡️", layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
  .stApp { background:#0d1117; color:#e8eaf0; }
  section[data-testid="stSidebar"] {
      background:#111827; border-right:1px solid #1e2d45; }

  .kpi { background:linear-gradient(135deg,#151f35,#1a2847);
         border:1px solid #243050; border-radius:12px;
         padding:16px 14px; text-align:center; margin-bottom:6px; }
  .kpi-lbl { font-size:10px; color:#6b7fa0; text-transform:uppercase;
              letter-spacing:1.3px; margin-bottom:5px; }
  .kpi-val { font-size:20px; font-weight:700; }
  .red  { color:#E63946; } .blue { color:#4e9fd4; }
  .gold { color:#E9C46A; } .grn  { color:#2A9D8F; }

  .rule-box { background:#0f1a2e; border:1px solid #1e3a5f;
              border-radius:10px; padding:16px 20px; margin-top:8px; }
  .rule-title { font-size:12px; font-weight:700; color:#4e9fd4;
                text-transform:uppercase; letter-spacing:1px; margin-bottom:10px; }
  .tag-ok   { background:#0d2b1e; color:#2A9D8F; border:1px solid #1a4a33;
              border-radius:6px; padding:4px 10px; font-size:12px; }
  .tag-warn { background:#2b2000; color:#E9C46A; border:1px solid #4a3800;
              border-radius:6px; padding:4px 10px; font-size:12px; }
  .tag-bad  { background:#2b0a0f; color:#E63946; border:1px solid #4a1520;
              border-radius:6px; padding:4px 10px; font-size:12px; }
  hr { border-color:#1e2d45; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 5 — SIDEBAR FILTROS
# ══════════════════════════════════════════════════════════════
df_full = cargar_datos()
MONTO_ABS_MAX = int(df_full[COL_MONTO].max())

with st.sidebar:
    st.markdown("## 🛡️ Fraud Rule Builder")
    st.markdown("---")

    # Segmento
    segs_disp   = sorted(df_full[COL_SEGMENTO].unique())
    segs_labels = {s: f"{s} — {SEG_NOMBRE.get(s, s)}" for s in segs_disp}
    seg_sel = st.multiselect(
        "Segmento", options=segs_disp,
        default=["31","32","33","34"],
        format_func=lambda s: segs_labels[s]
    )

    st.markdown("---")

    # Monto — inputs numéricos
    st.markdown("**Rango de monto (S/)**")
    c1, c2 = st.columns(2)
    with c1:
        monto_min = st.number_input(
            "Desde", min_value=0, max_value=9999999,
            value=2000, step=500)
    with c2:
        monto_max = st.number_input(
            "Hasta", min_value=0, max_value=9999999,
            value=MONTO_ABS_MAX, step=500)

    st.markdown("---")

    # Hora
    hora_rango = st.slider("Ventana horaria", 0, 23, (0, 23))

    st.markdown("---")

    # Canal Joy
    canales_disp = sorted(df_full["CANAL_JOY"].dropna().unique())
    canal_sel = st.multiselect(
        "Canal Joy", options=canales_disp, default=canales_disp)

    st.markdown("---")

    # Tipo validación
    vals_disp = sorted(df_full["TIPO_VALIDACION"].dropna().unique())
    val_sel = st.multiselect(
        "Tipo de validación", options=vals_disp, default=vals_disp)

    st.markdown("---")

    # Indicadores
    inds_disp = sorted(df_full[COL_IND].unique())
    inds_sel = st.multiselect(
        "Indicadores", options=inds_disp,
        default=[i for i in ["F","N","G"] if i in inds_disp])

    st.markdown("---")

    # Período
    meses_disp = sorted(df_full["MES_NUM"].dropna().unique().astype(int))
    meses_sel = st.multiselect(
        "Período", options=meses_disp, default=meses_disp,
        format_func=lambda m: MESES_ES.get(m, str(m)))

    st.markdown("---")
    st.caption("v3.0 · Scotiabank Perú · Prevención de Fraude")


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 6 — FILTRADO
# ══════════════════════════════════════════════════════════════
if not seg_sel or not inds_sel or not meses_sel or not canal_sel or not val_sel:
    st.warning("Selecciona al menos un valor en cada filtro.")
    st.stop()

mask = (
    df_full[COL_SEGMENTO].isin(seg_sel) &
    df_full[COL_MONTO].between(monto_min, monto_max) &
    df_full[COL_IND].isin(inds_sel) &
    df_full["MES_NUM"].isin(meses_sel) &
    df_full["CANAL_JOY"].isin(canal_sel) &
    df_full["HORA"].between(hora_rango[0], hora_rango[1]) &
    df_full["TIPO_VALIDACION"].isin(val_sel)
)

df   = df_full[mask].copy()
df_c = df[df[COL_IND].isin(["F","N","G"])].copy()

if len(df) == 0:
    st.warning("Sin datos con los filtros actuales.")
    st.stop()

# KPIs globales
n_total   = len(df_c)
n_f       = (df_c[COL_IND] == "F").sum()
fr        = n_f / n_total * 100 if n_total > 0 else 0
sev       = df_c.loc[df_c[COL_IND]=="F", COL_MONTO].mean() if n_f > 0 else 0
monto_f   = df_c.loc[df_c[COL_IND]=="F", COL_MONTO].sum()
monto_tot = df_c[COL_MONTO].sum()
pct_monto = monto_f / monto_tot * 100 if monto_tot > 0 else 0


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 7 — HEADER + KPIs
# ══════════════════════════════════════════════════════════════
seg_txt   = " · ".join([segs_labels[s] for s in seg_sel]) \
            if len(seg_sel) <= 3 else f"{len(seg_sel)} segmentos"
canal_txt = " · ".join(canal_sel) if len(canal_sel) <= 3 else f"{len(canal_sel)} canales"

st.markdown("## 🛡️ Fraud Rule Builder — Transferencias a Terceros")
st.markdown(
    f"**{seg_txt}** &nbsp;·&nbsp; {canal_txt} &nbsp;·&nbsp; "
    f"S/ {monto_min:,}–S/ {monto_max:,} &nbsp;·&nbsp; "
    f"Hora {hora_rango[0]:02d}–{hora_rango[1]:02d}h"
)
st.markdown("---")

c1,c2,c3,c4,c5,c6 = st.columns(6)
for col_k, lbl, val, cls in [
    (c1, "Total Txn",          f"{n_total:,}",           "blue"),
    (c2, "Fraudes (F)",        f"{n_f:,}",               "red"),
    (c3, "Fraud Rate",         f"{fr:.3f}%",             "red"),
    (c4, "Severidad Media",    f"S/ {sev:,.0f}",         "gold"),
    (c5, "Monto Total Fraude", f"S/ {monto_f:,.0f}",    "red"),
    (c6, "% Monto en Fraude",  f"{pct_monto:.2f}%",     "gold"),
]:
    with col_k:
        st.markdown(
            f'<div class="kpi"><div class="kpi-lbl">{lbl}</div>'
            f'<div class="kpi-val {cls}">{val}</div></div>',
            unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
#  HELPERS GRÁFICOS
# ══════════════════════════════════════════════════════════════
plt.rcParams.update({
    "figure.facecolor":"#0d1117","axes.facecolor":"#111827",
    "axes.edgecolor":"#1e2d45","axes.labelcolor":"#8892aa",
    "xtick.color":"#8892aa","ytick.color":"#8892aa",
    "text.color":"#e8eaf0","grid.color":"#1e2d45","grid.alpha":0.5,
})

def fmt_soles(ax, eje="x"):
    f = mticker.FuncFormatter(lambda x,_: f"S/{x:,.0f}")
    if eje == "x": ax.xaxis.set_major_formatter(f)
    else:          ax.yaxis.set_major_formatter(f)


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 8 — TABS
# ══════════════════════════════════════════════════════════════
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Perfil del Fraude",
    "🕐 Análisis Temporal",
    "📡 Canal & Validación",
    "⚙️ Constructor de Reglas",
])


# ────────────────────────────────────
# TAB 1 — PERFIL DEL FRAUDE
# ────────────────────────────────────
with tab1:
    BINS   = [0, 5000, 10000, 20000, 50000, np.inf]
    LABELS = ["<5k","5k–10k","10k–20k","20k–50k","50k+"]
    df_c["RANGO"] = pd.cut(df_c[COL_MONTO], bins=BINS, labels=LABELS, right=False)

    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### Distribución de Indicadores")
        vc = df_c[COL_IND].value_counts().reset_index()
        vc.columns = ["Indicador","N"]
        vc["%"] = (vc["N"]/vc["N"].sum()*100).round(2)
        fig, ax = plt.subplots(figsize=(5, 3.5))
        ax.bar(vc["Indicador"], vc["N"],
               color=[COLORES.get(i,"#555") for i in vc["Indicador"]],
               edgecolor="#0d1117")
        for i, row in vc.iterrows():
            ax.text(i, row["N"]*1.03, f"{row['%']:.1f}%",
                    ha="center", fontsize=9)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_:f"{x:,.0f}"))
        ax.spines[["top","right"]].set_visible(False)
        fig.tight_layout(); st.pyplot(fig); plt.close()

    with col_b:
        st.markdown("#### Fraudes y Monto por Rango")
        rp = df_c.groupby(["RANGO", COL_IND]).size().unstack(fill_value=0)
        if "F" in rp.columns:
            fig, axes = plt.subplots(1, 2, figsize=(7, 3.5))
            rfr = (rp.get("F",0)/rp.sum(axis=1)*100).fillna(0)
            clrs = plt.cm.YlOrRd(rfr/max(rfr.max(), 0.01))
            axes[0].bar(rfr.index, rfr.values, color=clrs, edgecolor="#0d1117")
            for i,(r,v) in enumerate(rfr.items()):
                nf = rp.get("F", pd.Series()).get(r, 0)
                axes[0].text(i, v+0.003, f"{v:.2f}%\nN={nf}",
                             ha="center", fontsize=7.5)
            axes[0].set_ylabel("FR (%)"); axes[0].tick_params(axis="x",rotation=30)
            axes[0].set_title("Fraud Rate por Rango", fontsize=10)
            axes[0].spines[["top","right"]].set_visible(False)

            mf_rango = df_c[df_c[COL_IND]=="F"].groupby("RANGO")[COL_MONTO].sum()
            axes[1].bar(mf_rango.index, mf_rango.values,
                        color="#E63946", alpha=0.8, edgecolor="#0d1117")
            axes[1].set_ylabel("S/"); axes[1].tick_params(axis="x",rotation=30)
            axes[1].set_title("Monto Fraude por Rango", fontsize=10)
            fmt_soles(axes[1],"y")
            axes[1].spines[["top","right"]].set_visible(False)
            fig.tight_layout(); st.pyplot(fig); plt.close()

    st.markdown("---")
    st.markdown("#### Distribución del Monto — F vs N vs G")
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    for ind in ["F","N","G"]:
        s = df_c.loc[df_c[COL_IND]==ind, COL_MONTO]
        if len(s) < 5: continue
        sns.kdeplot(s, ax=axes[0], label=ind, color=COLORES.get(ind,"#999"),
                    fill=True, alpha=0.2, linewidth=1.8)
    axes[0].set_xlabel("Monto (S/)"); fmt_soles(axes[0],"x")
    axes[0].set_title("KDE", fontsize=10)
    axes[0].legend(); axes[0].spines[["top","right"]].set_visible(False)

    bp_d = [df_c.loc[df_c[COL_IND]==i, COL_MONTO].values
            for i in ["F","N","G"] if len(df_c[df_c[COL_IND]==i]) > 0]
    bp_l = [i for i in ["F","N","G"] if len(df_c[df_c[COL_IND]==i]) > 0]
    if bp_d:
        bp = axes[1].boxplot(bp_d, labels=bp_l, patch_artist=True,
                             flierprops=dict(marker="o",markersize=2,alpha=0.3))
        for p, i in zip(bp["boxes"], bp_l):
            p.set_facecolor(COLORES.get(i,"#999")); p.set_alpha(0.7)
        fmt_soles(axes[1],"y"); axes[1].set_title("Boxplot", fontsize=10)
        axes[1].spines[["top","right"]].set_visible(False)

    fr_s = df_c.loc[df_c[COL_IND]=="F", COL_MONTO].dropna()
    if len(fr_s) > 0:
        pcts = np.arange(0, 101, 10)
        vals = np.percentile(fr_s, pcts)
        axes[2].plot(pcts, vals, "o-", color="#E63946", lw=2, ms=5)
        axes[2].fill_between(pcts, vals, alpha=0.15, color="#E63946")
        axes[2].set_xlabel("Percentil"); fmt_soles(axes[2],"y")
        axes[2].set_title("Percentiles Fraude", fontsize=10)
        axes[2].spines[["top","right"]].set_visible(False)

    fig.tight_layout(); st.pyplot(fig); plt.close()

    # Tabla descriptiva
    st.markdown("#### Descriptivos por Indicador")
    filas = []
    for ind in ["F","N","G"]:
        s = df_c.loc[df_c[COL_IND]==ind, COL_MONTO].dropna()
        if len(s) == 0: continue
        q1,q2,q3 = s.quantile([.25,.5,.75])
        filas.append({
            "Indicador":ind,"N":f"{len(s):,}",
            "Mediana":f"S/{s.median():,.0f}",
            "Media":f"S/{s.mean():,.0f}",
            "Trimedia":f"S/{(q1+2*q2+q3)/4:,.0f}",
            "P90":f"S/{s.quantile(.9):,.0f}",
            "P99":f"S/{s.quantile(.99):,.0f}",
            "Máx":f"S/{s.max():,.0f}",
            "CV%":f"{s.std()/s.mean()*100:.1f}%",
            "Asim.":f"{s.skew():.2f}",
        })
    if filas:
        st.dataframe(pd.DataFrame(filas).set_index("Indicador"),
                     use_container_width=True)


# ────────────────────────────────────
# TAB 2 — ANÁLISIS TEMPORAL
# ────────────────────────────────────
with tab2:
    hora_s = (df_c.groupby("HORA")
              .agg(N_total=(COL_IND,"count"),
                   N_f=(COL_IND, lambda x: (x=="F").sum()))
              .assign(FR=lambda x: x["N_f"]/x["N_total"]*100)
              .reset_index())

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Volumen por Hora")
        fig, ax = plt.subplots(figsize=(6, 3.5))
        ax.bar(hora_s["HORA"], hora_s["N_total"], color="#1e2d45", label="Total")
        ax.bar(hora_s["HORA"], hora_s["N_f"],    color="#E63946", label="Fraude")
        ax.set_xticks(range(0,24,2)); ax.legend(fontsize=8)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_:f"{x:,.0f}"))
        ax.spines[["top","right"]].set_visible(False)
        fig.tight_layout(); st.pyplot(fig); plt.close()

    with col2:
        st.markdown("#### Fraud Rate por Hora")
        media_fr = hora_s["FR"].mean()
        clrs_h = ["#E63946" if v > media_fr*2 else "#457B9D"
                  for v in hora_s["FR"]]
        fig, ax = plt.subplots(figsize=(6, 3.5))
        ax.bar(hora_s["HORA"], hora_s["FR"], color=clrs_h, edgecolor="#0d1117")
        ax.axhline(media_fr, color="#E9C46A", ls="--", lw=1.5,
                   label=f"Media {media_fr:.2f}%")
        ax.set_xticks(range(0,24,2)); ax.legend(fontsize=8)
        ax.spines[["top","right"]].set_visible(False)
        fig.tight_layout(); st.pyplot(fig); plt.close()

    st.markdown("#### Heatmap Fraud Rate — Hora × Día")
    pivot_hm = (df_c.assign(FB=lambda x: (x[COL_IND]=="F").astype(int))
                .groupby(["DIA_SEMANA","HORA"])["FB"]
                .mean() * 100).unstack("HORA")
    pivot_hm = pivot_hm.reindex([d for d in ORDEN_DIAS if d in pivot_hm.index])
    pivot_hm.index = [DIAS_ES.get(d,d) for d in pivot_hm.index]
    fig, ax = plt.subplots(figsize=(16, 4))
    sns.heatmap(pivot_hm, cmap="YlOrRd", annot=True, fmt=".1f",
                linewidths=0.3, ax=ax, annot_kws={"size":7},
                cbar_kws={"label":"FR%"})
    ax.set_xlabel("Hora"); ax.set_ylabel("")
    fig.tight_layout(); st.pyplot(fig); plt.close()

    col3, col4 = st.columns(2)
    with col3:
        st.markdown("#### Por Día de Semana")
        dia_s = (df_c.groupby("DIA_SEMANA")
                 .agg(N_total=(COL_IND,"count"),
                      N_f=(COL_IND, lambda x: (x=="F").sum()))
                 .assign(FR=lambda x: x["N_f"]/x["N_total"]*100)
                 .reindex([d for d in ORDEN_DIAS if d in df_c["DIA_SEMANA"].values])
                 .reset_index())
        clrs_d = ["#E63946" if d in ["Saturday","Sunday"] else "#457B9D"
                  for d in dia_s["DIA_SEMANA"]]
        fig, ax = plt.subplots(figsize=(6, 3.5))
        ax.bar([DIAS_ES.get(d,d) for d in dia_s["DIA_SEMANA"]],
               dia_s["FR"], color=clrs_d, edgecolor="#0d1117")
        ax.set_ylabel("FR (%)"); ax.tick_params(axis="x", rotation=30)
        ax.spines[["top","right"]].set_visible(False)
        fig.tight_layout(); st.pyplot(fig); plt.close()

    with col4:
        st.markdown("#### Evolución Mensual")
        mes_s = (df_c.groupby("MES_NUM")
                 .agg(N_total=(COL_IND,"count"),
                      N_f=(COL_IND, lambda x: (x=="F").sum()))
                 .assign(FR=lambda x: x["N_f"]/x["N_total"]*100)
                 .reset_index())
        mes_s["MES"] = mes_s["MES_NUM"].map(MESES_ES)
        fig, ax = plt.subplots(figsize=(6, 3.5))
        ax.bar(mes_s["MES"], mes_s["FR"], color="#1e2d45", edgecolor="#0d1117")
        ax.plot(mes_s["MES"], mes_s["FR"], "o-", color="#E63946", lw=2, ms=8)
        for _, row in mes_s.iterrows():
            ax.text(row.name, row["FR"]+0.002,
                    f"{row['FR']:.3f}%", ha="center", fontsize=9)
        ax.set_ylabel("FR (%)")
        ax.spines[["top","right"]].set_visible(False)
        fig.tight_layout(); st.pyplot(fig); plt.close()


# ────────────────────────────────────
# TAB 3 — CANAL & VALIDACIÓN
# ────────────────────────────────────
with tab3:
    col_ca, col_cb = st.columns(2)

    with col_ca:
        st.markdown("#### Fraud Rate por Canal Joy")
        canal_s = (df_c.groupby("CANAL_JOY")
                   .agg(N_total=(COL_IND,"count"),
                        N_f=(COL_IND, lambda x: (x=="F").sum()),
                        Monto_F=(COL_MONTO, lambda x:
                            df_c.loc[(df_c[COL_IND]=="F") &
                                     (df_c["CANAL_JOY"]==x.name), COL_MONTO].sum()))
                   .assign(FR=lambda x: x["N_f"]/x["N_total"]*100)
                   .sort_values("FR", ascending=False)
                   .reset_index())
        fig, ax = plt.subplots(figsize=(6, 4))
        clrs_c = plt.cm.RdYlGn_r(np.linspace(0.1, 0.9, len(canal_s)))
        ax.barh(canal_s["CANAL_JOY"], canal_s["FR"],
                color=clrs_c, edgecolor="#0d1117")
        for i, row in canal_s.iterrows():
            ax.text(row["FR"]+0.001, i,
                    f"{row['FR']:.3f}% (N={row['N_f']})",
                    va="center", fontsize=9)
        ax.set_xlabel("Fraud Rate (%)"); ax.invert_yaxis()
        ax.spines[["top","right"]].set_visible(False)
        fig.tight_layout(); st.pyplot(fig); plt.close()

        canal_s["Monto_F"] = canal_s["Monto_F"].map(lambda x: f"S/ {x:,.0f}")
        canal_s["FR"]      = canal_s["FR"].map(lambda x: f"{x:.3f}%")
        st.dataframe(canal_s[["CANAL_JOY","N_total","N_f","FR","Monto_F"]]
                     .rename(columns={"CANAL_JOY":"Canal","N_total":"Total",
                                      "N_f":"Fraudes","Monto_F":"Monto Fraude"}),
                     use_container_width=True, hide_index=True)

    with col_cb:
        st.markdown("#### Fraud Rate por Tipo de Validación")
        val_s = (df_c.groupby("TIPO_VALIDACION")
                 .agg(N_total=(COL_IND,"count"),
                      N_f=(COL_IND, lambda x: (x=="F").sum()),
                      Monto_F=(COL_MONTO, lambda x:
                          df_c.loc[(df_c[COL_IND]=="F") &
                                   (df_c["TIPO_VALIDACION"]==x.name), COL_MONTO].sum()))
                 .assign(FR=lambda x: x["N_f"]/x["N_total"]*100)
                 .sort_values("FR", ascending=False)
                 .reset_index())
        fig, ax = plt.subplots(figsize=(6, 4))
        clrs_v = plt.cm.RdYlGn_r(np.linspace(0.1, 0.9, len(val_s)))
        ax.barh(val_s["TIPO_VALIDACION"].astype(str),
                val_s["FR"], color=clrs_v, edgecolor="#0d1117")
        for i, row in val_s.iterrows():
            ax.text(row["FR"]+0.001, i,
                    f"{row['FR']:.3f}% (N={row['N_f']})",
                    va="center", fontsize=9)
        ax.set_xlabel("Fraud Rate (%)"); ax.invert_yaxis()
        ax.spines[["top","right"]].set_visible(False)
        fig.tight_layout(); st.pyplot(fig); plt.close()

        val_s["Monto_F"] = val_s["Monto_F"].map(lambda x: f"S/ {x:,.0f}")
        val_s["FR"]      = val_s["FR"].map(lambda x: f"{x:.3f}%")
        st.dataframe(val_s[["TIPO_VALIDACION","N_total","N_f","FR","Monto_F"]]
                     .rename(columns={"TIPO_VALIDACION":"Validación",
                                      "N_total":"Total","N_f":"Fraudes",
                                      "Monto_F":"Monto Fraude"}),
                     use_container_width=True, hide_index=True)

    # Heatmap Canal × Validación
    st.markdown("---")
    st.markdown("#### Heatmap — Canal × Tipo de Validación (Fraud Rate %)")
    pivot_cv = (df_c.assign(FB=lambda x: (x[COL_IND]=="F").astype(int))
                .groupby(["CANAL_JOY","TIPO_VALIDACION"])["FB"]
                .mean() * 100).unstack(fill_value=0)
    if pivot_cv.shape[1] > 0:
        fig, ax = plt.subplots(figsize=(12, 4))
        sns.heatmap(pivot_cv, cmap="YlOrRd", annot=True, fmt=".2f",
                    linewidths=0.3, ax=ax, cbar_kws={"label":"FR%"})
        ax.set_xlabel("Tipo de Validación"); ax.set_ylabel("Canal")
        fig.tight_layout(); st.pyplot(fig); plt.close()


# ────────────────────────────────────
# TAB 4 — CONSTRUCTOR DE REGLAS
# ────────────────────────────────────
with tab4:
    st.markdown("### ⚙️ Constructor de Reglas")
    st.markdown(
        "Activa los flags, ajusta los filtros del sidebar y ve en tiempo real "
        "cuánto fraude capturas, cuánto monto alertas y la precisión de la regla."
    )
    st.markdown("---")

    flags_presentes = [(c,d,dflt) for c,d,dflt in FLAGS_CONFIG if c in df_c.columns]

    if not flags_presentes:
        st.warning("No hay flags disponibles. Revisa que el parquet tenga las columnas "
                   "o que las columnas de cuenta origen/destino/cliente estén correctas.")
    else:
        st.markdown("#### 1. Selecciona los flags")
        flags_activos = []
        cols_chk = st.columns(3)
        for idx, (col_f, desc, dflt) in enumerate(flags_presentes):
            with cols_chk[idx % 3]:
                pct_F = df_c.loc[df_c[COL_IND]=="F", col_f].mean()*100 if n_f>0 else 0
                pct_N = df_c.loc[df_c[COL_IND]=="N", col_f].mean()*100
                sep   = pct_F - pct_N
                badge = "✅" if sep>5 else ("⚠️" if sep>2 else "❌")
                if st.checkbox(
                    f"{badge} {desc}",
                    value=dflt and sep>5,
                    help=f"F: {pct_F:.1f}%  |  N: {pct_N:.1f}%  |  Sep: {sep:+.1f}pp"
                ):
                    flags_activos.append(col_f)

        st.markdown("---")
        st.markdown("#### 2. Poder discriminante")
        filas_d = []
        for col_f, desc, _ in flags_presentes:
            pct_F = df_c.loc[df_c[COL_IND]=="F", col_f].mean()*100 if n_f>0 else 0
            pct_N = df_c.loc[df_c[COL_IND]=="N", col_f].mean()*100
            sep   = pct_F - pct_N
            filas_d.append({
                "Variable":col_f,"Descripción":desc,
                "% en F":round(pct_F,1),"% en N":round(pct_N,1),
                "Separación":round(sep,1),
                "Útil":"✅" if sep>5 else ("⚠️" if sep>2 else "❌"),
            })
        st.dataframe(
            pd.DataFrame(filas_d).style.background_gradient(
                subset=["Separación"], cmap="RdYlGn"),
            use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("#### 3. Resultado de la regla")

        if not flags_activos:
            st.info("Activa al menos un flag para ver el resultado.")
        else:
            mask_r = pd.Series(True, index=df_c.index)
            for f in flags_activos:
                mask_r = mask_r & (df_c[f] == 1)

            n_alertas      = mask_r.sum()
            n_cap_F        = (df_c.loc[mask_r, COL_IND] == "F").sum()
            monto_alertado = df_c.loc[mask_r, COL_MONTO].sum()
            monto_cap_F    = df_c.loc[mask_r & (df_c[COL_IND]=="F"), COL_MONTO].sum()
            precision      = n_cap_F / n_alertas * 100 if n_alertas > 0 else 0
            recall         = n_cap_F / n_f * 100        if n_f > 0      else 0
            pct_monto_cap  = monto_cap_F / monto_f * 100 if monto_f > 0 else 0
            falsas         = n_alertas - n_cap_F

            cond_txt = " AND ".join([f"`{f}=1`" for f in flags_activos])
            st.markdown(
                f'<div class="rule-box"><div class="rule-title">📋 Regla Activa</div>'
                f'<code style="color:#4e9fd4;font-size:13px">{cond_txt}</code><br><br>'
                f'Filtros: <b>Seg {", ".join(seg_sel)}</b> · '
                f'<b>S/{monto_min:,}–S/{monto_max:,}</b> · '
                f'<b>{canal_txt}</b> · '
                f'<b>Hora {hora_rango[0]:02d}–{hora_rango[1]:02d}h</b>'
                f'</div>', unsafe_allow_html=True)

            r1,r2,r3,r4,r5,r6 = st.columns(6)
            for col_k, lbl, val, cls in [
                (r1,"Alertas generadas",    f"{n_alertas:,}",          "blue"),
                (r2,"Fraudes capturados",   f"{n_cap_F} / {n_f}",     "red"),
                (r3,"Recall",               f"{recall:.1f}%",          "grn"),
                (r4,"Precisión",            f"{precision:.1f}%",       "gold"),
                (r5,"Falsas alarmas",       f"{falsas:,}",             "gold"),
                (r6,"Monto fraude captado", f"S/ {monto_cap_F:,.0f}", "red"),
            ]:
                with col_k:
                    st.markdown(
                        f'<div class="kpi"><div class="kpi-lbl">{lbl}</div>'
                        f'<div class="kpi-val {cls}">{val}</div></div>',
                        unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            col_m1, col_m2 = st.columns(2)
            with col_m1:
                st.markdown(
                    f'<div class="rule-box"><div class="rule-title">💰 Impacto Monto</div>'
                    f'Monto total alertado: <b style="color:#E9C46A">S/ {monto_alertado:,.0f}</b><br>'
                    f'Monto fraude capturado: <b style="color:#E63946">S/ {monto_cap_F:,.0f}</b><br>'
                    f'% fraude capturado: <b style="color:#2A9D8F">{pct_monto_cap:.1f}%</b><br>'
                    f'Monto fraude total visible: <b>S/ {monto_f:,.0f}</b>'
                    f'</div>', unsafe_allow_html=True)

            with col_m2:
                if precision > 30:
                    tag = '<span class="tag-ok">✅ Alta precisión — proponer al Checker</span>'
                    msg = "Buena relación señal/ruido. Candidata directa."
                elif precision > 10:
                    tag = '<span class="tag-warn">⚠️ Precisión media — afinar</span>'
                    msg = "Agrega hora, monto o canal para reducir falsas alarmas."
                else:
                    tag = '<span class="tag-bad">❌ Demasiado ruido</span>'
                    msg = "Combina más flags o restringe los filtros del sidebar."
                st.markdown(
                    f'<div class="rule-box"><div class="rule-title">🎯 Veredicto</div>'
                    f'{tag}<br><br>{msg}</div>', unsafe_allow_html=True)

            # Gráfico composición de alertas
            st.markdown("#### Composición de las alertas generadas")
            alertas_df = df_c[mask_r].copy()
            if len(alertas_df) > 0:
                fig, axes = plt.subplots(1, 2, figsize=(12, 4))
                for ind in ["F","N","G"]:
                    s = alertas_df.loc[alertas_df[COL_IND]==ind, COL_MONTO]
                    if len(s) < 3: continue
                    sns.kdeplot(s, ax=axes[0], label=ind,
                                color=COLORES.get(ind,"#999"),
                                fill=True, alpha=0.2, linewidth=1.8)
                axes[0].set_title("KDE monto en alertas", fontsize=10)
                fmt_soles(axes[0],"x"); axes[0].legend()
                axes[0].spines[["top","right"]].set_visible(False)

                vc_a = alertas_df[COL_IND].value_counts().reset_index()
                vc_a.columns = ["Ind","N"]
                axes[1].bar(vc_a["Ind"], vc_a["N"],
                            color=[COLORES.get(i,"#555") for i in vc_a["Ind"]],
                            edgecolor="#0d1117")
                axes[1].set_title("Indicadores en alertas", fontsize=10)
                axes[1].yaxis.set_major_formatter(
                    mticker.FuncFormatter(lambda x,_:f"{x:,.0f}"))
                axes[1].spines[["top","right"]].set_visible(False)
                fig.tight_layout(); st.pyplot(fig); plt.close()
