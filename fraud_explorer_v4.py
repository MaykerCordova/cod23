"""
fraud_explorer_v4.py  —  VERSIÓN FINAL
═══════════════════════════════════════
Explorador Interactivo de Fraude · Transferencias a Terceros
Scotiabank Perú · Prevención de Fraude

Todo en un solo archivo:
  · Carga parquet base y calcula toda la ingeniería de variables
  · Gráficos 100% Plotly (interactivos, hover, zoom)
  · Flags ampliados: velocidad, monto acumulado, IP, dispositivo
  · Interpretaciones automáticas por gráfico
  · Constructor de reglas con métricas en tiempo real

Uso:
    pip install streamlit pandas numpy plotly pyarrow
    streamlit run fraud_explorer_v4.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════
#  SECCIÓN 1 — CONFIG
#  ► Ajusta solo esta sección con tus nombres de columnas reales
# ══════════════════════════════════════════════════════════════

# Columnas base
COL_MONTO    = "MONTO"
COL_IND      = "INDICADOR"
COL_SEGMENTO = "SEGMENTO"
COL_FECHA    = "FECHA_HORA"
COL_ORIGEN   = "ACF_CUENTA_ORIGEN"        # <-- reemplaza
COL_DESTINO  = "ACF_CUENTA_DESTINO"       # <-- reemplaza
COL_CLIENTE  = "ACFYD_CLIENTE"            # <-- reemplaza
COL_SALDO    = "ACFSALDO_DISPONIBLE_EN_MONEDA_TRX"  # <-- reemplaza

# Canal Joy
COL_CANAL        = "CANAL"
COL_TIPO_PROD    = "ACF-TIPO PROD TC"
COL_COD_SERVICIO = "ACF-CODIGO DE SERVICIO"
COL_NOMBRE_COM   = "ACF-NOMBRE/LOCALIZACION COMERCIO"

# IP y dispositivo (si no los tienes, deja el string vacío "")
COL_IP           = "IP_ADDRESS"           # <-- reemplaza o deja ""
COL_HASH_DEVICE  = "HASH_DISPOSITIVO"     # <-- reemplaza o deja ""

PARQUET_PATH = "transferencias_consolidado.parquet"

# ── Paleta de colores ─────────────────────────────────────────
C = {
    "bg":      "#0d1117",
    "panel":   "#111827",
    "border":  "#1e2d45",
    "text":    "#e8eaf0",
    "muted":   "#6b7fa0",
    "F":       "#E63946",
    "N":       "#457B9D",
    "G":       "#2A9D8F",
    "D":       "#E9C46A",
    "P":       "#A8DADC",
    "accent":  "#4e9fd4",
    "gold":    "#E9C46A",
    "green":   "#2A9D8F",
}

# ── Diccionarios ──────────────────────────────────────────────
SEG_NOMBRE = {
    "30":"Polo Dirección","99":"Polo Dirección",
    "31":"Premium",       "32":"Preferente",
    "33":"Personal",      "34":"Estándar",
    "5" :"Inst. Financieras","21":"Corporativo",
    "2" :"Mediano Empresas","15":"Sector Gobierno",
    "16":"Otras Instituciones",
    "3" :"Pequeñas Empresas","4":"Negocios 2",
    "7" :"Negocios 3",    "8":"Negocios 1",
    "13":"Microempresas",
}
SEG_GRUPO = {
    "30":"Affluent",    "99":"Affluent",
    "31":"Emerging Affluent","32":"Emerging Affluent",
    "33":"Top of Mass", "34":"Mass",
    "5" :"Corporate",   "21":"Corporate",
    "2" :"Commercial",  "15":"Commercial","16":"Commercial",
    "3" :"Small Business","4":"Small Business",
    "7" :"Small Business","8":"Small Business","13":"Small Business",
}
MESES_ES  = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",
             5:"Mayo",6:"Junio",7:"Julio",8:"Agosto"}
ORDEN_DIAS = ["Monday","Tuesday","Wednesday","Thursday",
              "Friday","Saturday","Sunday"]
DIAS_ES    = {"Monday":"Lun","Tuesday":"Mar","Wednesday":"Mié",
              "Thursday":"Jue","Friday":"Vie",
              "Saturday":"Sáb","Sunday":"Dom"}


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 2 — INGENIERÍA DE VARIABLES
# ══════════════════════════════════════════════════════════════

def construir_canal_joy(df):
    cols = [COL_CANAL, COL_TIPO_PROD, COL_COD_SERVICIO, COL_NOMBRE_COM]
    if not all(c in df.columns for c in cols):
        df["CANAL_JOY"] = "Sin canal"
        return df
    canal     = df[COL_CANAL].astype(str).str.strip().str.upper()
    tipo_prod = df[COL_TIPO_PROD].astype(str).str.strip().str.upper()
    cod_serv  = df[COL_COD_SERVICIO].astype(str).str.strip()
    nombre    = df[COL_NOMBRE_COM].astype(str).str.strip()
    es_hb     = (canal == "MOT")
    cond_web_td1  = es_hb & (tipo_prod == "TD") & (cod_serv == "1")
    cond_app_td2  = es_hb & (tipo_prod == "TD") & (cod_serv == "2")
    cond_ios_td3  = es_hb & (tipo_prod == "TD") & (cod_serv == "3")
    cond_web_tc_w = es_hb & (tipo_prod == "TC") & (nombre.str[-1:].str.upper() == "W")
    cond_app_misc = es_hb & (cod_serv.isin(["0","201",""]))
    df["CANAL_JOY"] = np.select(
        [cond_web_td1, cond_app_td2, cond_ios_td3,
         cond_web_tc_w, cond_app_misc],
        ["JOY WEB","JOY APP ANDROID","JOY APP IOS","JOY WEB","JOY APP"],
        default=canal
    )
    return df


def construir_validacion(df):
    if COL_NOMBRE_COM not in df.columns:
        df["TIPO_VALIDACION"] = "Sin validación"
        return df
    cod = df[COL_NOMBRE_COM].astype(str).str.strip().str[-2:].str.upper()
    df["TIPO_VALIDACION"] = cod.map({
        "PN":"Push Notification","TI":"Touch ID",
        "FI":"Face ID","SM":"SMS","EM":"Email",
    }).fillna("Sin validación")
    return df


def construir_flags(df):
    """Calcula todos los flags de comportamiento."""

    tiene_fecha  = COL_FECHA   in df.columns
    tiene_origen = COL_ORIGEN  in df.columns
    tiene_dest   = COL_DESTINO in df.columns
    tiene_cliente= COL_CLIENTE in df.columns
    tiene_saldo  = COL_SALDO   in df.columns
    tiene_ip     = COL_IP != "" and COL_IP in df.columns
    tiene_hash   = COL_HASH_DEVICE != "" and COL_HASH_DEVICE in df.columns

    if tiene_fecha:
        df["_FECHA_DIA"] = df[COL_FECHA].dt.date

    # ── BLOQUE A: Velocidad (múltiples umbrales) ──────────────
    if "FLAG_VEL_2" not in df.columns and tiene_origen and tiene_fecha:
        df = df.sort_values([COL_ORIGEN, COL_FECHA]).reset_index(drop=True)
        vel = (df.groupby([COL_ORIGEN,"_FECHA_DIA"])
               .size().reset_index(name="VEL_TRX_DIA"))
        df = df.merge(vel, on=[COL_ORIGEN,"_FECHA_DIA"], how="left")
        df["FLAG_VEL_2"] = (df["VEL_TRX_DIA"] >= 2).astype(int)
        df["FLAG_VEL_3"] = (df["VEL_TRX_DIA"] >= 3).astype(int)
        df["FLAG_VEL_5"] = (df["VEL_TRX_DIA"] >= 5).astype(int)

    # ── BLOQUE B: Monto acumulado ─────────────────────────────
    if "MONTO_ACUM_DIA" not in df.columns and tiene_origen and tiene_fecha:
        mon = (df.groupby([COL_ORIGEN,"_FECHA_DIA"])[COL_MONTO]
               .sum().reset_index(name="MONTO_ACUM_DIA"))
        df = df.merge(mon, on=[COL_ORIGEN,"_FECHA_DIA"], how="left")
        # Flag: monto acumulado supera percentil 90 del total
        p90 = df["MONTO_ACUM_DIA"].quantile(0.90)
        p95 = df["MONTO_ACUM_DIA"].quantile(0.95)
        df["FLAG_MONTO_ACUM_P90"] = (df["MONTO_ACUM_DIA"] > p90).astype(int)
        df["FLAG_MONTO_ACUM_P95"] = (df["MONTO_ACUM_DIA"] > p95).astype(int)

    # ── BLOQUE C: Beneficiario nuevo ─────────────────────────
    if "FLAG_BENEF_NUEVO" not in df.columns and tiene_origen and tiene_dest:
        df = df.sort_values([COL_ORIGEN, COL_DESTINO, COL_FECHA]).reset_index(drop=True)
        df["FREC_PAR_ACUM"] = df.groupby([COL_ORIGEN, COL_DESTINO]).cumcount()
        df["FLAG_BENEF_NUEVO"] = (df["FREC_PAR_ACUM"] == 0).astype(int)

    # ── BLOQUE D: Cuenta mula ─────────────────────────────────
    if "FLAG_CUENTA_MULA" not in df.columns and tiene_dest and tiene_origen:
        mula = (df.groupby([COL_DESTINO,"_FECHA_DIA"])[COL_ORIGEN]
                .nunique().reset_index(name="ORIGENES_DIA"))
        df = df.merge(mula, on=[COL_DESTINO,"_FECHA_DIA"], how="left")
        df["FLAG_CUENTA_MULA"] = (df["ORIGENES_DIA"] >= 3).astype(int)

    # ── BLOQUE E: Cliente reincidente ─────────────────────────
    if "FLAG_CLIENTE_REINCIDENTE" not in df.columns and tiene_cliente:
        df = df.sort_values([COL_CLIENTE, COL_FECHA]).reset_index(drop=True)
        df["FRAUDES_PREV"] = (
            df.groupby(COL_CLIENTE)[COL_IND]
            .transform(lambda x: (x=="F").shift(1).fillna(0).cumsum())
            .astype(int)
        )
        df["FLAG_CLIENTE_REINCIDENTE"] = (df["FRAUDES_PREV"] >= 1).astype(int)

    # ── BLOQUE F: Vaciado de cuenta ───────────────────────────
    if "FLAG_VACIADO" not in df.columns and tiene_saldo:
        df[COL_SALDO] = pd.to_numeric(
            df[COL_SALDO].astype(str).str.replace(",","",regex=False),
            errors="coerce")
        df["RATIO_MONTO_SALDO"] = df[COL_MONTO] / (df[COL_SALDO] + 1e-9)
        df["FLAG_VACIADO"]      = (df["RATIO_MONTO_SALDO"] >= 0.80).astype(int)
        df["FLAG_VACIADO_50"]   = (df["RATIO_MONTO_SALDO"] >= 0.50).astype(int)

    # ── BLOQUE G: IP ──────────────────────────────────────────
    if tiene_ip and tiene_cliente and tiene_fecha:
        if "FLAG_IP_NUEVA" not in df.columns:
            df = df.sort_values([COL_CLIENTE, COL_FECHA]).reset_index(drop=True)
            # IP ya vista antes por este cliente
            df["_IP_PREV"] = (
                df.groupby(COL_CLIENTE)[COL_IP]
                .transform(lambda x: x.shift(1).fillna(""))
            )
            df["FLAG_IP_NUEVA"] = (
                df[COL_IP].astype(str) != df["_IP_PREV"].astype(str)
            ).astype(int)
        if "FLAG_IP_COMPARTIDA" not in df.columns:
            # Misma IP usada por más de 1 cliente el mismo día
            ip_clientes = (df.groupby([COL_IP,"_FECHA_DIA"])[COL_CLIENTE]
                           .nunique().reset_index(name="CLIENTES_X_IP"))
            df = df.merge(ip_clientes, on=[COL_IP,"_FECHA_DIA"], how="left")
            df["FLAG_IP_COMPARTIDA"] = (df["CLIENTES_X_IP"] > 1).astype(int)

    # ── BLOQUE H: Dispositivo (hash) ──────────────────────────
    if tiene_hash and tiene_cliente and tiene_fecha:
        if "FLAG_DISP_NUEVO" not in df.columns:
            df = df.sort_values([COL_CLIENTE, COL_FECHA]).reset_index(drop=True)
            df["_HASH_PREV"] = (
                df.groupby(COL_CLIENTE)[COL_HASH_DEVICE]
                .transform(lambda x: x.shift(1).fillna(""))
            )
            df["FLAG_DISP_NUEVO"] = (
                df[COL_HASH_DEVICE].astype(str) != df["_HASH_PREV"].astype(str)
            ).astype(int)
        if "FLAG_DISP_COMPARTIDO" not in df.columns:
            disp_cl = (df.groupby([COL_HASH_DEVICE,"_FECHA_DIA"])[COL_CLIENTE]
                       .nunique().reset_index(name="CLIENTES_X_DISP"))
            df = df.merge(disp_cl, on=[COL_HASH_DEVICE,"_FECHA_DIA"], how="left")
            df["FLAG_DISP_COMPARTIDO"] = (df["CLIENTES_X_DISP"] > 1).astype(int)

    # Limpiar auxiliares
    aux = ["_FECHA_DIA","_IP_PREV","_HASH_PREV","FREC_PAR_ACUM",
           "ORIGENES_DIA","FRAUDES_PREV","_IP_PREV","CLIENTES_X_IP","CLIENTES_X_DISP"]
    df.drop(columns=[c for c in aux if c in df.columns], inplace=True)

    return df


# Catálogo completo de flags para el constructor de reglas
FLAGS_CATALOGO = [
    # (columna, etiqueta, descripción, default_activo)
    ("FLAG_VEL_2",           "Vel ≥2/día",        "≥2 transferencias por cuenta origen en el mismo día", False),
    ("FLAG_VEL_3",           "Vel ≥3/día",        "≥3 transferencias por cuenta origen en el mismo día", True),
    ("FLAG_VEL_5",           "Vel ≥5/día",        "≥5 transferencias por cuenta origen en el mismo día", False),
    ("FLAG_MONTO_ACUM_P90",  "Monto Acum P90",   "Monto acumulado del día supera percentil 90",          False),
    ("FLAG_MONTO_ACUM_P95",  "Monto Acum P95",   "Monto acumulado del día supera percentil 95",          False),
    ("FLAG_BENEF_NUEVO",     "Benef. Nuevo",      "Primera vez que origen transfiere a ese destino",      True),
    ("FLAG_CUENTA_MULA",     "Cuenta Mula",       "Destino recibe de ≥3 cuentas origen distintas/día",   False),
    ("FLAG_CLIENTE_REINCIDENTE","Reincidente",    "Cliente ya tuvo un fraude confirmado anteriormente",   True),
    ("FLAG_VACIADO",         "Vaciado ≥80%",      "Monto ≥80% del saldo disponible",                     False),
    ("FLAG_VACIADO_50",      "Vaciado ≥50%",      "Monto ≥50% del saldo disponible",                     False),
    ("FLAG_IP_NUEVA",        "IP Nueva",          "IP diferente a la habitual del cliente",               False),
    ("FLAG_IP_COMPARTIDA",   "IP Compartida",     "Misma IP usada por múltiples clientes ese día",        False),
    ("FLAG_DISP_NUEVO",      "Disp. Nuevo",       "Hash de dispositivo diferente al habitual",            False),
    ("FLAG_DISP_COMPARTIDO", "Disp. Compartido",  "Mismo dispositivo usado por múltiples clientes",       False),
]


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 3 — CARGA
# ══════════════════════════════════════════════════════════════

@st.cache_data(show_spinner="⚙️ Cargando y procesando datos…")
def cargar_datos():
    df = pd.read_parquet(PARQUET_PATH)
    df[COL_IND]      = df[COL_IND].astype(str).str.strip().str.upper()
    df[COL_SEGMENTO] = df[COL_SEGMENTO].astype(str).str.strip()
    df[COL_MONTO]    = pd.to_numeric(df[COL_MONTO], errors="coerce")
    if COL_FECHA in df.columns:
        df[COL_FECHA] = pd.to_datetime(df[COL_FECHA], errors="coerce")
        if "HORA"       not in df.columns: df["HORA"]       = df[COL_FECHA].dt.hour
        if "DIA_SEMANA" not in df.columns: df["DIA_SEMANA"] = df[COL_FECHA].dt.day_name()
        if "MES_NUM"    not in df.columns: df["MES_NUM"]    = df[COL_FECHA].dt.month
    df = construir_canal_joy(df)
    df = construir_validacion(df)
    df = construir_flags(df)
    return df


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 4 — HELPERS PLOTLY
# ══════════════════════════════════════════════════════════════

LAYOUT_BASE = dict(
    paper_bgcolor=C["bg"],
    plot_bgcolor=C["panel"],
    font=dict(color=C["text"], family="sans-serif"),
    margin=dict(t=40, b=40, l=50, r=30),
    xaxis=dict(gridcolor=C["border"], linecolor=C["border"],
               showgrid=True, zeroline=False),
    yaxis=dict(gridcolor=C["border"], linecolor=C["border"],
               showgrid=True, zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=C["border"]),
    hoverlabel=dict(bgcolor=C["panel"], bordercolor=C["border"],
                    font=dict(color=C["text"])),
)

def apply_layout(fig, title="", height=380):
    fig.update_layout(**LAYOUT_BASE, height=height,
                      title=dict(text=title, font=dict(size=13), x=0.01))
    return fig

def interpretacion(texto):
    st.markdown(
        f'<div style="background:#0f1a2e;border-left:3px solid #4e9fd4;'
        f'border-radius:0 8px 8px 0;padding:8px 14px;margin:4px 0 16px 0;'
        f'font-size:12px;color:#8892aa;line-height:1.5">'
        f'💡 <b style="color:#4e9fd4">Interpretación:</b> {texto}</div>',
        unsafe_allow_html=True
    )

def kpi_card(label, value, color="blue"):
    mapa = {"red":C["F"],"blue":C["accent"],"gold":C["gold"],"green":C["G"]}
    col = mapa.get(color, C["accent"])
    return (f'<div style="background:linear-gradient(135deg,#151f35,#1a2847);'
            f'border:1px solid #243050;border-radius:12px;padding:14px 12px;'
            f'text-align:center;">'
            f'<div style="font-size:10px;color:{C["muted"]};text-transform:uppercase;'
            f'letter-spacing:1.2px;margin-bottom:4px">{label}</div>'
            f'<div style="font-size:19px;font-weight:700;color:{col}">{value}</div>'
            f'</div>')


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 5 — PÁGINA
# ══════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Fraud Rule Builder · Scotiabank",
    page_icon="🛡️", layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(f"""
<style>
  .stApp {{ background:{C["bg"]}; color:{C["text"]}; }}
  section[data-testid="stSidebar"] {{
      background:#0d1420; border-right:1px solid {C["border"]}; }}
  .stTabs [data-baseweb="tab-list"] {{
      gap:3px; background:#0d1420; border-radius:10px; padding:4px; }}
  .stTabs [data-baseweb="tab"] {{
      border-radius:8px; padding:7px 18px;
      color:{C["muted"]}; font-size:13px; }}
  .stTabs [aria-selected="true"] {{
      background:{C["F"]} !important; color:white !important; }}
  hr {{ border-color:{C["border"]}; margin:8px 0; }}
  .stDataFrame {{ background:{C["panel"]}; }}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 6 — SIDEBAR
# ══════════════════════════════════════════════════════════════

df_full = cargar_datos()
MONTO_MAX = int(df_full[COL_MONTO].max())

with st.sidebar:
    st.markdown(f"## 🛡️ Fraud Rule Builder")
    st.markdown(f"<span style='color:{C['muted']};font-size:11px'>"
                f"Scotiabank Perú · Prevención de Fraude</span>",
                unsafe_allow_html=True)
    st.markdown("---")

    segs_disp   = sorted(df_full[COL_SEGMENTO].unique())
    segs_labels = {s: f"{s} — {SEG_NOMBRE.get(s,s)}" for s in segs_disp}
    seg_sel = st.multiselect("🏷️ Segmento", segs_disp,
                              default=["31","32","33","34"],
                              format_func=lambda s: segs_labels[s])

    st.markdown("---")
    st.markdown(f"<span style='color:{C['muted']};font-size:12px'>"
                f"<b>Rango de Monto (S/)</b></span>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    monto_min = c1.number_input("Desde", 0, 9999999, 2000, 500)
    monto_max = c2.number_input("Hasta", 0, 9999999, MONTO_MAX, 500)

    st.markdown("---")
    hora_rango = st.slider("🕐 Ventana horaria", 0, 23, (0, 23))

    st.markdown("---")
    canales_disp = sorted(df_full["CANAL_JOY"].dropna().unique())
    canal_sel = st.multiselect("📱 Canal Joy", canales_disp, default=canales_disp)

    st.markdown("---")
    vals_disp = sorted(df_full["TIPO_VALIDACION"].dropna().unique())
    val_sel = st.multiselect("🔐 Validación", vals_disp, default=vals_disp)

    st.markdown("---")
    inds_disp = sorted(df_full[COL_IND].unique())
    inds_sel = st.multiselect("🎯 Indicadores", inds_disp,
                               default=[i for i in ["F","N","G"] if i in inds_disp])

    st.markdown("---")
    meses_disp = sorted(df_full["MES_NUM"].dropna().unique().astype(int))
    meses_sel = st.multiselect("📅 Período", meses_disp, default=meses_disp,
                                format_func=lambda m: MESES_ES.get(m,str(m)))

    st.markdown("---")
    st.caption("v4.0 · Fraud Rule Builder · Scotiabank Perú")


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 7 — FILTRADO
# ══════════════════════════════════════════════════════════════

if not all([seg_sel, inds_sel, meses_sel, canal_sel, val_sel]):
    st.warning("⚠️ Selecciona al menos un valor en cada filtro."); st.stop()

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
    st.warning("Sin datos con los filtros actuales."); st.stop()

n_total  = len(df_c)
n_f      = (df_c[COL_IND]=="F").sum()
fr       = n_f/n_total*100 if n_total>0 else 0
sev      = df_c.loc[df_c[COL_IND]=="F",COL_MONTO].mean() if n_f>0 else 0
med_f    = df_c.loc[df_c[COL_IND]=="F",COL_MONTO].median() if n_f>0 else 0
monto_f  = df_c.loc[df_c[COL_IND]=="F",COL_MONTO].sum()
monto_tot= df_c[COL_MONTO].sum()
pct_mon  = monto_f/monto_tot*100 if monto_tot>0 else 0


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 8 — HEADER + KPIs
# ══════════════════════════════════════════════════════════════

seg_txt   = " · ".join([segs_labels[s] for s in seg_sel]) if len(seg_sel)<=3 \
            else f"{len(seg_sel)} segmentos"
canal_txt = " · ".join(canal_sel) if len(canal_sel)<=3 else f"{len(canal_sel)} canales"

st.markdown("## 🛡️ Fraud Rule Builder — Transferencias a Terceros")
st.markdown(
    f"<span style='color:{C['muted']};font-size:13px'>"
    f"<b style='color:{C['text']}'>{seg_txt}</b> &nbsp;·&nbsp; "
    f"{canal_txt} &nbsp;·&nbsp; "
    f"S/ {monto_min:,} – S/ {monto_max:,} &nbsp;·&nbsp; "
    f"Hora {hora_rango[0]:02d}–{hora_rango[1]:02d}h</span>",
    unsafe_allow_html=True)
st.markdown("---")

kpis = [
    ("Total Txn",          f"{n_total:,}",          "blue"),
    ("Fraudes (F)",        f"{n_f:,}",               "red"),
    ("Fraud Rate",         f"{fr:.3f}%",             "red"),
    ("Severidad Media",    f"S/ {sev:,.0f}",         "gold"),
    ("Monto Total Fraude", f"S/ {monto_f:,.0f}",    "red"),
    ("% Monto en Fraude",  f"{pct_mon:.2f}%",       "gold"),
]
cols_kpi = st.columns(len(kpis))
for col_k, (lbl,val,clr) in zip(cols_kpi, kpis):
    with col_k: st.markdown(kpi_card(lbl,val,clr), unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
#  SECCIÓN 9 — TABS
# ══════════════════════════════════════════════════════════════

tab1,tab2,tab3,tab4 = st.tabs([
    "📊 Perfil del Fraude",
    "🕐 Análisis Temporal",
    "📡 Canal & Validación",
    "⚙️ Constructor de Reglas",
])

BINS_RANGO   = [0,5000,10000,20000,50000,np.inf]
LABELS_RANGO = ["<5k","5k–10k","10k–20k","20k–50k","50k+"]


# ────────────────────────────────────────────────────────────
# TAB 1 — PERFIL DEL FRAUDE
# ────────────────────────────────────────────────────────────
with tab1:

    # ── 1A: Distribución de indicadores ─────────────────────
    col_a, col_b = st.columns(2)
    with col_a:
        vc = df_c[COL_IND].value_counts().reset_index()
        vc.columns = ["Indicador","N"]
        vc["%"] = (vc["N"]/vc["N"].sum()*100).round(2)
        vc["Color"] = vc["Indicador"].map(C).fillna("#999")

        fig = go.Figure(go.Bar(
            x=vc["Indicador"], y=vc["N"],
            marker_color=vc["Color"],
            text=[f"{p:.1f}%" for p in vc["%"]],
            textposition="outside",
            customdata=vc["%"],
            hovertemplate="<b>%{x}</b><br>N: %{y:,}<br>%: %{customdata:.2f}%<extra></extra>"
        ))
        apply_layout(fig, "Distribución de Indicadores", 340)
        st.plotly_chart(fig, use_container_width=True)
        interpretacion(
            "Muestra qué proporción del total corresponde a cada etiqueta. "
            "Un % de F muy bajo es normal — el fraude es raro por diseño. "
            "Lo que importa es el monto acumulado, no la cantidad."
        )

    # ── 1B: Bubble chart FR% × N × Monto por rango ──────────
    with col_b:
        df_c["RANGO"] = pd.cut(df_c[COL_MONTO],
                                bins=BINS_RANGO, labels=LABELS_RANGO, right=False)
        rp = df_c.groupby("RANGO", observed=True).agg(
            N_total  =(COL_IND,"count"),
            N_fraude =(COL_IND, lambda x:(x=="F").sum()),
            Monto_F  =(COL_MONTO, lambda x:
                df_c.loc[(df_c[COL_IND]=="F")&
                         (df_c["RANGO"]==x.name),COL_MONTO].sum())
        ).reset_index()
        rp["FR"] = (rp["N_fraude"]/rp["N_total"]*100).fillna(0)
        rp["size_norm"] = rp["Monto_F"]/max(rp["Monto_F"].max(),1)*60+12

        fig = go.Figure(go.Scatter(
            x=rp["RANGO"], y=rp["FR"],
            mode="markers+text",
            marker=dict(
                size=rp["size_norm"],
                color=rp["N_fraude"],
                colorscale="YlOrRd", showscale=True,
                colorbar=dict(title="N Fraudes", thickness=12),
                line=dict(width=1, color=C["border"])
            ),
            text=["" for _ in rp["RANGO"]],
            customdata=np.stack([rp["N_fraude"],rp["Monto_F"],rp["N_total"]],axis=-1),
            hovertemplate=(
                "<b>%{x}</b><br>Fraud Rate: %{y:.3f}%<br>"
                "N Fraudes: %{customdata[0]}<br>"
                "Monto acum F: S/ %{customdata[1]:,.0f}<br>"
                "N Total: %{customdata[2]:,}<extra></extra>"
            )
        ))
        apply_layout(fig, "Burbuja: FR% × N Fraudes × Monto Acumulado", 340)
        fig.update_yaxes(title_text="Fraud Rate (%)")
        st.plotly_chart(fig, use_container_width=True)
        interpretacion(
            "Burbuja grande = mayor monto acumulado de fraude en ese rango. "
            "Color oscuro = más fraudes. Eje Y = qué tan frecuente es el fraude. "
            "Un rango con burbuja grande pero FR bajo significa: "
            "muchos fraudes de monto pequeño que en total generan más pérdida."
        )

    st.markdown("---")

    # ── 1C: KDE + Boxplot + Percentiles ─────────────────────
    st.markdown("#### Distribución del Monto — F vs N vs G")

    fig = make_subplots(rows=1, cols=3,
                        subplot_titles=["Densidad (KDE)","Boxplot","Percentiles Fraude"])

    for ind in ["F","N","G"]:
        s = df_c.loc[df_c[COL_IND]==ind, COL_MONTO].dropna()
        if len(s) < 5: continue
        # KDE manual
        from scipy.stats import gaussian_kde
        kde = gaussian_kde(s)
        x_range = np.linspace(s.min(), s.quantile(0.99), 200)
        fig.add_trace(go.Scatter(
            x=x_range, y=kde(x_range), name=ind,
            fill="tozeroy", fillcolor=f"rgba{tuple(list(px.colors.hex_to_rgb(C.get(ind,'#999')))+[0.2])}",
            line=dict(color=C.get(ind,"#999"), width=2),
            showlegend=True
        ), row=1, col=1)

    for ind in ["F","N","G"]:
        s = df_c.loc[df_c[COL_IND]==ind, COL_MONTO].dropna()
        if len(s) == 0: continue
        fig.add_trace(go.Box(
            y=s, name=ind, marker_color=C.get(ind,"#999"),
            boxmean=True, showlegend=False
        ), row=1, col=2)

    s_f = df_c.loc[df_c[COL_IND]=="F", COL_MONTO].dropna()
    if len(s_f) > 0:
        pcts = np.arange(0,101,10)
        vals = np.percentile(s_f, pcts)
        fig.add_trace(go.Scatter(
            x=pcts, y=vals, mode="lines+markers",
            line=dict(color=C["F"], width=2),
            marker=dict(size=6, color=C["F"]),
            fill="tozeroy",
            fillcolor=f"rgba(230,57,70,0.15)",
            name="Percentil F", showlegend=False,
            hovertemplate="P%{x}: S/ %{y:,.0f}<extra></extra>"
        ), row=1, col=3)

    apply_layout(fig, "", 400)
    fig.update_xaxes(title_text="Monto (S/)", row=1, col=1)
    fig.update_xaxes(title_text="Indicador",  row=1, col=2)
    fig.update_xaxes(title_text="Percentil",  row=1, col=3)
    fig.update_yaxes(title_text="Densidad",   row=1, col=1)
    fig.update_yaxes(title_text="Monto (S/)", row=1, col=2)
    fig.update_yaxes(title_text="Monto (S/)", row=1, col=3)
    st.plotly_chart(fig, use_container_width=True)
    interpretacion(
        "KDE: si las curvas F y N se superponen mucho, el monto solo no es buen discriminador. "
        "Boxplot: la caja de F más compacta que N indica que el atacante elige montos predecibles. "
        "Percentiles: si la curva sube bruscamente en P80+, hay fraudes de monto muy alto "
        "en la cola que concentran el mayor daño económico."
    )

    # ── 1D: Tabla descriptiva ────────────────────────────────
    st.markdown("#### Descriptivos por Indicador")
    filas=[]
    for ind in ["F","N","G"]:
        s = df_c.loc[df_c[COL_IND]==ind,COL_MONTO].dropna()
        if len(s)==0: continue
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
        interpretacion(
            "Trimedia = (Q1 + 2·Mediana + Q3)/4, más robusta que la media ante outliers. "
            "CV% alto (>100%) indica montos muy variables — difícil usar umbral fijo. "
            "Asimetría >1 confirma cola derecha: pocos fraudes de monto muy alto."
        )


# ────────────────────────────────────────────────────────────
# TAB 2 — ANÁLISIS TEMPORAL
# ────────────────────────────────────────────────────────────
with tab2:

    hora_s = (df_c.groupby("HORA")
              .agg(N_total=(COL_IND,"count"),
                   N_f=(COL_IND,lambda x:(x=="F").sum()))
              .assign(FR=lambda x:x["N_f"]/x["N_total"]*100)
              .reset_index())

    # ── Volumen + FR por hora ────────────────────────────────
    fig = make_subplots(rows=1, cols=2,
                        subplot_titles=["Volumen por Hora","Fraud Rate por Hora (%)"])
    fig.add_trace(go.Bar(x=hora_s["HORA"],y=hora_s["N_total"],
                         name="Total",marker_color=C["border"],
                         hovertemplate="Hora %{x}h<br>Total: %{y:,}<extra></extra>"),
                  row=1,col=1)
    fig.add_trace(go.Bar(x=hora_s["HORA"],y=hora_s["N_f"],
                         name="Fraude",marker_color=C["F"],
                         hovertemplate="Hora %{x}h<br>Fraudes: %{y}<extra></extra>"),
                  row=1,col=1)

    media_fr = hora_s["FR"].mean()
    fig.add_trace(go.Bar(
        x=hora_s["HORA"], y=hora_s["FR"],
        name="FR%",
        marker_color=[C["F"] if v>media_fr*2 else C["N"] for v in hora_s["FR"]],
        hovertemplate="Hora %{x}h<br>FR: %{y:.3f}%<extra></extra>"
    ), row=1, col=2)
    fig.add_hline(y=media_fr, line_dash="dash",
                  line_color=C["gold"], row=1, col=2,
                  annotation_text=f"Media {media_fr:.2f}%",
                  annotation_font_color=C["gold"])

    apply_layout(fig,"",380)
    fig.update_xaxes(title_text="Hora",dtick=2)
    st.plotly_chart(fig,use_container_width=True)
    interpretacion(
        "Barras rojas en FR% = horas con fraude muy por encima de la media. "
        "Compara siempre con el volumen: una hora con FR alto pero muy pocas "
        "transacciones puede ser un dato poco representativo. "
        "El pico de mayor FR es el candidato principal para restricción horaria en la regla."
    )

    # ── Heatmap hora × día ───────────────────────────────────
    st.markdown("#### Heatmap — Fraud Rate % por Hora y Día")
    pivot_hm = (df_c.assign(FB=lambda x:(x[COL_IND]=="F").astype(int))
                .groupby(["DIA_SEMANA","HORA"])["FB"]
                .mean()*100).unstack("HORA").fillna(0)
    pivot_hm = pivot_hm.reindex([d for d in ORDEN_DIAS if d in pivot_hm.index])
    pivot_hm.index = [DIAS_ES.get(d,d) for d in pivot_hm.index]

    fig = go.Figure(go.Heatmap(
        z=pivot_hm.values,
        x=[str(h) for h in pivot_hm.columns],
        y=pivot_hm.index.tolist(),
        colorscale="YlOrRd",
        hovertemplate="Día: %{y}<br>Hora: %{x}h<br>FR: %{z:.2f}%<extra></extra>",
        colorbar=dict(title="FR%", thickness=14)
    ))
    apply_layout(fig,"",300)
    fig.update_xaxes(title_text="Hora del día")
    st.plotly_chart(fig,use_container_width=True)
    interpretacion(
        "Pasa el mouse sobre cada celda para ver el FR exacto. "
        "Las celdas más oscuras (rojo intenso) son las combinaciones hora-día "
        "de mayor riesgo — esas son las candidatas directas para condiciones "
        "de horario en las reglas de Monitor."
    )

    # ── Por día y por mes ────────────────────────────────────
    col3, col4 = st.columns(2)
    with col3:
        dia_s = (df_c.groupby("DIA_SEMANA")
                 .agg(N_total=(COL_IND,"count"),
                      N_f=(COL_IND,lambda x:(x=="F").sum()))
                 .assign(FR=lambda x:x["N_f"]/x["N_total"]*100)
                 .reindex([d for d in ORDEN_DIAS if d in df_c["DIA_SEMANA"].values])
                 .reset_index())
        fig = go.Figure(go.Bar(
            x=[DIAS_ES.get(d,d) for d in dia_s["DIA_SEMANA"]],
            y=dia_s["FR"],
            marker_color=[C["F"] if d in ["Saturday","Sunday"] else C["N"]
                          for d in dia_s["DIA_SEMANA"]],
            hovertemplate="%{x}<br>FR: %{y:.3f}%<extra></extra>"
        ))
        apply_layout(fig,"Fraud Rate por Día de Semana",320)
        st.plotly_chart(fig,use_container_width=True)
        interpretacion(
            "Sábado y domingo en rojo porque habitualmente tienen FR más alto "
            "al haber menor supervisión operativa. Si en tu filtro actual "
            "los días laborales son más altos, el patrón es de ingeniería social "
            "en horario laboral — diferente mecanismo de ataque."
        )

    with col4:
        mes_s = (df_c.groupby("MES_NUM")
                 .agg(N_total=(COL_IND,"count"),
                      N_f=(COL_IND,lambda x:(x=="F").sum()))
                 .assign(FR=lambda x:x["N_f"]/x["N_total"]*100)
                 .reset_index())
        mes_s["MES"] = mes_s["MES_NUM"].map(MESES_ES)
        fig = go.Figure()
        fig.add_trace(go.Bar(x=mes_s["MES"],y=mes_s["FR"],
                             marker_color=C["border"],name="FR%",
                             hovertemplate="%{x}<br>FR: %{y:.3f}%<extra></extra>"))
        fig.add_trace(go.Scatter(x=mes_s["MES"],y=mes_s["FR"],
                                 mode="lines+markers",
                                 line=dict(color=C["F"],width=2),
                                 marker=dict(size=8,color=C["F"]),
                                 name="Tendencia",
                                 hovertemplate="%{x}: %{y:.3f}%<extra></extra>"))
        apply_layout(fig,"Evolución Fraud Rate por Mes",320)
        st.plotly_chart(fig,use_container_width=True)
        interpretacion(
            "Si el FR sube mes a mes, el patrón de ataque se está intensificando. "
            "Si baja, puede ser efecto de una regla activa o estacionalidad. "
            "Compara siempre contra el N de fraudes — un FR que sube con N constante "
            "indica menos transacciones normales, no más fraude real."
        )


# ────────────────────────────────────────────────────────────
# TAB 3 — CANAL & VALIDACIÓN
# ────────────────────────────────────────────────────────────
with tab3:
    col_ca, col_cb = st.columns(2)

    with col_ca:
        canal_s = (df_c.groupby("CANAL_JOY")
                   .agg(N_total=(COL_IND,"count"),
                        N_f=(COL_IND,lambda x:(x=="F").sum()),
                        Monto_F=(COL_MONTO,lambda x:
                            df_c.loc[(df_c[COL_IND]=="F")&
                                     (df_c["CANAL_JOY"]==x.name),COL_MONTO].sum()))
                   .assign(FR=lambda x:x["N_f"]/x["N_total"]*100)
                   .sort_values("FR",ascending=True)
                   .reset_index())
        fig = go.Figure(go.Bar(
            x=canal_s["FR"], y=canal_s["CANAL_JOY"],
            orientation="h",
            marker=dict(
                color=canal_s["FR"],
                colorscale="YlOrRd", showscale=False,
                line=dict(width=0)
            ),
            customdata=np.stack([canal_s["N_f"],canal_s["Monto_F"],
                                 canal_s["N_total"]],axis=-1),
            hovertemplate=(
                "<b>%{y}</b><br>FR: %{x:.3f}%<br>"
                "Fraudes: %{customdata[0]}<br>"
                "Monto F: S/ %{customdata[1]:,.0f}<br>"
                "Total txn: %{customdata[2]:,}<extra></extra>"
            )
        ))
        apply_layout(fig,"Fraud Rate por Canal Joy",350)
        fig.update_xaxes(title_text="Fraud Rate (%)")
        st.plotly_chart(fig,use_container_width=True)
        interpretacion(
            "El canal con mayor FR es el vector de ataque preferido. "
            "Joy Web suele tener mayor FR porque el atacante puede operar "
            "desde cualquier navegador sin necesidad del dispositivo físico del cliente. "
            "Un FR alto en App puede indicar ATO con dispositivo comprometido."
        )

    with col_cb:
        val_s = (df_c.groupby("TIPO_VALIDACION")
                 .agg(N_total=(COL_IND,"count"),
                      N_f=(COL_IND,lambda x:(x=="F").sum()),
                      Monto_F=(COL_MONTO,lambda x:
                          df_c.loc[(df_c[COL_IND]=="F")&
                                   (df_c["TIPO_VALIDACION"]==x.name),COL_MONTO].sum()))
                 .assign(FR=lambda x:x["N_f"]/x["N_total"]*100)
                 .sort_values("FR",ascending=True)
                 .reset_index())
        fig = go.Figure(go.Bar(
            x=val_s["FR"], y=val_s["TIPO_VALIDACION"].astype(str),
            orientation="h",
            marker=dict(color=val_s["FR"],colorscale="YlOrRd",showscale=False),
            customdata=np.stack([val_s["N_f"],val_s["Monto_F"],
                                 val_s["N_total"]],axis=-1),
            hovertemplate=(
                "<b>%{y}</b><br>FR: %{x:.3f}%<br>"
                "Fraudes: %{customdata[0]}<br>"
                "Monto F: S/ %{customdata[1]:,.0f}<br>"
                "Total: %{customdata[2]:,}<extra></extra>"
            )
        ))
        apply_layout(fig,"Fraud Rate por Tipo de Validación",350)
        fig.update_xaxes(title_text="Fraud Rate (%)")
        st.plotly_chart(fig,use_container_width=True)
        interpretacion(
            "Email y SMS son los tipos de validación con mayor FR porque "
            "son interceptables mediante ingeniería social — el atacante "
            "convence al cliente de compartir el código. "
            "Face ID y Touch ID tienen menor FR al requerir biometría física."
        )

    # ── Heatmap Canal × Validación ───────────────────────────
    st.markdown("#### Canal × Validación — Fraud Rate %")
    pivot_cv = (df_c.assign(FB=lambda x:(x[COL_IND]=="F").astype(int))
                .groupby(["CANAL_JOY","TIPO_VALIDACION"])["FB"]
                .mean()*100).unstack(fill_value=0)
    if pivot_cv.shape[1] > 0:
        fig = go.Figure(go.Heatmap(
            z=pivot_cv.values,
            x=pivot_cv.columns.tolist(),
            y=pivot_cv.index.tolist(),
            colorscale="YlOrRd",
            hovertemplate="Canal: %{y}<br>Validación: %{x}<br>FR: %{z:.2f}%<extra></extra>",
            colorbar=dict(title="FR%",thickness=14),
            text=np.round(pivot_cv.values,2),
            texttemplate="%{text}%"
        ))
        apply_layout(fig,"",300)
        st.plotly_chart(fig,use_container_width=True)
        interpretacion(
            "La combinación canal + validación más oscura es el vector de ataque "
            "más crítico — ahí es donde la regla tiene más impacto. "
            "Por ejemplo: Joy Web + Email sugiere phishing clásico. "
            "Joy Android + SMS sugiere SIM swapping o smishing."
        )


# ────────────────────────────────────────────────────────────
# TAB 4 — CONSTRUCTOR DE REGLAS
# ────────────────────────────────────────────────────────────
with tab4:
    st.markdown("### ⚙️ Constructor de Reglas")
    st.markdown(
        "Activa los flags que quieres combinar. La app calcula en tiempo real "
        "cuántos fraudes capturas, qué monto alertas y la precisión de la regla."
    )
    st.markdown("---")

    flags_presentes = [(c,e,d,dflt) for c,e,d,dflt in FLAGS_CATALOGO
                       if c in df_c.columns]

    if not flags_presentes:
        st.warning("No hay flags disponibles. Verifica las columnas de origen/destino/cliente.")
    else:
        # ── Selector de flags ────────────────────────────────
        st.markdown("#### 1. Selecciona los flags de la regla")
        flags_activos = []
        cols_chk = st.columns(3)
        for idx,(col_f,etiq,desc,dflt) in enumerate(flags_presentes):
            pct_F = df_c.loc[df_c[COL_IND]=="F",col_f].mean()*100 if n_f>0 else 0
            pct_N = df_c.loc[df_c[COL_IND]=="N",col_f].mean()*100
            sep   = pct_F - pct_N
            badge = "✅" if sep>5 else ("⚠️" if sep>2 else "❌")
            with cols_chk[idx%3]:
                if st.checkbox(
                    f"{badge} {etiq}",
                    value=dflt and sep>5,
                    help=f"{desc}\n\nF: {pct_F:.1f}%  |  N: {pct_N:.1f}%  |  Sep: {sep:+.1f}pp"
                ):
                    flags_activos.append(col_f)

        st.markdown("---")

        # ── Tabla de discriminación ──────────────────────────
        st.markdown("#### 2. Poder discriminante de cada flag")
        filas_d=[]
        for col_f,etiq,desc,_ in flags_presentes:
            pct_F = df_c.loc[df_c[COL_IND]=="F",col_f].mean()*100 if n_f>0 else 0
            pct_N = df_c.loc[df_c[COL_IND]=="N",col_f].mean()*100
            sep   = pct_F-pct_N
            filas_d.append({
                "Flag":etiq,"Descripción":desc,
                "% en F":round(pct_F,1),"% en N":round(pct_N,1),
                "Separación":round(sep,1),
                "Útil":"✅" if sep>5 else ("⚠️" if sep>2 else "❌"),
            })

        # Gráfico de barras horizontales de separación
        df_disc = pd.DataFrame(filas_d).sort_values("Separación",ascending=True)
        fig = go.Figure(go.Bar(
            x=df_disc["Separación"],
            y=df_disc["Flag"],
            orientation="h",
            marker=dict(
                color=df_disc["Separación"],
                colorscale=[[0,"#E63946"],[0.3,"#E9C46A"],[1,"#2A9D8F"]],
                cmin=0, cmax=df_disc["Separación"].max(),
                showscale=False
            ),
            customdata=np.stack([df_disc["% en F"],df_disc["% en N"]],axis=-1),
            hovertemplate=(
                "<b>%{y}</b><br>Separación: %{x:+.1f}pp<br>"
                "% en F: %{customdata[0]:.1f}%<br>"
                "% en N: %{customdata[1]:.1f}%<extra></extra>"
            )
        ))
        apply_layout(fig,"Separación F vs N por Flag (pp)",max(250,len(filas_d)*35))
        fig.update_xaxes(title_text="Separación (pp)",zeroline=True,
                         zerolinecolor=C["border"])
        st.plotly_chart(fig,use_container_width=True)
        interpretacion(
            "Separación = % del flag en fraudes MENOS % en normales. "
            "Verde = discrimina bien (>5pp). Amarillo = discrimina algo (2-5pp). "
            "Rojo = no discrimina o tiene efecto inverso. "
            "Los flags verdes son los candidatos para la regla."
        )

        st.markdown("---")

        # ── Resultado de la regla ────────────────────────────
        st.markdown("#### 3. Resultado de la regla combinada")

        if not flags_activos:
            st.info("Activa al menos un flag para ver el resultado.")
        else:
            mask_r = pd.Series(True, index=df_c.index)
            for f in flags_activos:
                mask_r = mask_r & (df_c[f]==1)

            n_alertas      = mask_r.sum()
            n_cap_F        = (df_c.loc[mask_r,COL_IND]=="F").sum()
            monto_alertado = df_c.loc[mask_r,COL_MONTO].sum()
            monto_cap_F    = df_c.loc[mask_r&(df_c[COL_IND]=="F"),COL_MONTO].sum()
            precision      = n_cap_F/n_alertas*100 if n_alertas>0 else 0
            recall         = n_cap_F/n_f*100        if n_f>0      else 0
            pct_mon_cap    = monto_cap_F/monto_f*100 if monto_f>0 else 0
            falsas         = n_alertas - n_cap_F

            # Condición de la regla
            cond_txt = " AND ".join([f"`{f}=1`" for f in flags_activos])
            st.markdown(
                f'<div style="background:#0f1a2e;border:1px solid #1e3a5f;'
                f'border-radius:10px;padding:14px 18px;margin-bottom:12px">'
                f'<span style="font-size:11px;color:{C["accent"]};'
                f'text-transform:uppercase;letter-spacing:1px">📋 Regla Activa</span><br><br>'
                f'<code style="color:{C["accent"]};font-size:12px">{cond_txt}</code><br><br>'
                f'<span style="color:{C["muted"]};font-size:12px">'
                f'Filtros activos: <b style="color:{C["text"]}">'
                f'Seg {", ".join(seg_sel)}</b> · '
                f'<b style="color:{C["text"]}">S/{monto_min:,}–S/{monto_max:,}</b> · '
                f'<b style="color:{C["text"]}">{canal_txt}</b> · '
                f'<b style="color:{C["text"]}">Hora {hora_rango[0]:02d}–{hora_rango[1]:02d}h'
                f'</b></span></div>',
                unsafe_allow_html=True
            )

            # KPIs de la regla
            rkpis = [
                ("Alertas generadas",    f"{n_alertas:,}",          "blue"),
                ("Fraudes capturados",   f"{n_cap_F} / {n_f}",     "red"),
                ("Recall",               f"{recall:.1f}%",          "green"),
                ("Precisión",            f"{precision:.1f}%",       "gold"),
                ("Falsas alarmas",       f"{falsas:,}",             "gold"),
                ("Monto fraude captado", f"S/ {monto_cap_F:,.0f}", "red"),
            ]
            rcols = st.columns(len(rkpis))
            for rc,(lbl,val,clr) in zip(rcols,rkpis):
                with rc: st.markdown(kpi_card(lbl,val,clr),unsafe_allow_html=True)

            st.markdown("<br>",unsafe_allow_html=True)

            # ── Gráfico gauge de precisión ───────────────────
            col_g1, col_g2, col_g3 = st.columns(3)

            with col_g1:
                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=precision,
                    number={"suffix":"%","font":{"color":C["text"]}},
                    gauge=dict(
                        axis=dict(range=[0,100],tickcolor=C["muted"]),
                        bar=dict(color=C["G"] if precision>30
                                 else (C["gold"] if precision>10 else C["F"])),
                        bgcolor=C["panel"],
                        bordercolor=C["border"],
                        steps=[
                            dict(range=[0,10],color="#2b0a0f"),
                            dict(range=[10,30],color="#2b2000"),
                            dict(range=[30,100],color="#0d2b1e"),
                        ],
                        threshold=dict(line=dict(color=C["gold"],width=2),value=30)
                    ),
                    title={"text":"Precisión","font":{"color":C["muted"],"size":12}}
                ))
                fig.update_layout(paper_bgcolor=C["bg"],
                                  font=dict(color=C["text"]),height=220,
                                  margin=dict(t=30,b=10,l=20,r=20))
                st.plotly_chart(fig,use_container_width=True)

            with col_g2:
                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=recall,
                    number={"suffix":"%","font":{"color":C["text"]}},
                    gauge=dict(
                        axis=dict(range=[0,100],tickcolor=C["muted"]),
                        bar=dict(color=C["G"] if recall>70
                                 else (C["gold"] if recall>40 else C["F"])),
                        bgcolor=C["panel"],
                        bordercolor=C["border"],
                        steps=[
                            dict(range=[0,40],color="#2b0a0f"),
                            dict(range=[40,70],color="#2b2000"),
                            dict(range=[70,100],color="#0d2b1e"),
                        ],
                        threshold=dict(line=dict(color=C["gold"],width=2),value=70)
                    ),
                    title={"text":"Recall","font":{"color":C["muted"],"size":12}}
                ))
                fig.update_layout(paper_bgcolor=C["bg"],
                                  font=dict(color=C["text"]),height=220,
                                  margin=dict(t=30,b=10,l=20,r=20))
                st.plotly_chart(fig,use_container_width=True)

            with col_g3:
                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=pct_mon_cap,
                    number={"suffix":"%","font":{"color":C["text"]}},
                    gauge=dict(
                        axis=dict(range=[0,100],tickcolor=C["muted"]),
                        bar=dict(color=C["G"] if pct_mon_cap>60
                                 else (C["gold"] if pct_mon_cap>30 else C["F"])),
                        bgcolor=C["panel"],
                        bordercolor=C["border"],
                    ),
                    title={"text":"% Monto Fraude Capturado",
                           "font":{"color":C["muted"],"size":12}}
                ))
                fig.update_layout(paper_bgcolor=C["bg"],
                                  font=dict(color=C["text"]),height=220,
                                  margin=dict(t=30,b=10,l=20,r=20))
                st.plotly_chart(fig,use_container_width=True)

            # ── Impacto en monto + veredicto ─────────────────
            col_m1, col_m2 = st.columns(2)
            with col_m1:
                st.markdown(
                    f'<div style="background:#0f1a2e;border:1px solid #1e3a5f;'
                    f'border-radius:10px;padding:14px 18px">'
                    f'<span style="font-size:11px;color:{C["accent"]};'
                    f'text-transform:uppercase;letter-spacing:1px">💰 Impacto en Monto</span><br><br>'
                    f'Monto total alertado: '
                    f'<b style="color:{C["gold"]}">S/ {monto_alertado:,.0f}</b><br>'
                    f'Monto fraude capturado: '
                    f'<b style="color:{C["F"]}">S/ {monto_cap_F:,.0f}</b><br>'
                    f'% del fraude capturado: '
                    f'<b style="color:{C["G"]}">{pct_mon_cap:.1f}%</b><br>'
                    f'Monto fraude visible total: '
                    f'<b style="color:{C["text"]}">S/ {monto_f:,.0f}</b>'
                    f'</div>',
                    unsafe_allow_html=True
                )

            with col_m2:
                if precision>30 and recall>50:
                    tag_color="#0d2b1e"; tag_bdr="#1a4a33"
                    tag_txt=f'<span style="color:{C["G"]}">✅ Regla sólida — proponer al Checker</span>'
                    msg="Alta precisión y buen recall. Candidata directa para implementación."
                elif precision>10:
                    tag_color="#2b2000"; tag_bdr="#4a3800"
                    tag_txt=f'<span style="color:{C["gold"]}">⚠️ Precisión media — afinar</span>'
                    msg="Agrega una condición de hora, monto o canal para reducir falsas alarmas."
                else:
                    tag_color="#2b0a0f"; tag_bdr="#4a1520"
                    tag_txt=f'<span style="color:{C["F"]}">❌ Demasiado ruido — no proponer aún</span>'
                    msg="Muy pocas alertas son fraude real. Combina más flags o restringe los filtros."
                st.markdown(
                    f'<div style="background:{tag_color};border:1px solid {tag_bdr};'
                    f'border-radius:10px;padding:14px 18px">'
                    f'<span style="font-size:11px;color:{C["accent"]};'
                    f'text-transform:uppercase;letter-spacing:1px">🎯 Veredicto</span><br><br>'
                    f'{tag_txt}<br><br>'
                    f'<span style="color:{C["muted"]};font-size:12px">{msg}</span>'
                    f'</div>',
                    unsafe_allow_html=True
                )

            # ── Composición de alertas ────────────────────────
            st.markdown("---")
            st.markdown("#### Composición de las alertas generadas")
            alertas_df = df_c[mask_r].copy()
            if len(alertas_df) > 0:
                alertas_df["RANGO"] = pd.cut(
                    alertas_df[COL_MONTO],
                    bins=BINS_RANGO, labels=LABELS_RANGO, right=False)

                col_c1, col_c2 = st.columns(2)
                with col_c1:
                    vc_a = alertas_df[COL_IND].value_counts().reset_index()
                    vc_a.columns=["Ind","N"]
                    fig = go.Figure(go.Bar(
                        x=vc_a["Ind"],y=vc_a["N"],
                        marker_color=[C.get(i,"#555") for i in vc_a["Ind"]],
                        hovertemplate="%{x}: %{y:,}<extra></extra>"
                    ))
                    apply_layout(fig,"Indicadores en las alertas",300)
                    st.plotly_chart(fig,use_container_width=True)

                with col_c2:
                    rp_a = alertas_df.groupby(
                        ["RANGO","INDICADOR"],observed=True).size().unstack(fill_value=0)
                    fig = go.Figure()
                    for ind in ["F","N","G"]:
                        if ind in rp_a.columns:
                            fig.add_trace(go.Bar(
                                name=ind, x=rp_a.index.tolist(),
                                y=rp_a[ind].values,
                                marker_color=C.get(ind,"#555"),
                                hovertemplate=f"{ind} — %{{x}}: %{{y:,}}<extra></extra>"
                            ))
                    fig.update_layout(barmode="stack")
                    apply_layout(fig,"Alertas por Rango de Monto",300)
                    st.plotly_chart(fig,use_container_width=True)

                interpretacion(
                    "Si en las alertas la proporción de F es alta → la regla es precisa. "
                    "Si hay muchas N → revisa qué flag está generando falsos positivos. "
                    "El rango de monto con más F concentrado es donde la regla tiene "
                    "mayor impacto económico."
                )
