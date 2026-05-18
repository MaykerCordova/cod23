# =============================================================================
# SIMULADOR DE REGLAS — MCC 7995 | Juegos de Azar
# Carga el parquet, recalcula features con caché, y permite explorar
# combinaciones de condiciones para detectar fraude con mínimo FP.
# =============================================================================
import streamlit as st
import pandas as pd
import numpy as np
import polars as pl
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(
    page_title="Simulador Reglas MCC 7995",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CONSTANTES (mismas que el script de análisis) ───────────────────────────
COLS = {
    "FECHA"           : "ACF-FECHA TRX",
    "HORA"            : "ACF-HORA TRX",
    "DATETIME"        : "FECHA_HORA",
    "COMERCIO"        : "ACF-NOMBRE/LOCALIZACION COMERCIO",
    "INDICADOR"       : "ACF-INDICADOR DE FRAUDE",
    "COD_RESPUESTA"   : "ACF-COD RPTA",
    "CONDICION_RT"    : "CONDICION RT",
    "CANAL"           : "ACF-CANAL",
    "ENTRY_MODE"      : "ACF-ENTRY MODE",
    "SALDO"           : "ACF-SALDO DISPONIBLE EN MONEDA TRX",
    "ID_CLIENTE"      : "ACF-ID CLIENTE",
    "ESI_UCAP"        : "ACF-ECI/UCAF",
    "PAIS"            : "ACF-PAIS ORIGEN 87519",
    "COD_RED_COMERCIO": "ACF-COD RED COMERCIO",
    "MONTO"           : "ACF-MONTO EN MONEDA LOCAL",
    "SEGMENTO"        : "VAA-EVENTO DE COMPROMISO OTRA FUENTE",
    "TIPO_PRODUCTO"   : "ACF-TIPO PROD TC",
    "BIN"             : "ACF-BIN",
}
SEG_NOMBRE = {
    "30":"Polo Direccion","99":"Polo Direccion","31":"Premium","32":"Preferente",
    "33":"Personal","34":"Estandar","5":"Inst. Financieras","21":"Corporativo",
    "2":"Mediano Empresas","15":"Sector Gobierno","16":"Otras Instituciones",
    "3":"Pequenas Empresas","4":"Negocios 2","7":"Negocios 3","8":"Negocios 1",
    "13":"Microempresas",
}
SEG_GRUPO = {
    "30":"Affluent","99":"Affluent","31":"Emerging Affluent","32":"Emerging Affluent",
    "33":"Top of Mass","34":"Mass","5":"Corporate","21":"Corporate","2":"Commercial",
    "15":"Commercial","16":"Commercial","3":"Small Business","4":"Small Business",
    "7":"Small Business","8":"Small Business","13":"Small Business",
}
COD_RED_LABEL = {
    "S":"Estatico (TD)","D":"Dinamico (TD/TC)","E":"Estatico (TC)","N":"No Match / Sin CVV",
}


# ─── CARGA Y FEATURE ENGINEERING (cacheado) ──────────────────────────────────
@st.cache_data(show_spinner="Calculando features... (solo la primera vez)")
def cargar_datos(ruta_parquet: str) -> pd.DataFrame:
    df_raw = pd.read_parquet(ruta_parquet)
    col_map = {v: k for k, v in COLS.items()}
    df = df_raw.rename(columns=col_map).copy()

    df["MONTO"]    = pd.to_numeric(df["MONTO"],   errors="coerce")
    df["SALDO"]    = pd.to_numeric(df["SALDO"],   errors="coerce")
    df["DATETIME"] = pd.to_datetime(df["DATETIME"], errors="coerce")
    df["FECHA"]    = df["DATETIME"].dt.normalize()
    df["MES"]      = df["DATETIME"].dt.to_period("M").astype(str)

    for c in ["INDICADOR","COD_RESPUESTA","CONDICION_RT","CANAL",
              "ESI_UCAP","COD_RED_COMERCIO","SEGMENTO","TIPO_PRODUCTO","ENTRY_MODE"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.upper()
    if "BIN" in df.columns:
        df["BIN"] = df["BIN"].astype(str).str.split(".").str[0].str.strip()

    df["ESTADO"]             = df["COD_RESPUESTA"].apply(
        lambda x: "APROBADA" if str(x).strip() in ["00","0000","000","0"] else "DENEGADA"
    )
    df["ES_FRAUDE"]          = (df["INDICADOR"] == "F").astype(int)
    df["ES_FRAUDE_APROBADO"] = ((df["INDICADOR"] == "F") & (df["ESTADO"] == "APROBADA")).astype(int)
    df["COND_088"]           = df["CONDICION_RT"].str.contains("088", na=False)
    df["SEGURO"]             = df["ESI_UCAP"].apply(
        lambda x: "Seguro" if str(x).strip() in ["2","5","02","05"] else "No Seguro"
    )
    df["SEG_NOMBRE"]   = df["SEGMENTO"].map(SEG_NOMBRE).fillna("Otro/Sin seg")
    df["SEG_GRUPO"]    = df["SEGMENTO"].map(SEG_GRUPO).fillna("Otro/Sin seg")
    df["COD_RED_LABEL"]= df["COD_RED_COMERCIO"].map(COD_RED_LABEL).fillna("Otro")

    df_ap  = df[df["ESTADO"] == "APROBADA"].copy()
    df_den = df[df["ESTADO"] == "DENEGADA"].copy()

    # ── Feature engineering con Polars ───────────────────────────────────────
    cols_feat = ["ID_CLIENTE","DATETIME","COMERCIO","MONTO","SALDO",
                 "ES_FRAUDE","ES_FRAUDE_APROBADO","ESTADO","INDICADOR",
                 "SEG_NOMBRE","SEG_GRUPO","TIPO_PRODUCTO","SEGURO",
                 "CANAL","COD_RED_LABEL","PAIS","MES","COND_088"]
    if "BIN" in df.columns:
        cols_feat.append("BIN")

    plf_all = pl.from_pandas(df[cols_feat].reset_index(drop=True))
    plf_all = plf_all.with_columns(pl.col("DATETIME").cast(pl.Datetime("us")))
    plf_all = plf_all.sort(["ID_CLIENTE","DATETIME"])
    plf_ap  = plf_all.filter(pl.col("ESTADO") == "APROBADA")
    plf_den = plf_all.filter(pl.col("ESTADO") == "DENEGADA")

    # F1: Ratio Monto/Saldo
    plf_ap = plf_ap.with_columns(
        pl.when(pl.col("SALDO") > 0)
        .then(pl.col("MONTO") / pl.col("SALDO"))
        .otherwise(None)
        .alias("RATIO_MONTO_SALDO")
    )
    # F2: Z-score monto por cliente
    plf_ap = plf_ap.with_columns([
        pl.col("MONTO").mean().over("ID_CLIENTE").alias("_mean_cli"),
        pl.col("MONTO").std().over("ID_CLIENTE").alias("_std_cli"),
    ])
    plf_ap = plf_ap.with_columns(
        pl.when(pl.col("_std_cli") > 0)
        .then((pl.col("MONTO") - pl.col("_mean_cli")) / pl.col("_std_cli"))
        .otherwise(0.0)
        .alias("ZSCORE_MONTO_CLI")
    ).drop(["_mean_cli","_std_cli"])

    # F3: Comercio nuevo
    plf_ap = plf_ap.with_columns(
        pl.lit(1).cum_sum().over(["ID_CLIENTE","COMERCIO"]).alias("_rank_com")
    )
    plf_ap = plf_ap.with_columns(
        (pl.col("_rank_com") == 1).cast(pl.Int32).alias("ES_COMERCIO_NUEVO")
    ).drop("_rank_com")

    # F4: N° comercios distintos en el día
    plf_ap = plf_ap.with_columns(pl.col("DATETIME").dt.date().alias("FECHA_DIA"))
    n_com_dia = (
        plf_ap.group_by(["ID_CLIENTE","FECHA_DIA"])
        .agg(pl.col("COMERCIO").n_unique().alias("N_COMERCIOS_DIA"))
    )
    plf_ap = plf_ap.join(n_com_dia, on=["ID_CLIENTE","FECHA_DIA"], how="left")

    # F5: Franja horaria
    plf_ap = plf_ap.with_columns(pl.col("DATETIME").dt.hour().alias("HORA_NUM"))
    plf_ap = plf_ap.with_columns(
        pl.when(pl.col("HORA_NUM") <= 5).then(pl.lit("00-05 Madrugada"))
        .when(pl.col("HORA_NUM") <= 11).then(pl.lit("06-11 Manana"))
        .when(pl.col("HORA_NUM") <= 17).then(pl.lit("12-17 Tarde"))
        .when(pl.col("HORA_NUM") <= 20).then(pl.lit("18-20 Noche"))
        .otherwise(pl.lit("21-23 Noche Tardia"))
        .alias("FRANJA_HORARIA")
    )

    # F6: Días desde última transacción
    plf_ap = plf_ap.with_columns(
        pl.col("DATETIME").shift(1).over("ID_CLIENTE").alias("_prev_dt")
    )
    plf_ap = plf_ap.with_columns(
        ((pl.col("DATETIME") - pl.col("_prev_dt")).dt.total_seconds() / 86400)
        .alias("DIAS_DESDE_ULTIMA_TRX")
    ).drop("_prev_dt")

    # F7: Monto acumulado últimas 2 horas
    plf_ap_sorted = plf_ap.sort("DATETIME")
    monto_rolling = (
        plf_ap_sorted
        .rolling(index_column="DATETIME", period="2h", group_by="ID_CLIENTE")
        .agg(pl.col("MONTO").sum().alias("_monto_acum_2h_total"))
    )
    plf_ap = plf_ap.with_row_index("_idx")
    monto_rolling = monto_rolling.with_row_index("_idx")
    plf_ap = plf_ap.join(
        monto_rolling.select(["_idx","_monto_acum_2h_total"]), on="_idx", how="left"
    )
    plf_ap = plf_ap.with_columns(
        (pl.col("_monto_acum_2h_total") - pl.col("MONTO")).clip(0, None).alias("MONTO_ACUM_2H")
    ).drop(["_monto_acum_2h_total","_idx"])

    # F8-F9: Rechazos previos 24h
    if len(plf_den) > 0:
        plf_den_cum = (
            plf_den.sort(["ID_CLIENTE","DATETIME"])
            .with_columns(pl.lit(1).cum_sum().over("ID_CLIENTE").alias("CUM_DEN"))
            .select(["ID_CLIENTE","DATETIME","CUM_DEN"])
        )
        plf_ap_rec = plf_ap.sort("DATETIME").with_row_index("_idx_ap")
        merged_now = plf_ap_rec.select(["_idx_ap","ID_CLIENTE","DATETIME"]).join_asof(
            plf_den_cum.rename({"CUM_DEN":"CUM_DEN_NOW"}),
            on="DATETIME", by="ID_CLIENTE", strategy="backward"
        )
        plf_ap_24h = plf_ap_rec.with_columns(
            (pl.col("DATETIME") - pl.duration(hours=24)).alias("DT_24H")
        ).select(["_idx_ap","ID_CLIENTE","DT_24H"])
        merged_24h = plf_ap_24h.rename({"DT_24H":"DATETIME"}).join_asof(
            plf_den_cum.rename({"CUM_DEN":"CUM_DEN_24H"}),
            on="DATETIME", by="ID_CLIENTE", strategy="backward"
        )
        rechazos = merged_now.select(["_idx_ap","CUM_DEN_NOW"]).join(
            merged_24h.select(["_idx_ap","CUM_DEN_24H"]), on="_idx_ap", how="left"
        ).with_columns(
            (pl.col("CUM_DEN_NOW").fill_null(0) - pl.col("CUM_DEN_24H").fill_null(0))
            .clip(0, None).cast(pl.Int32).alias("N_RECHAZOS_24H")
        )
        plf_ap = plf_ap.with_row_index("_idx_ap").join(
            rechazos.select(["_idx_ap","N_RECHAZOS_24H"]), on="_idx_ap", how="left"
        ).drop("_idx_ap")
    else:
        plf_ap = plf_ap.with_columns(pl.lit(0).alias("N_RECHAZOS_24H"))

    plf_ap = plf_ap.with_columns(
        (pl.col("N_RECHAZOS_24H") / (pl.col("N_RECHAZOS_24H") + 1)).alias("RATIO_FALLIDOS")
    )

    # F10: FR histórico del comercio
    fr_com = (
        plf_ap.group_by("COMERCIO")
        .agg(
            (pl.col("ES_FRAUDE_APROBADO").sum() / pl.col("ES_FRAUDE_APROBADO").count())
            .alias("FR_HISTORICO_COMERCIO")
        )
    )
    plf_ap = plf_ap.join(fr_com, on="COMERCIO", how="left")

    # F11: Concentración BIN
    if "BIN" in plf_ap.columns:
        total_com = plf_ap.group_by("COMERCIO").agg(pl.count().alias("_n_total"))
        top_bin = (
            plf_ap.group_by(["COMERCIO","BIN"]).agg(pl.count().alias("_n_bin"))
            .sort("_n_bin", descending=True)
            .group_by("COMERCIO").first()
            .select(["COMERCIO","_n_bin"])
        )
        conc = top_bin.join(total_com, on="COMERCIO").with_columns(
            (pl.col("_n_bin") / pl.col("_n_total")).alias("CONC_BIN_COMERCIO")
        ).select(["COMERCIO","CONC_BIN_COMERCIO"])
        plf_ap = plf_ap.join(conc, on="COMERCIO", how="left")
    else:
        plf_ap = plf_ap.with_columns(pl.lit(None).cast(pl.Float64).alias("CONC_BIN_COMERCIO"))

    df_feat = plf_ap.to_pandas()

    # Buckets
    df_feat["BUCKET_RATIO_SALDO"] = pd.cut(
        df_feat["RATIO_MONTO_SALDO"].clip(0, 2),
        bins=[0, 0.1, 0.3, 0.5, 0.8, 2],
        labels=["<10%","10-30%","30-50%","50-80%",">80%"], include_lowest=True
    )
    df_feat["BUCKET_ZSCORE"] = pd.cut(
        df_feat["ZSCORE_MONTO_CLI"].clip(-3, 3),
        bins=[-3, -2, -1, 0, 1, 2, 3],
        labels=["<-2SD","-2a-1SD","-1a0SD","0a1SD","1a2SD",">2SD"], include_lowest=True
    )
    df_feat["BUCKET_RECHAZOS"] = pd.cut(
        df_feat["N_RECHAZOS_24H"].clip(0, 10),
        bins=[-0.001, 0, 1, 2, 3, 10],
        labels=["0 rech","1 rech","2 rech","3 rech","4+ rech"], include_lowest=True
    )
    df_feat["BUCKET_FR_COMERCIO"] = pd.cut(
        df_feat["FR_HISTORICO_COMERCIO"].clip(0, 0.1),
        bins=[-0.001, 0.001, 0.005, 0.01, 0.05, 0.1],
        labels=["<0.1%","0.1-0.5%","0.5-1%","1-5%",">5%"], include_lowest=True
    )

    return df_feat


# ─── HELPERS ─────────────────────────────────────────────────────────────────
def metric_card(col, label: str, value: str, delta: str = "", color: str = "#1F3864"):
    col.markdown(
        f"""
        <div style="background:{color};border-radius:8px;padding:14px 18px;text-align:center;">
          <div style="color:#cce4ff;font-size:0.78rem;font-weight:600;letter-spacing:0.05em">{label}</div>
          <div style="color:#ffffff;font-size:1.55rem;font-weight:700;margin:4px 0">{value}</div>
          <div style="color:#aad0f5;font-size:0.80rem">{delta}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def barra_doble(label_a, n_a, label_b, n_b, total):
    pct_a = n_a / total * 100 if total > 0 else 0
    pct_b = n_b / total * 100 if total > 0 else 0
    fig = go.Figure()
    fig.add_trace(go.Bar(name=label_a, x=[pct_a], orientation="h",
                         marker_color="#D9534F", text=[f"{pct_a:.2f}%"], textposition="inside"))
    fig.add_trace(go.Bar(name=label_b, x=[pct_b], orientation="h",
                         marker_color="#5B9BD5", text=[f"{pct_b:.2f}%"], textposition="inside"))
    fig.update_layout(barmode="group", height=120, margin=dict(l=0,r=0,t=0,b=0),
                      xaxis_title="%", showlegend=True,
                      legend=dict(orientation="h", y=1.3))
    return fig


def breakdown_chart(df_base, col_dim, title):
    gb = (df_base.groupby(col_dim, observed=True)
          .agg(Total=("ES_FRAUDE_APROBADO","count"),
               Fraude=("ES_FRAUDE_APROBADO","sum"))
          .reset_index())
    gb["Legitimas"] = gb["Total"] - gb["Fraude"]
    gb["FR%"] = (gb["Fraude"] / gb["Total"] * 100).round(2)
    gb = gb.sort_values("Fraude", ascending=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(name="Fraude Ap.", y=gb[col_dim], x=gb["Fraude"],
                         orientation="h", marker_color="#D9534F",
                         text=gb["FR%"].map(lambda v: f"{v:.2f}%"), textposition="outside"))
    fig.add_trace(go.Bar(name="Legítimas", y=gb[col_dim], x=gb["Legitimas"],
                         orientation="h", marker_color="#5B9BD5"))
    fig.update_layout(barmode="stack", title=title, height=max(200, len(gb)*40+80),
                      margin=dict(l=10, r=10, t=40, b=10),
                      legend=dict(orientation="h", y=-0.15))
    return fig


# ─── UI ───────────────────────────────────────────────────────────────────────
st.markdown(
    "<h2 style='margin-bottom:0'>🎰 Simulador de Reglas — MCC 7995</h2>"
    "<p style='color:#666;margin-top:4px'>Ajusta las condiciones en el panel izquierdo y ve en tiempo real cuánto fraude capturarías y cuántas transacciones legítimas bloquearías.</p>",
    unsafe_allow_html=True,
)
st.divider()

# ─── SIDEBAR: CONFIG ─────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Configuración")
    ruta_parquet = st.text_input(
        "Ruta del parquet",
        value=r"C:\ruta\al\consolidado_7995.parquet",
        help="Ruta completa al archivo .parquet con los datos MCC 7995",
    )
    cargar = st.button("🔄 Cargar / Recargar datos", use_container_width=True)

# Cargar datos
if cargar or "df_feat" in st.session_state:
    if cargar or "df_feat" not in st.session_state:
        try:
            st.session_state["df_feat"] = cargar_datos(ruta_parquet)
        except FileNotFoundError:
            st.error(f"No se encontró el archivo: `{ruta_parquet}`")
            st.stop()
        except Exception as e:
            st.error(f"Error al cargar: {e}")
            st.stop()

    df = st.session_state["df_feat"]

    # ─── SIDEBAR: REGLA ──────────────────────────────────────────────────────
    with st.sidebar:
        st.divider()
        st.header("🔧 Constructor de Regla")
        st.caption("Selecciona las condiciones que formarán la regla. Deja 'Todos' para no filtrar esa dimensión.")

        # --- Dimensiones categóricas ---
        def multiselect_todos(label, opciones, key):
            todas = ["(Todos)"] + sorted([str(x) for x in opciones if pd.notna(x)])
            sel = st.multiselect(label, todas, default=["(Todos)"], key=key)
            if "(Todos)" in sel or not sel:
                return None  # sin filtro
            return sel

        tipo_sel    = multiselect_todos("Tipo Producto (TC/TD)",
                                        df["TIPO_PRODUCTO"].unique(), "tipo")
        seguro_sel  = multiselect_todos("Seguridad (ECI/UCAF)",
                                        df["SEGURO"].unique(), "seguro")
        canal_sel   = multiselect_todos("Canal",
                                        df["CANAL"].unique(), "canal")
        cvv_sel     = multiselect_todos("CVV / Red Comercio",
                                        df["COD_RED_LABEL"].unique(), "cvv")
        franja_sel  = multiselect_todos("Franja Horaria",
                                        df["FRANJA_HORARIA"].unique() if "FRANJA_HORARIA" in df.columns else [], "franja")
        pais_sel    = multiselect_todos("País",
                                        df["PAIS"].unique() if "PAIS" in df.columns else [], "pais")

        st.markdown("---")
        st.markdown("**Variables numéricas / buckets**")

        # Rechazos previos 24h
        rechazos_sel = multiselect_todos(
            "Rechazos previos 24h",
            df["BUCKET_RECHAZOS"].cat.categories.tolist() if "BUCKET_RECHAZOS" in df.columns else [],
            "rechazos"
        )

        # Ratio Monto/Saldo
        ratio_sel = multiselect_todos(
            "Ratio Monto/Saldo",
            df["BUCKET_RATIO_SALDO"].cat.categories.tolist() if "BUCKET_RATIO_SALDO" in df.columns else [],
            "ratio"
        )

        # Z-score
        zscore_sel = multiselect_todos(
            "Z-Score Monto (vs cliente)",
            df["BUCKET_ZSCORE"].cat.categories.tolist() if "BUCKET_ZSCORE" in df.columns else [],
            "zscore"
        )

        # FR histórico comercio
        fr_com_sel = multiselect_todos(
            "FR Histórico Comercio",
            df["BUCKET_FR_COMERCIO"].cat.categories.tolist() if "BUCKET_FR_COMERCIO" in df.columns else [],
            "fr_com"
        )

        st.markdown("---")
        # Comercio nuevo
        com_nuevo = st.checkbox("Solo comercios nuevos para el cliente", value=False, key="com_nuevo")

        # Rango de monto
        monto_min_g = float(df["MONTO"].min()) if df["MONTO"].notna().any() else 0.0
        monto_max_g = float(df["MONTO"].max()) if df["MONTO"].notna().any() else 9999.0
        monto_rango = st.slider(
            "Rango de monto (S/)",
            min_value=monto_min_g,
            max_value=monto_max_g,
            value=(monto_min_g, monto_max_g),
            step=1.0,
            key="monto",
        )

        st.markdown("---")
        st.caption("La regla combina todas las condiciones seleccionadas con lógica **AND**.")

    # ─── APLICAR REGLA ───────────────────────────────────────────────────────
    mask = pd.Series(True, index=df.index)

    if tipo_sel:
        mask &= df["TIPO_PRODUCTO"].isin(tipo_sel)
    if seguro_sel:
        mask &= df["SEGURO"].isin(seguro_sel)
    if canal_sel:
        mask &= df["CANAL"].isin(canal_sel)
    if cvv_sel:
        mask &= df["COD_RED_LABEL"].isin(cvv_sel)
    if franja_sel and "FRANJA_HORARIA" in df.columns:
        mask &= df["FRANJA_HORARIA"].isin(franja_sel)
    if pais_sel and "PAIS" in df.columns:
        mask &= df["PAIS"].isin(pais_sel)
    if rechazos_sel and "BUCKET_RECHAZOS" in df.columns:
        mask &= df["BUCKET_RECHAZOS"].astype(str).isin(rechazos_sel)
    if ratio_sel and "BUCKET_RATIO_SALDO" in df.columns:
        mask &= df["BUCKET_RATIO_SALDO"].astype(str).isin(ratio_sel)
    if zscore_sel and "BUCKET_ZSCORE" in df.columns:
        mask &= df["BUCKET_ZSCORE"].astype(str).isin(zscore_sel)
    if fr_com_sel and "BUCKET_FR_COMERCIO" in df.columns:
        mask &= df["BUCKET_FR_COMERCIO"].astype(str).isin(fr_com_sel)
    if com_nuevo and "ES_COMERCIO_NUEVO" in df.columns:
        mask &= df["ES_COMERCIO_NUEVO"] == 1
    mask &= df["MONTO"].between(monto_rango[0], monto_rango[1])

    df_regla  = df[mask]
    df_fuera  = df[~mask]

    # ─── MÉTRICAS GLOBALES ───────────────────────────────────────────────────
    total_trx    = len(df)
    total_fraude = int(df["ES_FRAUDE_APROBADO"].sum())
    total_leg    = total_trx - total_fraude

    en_regla     = len(df_regla)
    f_capturado  = int(df_regla["ES_FRAUDE_APROBADO"].sum())
    leg_bloqueado= en_regla - f_capturado

    recall       = f_capturado / total_fraude   if total_fraude > 0 else 0
    precision    = f_capturado / en_regla       if en_regla > 0     else 0
    fp_rate      = leg_bloqueado / total_leg    if total_leg > 0    else 0
    monto_f_cap  = df_regla.loc[df_regla["ES_FRAUDE_APROBADO"]==1, "MONTO"].sum()
    monto_f_tot  = df.loc[df["ES_FRAUDE_APROBADO"]==1, "MONTO"].sum()
    monto_recall = monto_f_cap / monto_f_tot if monto_f_tot > 0 else 0

    # ─── PANEL PRINCIPAL ─────────────────────────────────────────────────────
    # Fila de KPIs
    c1, c2, c3, c4, c5 = st.columns(5)
    metric_card(c1, "FRAUDE CAPTURADO",
                f"{f_capturado:,}",
                f"{recall*100:.2f}% del total",
                color="#8B1A1A")
    metric_card(c2, "LEG. BLOQUEADAS",
                f"{leg_bloqueado:,}",
                f"{fp_rate*100:.2f}% del total",
                color="#2B547E")
    metric_card(c3, "PRECISIÓN REGLA",
                f"{precision*100:.2f}%",
                f"{en_regla:,} trx en regla",
                color="#1C4B2A")
    metric_card(c4, "MONTO FRAUDE CAP.",
                f"S/ {monto_f_cap:,.0f}",
                f"{monto_recall*100:.2f}% del monto fraude",
                color="#5C3317")
    metric_card(c5, "TRXS EN REGLA",
                f"{en_regla:,}",
                f"{en_regla/total_trx*100:.2f}% del universo",
                color="#2E2E6A")

    st.markdown("<br>", unsafe_allow_html=True)

    # Barra de trade-off
    st.markdown("#### Trade-off: Captura de Fraude vs Falsos Positivos")
    col_tf1, col_tf2 = st.columns(2)
    with col_tf1:
        fig_fr = go.Figure(go.Indicator(
            mode="gauge+number",
            value=recall * 100,
            title={"text": "% Fraude Capturado", "font": {"size": 14}},
            number={"suffix": "%", "font": {"size": 28}},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#D9534F"},
                "steps": [
                    {"range": [0, 30],  "color": "#fce8e8"},
                    {"range": [30, 60], "color": "#f5c0c0"},
                    {"range": [60, 100],"color": "#e88080"},
                ],
                "threshold": {"line": {"color": "#8B0000", "width": 3}, "value": recall*100},
            }
        ))
        fig_fr.update_layout(height=220, margin=dict(l=20, r=20, t=40, b=10))
        st.plotly_chart(fig_fr, use_container_width=True)

    with col_tf2:
        fig_fp = go.Figure(go.Indicator(
            mode="gauge+number",
            value=fp_rate * 100,
            title={"text": "% Legítimas Bloqueadas (FP)", "font": {"size": 14}},
            number={"suffix": "%", "font": {"size": 28}},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#5B9BD5"},
                "steps": [
                    {"range": [0, 5],   "color": "#e0f0ff"},
                    {"range": [5, 20],  "color": "#b0d4f0"},
                    {"range": [20, 100],"color": "#7ab0e0"},
                ],
                "threshold": {"line": {"color": "#1a3f6e", "width": 3}, "value": fp_rate*100},
            }
        ))
        fig_fp.update_layout(height=220, margin=dict(l=20, r=20, t=40, b=10))
        st.plotly_chart(fig_fp, use_container_width=True)

    st.divider()

    # ─── BREAKDOWNS ──────────────────────────────────────────────────────────
    st.markdown("#### Composición de la regla por dimensión")
    st.caption("Sobre las transacciones que **caen en la regla actual**.")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Tipo Producto", "Seguridad", "Canal", "CVV/Red", "Franja Horaria"
    ])

    with tab1:
        if len(df_regla) > 0:
            st.plotly_chart(breakdown_chart(df_regla, "TIPO_PRODUCTO", "Tipo Producto"), use_container_width=True)
        else:
            st.info("Sin transacciones con las condiciones actuales.")

    with tab2:
        if len(df_regla) > 0:
            st.plotly_chart(breakdown_chart(df_regla, "SEGURO", "Seguridad Comercio"), use_container_width=True)

    with tab3:
        if len(df_regla) > 0:
            st.plotly_chart(breakdown_chart(df_regla, "CANAL", "Canal"), use_container_width=True)

    with tab4:
        if len(df_regla) > 0:
            st.plotly_chart(breakdown_chart(df_regla, "COD_RED_LABEL", "CVV / Red Comercio"), use_container_width=True)

    with tab5:
        if "FRANJA_HORARIA" in df_regla.columns and len(df_regla) > 0:
            st.plotly_chart(breakdown_chart(df_regla, "FRANJA_HORARIA", "Franja Horaria"), use_container_width=True)

    st.divider()

    # ─── TABLA RESUMEN DE LA REGLA ───────────────────────────────────────────
    st.markdown("#### Resumen: dentro vs fuera de la regla")
    resumen = pd.DataFrame({
        "Grupo": ["Dentro de la regla", "Fuera de la regla", "TOTAL"],
        "Trxs": [en_regla, total_trx - en_regla, total_trx],
        "Fraude Aprobado": [
            f_capturado,
            total_fraude - f_capturado,
            total_fraude,
        ],
        "Legítimas": [
            leg_bloqueado,
            total_leg - leg_bloqueado,
            total_leg,
        ],
        "FR% (en grupo)": [
            f"{f_capturado/en_regla*100:.4f}%" if en_regla > 0 else "—",
            f"{(total_fraude-f_capturado)/(total_trx-en_regla)*100:.4f}%" if (total_trx-en_regla) > 0 else "—",
            f"{total_fraude/total_trx*100:.4f}%",
        ],
        "% Fraude capturado": [
            f"{recall*100:.2f}%", f"{(1-recall)*100:.2f}%", "100%"
        ],
    })
    st.dataframe(resumen, use_container_width=True, hide_index=True)

    st.divider()

    # ─── EXPLORADOR DE FEATURES ──────────────────────────────────────────────
    st.markdown("#### Explorador de features")
    st.caption("Analiza cómo se distribuye el fraude en cada variable para afinar tu regla.")

    feat_options = {
        "Rechazos 24h": "BUCKET_RECHAZOS",
        "Ratio Monto/Saldo": "BUCKET_RATIO_SALDO",
        "Z-Score Monto": "BUCKET_ZSCORE",
        "FR Histórico Comercio": "BUCKET_FR_COMERCIO",
        "Tipo Producto": "TIPO_PRODUCTO",
        "Seguridad": "SEGURO",
        "Canal": "CANAL",
        "CVV/Red": "COD_RED_LABEL",
        "Franja Horaria": "FRANJA_HORARIA",
        "Comercio Nuevo": "ES_COMERCIO_NUEVO",
    }
    feat_options = {k: v for k, v in feat_options.items() if v in df.columns}

    feat_label = st.selectbox("Variable a explorar", list(feat_options.keys()), key="feat_exp")
    feat_col   = feat_options[feat_label]

    df_exp = (
        df.groupby(feat_col, observed=True)
        .agg(
            Trxs=("ES_FRAUDE_APROBADO", "count"),
            Fraude_Ap=("ES_FRAUDE_APROBADO", "sum"),
        )
        .reset_index()
    )
    df_exp["Legítimas"] = df_exp["Trxs"] - df_exp["Fraude_Ap"]
    df_exp["FR%"]       = (df_exp["Fraude_Ap"] / df_exp["Trxs"] * 100).round(4)
    df_exp["% del total fraude"] = (df_exp["Fraude_Ap"] / total_fraude * 100).round(2) if total_fraude > 0 else 0
    df_exp = df_exp.rename(columns={feat_col: feat_label})
    df_exp = df_exp.sort_values("Fraude_Ap", ascending=False)

    col_tbl, col_cht = st.columns([1, 2])
    with col_tbl:
        st.dataframe(df_exp, use_container_width=True, hide_index=True,
                     column_config={"FR%": st.column_config.NumberColumn(format="%.4f %%")})
    with col_cht:
        fig_exp = px.bar(
            df_exp, x=feat_label, y=["Fraude_Ap","Legítimas"],
            barmode="stack",
            color_discrete_map={"Fraude_Ap": "#D9534F", "Legítimas": "#5B9BD5"},
            title=f"Distribución: {feat_label}",
        )
        fig_exp.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=10),
                              legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig_exp, use_container_width=True)

else:
    st.info("👈 Ingresa la ruta del parquet en el panel izquierdo y presiona **Cargar / Recargar datos** para comenzar.")
    st.markdown("""
    **¿Qué hace esta app?**
    - Carga tu parquet MCC 7995 y recalcula todos los features (ratio monto/saldo, z-score, rechazos 24h, etc.)
    - Te permite combinar condiciones en el sidebar para construir una **regla de fraude**
    - Muestra en tiempo real:
      - % de fraude que capturarías (**recall**)
      - % de legítimas que bloquearías (**falsos positivos**)
      - **Precisión** de la regla (de lo que capturas, ¿cuánto es realmente fraude?)
    - Incluye gráficos de breakdown por tipo producto, seguridad, canal, CVV y franja horaria
    """)
