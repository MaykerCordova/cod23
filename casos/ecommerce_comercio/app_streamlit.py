# =============================================================================
# SIMULADOR DE REGLAS — ECOMMERCE (comercios de alta transaccionalidad)
# Carga el parquet de features generado por analisis_ecommerce.py
# y permite simular reglas con los features específicos de ecommerce.
# =============================================================================
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(
    page_title="Simulador Ecommerce",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CARGA ───────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Cargando features...")
def cargar_features(ruta: str) -> pd.DataFrame:
    df = pd.read_parquet(ruta)
    # Asegurar tipos categóricos para columnas bucket
    for c in df.columns:
        if c.startswith("BUCKET_") and df[c].dtype.name != "category":
            df[c] = df[c].astype("category")
    return df


# ─── HELPERS ─────────────────────────────────────────────────────────────────
def metric_card(col, label, value, delta="", color="#1F3864"):
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


def multiselect_todos(label, opciones, key):
    todas = ["(Todos)"] + sorted([str(x) for x in opciones if pd.notna(x)])
    sel = st.multiselect(label, todas, default=["(Todos)"], key=key)
    if "(Todos)" in sel or not sel:
        return None
    return sel


def breakdown_chart(df_base, col_dim, title):
    if col_dim not in df_base.columns or len(df_base) == 0:
        return None
    gb = (df_base.groupby(col_dim, observed=True)
          .agg(Total=("ES_FRAUDE_APROBADO", "count"),
               Fraude=("ES_FRAUDE_APROBADO", "sum"))
          .reset_index())
    gb["Legitimas"] = gb["Total"] - gb["Fraude"]
    gb["FR%"] = (gb["Fraude"] / gb["Total"] * 100).round(2)
    gb = gb.sort_values("Fraude", ascending=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(name="Fraude Ap.", y=gb[col_dim].astype(str), x=gb["Fraude"],
                         orientation="h", marker_color="#D9534F",
                         text=gb["FR%"].map(lambda v: f"{v:.2f}%"), textposition="outside"))
    fig.add_trace(go.Bar(name="Legítimas", y=gb[col_dim].astype(str), x=gb["Legitimas"],
                         orientation="h", marker_color="#5B9BD5"))
    fig.update_layout(barmode="stack", title=title, height=max(220, len(gb)*38+80),
                      margin=dict(l=10, r=10, t=40, b=10),
                      legend=dict(orientation="h", y=-0.15))
    return fig


# ─── HEADER ──────────────────────────────────────────────────────────────────
st.markdown(
    "<h2 style='margin-bottom:0'>🛒 Simulador de Reglas — Ecommerce</h2>"
    "<p style='color:#666;margin-top:4px'>Simula reglas sobre comercios de alta transaccionalidad. "
    "Ajusta condiciones y observa en tiempo real el trade-off entre captura de fraude y falsos positivos.</p>",
    unsafe_allow_html=True,
)
st.divider()

# ─── SIDEBAR: CARGA ──────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Configuración")
    ruta_features = st.text_input(
        "Ruta del parquet de features",
        value=r"C:\ruta\al\parquet_ecommerce_features.parquet",
        help="Archivo generado por analisis_ecommerce.py (sufijo _features.parquet)",
    )
    cargar = st.button("🔄 Cargar / Recargar", use_container_width=True)

if cargar or "df_feat" in st.session_state:
    if cargar or "df_feat" not in st.session_state:
        try:
            st.session_state["df_feat"] = cargar_features(ruta_features)
        except FileNotFoundError:
            st.error(f"Archivo no encontrado: `{ruta_features}`")
            st.stop()
        except Exception as e:
            st.error(f"Error al cargar: {e}")
            st.stop()

    df = st.session_state["df_feat"]
    nombre_comercio = df["COMERCIO"].mode().iloc[0] if "COMERCIO" in df.columns else "Comercio"

    st.markdown(f"**Comercio analizado:** `{nombre_comercio}` · **{len(df):,} trxs aprobadas**")

    # ─── SIDEBAR: REGLA ──────────────────────────────────────────────────────
    with st.sidebar:
        st.divider()
        st.header("🔧 Constructor de Regla")
        st.caption("Cada condición se combina con **AND**.")

        with st.expander("A · Velocidad / Frecuencia", expanded=True):
            n_trx_5min = multiselect_todos(
                "N trx en 5 min",
                df["BUCKET_N_TRX_5MIN"].cat.categories if "BUCKET_N_TRX_5MIN" in df.columns else [],
                "b_5min"
            )
            n_trx_24h = multiselect_todos(
                "N trx en 24h",
                df["BUCKET_N_TRX_24H"].cat.categories if "BUCKET_N_TRX_24H" in df.columns else [],
                "b_24h"
            )
            gap_sel = multiselect_todos(
                "Gap min desde última trx",
                df["BUCKET_GAP_MIN"].cat.categories if "BUCKET_GAP_MIN" in df.columns else [],
                "b_gap"
            )
            rafaga = st.checkbox("Solo ráfagas (>=3 trx en <10 min)", value=False, key="rafaga")

        with st.expander("B · Monto", expanded=False):
            zscore_sel = multiselect_todos(
                "Z-score monto vs cliente",
                df["BUCKET_ZSCORE"].cat.categories if "BUCKET_ZSCORE" in df.columns else [],
                "b_z"
            )
            mon_redondo = st.checkbox("Solo monto redondo (múltiplo de 50)", value=False, key="redondo")
            mon_bajo    = st.checkbox("Solo monto bajo (<S/ 20)", value=False, key="bajo")
            if "MONTO" in df.columns and df["MONTO"].notna().any():
                m_min = float(df["MONTO"].min()); m_max = float(df["MONTO"].max())
                monto_rango = st.slider("Rango de monto (S/)",
                                        min_value=m_min, max_value=m_max,
                                        value=(m_min, m_max), step=1.0, key="m_rng")
            else:
                monto_rango = None

        with st.expander("C · Cliente en el comercio", expanded=False):
            primera_vez = st.checkbox("Solo primera vez del cliente en el comercio",
                                      value=False, key="primera")
            dias_sel = multiselect_todos(
                "Días desde primera compra",
                df["BUCKET_DIAS_PRIMERA"].cat.categories if "BUCKET_DIAS_PRIMERA" in df.columns else [],
                "b_dias"
            )

        with st.expander("D · Cascada de fraude", expanded=True):
            f24h = st.checkbox("Tuvo fraude previo en 24h", value=False, key="f24")
            f7d  = st.checkbox("Tuvo fraude previo en 7d",  value=False, key="f7d")
            prev_fraud = st.checkbox("La transacción anterior fue fraude", value=False, key="pfraud")

        with st.expander("E · Geográficas / IP", expanded=False):
            if "PAIS_DISTINTO_HABITUAL" in df.columns:
                pais_dist = st.checkbox("Solo país distinto al habitual del cliente",
                                        value=False, key="pais_dist")
                cambio_pais = st.checkbox("Solo cambio de país vs trx anterior",
                                          value=False, key="cambio_pais")
            else:
                pais_dist = False; cambio_pais = False
            if "PAIS" in df.columns:
                pais_sel = multiselect_todos("País", df["PAIS"].unique(), "b_pais")
            else:
                pais_sel = None
            if "IP_NUEVA_CLIENTE" in df.columns:
                ip_nueva = st.checkbox("Solo IP nueva para el cliente",
                                       value=False, key="ip_nueva")
            else:
                ip_nueva = False
            if "BUCKET_CLIS_IP" in df.columns:
                clis_ip_sel = multiselect_todos(
                    "N clientes distintos misma IP 24h",
                    df["BUCKET_CLIS_IP"].cat.categories, "b_clis_ip"
                )
            else:
                clis_ip_sel = None

        with st.expander("F · Motivos de rechazo previos", expanded=True):
            rech_sel = multiselect_todos(
                "Rechazos 24h",
                df["BUCKET_RECHAZOS"].cat.categories if "BUCKET_RECHAZOS" in df.columns else [],
                "b_rech"
            )
            cvv_sel = multiselect_todos(
                "CVV fails 24h",
                df["BUCKET_CVV_FAIL"].cat.categories if "BUCKET_CVV_FAIL" in df.columns else [],
                "b_cvv"
            )
            cvv_previo = st.checkbox("Tuvo CVV fail previo", value=False, key="cvv_prev")

        with st.expander("Dimensiones (tipo, seguro, etc.)", expanded=False):
            tipo_sel    = multiselect_todos("Tipo Producto", df["TIPO_PRODUCTO"].unique(), "b_tipo") if "TIPO_PRODUCTO" in df.columns else None
            seguro_sel  = multiselect_todos("Seguridad",     df["SEGURO"].unique(),        "b_seg") if "SEGURO" in df.columns else None
            cvv_red_sel = multiselect_todos("CVV/Red",       df["COD_RED_LABEL"].unique(), "b_cred") if "COD_RED_LABEL" in df.columns else None
            franja_sel  = multiselect_todos("Franja horaria",df["FRANJA_HORARIA"].unique(),"b_fr") if "FRANJA_HORARIA" in df.columns else None

    # ─── APLICAR REGLA ───────────────────────────────────────────────────────
    mask = pd.Series(True, index=df.index)

    def aplicar_bucket(col, sel):
        nonlocal mask
        if sel and col in df.columns:
            mask &= df[col].astype(str).isin(sel)

    aplicar_bucket("BUCKET_N_TRX_5MIN", n_trx_5min)
    aplicar_bucket("BUCKET_N_TRX_24H",  n_trx_24h)
    aplicar_bucket("BUCKET_GAP_MIN",    gap_sel)
    aplicar_bucket("BUCKET_ZSCORE",     zscore_sel)
    aplicar_bucket("BUCKET_DIAS_PRIMERA", dias_sel)
    aplicar_bucket("BUCKET_RECHAZOS",   rech_sel)
    aplicar_bucket("BUCKET_CVV_FAIL",   cvv_sel)
    if "BUCKET_CLIS_IP" in df.columns:
        aplicar_bucket("BUCKET_CLIS_IP", clis_ip_sel)

    if rafaga and "ES_RAFAGA" in df.columns:                  mask &= df["ES_RAFAGA"] == 1
    if mon_redondo and "ES_MONTO_REDONDO" in df.columns:      mask &= df["ES_MONTO_REDONDO"] == 1
    if mon_bajo and "ES_MONTO_BAJO" in df.columns:            mask &= df["ES_MONTO_BAJO"] == 1
    if primera_vez and "ES_PRIMERA_VEZ_COMERCIO" in df.columns: mask &= df["ES_PRIMERA_VEZ_COMERCIO"] == 1
    if f24h and "HUBO_FRAUDE_PREVIO_24H" in df.columns:       mask &= df["HUBO_FRAUDE_PREVIO_24H"] == 1
    if f7d  and "HUBO_FRAUDE_PREVIO_7D" in df.columns:        mask &= df["HUBO_FRAUDE_PREVIO_7D"] == 1
    if prev_fraud and "PREV_FUE_FRAUDE" in df.columns:        mask &= df["PREV_FUE_FRAUDE"] == 1
    if pais_dist and "PAIS_DISTINTO_HABITUAL" in df.columns:  mask &= df["PAIS_DISTINTO_HABITUAL"] == 1
    if cambio_pais and "CAMBIO_PAIS_VS_PREV" in df.columns:   mask &= df["CAMBIO_PAIS_VS_PREV"] == 1
    if ip_nueva and "IP_NUEVA_CLIENTE" in df.columns:         mask &= df["IP_NUEVA_CLIENTE"] == 1
    if cvv_previo and "HUBO_CVV_FAIL_PREVIO" in df.columns:   mask &= df["HUBO_CVV_FAIL_PREVIO"] == 1

    if pais_sel and "PAIS" in df.columns:           mask &= df["PAIS"].isin(pais_sel)
    if tipo_sel:                                     mask &= df["TIPO_PRODUCTO"].isin(tipo_sel)
    if seguro_sel:                                   mask &= df["SEGURO"].isin(seguro_sel)
    if cvv_red_sel:                                  mask &= df["COD_RED_LABEL"].isin(cvv_red_sel)
    if franja_sel:                                   mask &= df["FRANJA_HORARIA"].isin(franja_sel)

    if monto_rango is not None:
        mask &= df["MONTO"].between(monto_rango[0], monto_rango[1])

    df_regla = df[mask]

    # ─── MÉTRICAS ────────────────────────────────────────────────────────────
    total_trx     = len(df)
    total_fraude  = int(df["ES_FRAUDE_APROBADO"].sum())
    total_leg     = total_trx - total_fraude
    en_regla      = len(df_regla)
    f_capturado   = int(df_regla["ES_FRAUDE_APROBADO"].sum())
    leg_bloqueado = en_regla - f_capturado
    recall    = f_capturado / total_fraude  if total_fraude > 0 else 0
    precision = f_capturado / en_regla      if en_regla     > 0 else 0
    fp_rate   = leg_bloqueado / total_leg   if total_leg    > 0 else 0
    monto_f_cap  = df_regla.loc[df_regla["ES_FRAUDE_APROBADO"]==1, "MONTO"].sum()
    monto_f_tot  = df.loc[df["ES_FRAUDE_APROBADO"]==1, "MONTO"].sum()
    monto_recall = monto_f_cap / monto_f_tot if monto_f_tot > 0 else 0

    # ─── KPIs ────────────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    metric_card(c1, "FRAUDE CAPTURADO", f"{f_capturado:,}",
                f"{recall*100:.2f}% del total", color="#8B1A1A")
    metric_card(c2, "LEG. BLOQUEADAS", f"{leg_bloqueado:,}",
                f"{fp_rate*100:.2f}% del total", color="#2B547E")
    metric_card(c3, "PRECISIÓN REGLA", f"{precision*100:.2f}%",
                f"{en_regla:,} trx en regla", color="#1C4B2A")
    metric_card(c4, "MONTO FRAUDE CAP.", f"S/ {monto_f_cap:,.0f}",
                f"{monto_recall*100:.2f}% del monto fraude", color="#5C3317")
    metric_card(c5, "TRXS EN REGLA", f"{en_regla:,}",
                f"{en_regla/total_trx*100:.2f}% del universo", color="#2E2E6A")

    st.markdown("<br>", unsafe_allow_html=True)

    # ─── GAUGES ──────────────────────────────────────────────────────────────
    st.markdown("#### Trade-off: Captura vs Falsos Positivos")
    g1, g2 = st.columns(2)
    with g1:
        fig = go.Figure(go.Indicator(
            mode="gauge+number", value=recall*100,
            title={"text": "% Fraude Capturado", "font": {"size": 14}},
            number={"suffix": "%", "font": {"size": 28}},
            gauge={"axis": {"range": [0, 100]},
                   "bar":  {"color": "#D9534F"},
                   "steps":[{"range":[0,30],"color":"#fce8e8"},
                            {"range":[30,60],"color":"#f5c0c0"},
                            {"range":[60,100],"color":"#e88080"}]}
        ))
        fig.update_layout(height=220, margin=dict(l=20, r=20, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)
    with g2:
        fig = go.Figure(go.Indicator(
            mode="gauge+number", value=fp_rate*100,
            title={"text": "% Legítimas Bloqueadas", "font": {"size": 14}},
            number={"suffix": "%", "font": {"size": 28}},
            gauge={"axis": {"range": [0, 100]},
                   "bar":  {"color": "#5B9BD5"},
                   "steps":[{"range":[0,5], "color":"#e0f0ff"},
                            {"range":[5,20],"color":"#b0d4f0"},
                            {"range":[20,100],"color":"#7ab0e0"}]}
        ))
        fig.update_layout(height=220, margin=dict(l=20, r=20, t=40, b=10))
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ─── BREAKDOWN ───────────────────────────────────────────────────────────
    st.markdown("#### Composición de la regla por dimensión")
    tabs_dims = [("Tipo Producto","TIPO_PRODUCTO"),
                 ("Seguridad","SEGURO"),
                 ("CVV/Red","COD_RED_LABEL"),
                 ("Franja Horaria","FRANJA_HORARIA"),
                 ("País","PAIS")]
    tabs_disp = [(n, c) for n, c in tabs_dims if c in df.columns]
    if tabs_disp:
        tabs_ui = st.tabs([n for n, _ in tabs_disp])
        for (lbl, col), tab in zip(tabs_disp, tabs_ui):
            with tab:
                fig = breakdown_chart(df_regla, col, lbl)
                if fig: st.plotly_chart(fig, use_container_width=True)
                else: st.info("Sin transacciones con las condiciones actuales.")

    st.divider()

    # ─── TABLA RESUMEN ───────────────────────────────────────────────────────
    st.markdown("#### Resumen: dentro vs fuera de la regla")
    resumen = pd.DataFrame({
        "Grupo": ["Dentro de la regla", "Fuera de la regla", "TOTAL"],
        "Trxs": [en_regla, total_trx - en_regla, total_trx],
        "Fraude Aprobado": [f_capturado, total_fraude - f_capturado, total_fraude],
        "Legítimas": [leg_bloqueado, total_leg - leg_bloqueado, total_leg],
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
    st.caption("Cómo se distribuye el fraude por cada variable.")

    feat_options = {
        "Velocidad - N trx 5 min":  "BUCKET_N_TRX_5MIN",
        "Velocidad - N trx 24h":    "BUCKET_N_TRX_24H",
        "Velocidad - Gap minutos":  "BUCKET_GAP_MIN",
        "Velocidad - Ráfaga":       "ES_RAFAGA",
        "Monto - Z-score":          "BUCKET_ZSCORE",
        "Monto - Redondo":          "ES_MONTO_REDONDO",
        "Monto - Bajo":             "ES_MONTO_BAJO",
        "Cliente - Primera vez":    "ES_PRIMERA_VEZ_COMERCIO",
        "Cliente - Días primera compra": "BUCKET_DIAS_PRIMERA",
        "Cascada - Fraude prev 24h":     "HUBO_FRAUDE_PREVIO_24H",
        "Cascada - Fraude prev 7d":      "HUBO_FRAUDE_PREVIO_7D",
        "Cascada - Anterior fue fraude": "PREV_FUE_FRAUDE",
        "Geo - País distinto habitual":  "PAIS_DISTINTO_HABITUAL",
        "Geo - Cambio país":             "CAMBIO_PAIS_VS_PREV",
        "Geo - IP nueva cliente":        "IP_NUEVA_CLIENTE",
        "Geo - N clientes misma IP":     "BUCKET_CLIS_IP",
        "Rechazos - N 24h":              "BUCKET_RECHAZOS",
        "Rechazos - CVV fails":          "BUCKET_CVV_FAIL",
        "Rechazos - Hubo CVV fail":      "HUBO_CVV_FAIL_PREVIO",
        "Franja horaria":                "FRANJA_HORARIA",
    }
    feat_options = {k: v for k, v in feat_options.items() if v in df.columns}

    if feat_options:
        feat_label = st.selectbox("Variable a explorar", list(feat_options.keys()), key="feat_exp")
        feat_col   = feat_options[feat_label]

        df_exp = (df.groupby(feat_col, observed=True)
                  .agg(Trxs=("ES_FRAUDE_APROBADO","count"),
                       Fraude_Ap=("ES_FRAUDE_APROBADO","sum"))
                  .reset_index())
        df_exp["Legítimas"] = df_exp["Trxs"] - df_exp["Fraude_Ap"]
        df_exp["FR%"]       = (df_exp["Fraude_Ap"] / df_exp["Trxs"] * 100).round(4)
        df_exp["% del total fraude"] = (df_exp["Fraude_Ap"] / total_fraude * 100).round(2) if total_fraude > 0 else 0
        df_exp = df_exp.rename(columns={feat_col: feat_label}).sort_values("Fraude_Ap", ascending=False)

        ctl, ccr = st.columns([1, 2])
        with ctl:
            st.dataframe(df_exp, use_container_width=True, hide_index=True,
                         column_config={"FR%": st.column_config.NumberColumn(format="%.4f %%")})
        with ccr:
            fig = px.bar(df_exp, x=feat_label, y=["Fraude_Ap","Legítimas"], barmode="stack",
                         color_discrete_map={"Fraude_Ap":"#D9534F","Legítimas":"#5B9BD5"},
                         title=f"Distribución: {feat_label}")
            fig.update_layout(height=340, margin=dict(l=0, r=0, t=40, b=10),
                              legend=dict(orientation="h", y=-0.2),
                              xaxis={"type":"category"})
            st.plotly_chart(fig, use_container_width=True)

else:
    st.info("👈 Ingresa la ruta del parquet de features y presiona **Cargar / Recargar**.")
    st.markdown("""
    **¿Cómo se usa esta app?**

    1. **Correr primero** `analisis_ecommerce.py` con tu parquet pre-filtrado (1 comercio + ecommerce + ap/den).
       - El script genera un Excel con todos los análisis.
       - Y también guarda un parquet de features (`*_features.parquet`).
    2. **Apuntar esta app** a ese parquet de features.
    3. **Construir la regla** combinando condiciones en el sidebar (velocidad, monto, cliente, cascada, geo, rechazos).
    4. La app te dice **en tiempo real**:
       - % de fraude que capturarías (**recall**)
       - % de legítimas que bloquearías (**falsos positivos**)
       - **Precisión** de la regla

    **Features disponibles:**
    - **A. Velocidad**: N trx 5min/15min/1h/24h, gap minutos, ráfaga
    - **B. Monto**: z-score, monto acum 24h, monto redondo, monto bajo
    - **C. Cliente**: primera vez en comercio, días desde primera compra
    - **D. Cascada fraude**: fraude previo 24h/7d, anterior fue fraude
    - **E. Geo/IP**: país distinto al habitual, IP nueva, N clientes misma IP
    - **F. Rechazos**: rechazos 24h, CVV fails 24h, hubo CVV fail previo
    """)
