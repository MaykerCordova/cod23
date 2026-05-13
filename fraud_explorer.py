"""
fraud_explorer.py
─────────────────
Streamlit — Explorador Interactivo de Fraude en Transferencias a Terceros
Scotiabank Perú · Prevención de Fraude

Uso:
    streamlit run fraud_explorer.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────
# CONFIG — ajusta los nombres de columnas si difieren
# ─────────────────────────────────────────────────────────────
COL_MONTO     = "MONTO"
COL_IND       = "INDICADOR"
COL_SEGMENTO  = "SEGMENTO"
COL_FECHA     = "FECHA_HORA"
COL_ORIGEN    = "ACF_CUENTA_ORIGEN"
COL_DESTINO   = "ACF_CUENTA_DESTINO"
COL_CLIENTE   = "ACFYD_CLIENTE"

PARQUET_PATH  = "transferencias_features.parquet"

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

COLORES = {
    "F":"#E63946","N":"#457B9D",
    "G":"#2A9D8F","D":"#E9C46A","P":"#A8DADC"
}
MESES_ES = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",
            5:"Mayo",6:"Junio",7:"Julio",8:"Agosto"}
ORDEN_DIAS = ["Monday","Tuesday","Wednesday","Thursday",
              "Friday","Saturday","Sunday"]
DIAS_ES = {"Monday":"Lun","Tuesday":"Mar","Wednesday":"Mié",
           "Thursday":"Jue","Friday":"Vie",
           "Saturday":"Sáb","Sunday":"Dom"}

# ─────────────────────────────────────────────────────────────
# SETUP PÁGINA
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Fraud Explorer · Scotiabank",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* Fondo principal oscuro elegante */
    .stApp { background-color: #0f1117; color: #e8eaf0; }
    section[data-testid="stSidebar"] { background-color: #161b27; }

    /* KPI cards */
    .kpi-card {
        background: linear-gradient(135deg, #1a2035, #1f2847);
        border: 1px solid #2a3555;
        border-radius: 12px;
        padding: 18px 22px;
        text-align: center;
        margin-bottom: 8px;
    }
    .kpi-label { font-size: 11px; color: #8892aa; text-transform: uppercase;
                 letter-spacing: 1.2px; margin-bottom: 6px; }
    .kpi-value { font-size: 26px; font-weight: 700; color: #e8eaf0; }
    .kpi-value.red   { color: #E63946; }
    .kpi-value.blue  { color: #5b9bd5; }
    .kpi-value.green { color: #2A9D8F; }
    .kpi-value.gold  { color: #E9C46A; }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { gap: 4px; background: #161b27;
        border-radius: 10px; padding: 4px; }
    .stTabs [data-baseweb="tab"] { border-radius: 8px; padding: 8px 20px;
        color: #8892aa; font-size: 13px; }
    .stTabs [aria-selected="true"] { background: #E63946 !important;
        color: white !important; }

    /* Separador */
    hr { border-color: #2a3555; }

    /* Títulos */
    h1, h2, h3 { color: #e8eaf0 !important; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# CARGA DE DATOS
# ─────────────────────────────────────────────────────────────
@st.cache_data
def cargar_datos():
    df = pd.read_parquet(PARQUET_PATH)
    df[COL_IND]      = df[COL_IND].astype(str).str.strip().str.upper()
    df[COL_SEGMENTO] = df[COL_SEGMENTO].astype(str).str.strip()
    df[COL_MONTO]    = pd.to_numeric(df[COL_MONTO], errors="coerce")
    if COL_FECHA in df.columns:
        df[COL_FECHA] = pd.to_datetime(df[COL_FECHA], errors="coerce")
        if "HORA" not in df.columns:
            df["HORA"] = df[COL_FECHA].dt.hour
        if "DIA_SEMANA" not in df.columns:
            df["DIA_SEMANA"] = df[COL_FECHA].dt.day_name()
        if "MES_NUM" not in df.columns:
            df["MES_NUM"] = df[COL_FECHA].dt.month
    return df

df_full = cargar_datos()


# ─────────────────────────────────────────────────────────────
# SIDEBAR — FILTROS
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔍 Filtros")
    st.markdown("---")

    # Segmento
    segs_disponibles = sorted(df_full[COL_SEGMENTO].unique())
    segs_labels = {
        s: f"{s} — {SEG_NOMBRE.get(s, s)}" for s in segs_disponibles
    }
    seg_sel = st.multiselect(
        "Segmento",
        options=segs_disponibles,
        default=["31","32","33","34"],
        format_func=lambda s: segs_labels[s],
        help="Filtra por segmento de tarjeta"
    )

    st.markdown("---")

    # Rango de monto
    monto_min = int(df_full[COL_MONTO].min())
    monto_max = int(df_full[COL_MONTO].max())
    rango_monto = st.slider(
        "Rango de monto (S/)",
        min_value=monto_min,
        max_value=monto_max,
        value=(2000, monto_max),
        step=500,
        format="S/ %d"
    )

    st.markdown("---")

    # Indicadores
    inds_disp = sorted(df_full[COL_IND].unique())
    inds_sel = st.multiselect(
        "Indicadores",
        options=inds_disp,
        default=[i for i in ["F","N","G"] if i in inds_disp],
    )

    st.markdown("---")

    # Período (mes)
    meses_disp = sorted(df_full["MES_NUM"].dropna().unique().astype(int))
    meses_labels = {m: MESES_ES.get(m, str(m)) for m in meses_disp}
    meses_sel = st.multiselect(
        "Período",
        options=meses_disp,
        default=meses_disp,
        format_func=lambda m: meses_labels[m],
    )

    st.markdown("---")
    st.markdown(
        "<div style='font-size:11px;color:#556;text-align:center'>"
        "Fraud Explorer v1.0<br>Scotiabank Perú · Prevención de Fraude"
        "</div>",
        unsafe_allow_html=True
    )


# ─────────────────────────────────────────────────────────────
# FILTRADO
# ─────────────────────────────────────────────────────────────
if not seg_sel or not inds_sel or not meses_sel:
    st.warning("Selecciona al menos un valor en cada filtro.")
    st.stop()

df = df_full[
    (df_full[COL_SEGMENTO].isin(seg_sel)) &
    (df_full[COL_MONTO].between(rango_monto[0], rango_monto[1])) &
    (df_full[COL_IND].isin(inds_sel)) &
    (df_full["MES_NUM"].isin(meses_sel))
].copy()

df_clean = df[df[COL_IND].isin(["F","N","G"])].copy()

if len(df) == 0:
    st.warning("No hay datos con los filtros seleccionados.")
    st.stop()


# ─────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────
seg_desc = " · ".join([segs_labels[s] for s in seg_sel]) if len(seg_sel) <= 3 \
           else f"{len(seg_sel)} segmentos seleccionados"

st.markdown(f"## 🏦 Fraud Explorer — Transferencias a Terceros")
st.markdown(f"**{seg_desc}** &nbsp;|&nbsp; S/ {rango_monto[0]:,} – S/ {rango_monto[1]:,}")
st.markdown("---")


# ─────────────────────────────────────────────────────────────
# KPIs
# ─────────────────────────────────────────────────────────────
n_total   = len(df_clean)
n_fraude  = (df_clean[COL_IND] == "F").sum()
fr        = n_fraude / n_total * 100 if n_total > 0 else 0
sev       = df_clean.loc[df_clean[COL_IND]=="F", COL_MONTO].mean()
monto_tot = df_clean.loc[df_clean[COL_IND]=="F", COL_MONTO].sum()
med_f     = df_clean.loc[df_clean[COL_IND]=="F", COL_MONTO].median()

c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.markdown(f"""<div class="kpi-card">
        <div class="kpi-label">Total Transacciones</div>
        <div class="kpi-value blue">{n_total:,}</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""<div class="kpi-card">
        <div class="kpi-label">Fraudes (F)</div>
        <div class="kpi-value red">{n_fraude:,}</div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""<div class="kpi-card">
        <div class="kpi-label">Fraud Rate</div>
        <div class="kpi-value red">{fr:.3f}%</div>
    </div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""<div class="kpi-card">
        <div class="kpi-label">Severidad Media</div>
        <div class="kpi-value gold">S/ {sev:,.0f}</div>
    </div>""", unsafe_allow_html=True)
with c5:
    st.markdown(f"""<div class="kpi-card">
        <div class="kpi-label">Monto Total Fraude</div>
        <div class="kpi-value red">S/ {monto_tot:,.0f}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs([
    "📊  Perfil del Fraude",
    "🕐  Análisis Temporal",
    "⚙️  Variables de Comportamiento",
])

plt.rcParams.update({
    "figure.facecolor": "#0f1117",
    "axes.facecolor":   "#161b27",
    "axes.edgecolor":   "#2a3555",
    "axes.labelcolor":  "#8892aa",
    "xtick.color":      "#8892aa",
    "ytick.color":      "#8892aa",
    "text.color":       "#e8eaf0",
    "grid.color":       "#2a3555",
    "grid.alpha":       0.5,
})


# ══════════════════════════════════════════════════════════════
# TAB 1 — PERFIL DEL FRAUDE
# ══════════════════════════════════════════════════════════════
with tab1:
    col_a, col_b = st.columns(2)

    # Distribución de indicadores
    with col_a:
        st.markdown("#### Distribución de Indicadores")
        vc = df_clean[COL_IND].value_counts().reset_index()
        vc.columns = ["Indicador","N"]
        vc["%"] = (vc["N"] / vc["N"].sum() * 100).round(2)

        fig, ax = plt.subplots(figsize=(5, 4))
        colores_bar = [COLORES.get(i,"#999") for i in vc["Indicador"]]
        ax.bar(vc["Indicador"], vc["N"], color=colores_bar, edgecolor="#0f1117", linewidth=0.8)
        for i, row in vc.iterrows():
            ax.text(i, row["N"]*1.03, f"{row['%']:.1f}%",
                    ha="center", fontsize=9, color="#e8eaf0")
        ax.set_xlabel("Indicador"); ax.set_ylabel("N transacciones")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{x:,.0f}"))
        ax.spines[["top","right"]].set_visible(False)
        fig.tight_layout()
        st.pyplot(fig); plt.close()

    # Rangos de monto
    with col_b:
        st.markdown("#### Fraud Rate por Rango de Monto")
        BINS   = [0, 5000, 10000, 20000, 50000, np.inf]
        LABELS = ["<5k","5k–10k","10k–20k","20k–50k","50k+"]
        df_clean["RANGO"] = pd.cut(df_clean[COL_MONTO],
                                    bins=BINS, labels=LABELS, right=False)
        rango_pivot = (df_clean.groupby(["RANGO", COL_IND])
                       .size().unstack(fill_value=0))
        if "F" in rango_pivot.columns:
            rango_fr = (rango_pivot.get("F",0) /
                        rango_pivot.sum(axis=1) * 100).fillna(0)
            fig, ax = plt.subplots(figsize=(5, 4))
            colores_r = plt.cm.YlOrRd(rango_fr / max(rango_fr.max(), 0.01))
            ax.bar(rango_fr.index, rango_fr.values,
                   color=colores_r, edgecolor="#0f1117")
            for i, (rango, val) in enumerate(rango_fr.items()):
                n_f = rango_pivot.get("F", pd.Series()).get(rango, 0)
                ax.text(i, val + 0.005, f"{val:.2f}%\n(N={n_f})",
                        ha="center", fontsize=8, color="#e8eaf0")
            ax.set_xlabel("Rango (S/)"); ax.set_ylabel("Fraud Rate (%)")
            ax.spines[["top","right"]].set_visible(False)
            fig.tight_layout()
            st.pyplot(fig); plt.close()

    st.markdown("---")

    # KDE monto F vs N
    st.markdown("#### Distribución del Monto por Indicador")
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for ind in ["F","N","G"]:
        s = df_clean.loc[df_clean[COL_IND]==ind, COL_MONTO]
        if len(s) < 5: continue
        sns.kdeplot(s, ax=axes[0], label=ind,
                    color=COLORES.get(ind,"#999"),
                    fill=True, alpha=0.2, linewidth=1.8)
    axes[0].set_xlabel("Monto (S/)"); axes[0].set_ylabel("Densidad")
    axes[0].xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x,_: f"S/{x:,.0f}"))
    axes[0].legend(); axes[0].spines[["top","right"]].set_visible(False)

    bp_data   = [df_clean.loc[df_clean[COL_IND]==i, COL_MONTO].values
                 for i in ["F","N","G"] if len(df_clean[df_clean[COL_IND]==i])>0]
    bp_labels = [i for i in ["F","N","G"]
                 if len(df_clean[df_clean[COL_IND]==i])>0]
    if bp_data:
        bp = axes[1].boxplot(bp_data, labels=bp_labels, patch_artist=True,
                             flierprops=dict(marker="o",markersize=2,alpha=0.3))
        for patch, ind in zip(bp["boxes"], bp_labels):
            patch.set_facecolor(COLORES.get(ind,"#999")); patch.set_alpha(0.7)
        axes[1].set_ylabel("Monto (S/)")
        axes[1].yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x,_: f"S/{x:,.0f}"))
        axes[1].spines[["top","right"]].set_visible(False)

    fig.tight_layout()
    st.pyplot(fig); plt.close()

    # Tabla descriptiva compacta
    st.markdown("#### Descriptivos por Indicador")
    filas = []
    for ind in ["F","N","G"]:
        s = df_clean.loc[df_clean[COL_IND]==ind, COL_MONTO].dropna()
        if len(s) == 0: continue
        filas.append({
            "Indicador": ind, "N": f"{len(s):,}",
            "Mediana": f"S/ {s.median():,.0f}",
            "Media":   f"S/ {s.mean():,.0f}",
            "P90":     f"S/ {s.quantile(.9):,.0f}",
            "Máx":     f"S/ {s.max():,.0f}",
            "Asimetría": f"{s.skew():.2f}",
        })
    if filas:
        st.dataframe(pd.DataFrame(filas).set_index("Indicador"),
                     use_container_width=True)


# ══════════════════════════════════════════════════════════════
# TAB 2 — ANÁLISIS TEMPORAL
# ══════════════════════════════════════════════════════════════
with tab2:
    if "HORA" not in df_clean.columns:
        st.warning("Columna HORA no disponible.")
    else:
        hora_stats = (df_clean
            .groupby("HORA")
            .agg(N_total=(COL_IND,"count"),
                 N_fraude=(COL_IND, lambda x: (x=="F").sum()))
            .assign(FR=lambda x: x["N_fraude"]/x["N_total"]*100)
            .reset_index()
        )

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Volumen por Hora")
            fig, ax = plt.subplots(figsize=(6, 3.5))
            ax.bar(hora_stats["HORA"], hora_stats["N_total"],
                   color="#2a3555", label="Total")
            ax.bar(hora_stats["HORA"], hora_stats["N_fraude"],
                   color="#E63946", label="Fraude")
            ax.set_xlabel("Hora"); ax.set_ylabel("N")
            ax.set_xticks(range(0,24,2))
            ax.legend(fontsize=8)
            ax.spines[["top","right"]].set_visible(False)
            fig.tight_layout(); st.pyplot(fig); plt.close()

        with col2:
            st.markdown("#### Fraud Rate por Hora")
            media_fr = hora_stats["FR"].mean()
            colores_h = ["#E63946" if fr > media_fr*2 else "#457B9D"
                         for fr in hora_stats["FR"]]
            fig, ax = plt.subplots(figsize=(6, 3.5))
            ax.bar(hora_stats["HORA"], hora_stats["FR"],
                   color=colores_h, edgecolor="#0f1117")
            ax.axhline(media_fr, color="#E9C46A", linestyle="--",
                       lw=1.5, label=f"Media: {media_fr:.2f}%")
            ax.set_xlabel("Hora"); ax.set_ylabel("FR (%)")
            ax.set_xticks(range(0,24,2)); ax.legend(fontsize=8)
            ax.spines[["top","right"]].set_visible(False)
            fig.tight_layout(); st.pyplot(fig); plt.close()

        # Heatmap hora × día
        st.markdown("#### Heatmap Fraud Rate — Hora × Día")
        pivot_hm = (df_clean
            .assign(FB=lambda x: (x[COL_IND]=="F").astype(int))
            .groupby(["DIA_SEMANA","HORA"])["FB"]
            .mean() * 100
        ).unstack("HORA")
        pivot_hm = pivot_hm.reindex(
            [d for d in ORDEN_DIAS if d in pivot_hm.index]
        )
        pivot_hm.index = [DIAS_ES.get(d,d) for d in pivot_hm.index]
        fig, ax = plt.subplots(figsize=(14, 4))
        sns.heatmap(pivot_hm, cmap="YlOrRd", annot=True, fmt=".1f",
                    linewidths=0.3, ax=ax,
                    cbar_kws={"label":"FR%"},
                    annot_kws={"size":7})
        ax.set_xlabel("Hora"); ax.set_ylabel("")
        fig.tight_layout(); st.pyplot(fig); plt.close()

        # Fraud rate por mes
        st.markdown("#### Evolución del Fraud Rate por Mes")
        mes_stats = (df_clean
            .groupby("MES_NUM")
            .agg(N_total=(COL_IND,"count"),
                 N_fraude=(COL_IND, lambda x: (x=="F").sum()))
            .assign(FR=lambda x: x["N_fraude"]/x["N_total"]*100)
            .reset_index()
        )
        mes_stats["MES_NOMBRE"] = mes_stats["MES_NUM"].map(MESES_ES)
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.bar(mes_stats["MES_NOMBRE"], mes_stats["FR"],
               color="#2a3555", edgecolor="#0f1117")
        ax.plot(mes_stats["MES_NOMBRE"], mes_stats["FR"],
                "o-", color="#E63946", lw=2, ms=8)
        for _, row in mes_stats.iterrows():
            ax.text(row.name, row["FR"]+0.003,
                    f"{row['FR']:.3f}%", ha="center", fontsize=9)
        ax.set_ylabel("Fraud Rate (%)"); ax.set_xlabel("")
        ax.spines[["top","right"]].set_visible(False)
        fig.tight_layout(); st.pyplot(fig); plt.close()


# ══════════════════════════════════════════════════════════════
# TAB 3 — VARIABLES DE COMPORTAMIENTO
# ══════════════════════════════════════════════════════════════
with tab3:
    FLAGS = [
        ("FLAG_VEL_ALTA",           "≥2 txn mismo día (cuenta origen)"),
        ("FLAG_VEL_CRITICA",        "≥3 txn mismo día (cuenta origen)"),
        ("FLAG_BENEF_NUEVO",        "Primera vez origen → ese destino"),
        ("FLAG_CUENTA_MULA",        "≥3 orígenes distintos → mismo destino/día"),
        ("FLAG_CLIENTE_REINCIDENTE","Cliente con fraude previo confirmado"),
        ("FLAG_VACIADO",            "Monto ≥ 80% del saldo disponible"),
    ]

    flags_presentes = [(c,d) for c,d in FLAGS if c in df_clean.columns]

    if not flags_presentes:
        st.warning("No se encontraron variables de comportamiento. "
                   "Verifica que el parquet venga del NB3.")
    else:
        # Tabla resumen
        st.markdown("#### Resumen de Discriminación")
        filas = []
        for col, desc in flags_presentes:
            pct_F = df_clean.loc[df_clean[COL_IND]=="F", col].mean() * 100
            pct_N = df_clean.loc[df_clean[COL_IND]=="N", col].mean() * 100
            sep   = pct_F - pct_N
            filas.append({
                "Variable":    col,
                "Descripción": desc,
                "% en F":      round(pct_F, 1),
                "% en N":      round(pct_N, 1),
                "Separación":  round(sep, 1),
                "Útil":        "✅" if sep>5 else ("⚠️" if sep>2 else "❌"),
            })
        tabla_flags = pd.DataFrame(filas)
        st.dataframe(
            tabla_flags.style.background_gradient(
                subset=["Separación"], cmap="RdYlGn"
            ),
            use_container_width=True,
            hide_index=True
        )

        st.markdown("---")
        st.markdown("#### Detalle por Variable")

        # Gráficos individuales por flag
        cols_graf = st.columns(2)
        for idx, (col, desc) in enumerate(flags_presentes):
            with cols_graf[idx % 2]:
                st.markdown(f"**{col}** — {desc}")
                pct = (df_clean.groupby(COL_IND)[col]
                       .mean().mul(100)
                       .reindex(["F","N","G"]).dropna()
                       .reset_index())
                pct.columns = ["Indicador", "Pct"]
                fig, ax = plt.subplots(figsize=(4, 2.8))
                colores_f = [COLORES.get(i,"#999") for i in pct["Indicador"]]
                ax.bar(pct["Indicador"], pct["Pct"],
                       color=colores_f, edgecolor="#0f1117")
                for _, row in pct.iterrows():
                    ax.text(row.name, row["Pct"]+0.3,
                            f"{row['Pct']:.1f}%",
                            ha="center", fontsize=9, color="#e8eaf0")
                ax.set_ylabel("%"); ax.set_xlabel("Indicador")
                ax.spines[["top","right"]].set_visible(False)
                fig.tight_layout()
                st.pyplot(fig); plt.close()

        # Regla candidata automática
        st.markdown("---")
        st.markdown("#### 🎯 Regla Candidata (basada en filtros actuales)")

        utiles = [f for f,_ in flags_presentes
                  if tabla_flags.loc[tabla_flags["Variable"]==f,
                                     "Separación"].values[0] > 5]

        if len(utiles) >= 2 and n_fraude > 0:
            # Simular regla: combinación de flags útiles
            mascara = df_clean[utiles[0]] == 1
            for u in utiles[1:2]:
                mascara = mascara & (df_clean[u] == 1)

            n_regla       = mascara.sum()
            captura_F     = (df_clean.loc[mascara, COL_IND]=="F").sum()
            precision     = captura_F / n_regla * 100 if n_regla > 0 else 0
            recall        = captura_F / n_fraude * 100 if n_fraude > 0 else 0
            falsos_alarma = n_regla - captura_F

            st.markdown(f"""
            > **Condición:** `{" AND ".join(utiles[:2])} = 1`
            """)
            r1, r2, r3, r4 = st.columns(4)
            r1.metric("Alertas generadas", f"{n_regla:,}")
            r2.metric("Fraudes capturados", f"{captura_F:,}",
                      delta=f"{recall:.1f}% recall")
            r3.metric("Precisión", f"{precision:.1f}%")
            r4.metric("Falsas alarmas", f"{falsos_alarma:,}")

            if precision > 50:
                st.success("✅ Regla con buena precisión — candidata para propuesta")
            elif precision > 20:
                st.warning("⚠️ Precisión media — evaluar umbral más restrictivo")
            else:
                st.error("❌ Demasiadas falsas alarmas — agregar condición adicional")
        else:
            st.info("Selecciona segmentos con más fraudes para ver la regla candidata.")
