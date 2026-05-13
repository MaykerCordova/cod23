"""
consolidar_journals.py
──────────────────────
Une los 8 journals de Monitor (enero–abril) en un solo DataFrame.

Reglas de carga:
  · Header en fila 4  → skiprows=3
  · Todo se lee como texto (dtype=str) excepto MONTO
  · Se construye FECHA_HORA combinando FECHA_TRX + HORA_TRX
  · Se agrega columna MES para trazabilidad
"""

import pandas as pd
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# 1. AJUSTA AQUÍ: rutas y nombre exacto de columnas en Monitor
# ─────────────────────────────────────────────────────────────

ARCHIVOS = [
    # (ruta_al_archivo,          etiqueta para columna MES)
    ("journal_enero_q1.xlsx",    "Ene-Q1"),
    ("journal_enero_q2.xlsx",    "Ene-Q2"),
    ("journal_febrero_q1.xlsx",  "Feb-Q1"),
    ("journal_febrero_q2.xlsx",  "Feb-Q2"),
    ("journal_marzo_q1.xlsx",    "Mar-Q1"),
    ("journal_marzo_q2.xlsx",    "Mar-Q2"),
    ("journal_abril_q1.xlsx",    "Abr-Q1"),
    ("journal_abril_q2.xlsx",    "Abr-Q2"),
]

# Nombre exacto de las columnas en Monitor (ajustar si difieren)
COL_MONTO = "MONTO"          # columna monto → se castea a float
COL_FECHA = "FECHA_TRX"      # formato AAAAMMDD  (ej. 20250115)
COL_HORA  = "HORA_TRX"       # formato HH:MM:SS  (ej. 14:32:07)

SKIPROWS  = 3                # header en fila 4 → saltar las 3 primeras


# ─────────────────────────────────────────────────────────────
# 2. CARGA Y CONSOLIDACIÓN
# ─────────────────────────────────────────────────────────────

def cargar_journal(ruta: str, etiqueta: str) -> pd.DataFrame:
    path = Path(ruta)
    if not path.exists():
        print(f"  ⚠️  Archivo no encontrado: {ruta} — se omite")
        return pd.DataFrame()

    df = pd.read_excel(
        path,
        skiprows=SKIPROWS,   # header en fila 4
        dtype=str,           # todo como texto por defecto
        header=0,
    )

    # Eliminar filas completamente vacías que algunos exports generan al final
    df.dropna(how="all", inplace=True)

    # Limpiar espacios en nombres de columnas
    df.columns = df.columns.str.strip()

    # Castear MONTO a float (único numérico que necesitamos ahora)
    if COL_MONTO in df.columns:
        df[COL_MONTO] = (
            df[COL_MONTO]
            .str.strip()
            .str.replace(",", "", regex=False)   # quitar separadores de miles
            .pipe(pd.to_numeric, errors="coerce")
        )
    else:
        print(f"  ⚠️  Columna '{COL_MONTO}' no encontrada en {ruta}")

    # Trazabilidad
    df["QUINCENA"] = etiqueta

    print(f"  ✅ {etiqueta:10s} → {len(df):>6,} filas | {df.shape[1]} columnas")
    return df


def consolidar() -> pd.DataFrame:
    print("═" * 55)
    print("CARGANDO JOURNALS")
    print("═" * 55)

    partes = []
    for ruta, etiqueta in ARCHIVOS:
        df_part = cargar_journal(ruta, etiqueta)
        if not df_part.empty:
            partes.append(df_part)

    if not partes:
        raise ValueError("No se cargó ningún archivo. Verifica las rutas.")

    df = pd.concat(partes, ignore_index=True)
    print(f"\n  📦 Total consolidado: {len(df):,} filas")
    return df


# ─────────────────────────────────────────────────────────────
# 3. CONSTRUCCIÓN DE FECHA_HORA
# ─────────────────────────────────────────────────────────────

def construir_fecha_hora(df: pd.DataFrame) -> pd.DataFrame:
    """
    FECHA_TRX: string AAAAMMDD  →  '20250115'
    HORA_TRX : string HH:MM:SS  →  '14:32:07'
    Resultado: FECHA_HORA como datetime64
    """
    tiene_fecha = COL_FECHA in df.columns
    tiene_hora  = COL_HORA  in df.columns

    if not tiene_fecha:
        print(f"  ⚠️  Columna '{COL_FECHA}' no encontrada — FECHA_HORA no se crea")
        return df

    # Parsear fecha (AAAAMMDD)
    df["FECHA_TRX_dt"] = pd.to_datetime(
        df[COL_FECHA].str.strip(),
        format="%Y%m%d",
        errors="coerce"
    )

    if tiene_hora:
        # Combinar fecha + hora en un solo datetime
        fecha_hora_str = (
            df["FECHA_TRX_dt"].dt.strftime("%Y-%m-%d") +
            " " +
            df[COL_HORA].str.strip()
        )
        df["FECHA_HORA"] = pd.to_datetime(fecha_hora_str, format="%Y-%m-%d %H:%M:%S",
                                          errors="coerce")
    else:
        print(f"  ⚠️  Columna '{COL_HORA}' no encontrada — FECHA_HORA solo tendrá fecha")
        df["FECHA_HORA"] = df["FECHA_TRX_dt"]

    # Columnas auxiliares útiles para el análisis
    df["HORA"]         = df["FECHA_HORA"].dt.hour
    df["DIA_SEMANA"]   = df["FECHA_HORA"].dt.day_name()   # Monday, Tuesday…
    df["MES_NUM"]      = df["FECHA_HORA"].dt.month
    df["ES_FINDE"]     = df["FECHA_HORA"].dt.dayofweek.isin([5, 6]).astype(int)
    df["ES_MADRUGADA"] = df["HORA"].between(0, 5).astype(int)

    # Limpiar columna auxiliar
    df.drop(columns=["FECHA_TRX_dt"], inplace=True)

    nulos = df["FECHA_HORA"].isna().sum()
    if nulos:
        print(f"  ⚠️  {nulos:,} filas con FECHA_HORA nula (revisar formato en Monitor)")
    else:
        print("  ✅ FECHA_HORA construida sin nulos")

    return df


# ─────────────────────────────────────────────────────────────
# 4. RESUMEN RÁPIDO POST-CARGA
# ─────────────────────────────────────────────────────────────

def resumen(df: pd.DataFrame):
    print("\n" + "═" * 55)
    print("RESUMEN DEL DATASET CONSOLIDADO")
    print("═" * 55)
    print(f"  Filas totales      : {len(df):,}")
    print(f"  Columnas           : {df.shape[1]}")
    print(f"  Rango de fechas    : {df['FECHA_HORA'].min()}  →  {df['FECHA_HORA'].max()}")
    print(f"\n  Distribución por quincena:")
    print(df["QUINCENA"].value_counts().sort_index().to_string(header=False))

    if "INDICADOR" in df.columns:
        print(f"\n  Distribución por Indicador:")
        print(df["INDICADOR"].value_counts().to_string(header=False))

    if COL_MONTO in df.columns:
        print(f"\n  Monto (S/):")
        print(f"    Mín     : {df[COL_MONTO].min():>12,.2f}")
        print(f"    Mediana : {df[COL_MONTO].median():>12,.2f}")
        print(f"    Media   : {df[COL_MONTO].mean():>12,.2f}")
        print(f"    Máx     : {df[COL_MONTO].max():>12,.2f}")
        print(f"    Nulos   : {df[COL_MONTO].isna().sum():>12,}")


# ─────────────────────────────────────────────────────────────
# 5. MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":

    # Carga y consolidación
    df_total = consolidar()

    # Fecha + hora + flags temporales
    df_total = construir_fecha_hora(df_total)

    # Resumen
    resumen(df_total)

    # Guardar consolidado (pickle para preservar dtypes, + Excel para revisión)
    df_total.to_pickle("transferencias_consolidado.pkl")
    df_total.to_excel("transferencias_consolidado.xlsx", index=False)

    print("\n✅ Archivos guardados:")
    print("   · transferencias_consolidado.pkl   ← usa este en los análisis")
    print("   · transferencias_consolidado.xlsx  ← para revisión visual")
