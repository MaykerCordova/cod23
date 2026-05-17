"""
consolidar_journals.py  [Polars edition]
────────────────────────────────────────
Une los journals de Monitor en un solo DataFrame.

Reglas de carga:
  · Header en fila 5  →  read_options={"header_row": 4}   (0-indexed)
  · Todo se lee como texto excepto MONTO
  · Se construye FECHA_HORA combinando FECHA_TRX + HORA_TRX
  · Se agrega columna QUINCENA para trazabilidad
"""

import polars as pl
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# 1. AJUSTA AQUÍ: rutas y nombre exacto de columnas en Monitor
# ─────────────────────────────────────────────────────────────

ARCHIVOS = [
    # (ruta_al_archivo,            etiqueta para columna QUINCENA)
    ("mcc_01_10_abril.xlsx",      "Abril-Q1"),
    ("mcc_11_20_abril.xlsx",      "Abril-Q2"),
    ("mcc_21_30_abril.xlsx",      "Abril-Q3"),
    ("mcc_01_10_mayo.xlsx",       "Mayo-Q1"),
    ("mcc_11_16_mayo.xlsx",       "Mayo-Q2"),
]

# Nombre exacto de las columnas en Monitor (ajustar si difieren)
COL_MONTO = "ACF-MONTO EN MONEDA LOCAL"
COL_FECHA = "ACF-FECHA TRX"       # formato AAAAMMDD  (ej. 20250115)
COL_HORA  = "ACF-HORA TRX"        # formato HH:MM:SS  (ej. 14:32:07)

HEADER_ROW = 4   # fila 5 del Excel → índice 4 (0-indexed)

# ─────────────────────────────────────────────────────────────
# 2. CARGA Y CONSOLIDACIÓN
# ─────────────────────────────────────────────────────────────

def cargar_journal(ruta: str, etiqueta: str) -> pl.DataFrame:
    path = Path(ruta)
    if not path.exists():
        print(f"  ⚠  Archivo no encontrado: {ruta} — se omite")
        return pl.DataFrame()

    # ── Paso 1: leer solo el header para obtener nombres de columnas
    df_header = pl.read_excel(
        path,
        read_options={"header_row": HEADER_ROW, "n_rows": 0},
    )
    cols = [c.strip() for c in df_header.columns]

    # ── Paso 2: leer el archivo completo forzando todo a String
    #    schema_overrides con cada columna → pl.String  (soportado por fastexcel)
    schema_all_str = {c: pl.String for c in cols}

    df = pl.read_excel(
        path,
        read_options={
            "header_row": HEADER_ROW,
            "schema_overrides": schema_all_str,
        },
    )

    # Limpiar nombres de columna (strip de espacios)
    df = df.rename({c: c.strip() for c in df.columns})

    # Eliminar filas completamente vacías
    df = df.filter(
        ~pl.all_horizontal(pl.all().is_null())
    )

    # Castear MONTO a Float64
    if COL_MONTO in df.columns:
        df = df.with_columns(
            pl.col(COL_MONTO)
            .str.strip_chars()
            .str.replace(",", "", literal=True)   # quitar separador de miles
            .cast(pl.Float64, strict=False)
            .alias(COL_MONTO)
        )
    else:
        print(f"  ⚠  Columna '{COL_MONTO}' no encontrada en {ruta}")

    # Trazabilidad
    df = df.with_columns(pl.lit(etiqueta).alias("QUINCENA"))

    print(f"  ✅  {etiqueta:10s} → {len(df):>6,} filas | {df.width} columnas")
    return df


def consolidar() -> pl.DataFrame:
    print("=" * 55)
    print("CARGANDO JOURNALS")
    print("=" * 55)

    partes = [
        df_part
        for ruta, etiqueta in ARCHIVOS
        if not (df_part := cargar_journal(ruta, etiqueta)).is_empty()
    ]

    if not partes:
        raise ValueError("No se cargó ningún archivo. Verifica las rutas.")

    df = pl.concat(partes, how="diagonal_relaxed")
    print(f"\n  🔵  Total consolidado: {len(df):,} filas")
    return df


# ─────────────────────────────────────────────────────────────
# 3. CONSTRUCCIÓN DE FECHA_HORA
# ─────────────────────────────────────────────────────────────

def construir_fecha_hora(df: pl.DataFrame) -> pl.DataFrame:
    """
    FECHA_TRX: string AAAAMMDD → '20250115'
    HORA_TRX : string HH:MM:SS → '14:32:07'
    Resultado: FECHA_HORA como Datetime
    """
    tiene_fecha = COL_FECHA in df.columns
    tiene_hora  = COL_HORA  in df.columns

    if not tiene_fecha:
        print(f"  ⚠  Columna '{COL_FECHA}' no encontrada — FECHA_HORA no se crea")
        return df

    if tiene_hora:
        # Combinar fecha + hora → "20250115 14:32:07" → parse
        df = df.with_columns(
            (
                pl.col(COL_FECHA).str.strip_chars() +
                pl.lit(" ") +
                pl.col(COL_HORA).str.strip_chars()
            )
            .str.to_datetime(format="%Y%m%d %H:%M:%S", strict=False)
            .alias("FECHA_HORA")
        )
    else:
        print(f"  ⚠  Columna '{COL_HORA}' no encontrada — FECHA_HORA solo tendrá fecha")
        df = df.with_columns(
            pl.col(COL_FECHA)
            .str.strip_chars()
            .str.to_datetime(format="%Y%m%d", strict=False)
            .alias("FECHA_HORA")
        )

    # Columnas auxiliares útiles para el análisis
    df = df.with_columns([
        pl.col("FECHA_HORA").dt.hour().alias("HORA"),
        pl.col("FECHA_HORA").dt.weekday().alias("DIA_SEMANA_NUM"),   # 0=Lun … 6=Dom
        pl.col("FECHA_HORA").dt.to_string("%A").alias("DIA_SEMANA"), # Monday, Tuesday…
        pl.col("FECHA_HORA").dt.month().alias("MES_NUM"),
        pl.col("FECHA_HORA").dt.weekday().is_in([5, 6]).cast(pl.Int8).alias("ES_FINDE"),
        pl.col("FECHA_HORA").dt.hour().is_between(0, 5).cast(pl.Int8).alias("ES_MADRUGADA"),
    ])

    nulos = df["FECHA_HORA"].is_null().sum()
    if nulos:
        print(f"  ⚠  {nulos:,} filas con FECHA_HORA nula (revisar formato en Monitor)")
    else:
        print("  ✅  FECHA_HORA construida sin nulos")

    return df


# ─────────────────────────────────────────────────────────────
# 4. RESUMEN RÁPIDO POST-CARGA
# ─────────────────────────────────────────────────────────────

def resumen(df: pl.DataFrame):
    print("\n" + "=" * 55)
    print("RESUMEN DEL DATASET CONSOLIDADO")
    print("=" * 55)
    print(f"  Filas totales  : {len(df):,}")
    print(f"  Columnas       : {df.width}")

    if "FECHA_HORA" in df.columns:
        print(f"  Rango de fechas: {df['FECHA_HORA'].min()} → {df['FECHA_HORA'].max()}")

    print("\n  Distribución por quincena:")
    print(
        df.group_by("QUINCENA")
        .agg(pl.len().alias("filas"))
        .sort("QUINCENA")
    )

    if COL_MONTO in df.columns:
        stats = df.select(pl.col(COL_MONTO).drop_nulls())
        print(f"\n  Monto (S/):")
        print(f"    Mín     : {df[COL_MONTO].min():>14,.2f}")
        print(f"    Mediana : {df[COL_MONTO].median():>14,.2f}")
        print(f"    Media   : {df[COL_MONTO].mean():>14,.2f}")
        print(f"    Máx     : {df[COL_MONTO].max():>14,.2f}")
        print(f"    Nulos   : {df[COL_MONTO].is_null().sum():>14,}")


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

    # Guardar consolidado
    df_total.write_parquet("mcc_7995_consolidado.parquet")

    print("\n✅  Archivos guardados:")
    print("   - mcc_7995_consolidado.parquet  ← producción (rápido, tipado)")
