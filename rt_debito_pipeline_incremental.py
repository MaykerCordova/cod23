"""
==============================================================================
PIPELINE DE INGESTIÓN INCREMENTAL - RT DÉBITO (Real Time TD)
==============================================================================
Arquitectura: Medallion (Bronze → Silver → Gold)
Motor: Python + Polars
Almacenamiento: Parquet (Silver) + SQLite (Metadata)

Flujo:
  1. Bootstrap: Access (consolidado BBDD_REAL_TIME_TD) → Parquet Silver base
  2. Diario: Excel (.xlsx) → Bronze → Transformación → Append Silver

Autor: Mayker Cordova
Fecha: Abril 2026
==============================================================================
"""

import polars as pl
import hashlib
import sqlite3
from pathlib import Path
from datetime import datetime, date
import shutil
import warnings

warnings.filterwarnings("ignore")


# ============================================================================
# CONFIGURACIÓN CENTRAL
# ============================================================================

class Config:
    """Configuración centralizada del pipeline RT Débito."""

    # === RUTAS BASE ===
    BASE_DIR = Path(r"C:\Users\s4930359\Data_Herramientas")

    # Estructura Medallion
    BRONZE_DIR = BASE_DIR / "data" / "bronze" / "rt_debito"
    SILVER_DIR = BASE_DIR / "data" / "silver"
    GOLD_DIR = BASE_DIR / "data" / "gold"
    METADATA_DIR = BASE_DIR / "data" / "metadata"

    # Archivos Silver
    SILVER_PARQUET = SILVER_DIR / "rt_debito_consolidated.parquet"
    METADATA_DB = METADATA_DIR / "ingestion_log.db"

    # === ACCESS (consolidado - solo para bootstrap) ===
    ACCESS_PATH = BASE_DIR / "BBDD_Real_Time.accdb"
    ACCESS_TABLE = "BBDD_REAL_TIME_TD"

    # === SCHEMA: Columnas del Silver final ===
    SILVER_SCHEMA = [
        "USUARIO",
        "GENERO ALERTA",
        "ACF-MONTO DOLLAR",
        "ACF-FECHA TRX",
        "ACF-MONTO EN MONEDA LOCAL",
        "ACF-MCC +",
        "ACF-NOMBRE/LOCALIZACION COMERCIO",
        "ACF-PAIS ORIGEN 87519",
        "ACF-TARJETA REGISTRO 750",
        "ACF-INDICADOR DE FRAUDE",
        "ACF-BIN",
        "ACF-Entry Mode",
        "ACF-TVR",
        "Condiciones Cumplidas",
        "ACF_Fecha_TRX",
        "Anomes_TRX",
        "Dia_TRX",
    ]

    # === Mapeo Excel diario → Silver ===
    # Las columnas del Excel diario que corresponden al Silver
    # El Excel viene del journal/monitor con ~17 columnas relevantes
    COLUMN_MAP = {
        "USUARIO":                           "USUARIO",
        "GENERO ALERTA":                     "GENERO ALERTA",
        "ACF-MONTO DOLLAR":                  "ACF-MONTO DOLLAR",
        "ACF-FECHA TRX":                     "ACF-FECHA TRX",
        "ACF-MONTO EN MONEDA LOCAL":         "ACF-MONTO EN MONEDA LOCAL",
        "ACF-MCC +":                         "ACF-MCC +",
        "ACF-NOMBRE/LOCALIZACION COMERCIO":  "ACF-NOMBRE/LOCALIZACION COMERCIO",
        "ACF-PAIS ORIGEN 87519":             "ACF-PAIS ORIGEN 87519",
        "ACF-TARJETA REGISTRO 750":          "ACF-TARJETA REGISTRO 750",
        "ACF-INDICADOR DE FRAUDE":           "ACF-INDICADOR DE FRAUDE",
        "ACF-BIN":                           "ACF-BIN",
        "ACF-Entry Mode":                    "ACF-Entry Mode",
        "ACF-ENTRY MODE":                    "ACF-Entry Mode",  # fallback mayúsculas
        "CONDICION RT":                      "_CONDICION_RT_RAW",
    }


# ============================================================================
# UTILIDADES
# ============================================================================

def crear_estructura_directorios():
    """Crea la estructura de carpetas Medallion si no existe."""
    for d in [Config.BRONZE_DIR, Config.SILVER_DIR, Config.GOLD_DIR, Config.METADATA_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    print("Estructura de directorios verificada.")


def log_ingestion(metadata: dict):
    """Registra la ejecución en la base de metadata SQLite."""
    Config.METADATA_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(Config.METADATA_DB))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            run_id TEXT,
            tool_name TEXT,
            source_file TEXT,
            rows_read INTEGER,
            rows_new INTEGER,
            rows_updated INTEGER,
            rows_skipped INTEGER,
            errors INTEGER,
            duration_sec REAL,
            status TEXT,
            created_at TEXT
        )
    """)
    conn.execute(
        """INSERT INTO ingestion_log VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            metadata.get("run_id", ""),
            metadata.get("tool_name", "rt_debito"),
            metadata.get("source_file", ""),
            metadata.get("rows_read", 0),
            metadata.get("rows_new", 0),
            metadata.get("rows_updated", 0),
            metadata.get("rows_skipped", 0),
            metadata.get("errors", 0),
            metadata.get("duration_sec", 0.0),
            metadata.get("status", "SUCCESS"),
            datetime.now().isoformat(),
        )
    )
    conn.commit()
    conn.close()


# ============================================================================
# EXTRACTOR
# ============================================================================

class RTDebitoExtractor:
    """Lee el Excel diario de RT Débito."""

    @staticmethod
    def guardar_en_bronze(excel_path: str, fecha_descarga: str = None):
        """Copia el Excel original a Bronze particionado por fecha."""
        if fecha_descarga is None:
            fecha_descarga = date.today().isoformat()

        year, month, day = fecha_descarga.split("-")
        bronze_folder = Config.BRONZE_DIR / year / month / day
        bronze_folder.mkdir(parents=True, exist_ok=True)

        filename = Path(excel_path).name
        shutil.copy2(excel_path, bronze_folder / filename)

        print(f"  Bronze: Archivo guardado en {bronze_folder}")
        return bronze_folder

    @staticmethod
    def leer_excel(excel_path: str, skip_rows: int = 4) -> pl.DataFrame:
        """Lee el Excel diario de RT Débito."""
        try:
            df = pl.read_excel(
                excel_path,
                read_options={"skip_rows": skip_rows},
            )
        except Exception as e:
            print(f"  Error leyendo con Polars: {e}")
            print(f"  Reintentando con Pandas...")
            import pandas as pd
            df_pd = pd.read_excel(excel_path, skiprows=skip_rows)
            df = pl.from_pandas(df_pd)

        print(f"  Excel leido: {df.shape[0]:,} filas x {df.shape[1]} columnas")
        return df


# ============================================================================
# TRANSFORMER
# ============================================================================

class RTDebitoTransformer:
    """Transforma los datos crudos de RT Débito al schema Silver."""

    @staticmethod
    def mapear_columnas(df: pl.DataFrame) -> pl.DataFrame:
        """Selecciona y renombra columnas del Excel al schema Silver."""
        rename_map = {}
        columnas_presentes = set(df.columns)

        for col_excel, col_silver in Config.COLUMN_MAP.items():
            if col_excel in columnas_presentes:
                if col_silver not in rename_map.values():
                    rename_map[col_excel] = col_silver

        df = df.rename(rename_map)

        # Quedarnos con columnas mapeadas
        cols_to_keep = [col for col in df.columns if col in Config.COLUMN_MAP.values()]
        df = df.select(cols_to_keep)

        print(f"  Columnas mapeadas: {len(rename_map)}")
        return df

    @staticmethod
    def parsear_fecha(df: pl.DataFrame) -> pl.DataFrame:
        """
        Parsea ACF-FECHA TRX con formatos mixtos:
        - "2024/11/01" (con slash, formato YYYY/MM/DD)
        - "20250625" (sin slash, formato YYYYMMDD)
        - "0" o vacío → null

        Genera: ACF_Fecha_TRX (Date), Anomes_TRX, Dia_TRX
        """
        fecha_col = "ACF-FECHA TRX"

        # Si ya es datetime, convertir directo
        if df[fecha_col].dtype in [pl.Date, pl.Datetime]:
            df = df.with_columns(
                pl.col(fecha_col).cast(pl.Date).alias("ACF_Fecha_TRX")
            )
        else:
            # Convertir a texto y limpiar
            df = df.with_columns(
                pl.col(fecha_col)
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.replace_all(r"\.0$", "")  # quitar .0 si viene como float
                .alias("_fecha_raw")
            )

            # Reemplazar "0", vacíos y "None" con null
            df = df.with_columns(
                pl.when(
                    (pl.col("_fecha_raw") == "0")
                    | (pl.col("_fecha_raw") == "")
                    | (pl.col("_fecha_raw") == "None")
                    | (pl.col("_fecha_raw") == "null")
                    | (pl.col("_fecha_raw").is_null())
                )
                .then(pl.lit(None).cast(pl.Utf8))
                .otherwise(pl.col("_fecha_raw"))
                .alias("_fecha_clean")
            )

            # Detectar formato: con slash (YYYY/MM/DD) o sin slash (YYYYMMDD)
            df = df.with_columns(
                pl.when(pl.col("_fecha_clean").is_null())
                .then(pl.lit(None).cast(pl.Date))
                # Formato con slash: YYYY/MM/DD
                .when(pl.col("_fecha_clean").str.contains("/"))
                .then(
                    pl.col("_fecha_clean").str.to_date("%Y/%m/%d", strict=False)
                )
                # Formato sin slash: YYYYMMDD
                .otherwise(
                    pl.col("_fecha_clean").str.to_date("%Y%m%d", strict=False)
                )
                .alias("ACF_Fecha_TRX")
            )

            # Limpiar temporales
            df = df.drop(["_fecha_raw", "_fecha_clean"])

        # Derivar Anomes_TRX y Dia_TRX
        df = df.with_columns([
            pl.when(pl.col("ACF_Fecha_TRX").is_not_null())
            .then(
                pl.col("ACF_Fecha_TRX").dt.year().cast(pl.Utf8)
                + pl.col("ACF_Fecha_TRX").dt.month().cast(pl.Utf8).str.pad_start(2, "0")
            )
            .otherwise(pl.lit(None).cast(pl.Utf8))
            .alias("Anomes_TRX"),

            pl.when(pl.col("ACF_Fecha_TRX").is_not_null())
            .then(
                pl.col("ACF_Fecha_TRX").dt.day().cast(pl.Utf8).str.pad_start(2, "0")
            )
            .otherwise(pl.lit(None).cast(pl.Utf8))
            .alias("Dia_TRX"),
        ])

        # Contar fechas parseadas vs nulls
        total = df.shape[0]
        nulls = df["ACF_Fecha_TRX"].null_count()
        parsed = total - nulls
        print(f"  Fechas parseadas: {parsed:,} de {total:,} ({nulls:,} nulls)")

        if parsed > 0:
            print(f"  Rango: {df['ACF_Fecha_TRX'].drop_nulls().min()} a {df['ACF_Fecha_TRX'].drop_nulls().max()}")

        return df

    @staticmethod
    def aplicar_reglas_negocio(df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica reglas de negocio:
        - ACF-TVR: LEFT(CONDICION RT, 4) — primeros 4 caracteres
        - Condiciones Cumplidas: valor completo de CONDICION RT
        - Tipado de columnas numéricas
        """
        # Si viene _CONDICION_RT_RAW del mapeo del Excel
        if "_CONDICION_RT_RAW" in df.columns:
            df = df.with_columns([
                # ACF-TVR = primeros 4 caracteres
                pl.col("_CONDICION_RT_RAW")
                .cast(pl.Utf8)
                .str.slice(0, 4)
                .alias("ACF-TVR"),

                # Condiciones Cumplidas = valor completo
                pl.col("_CONDICION_RT_RAW")
                .cast(pl.Utf8)
                .alias("Condiciones Cumplidas"),
            ])
            df = df.drop("_CONDICION_RT_RAW")
        else:
            # En bootstrap, ya vienen las columnas correctas del Access
            pass

        # Asegurar que columnas existan
        for col in ["ACF-TVR", "Condiciones Cumplidas"]:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))

        print(f"  Reglas de negocio aplicadas.")
        return df

    @staticmethod
    def tipar_columnas(df: pl.DataFrame) -> pl.DataFrame:
        """Aplica tipos de datos correctos."""
        # Columnas numéricas
        for col in ["ACF-MONTO DOLLAR", "ACF-MONTO EN MONEDA LOCAL", "USUARIO"]:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Float64, strict=False).alias(col)
                )

        for col in ["ACF-MCC +", "ACF-BIN", "ACF-Entry Mode"]:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Utf8, strict=False).alias(col)
                )

        # Resto a texto
        text_cols = [
            c for c in df.columns
            if c not in ["ACF-MONTO DOLLAR", "ACF-MONTO EN MONEDA LOCAL", "USUARIO", "ACF_Fecha_TRX"]
        ]
        for col in text_cols:
            if col in df.columns and df[col].dtype != pl.Utf8:
                df = df.with_columns(
                    pl.col(col).cast(pl.Utf8, strict=False).alias(col)
                )

        print(f"  Tipos de datos aplicados.")
        return df

    @staticmethod
    def generar_hashes(df: pl.DataFrame) -> pl.DataFrame:
        """
        Hash SHA-256 para deduplicación.
        Composite key: ACF_Fecha_TRX + ACF-MONTO DOLLAR + ACF-BIN +
                        ACF-NOMBRE/LOCALIZACION COMERCIO + Condiciones Cumplidas
        """
        df = df.with_columns(
            (
                pl.col("ACF_Fecha_TRX").cast(pl.Utf8).fill_null("")
                + "|" + pl.col("ACF-MONTO DOLLAR").cast(pl.Utf8).fill_null("")
                + "|" + pl.col("ACF-BIN").cast(pl.Utf8).fill_null("")
                + "|" + pl.col("ACF-NOMBRE/LOCALIZACION COMERCIO").cast(pl.Utf8).fill_null("")
                + "|" + pl.col("Condiciones Cumplidas").cast(pl.Utf8).fill_null("")
            ).alias("_composite_key")
        )

        df = df.with_columns(
            pl.col("_composite_key")
            .map_elements(
                lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest(),
                return_dtype=pl.Utf8
            )
            .alias("_row_hash")
        )

        df = df.drop("_composite_key")
        print(f"  Hashes generados: {df.shape[0]:,} registros")
        return df

    @classmethod
    def transformar(cls, df: pl.DataFrame) -> pl.DataFrame:
        """Pipeline completo de transformación Silver."""
        print("\n--- TRANSFORMACION SILVER ---")
        df = cls.mapear_columnas(df)
        df = cls.parsear_fecha(df)
        df = cls.aplicar_reglas_negocio(df)
        df = cls.tipar_columnas(df)
        df = cls.generar_hashes(df)

        # Reordenar
        final_cols = [c for c in Config.SILVER_SCHEMA if c in df.columns]
        final_cols.append("_row_hash")
        df = df.select(final_cols)

        print(f"  Schema final: {len(final_cols)} columnas")
        print(f"  Filas: {df.shape[0]:,}")
        return df


# ============================================================================
# LOADER
# ============================================================================

class SilverLoader:
    """Persistencia y upsert del Parquet Silver."""

    @staticmethod
    def bootstrap_from_access(access_df: pl.DataFrame) -> pl.DataFrame:
        """Inicializa Silver desde el consolidado de Access."""
        print("\n--- BOOTSTRAP DESDE ACCESS ---")
        print(f"  Input: {access_df.shape[0]:,} filas x {access_df.shape[1]} columnas")

        # Parsear fecha
        access_df = RTDebitoTransformer.parsear_fecha(access_df)

        # Tipar
        access_df = RTDebitoTransformer.tipar_columnas(access_df)

        # Hashes
        access_df = RTDebitoTransformer.generar_hashes(access_df)

        # Reordenar
        final_cols = [c for c in Config.SILVER_SCHEMA if c in access_df.columns]
        final_cols.append("_row_hash")
        access_df = access_df.select(final_cols)

        # Guardar
        Config.SILVER_DIR.mkdir(parents=True, exist_ok=True)
        access_df.write_parquet(Config.SILVER_PARQUET, compression="zstd")

        size_mb = Config.SILVER_PARQUET.stat().st_size / (1024 * 1024)
        print(f"  Silver base creado: {Config.SILVER_PARQUET}")
        print(f"  Filas: {access_df.shape[0]:,} | Columnas: {access_df.shape[1]}")
        print(f"  Tamano: {size_mb:.1f} MB")

        return access_df

    @staticmethod
    def upsert_incremental(df_new: pl.DataFrame) -> dict:
        """Upsert incremental contra Silver existente."""
        print("\n--- UPSERT INCREMENTAL ---")
        metrics = {"rows_read": df_new.shape[0], "rows_new": 0, "rows_updated": 0, "rows_skipped": 0}

        if not Config.SILVER_PARQUET.exists():
            print("  No existe Silver base. Creando desde cero...")
            Config.SILVER_DIR.mkdir(parents=True, exist_ok=True)
            df_new.write_parquet(Config.SILVER_PARQUET, compression="zstd")
            metrics["rows_new"] = df_new.shape[0]
            print(f"  Silver creado: {df_new.shape[0]:,} filas nuevas")
            return metrics

        df_existing = pl.read_parquet(Config.SILVER_PARQUET)
        print(f"  Silver existente: {df_existing.shape[0]:,} filas")

        # Schema evolution
        for col in df_new.columns:
            if col not in df_existing.columns:
                print(f"  Schema evolution: agregando '{col}' al historico")
                df_existing = df_existing.with_columns(
                    pl.lit(None).cast(df_new[col].dtype).alias(col)
                )

        for col in df_existing.columns:
            if col not in df_new.columns:
                print(f"  Schema evolution: agregando '{col}' al delta")
                df_new = df_new.with_columns(
                    pl.lit(None).cast(df_existing[col].dtype).alias(col)
                )

        # Comparar hashes
        existing_hashes = set(df_existing["_row_hash"].to_list())
        new_hashes = set(df_new["_row_hash"].to_list())

        hashes_nuevos = new_hashes - existing_hashes
        metrics["rows_new"] = len(hashes_nuevos)
        metrics["rows_skipped"] = len(new_hashes & existing_hashes)

        print(f"  Registros nuevos: {metrics['rows_new']:,}")
        print(f"  Ya existentes (skip): {metrics['rows_skipped']:,}")

        if metrics["rows_new"] == 0:
            print("  No hay registros nuevos. Silver sin cambios.")
            return metrics

        df_to_append = df_new.filter(pl.col("_row_hash").is_in(list(hashes_nuevos)))
        df_to_append = df_to_append.select(df_existing.columns)

        df_final = pl.concat([df_existing, df_to_append], how="diagonal")

        # Backup
        backup_path = Config.SILVER_DIR / f"rt_debito_consolidated_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        shutil.copy2(Config.SILVER_PARQUET, backup_path)
        print(f"  Backup: {backup_path.name}")

        df_final.write_parquet(Config.SILVER_PARQUET, compression="zstd")

        size_mb = Config.SILVER_PARQUET.stat().st_size / (1024 * 1024)
        print(f"  Silver actualizado: {df_final.shape[0]:,} filas | {size_mb:.1f} MB")

        return metrics


# ============================================================================
# ORQUESTADOR
# ============================================================================

def run_bootstrap(access_df: pl.DataFrame):
    """BOOTSTRAP: Access → Silver Parquet (una sola vez)."""
    print("=" * 60)
    print("BOOTSTRAP: Access -> Silver Parquet (RT DEBITO)")
    print("=" * 60)

    start = datetime.now()
    crear_estructura_directorios()
    df_silver = SilverLoader.bootstrap_from_access(access_df)
    duration = (datetime.now() - start).total_seconds()

    log_ingestion({
        "run_id": datetime.now().isoformat(),
        "tool_name": "rt_debito",
        "source_file": "ACCESS_BOOTSTRAP",
        "rows_read": df_silver.shape[0],
        "rows_new": df_silver.shape[0],
        "duration_sec": duration,
        "status": "SUCCESS",
    })

    print(f"\nBootstrap completado en {duration:.1f} segundos.")
    return df_silver


def run_daily(excel_path: str, fecha_descarga: str = None):
    """CARGA DIARIA: Excel → Bronze → Silver."""
    print("=" * 60)
    print("CARGA INCREMENTAL DIARIA (RT DEBITO)")
    print("=" * 60)

    start = datetime.now()
    crear_estructura_directorios()

    print("\n1. BRONZE: Guardando archivo original...")
    RTDebitoExtractor.guardar_en_bronze(excel_path, fecha_descarga)

    print("\n2. EXTRACCION: Leyendo Excel...")
    df_raw = RTDebitoExtractor.leer_excel(excel_path)

    print("\n3. TRANSFORMACION:")
    df_transformed = RTDebitoTransformer.transformar(df_raw)

    print("\n4. CARGA:")
    metrics = SilverLoader.upsert_incremental(df_transformed)

    duration = (datetime.now() - start).total_seconds()

    log_ingestion({
        "run_id": datetime.now().isoformat(),
        "tool_name": "rt_debito",
        "source_file": f"{Path(excel_path).name}_{fecha_descarga or date.today().isoformat()}",
        "rows_read": metrics["rows_read"],
        "rows_new": metrics["rows_new"],
        "rows_skipped": metrics.get("rows_skipped", 0),
        "duration_sec": duration,
        "status": "SUCCESS",
    })

    print(f"\n{'=' * 60}")
    print(f"RESUMEN")
    print(f"{'=' * 60}")
    print(f"  Filas leidas:     {metrics['rows_read']:,}")
    print(f"  Nuevas:           {metrics['rows_new']:,}")
    print(f"  Ya existentes:    {metrics.get('rows_skipped', 0):,}")
    print(f"  Duracion:         {duration:.1f} seg")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    print("Pipeline RT Debito listo. Descomenta el paso que necesites ejecutar.")
