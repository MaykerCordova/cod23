"""
==============================================================================
PIPELINE DE INGESTIÓN INCREMENTAL - VRM (Visa Risk Management)
==============================================================================
Arquitectura: Medallion (Bronze → Silver → Gold)
Motor: Python + Polars
Almacenamiento: Parquet (Silver) + SQLite (Metadata)

Flujo:
  1. Bootstrap: Access (consolidado Marcial) → Parquet Silver base
  2. Diario: CSV (2 listas VRM) → Bronze → Transformación → Append Silver

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
    """Configuración centralizada del pipeline."""

    # === RUTAS BASE ===
    # AJUSTAR según tu entorno
    BASE_DIR = Path(r"C:\FRAUDES\HERRAMIENTAS\VRM")

    # Estructura Medallion
    BRONZE_DIR = BASE_DIR / "data" / "bronze" / "vrm"
    SILVER_DIR = BASE_DIR / "data" / "silver"
    GOLD_DIR = BASE_DIR / "data" / "gold"
    METADATA_DIR = BASE_DIR / "data" / "metadata"

    # Archivos Silver
    SILVER_PARQUET = SILVER_DIR / "vrm_consolidated.parquet"
    METADATA_DB = METADATA_DIR / "ingestion_log.db"

    # === ACCESS (consolidado Marcial - solo para bootstrap) ===
    ACCESS_PATH = BASE_DIR / "Datos" / "vrm_consolidado.accdb"
    ACCESS_TABLE = "NOMBRE_DE_TU_TABLA"  # <-- CAMBIAR

    # === SCHEMA: Mapeo CSV (107 cols) → Silver (33 cols) ===
    # Clave: nombre en CSV de VRM | Valor: nombre estandarizado Silver
    COLUMN_MAP = {
        "Account Number":                         "TARJETA",
        "Transaction Amount (U.S. $)":            "MONTO USD",
        "Acquirer Currency Code":                  "TIPO DE MONEDA",
        "Authorization Timestamp (America/Bogota)":"_TIMESTAMP_RAW",
        "Authorization Timestamp (America/Lima)":  "_TIMESTAMP_RAW",  # fallback legacy
        "Acquirer BIN":                            "BIN",
        "Merchant Category Code (MCC)":            "MCC",
        "Merchant Location":                       "LOCALIDAD",
        "Merchant Name":                           "NOMBRE DE COMERCIO",
        "Authorization Response Code":             "COD REDP 59",
        "POS Entry Mode":                          "ENTRY MODE",
        "Status":                                  "CALIFICACION",
        "ECI":                                     "ECI",
        "Visa Transaction ID":                     "VISA TRANSSACTION ID",
        "Merchant ID":                             "CODIGO DE COMERCIO",
        "Statused By User ID":                     "ID ANALISTA",
        "Statused By First Name":                  "NAME ANALISTA",
        "Statused By Last Name":                   "LAST NAME ANALISTA",
        "Card Acceptor Country Code":              "CODIGO PAIS",
        "RTD Rule Version":                        "RTD REGLA",
        "CVV2 Result Code":                        "CVV2",
        "CC Rule Name":                            "NOMBRE REGLA",
        "CC Rule Version":                         "CODIGO REGLA",
        "Token Type":                              "TIPO DE TOKEN",
        "Token Number":                            "NUMERO DE TOKEN",
        # --- 3 COLUMNAS NUEVAS (scores) ---
        "Advanced Authorization Risk Score":       "SCORE_AARS",
        "VCAS Score":                              "SCORE_VCAS",
        "VAAI Score":                              "SCORE_VAAI",
    }

    # Columnas del Silver final (orden definitivo)
    SILVER_SCHEMA = [
        "TARJETA",
        "MONTO USD",
        "TIPO DE MONEDA",
        "Fecha",
        "BIN",
        "MCC",
        "LOCALIDAD",
        "NOMBRE DE COMERCIO",
        "COD REDP 59",
        "ENTRY MODE",
        "CALIFICACION",
        "ECI",
        "VISA TRANSSACTION ID",
        "CODIGO DE COMERCIO",
        "ID ANALISTA",
        "NAME ANALISTA",
        "LAST NAME ANALISTA",
        "CODIGO PAIS",
        "RTD REGLA",
        "CVV2",
        "NOMBRE REGLA",
        "CODIGO REGLA",
        "TIPO DE TOKEN",
        "NUMERO DE TOKEN",
        "Dia_reporte",
        "AnoMes_reporte",
        "Entidad",
        "Gestion",
        "Cuenta",
        "Fuente",
        # --- Nuevas ---
        "SCORE_AARS",
        "SCORE_VCAS",
        "SCORE_VAAI",
    ]

    # === REGLAS DE NEGOCIO ===
    BIN_CSF = "422052"  # BIN para entidad CSF (Santander)

    # Mapeo de meses español → número (para parseo de timestamp)
    MESES_MAP = {
        "ene": "01", "feb": "02", "mar": "03", "abr": "04",
        "may": "05", "jun": "06", "jul": "07", "ago": "08",
        "sept": "09", "sep": "09", "oct": "10", "nov": "11", "dic": "12",
    }


# ============================================================================
# UTILIDADES
# ============================================================================

def crear_estructura_directorios():
    """Crea la estructura de carpetas Medallion si no existe."""
    for d in [Config.BRONZE_DIR, Config.SILVER_DIR, Config.GOLD_DIR, Config.METADATA_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    print("Estructura de directorios verificada.")


def generar_hash(row_dict: dict) -> str:
    """
    Genera hash SHA-256 para deduplicación basado en composite key.
    Composite key: Fecha + MONTO USD + BIN + NOMBRE DE COMERCIO + VISA TRANSSACTION ID
    """
    key_parts = [
        str(row_dict.get("Fecha", "")),
        str(row_dict.get("MONTO USD", "")),
        str(row_dict.get("BIN", "")),
        str(row_dict.get("NOMBRE DE COMERCIO", "")),
        str(row_dict.get("VISA TRANSSACTION ID", "")),
    ]
    key_string = "|".join(key_parts)
    return hashlib.sha256(key_string.encode("utf-8")).hexdigest()


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
            metadata.get("tool_name", "vrm"),
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
# EXTRACTOR: Lectura de CSVs diarios (Bronze)
# ============================================================================

class VRMExtractor:
    """Lee y combina las 2 listas diarias de VRM."""

    @staticmethod
    def guardar_en_bronze(lista1_path: str, lista2_path: str, fecha_descarga: str = None):
        """
        Copia los CSVs originales a la carpeta Bronze particionada por fecha.

        Args:
            lista1_path: Ruta al CSV de la Lista 1
            lista2_path: Ruta al CSV de la Lista 2
            fecha_descarga: Fecha de descarga en formato YYYY-MM-DD (default: hoy)
        """
        if fecha_descarga is None:
            fecha_descarga = date.today().isoformat()

        year, month, day = fecha_descarga.split("-")
        bronze_folder = Config.BRONZE_DIR / year / month / day
        bronze_folder.mkdir(parents=True, exist_ok=True)

        # Copiar archivos originales sin modificar
        shutil.copy2(lista1_path, bronze_folder / f"vrm_lista1_{fecha_descarga.replace('-', '')}.csv")
        shutil.copy2(lista2_path, bronze_folder / f"vrm_lista2_{fecha_descarga.replace('-', '')}.csv")

        print(f"Bronze: Archivos guardados en {bronze_folder}")
        return bronze_folder

    @staticmethod
    def leer_listas(lista1_path: str, lista2_path: str, encoding: str = "utf-8") -> pl.DataFrame:
        """
        Lee las 2 listas de VRM y las concatena verticalmente.

        Args:
            lista1_path: Ruta al CSV de la Lista 1
            lista2_path: Ruta al CSV de la Lista 2
            encoding: Encoding del archivo (utf-8 o latin-1)

        Returns:
            DataFrame Polars con las 2 listas unidas
        """
        try:
            df1 = pl.read_csv(lista1_path, encoding=encoding, infer_schema_length=0, truncate_ragged_lines=True)
        except Exception:
            print(f"  Reintentando Lista 1 con encoding latin-1...")
            df1 = pl.read_csv(lista1_path, encoding="latin-1", infer_schema_length=0, truncate_ragged_lines=True)

        try:
            df2 = pl.read_csv(lista2_path, encoding=encoding, infer_schema_length=0, truncate_ragged_lines=True)
        except Exception:
            print(f"  Reintentando Lista 2 con encoding latin-1...")
            df2 = pl.read_csv(lista2_path, encoding="latin-1", infer_schema_length=0, truncate_ragged_lines=True)

        print(f"  Lista 1: {df1.shape[0]:,} filas × {df1.shape[1]} columnas")
        print(f"  Lista 2: {df2.shape[0]:,} filas × {df2.shape[1]} columnas")

        # Concat vertical (mismas 107 columnas)
        df = pl.concat([df1, df2], how="diagonal")
        print(f"  Unión: {df.shape[0]:,} filas × {df.shape[1]} columnas")

        return df


# ============================================================================
# TRANSFORMER: Mapeo de columnas + reglas de negocio
# ============================================================================

class VRMTransformer:
    """Transforma los datos crudos de VRM al schema Silver estandarizado."""

    @staticmethod
    def parsear_timestamp(df: pl.DataFrame) -> pl.DataFrame:
        """
        Parsea el timestamp de VRM.
        Formato real: "31 mar. 2026 22:52:29"
        Patrón: DD MMM. YYYY HH:MM:SS (meses abreviados en español con punto)

        Genera: Fecha (Date), Dia_reporte (Int), AnoMes_reporte (String)
        """
        if "_TIMESTAMP_RAW" not in df.columns:
            print("  WARN: No se encontró columna de timestamp. Fecha no será parseada.")
            return df

        # Limpiar espacios extra
        df = df.with_columns(
            pl.col("_TIMESTAMP_RAW")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .alias("_ts_clean")
        )

        # Reemplazar meses en español por números
        # Formato: "31 mar. 2026 22:52:29" → "31 03 2026 22:52:29"
        for mes_es, mes_num in Config.MESES_MAP.items():
            # Con punto: "mar." → "03"
            df = df.with_columns(
                pl.col("_ts_clean")
                .str.replace(f" {mes_es}. ", f" {mes_num} ", literal=True)
                .alias("_ts_clean")
            )
            # Sin punto (por si acaso): "mar " → "03 "
            df = df.with_columns(
                pl.col("_ts_clean")
                .str.replace(f" {mes_es} ", f" {mes_num} ", literal=True)
                .alias("_ts_clean")
            )

        # Ahora el formato es: "31 03 2026 22:52:29"
        # Extraer día, mes, año
        df = df.with_columns([
            pl.col("_ts_clean").str.extract(r"^(\d{1,2})\s", 1)
            .cast(pl.Int32, strict=False)
            .alias("_dia"),

            pl.col("_ts_clean").str.extract(r"^\d{1,2}\s(\d{1,2})\s", 1)
            .cast(pl.Int32, strict=False)
            .alias("_mes"),

            pl.col("_ts_clean").str.extract(r"^\d{1,2}\s\d{1,2}\s(\d{4})", 1)
            .cast(pl.Int32, strict=False)
            .alias("_anio"),
        ])

        # Construir fecha
        df = df.with_columns(
            pl.date(pl.col("_anio"), pl.col("_mes"), pl.col("_dia"))
            .alias("Fecha")
        )

        # Derivar Dia_reporte y AnoMes_reporte
        df = df.with_columns([
            pl.col("Fecha").dt.day().cast(pl.Int32).alias("Dia_reporte"),
            (
                pl.col("Fecha").dt.year().cast(pl.Utf8)
                + pl.col("Fecha").dt.month().cast(pl.Utf8).str.pad_start(2, "0")
            ).alias("AnoMes_reporte"),
        ])

        # Limpiar columnas temporales
        df = df.drop(["_TIMESTAMP_RAW", "_ts_clean", "_dia", "_mes", "_anio"])

        print(f"  Timestamp parseado. Rango de fechas: {df['Fecha'].min()} a {df['Fecha'].max()}")

        return df

    @staticmethod
    def mapear_columnas(df: pl.DataFrame) -> pl.DataFrame:
        """
        Selecciona y renombra las columnas del CSV (107) al schema Silver (33).
        Las columnas que no existen en el CSV se crean con null.
        """
        rename_map = {}
        columnas_presentes = set(df.columns)

        for col_csv, col_silver in Config.COLUMN_MAP.items():
            if col_csv in columnas_presentes:
                rename_map[col_csv] = col_silver

        # Renombrar las que encontramos
        df = df.rename(rename_map)

        # Quedarnos solo con las columnas mapeadas
        cols_to_keep = [col for col in df.columns if col in Config.COLUMN_MAP.values()]
        df = df.select(cols_to_keep)

        # Agregar columnas faltantes con null
        cols_existentes = set(df.columns)
        cols_silver = set(Config.COLUMN_MAP.values())
        for col_faltante in cols_silver - cols_existentes:
            if col_faltante != "_TIMESTAMP_RAW":  # se maneja en parsear_timestamp
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col_faltante))

        print(f"  Columnas mapeadas: {len(rename_map)} de {len(Config.COLUMN_MAP)}")
        return df

    @staticmethod
    def aplicar_reglas_negocio(df: pl.DataFrame) -> pl.DataFrame:
        """
        Aplica las reglas de negocio para columnas derivadas:
        - Entidad: CCF si BIN == 422052, sino SBP
        - Gestion: GESTIONADA si ID ANALISTA tiene valor, sino NO GESTIONADA
        - Cuenta: siempre "1"
        - Fuente: siempre "VRM"
        - Limpieza de COD REDP 59
        """
        df = df.with_columns([
            # Entidad
            pl.when(pl.col("BIN").cast(pl.Utf8).str.strip_chars() == Config.BIN_CSF)
            .then(pl.lit("CSF"))
            .otherwise(pl.lit("SBP"))
            .alias("Entidad"),

            # Gestion
            pl.when(
                pl.col("ID ANALISTA").is_not_null()
                & (pl.col("ID ANALISTA").cast(pl.Utf8).str.strip_chars() != "")
                & (pl.col("ID ANALISTA").cast(pl.Utf8).str.to_lowercase() != "na")
                & (pl.col("ID ANALISTA").cast(pl.Utf8).str.to_lowercase() != "nan")
            )
            .then(pl.lit("GESTIONADA"))
            .otherwise(pl.lit("NO GESTIONADA"))
            .alias("Gestion"),

            # Cuenta
            pl.lit("1").alias("Cuenta"),

            # Fuente
            pl.lit("VRM").alias("Fuente"),

            # Limpieza de COD REDP 59 (quitar caracteres especiales, dejar solo alfanuméricos)
            pl.col("COD REDP 59")
            .cast(pl.Utf8)
            .str.replace_all(r"[^a-zA-Z0-9]", "")
            .alias("COD REDP 59"),
        ])

        print(f"  Reglas de negocio aplicadas.")
        return df

    @staticmethod
    def tipar_columnas(df: pl.DataFrame) -> pl.DataFrame:
        """Aplica tipos de datos correctos a las columnas."""
        df = df.with_columns([
            # MONTO USD a Float64
            pl.col("MONTO USD")
            .cast(pl.Utf8)
            .str.replace_all(",", "")  # por si tiene separador de miles
            .cast(pl.Float64, strict=False)
            .alias("MONTO USD"),

            # Scores a Float64
            pl.col("SCORE_AARS").cast(pl.Float64, strict=False).alias("SCORE_AARS"),
            pl.col("SCORE_VCAS").cast(pl.Float64, strict=False).alias("SCORE_VCAS"),
            pl.col("SCORE_VAAI").cast(pl.Float64, strict=False).alias("SCORE_VAAI"),

            # Dia_reporte a Int32
            pl.col("Dia_reporte").cast(pl.Int32, strict=False).alias("Dia_reporte"),
        ])

        # Todas las demás columnas de texto a Utf8 (por consistencia)
        text_cols = [
            c for c in df.columns
            if c not in ["MONTO USD", "Fecha", "Dia_reporte", "SCORE_AARS", "SCORE_VCAS", "SCORE_VAAI"]
        ]
        df = df.with_columns([
            pl.col(c).cast(pl.Utf8, strict=False) for c in text_cols
        ])

        print(f"  Tipos de datos aplicados.")
        return df

    @staticmethod
    def generar_hashes(df: pl.DataFrame) -> pl.DataFrame:
        """Genera hash SHA-256 para cada fila basado en composite key."""
        # Crear composite key como string concatenado
        df = df.with_columns(
            (
                pl.col("Fecha").cast(pl.Utf8).fill_null("")
                + "|" + pl.col("MONTO USD").cast(pl.Utf8).fill_null("")
                + "|" + pl.col("BIN").cast(pl.Utf8).fill_null("")
                + "|" + pl.col("NOMBRE DE COMERCIO").cast(pl.Utf8).fill_null("")
                + "|" + pl.col("VISA TRANSSACTION ID").cast(pl.Utf8).fill_null("")
            ).alias("_composite_key")
        )

        # Generar hash
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
        print("\n--- TRANSFORMACIÓN SILVER ---")
        df = cls.mapear_columnas(df)
        df = cls.parsear_timestamp(df)
        df = cls.aplicar_reglas_negocio(df)
        df = cls.tipar_columnas(df)
        df = cls.generar_hashes(df)

        # Reordenar columnas según schema + hash
        final_cols = [c for c in Config.SILVER_SCHEMA if c in df.columns]
        final_cols.append("_row_hash")
        df = df.select(final_cols)

        print(f"  Schema final: {len(final_cols)} columnas")
        print(f"  Filas: {df.shape[0]:,}")
        return df


# ============================================================================
# LOADER: Persistencia en Parquet Silver
# ============================================================================

class SilverLoader:
    """Maneja la persistencia y upsert del Parquet Silver."""

    @staticmethod
    def bootstrap_from_access(access_df: pl.DataFrame) -> pl.DataFrame:
        """
        Inicializa el Silver Parquet desde el consolidado de Marcial (Access).
        Agrega las 3 columnas nuevas con null y genera hashes.

        Args:
            access_df: DataFrame leído del Access (30 columnas)

        Returns:
            DataFrame con schema Silver completo (33 + hash)
        """
        print("\n--- BOOTSTRAP DESDE ACCESS ---")
        print(f"  Input: {access_df.shape[0]:,} filas × {access_df.shape[1]} columnas")

        # Asegurar que Fecha sea tipo Date
        if access_df["Fecha"].dtype != pl.Date:
            access_df = access_df.with_columns(
                pl.col("Fecha").cast(pl.Date, strict=False).alias("Fecha")
            )

        # Agregar las 3 columnas nuevas con null
        access_df = access_df.with_columns([
            pl.lit(None).cast(pl.Float64).alias("SCORE_AARS"),
            pl.lit(None).cast(pl.Float64).alias("SCORE_VCAS"),
            pl.lit(None).cast(pl.Float64).alias("SCORE_VAAI"),
        ])

        # Asegurar tipos consistentes
        access_df = VRMTransformer.tipar_columnas(access_df)

        # Generar hashes
        access_df = VRMTransformer.generar_hashes(access_df)

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
        print(f"  Tamaño: {size_mb:.1f} MB")

        return access_df

    @staticmethod
    def upsert_incremental(df_new: pl.DataFrame) -> dict:
        """
        Ejecuta upsert incremental contra el Parquet Silver existente.
        Lógica: Last Record Wins basado en _row_hash.

        Args:
            df_new: DataFrame transformado del día (con _row_hash)

        Returns:
            dict con métricas de la operación
        """
        print("\n--- UPSERT INCREMENTAL ---")
        metrics = {"rows_read": df_new.shape[0], "rows_new": 0, "rows_updated": 0, "rows_skipped": 0}

        if not Config.SILVER_PARQUET.exists():
            print("  No existe Silver base. Creando desde cero...")
            Config.SILVER_DIR.mkdir(parents=True, exist_ok=True)
            df_new.write_parquet(Config.SILVER_PARQUET, compression="zstd")
            metrics["rows_new"] = df_new.shape[0]
            print(f"  Silver creado: {df_new.shape[0]:,} filas nuevas")
            return metrics

        # Leer Silver existente
        df_existing = pl.read_parquet(Config.SILVER_PARQUET)
        print(f"  Silver existente: {df_existing.shape[0]:,} filas")

        # Schema evolution: agregar columnas nuevas al existente si faltan
        for col in df_new.columns:
            if col not in df_existing.columns:
                print(f"  Schema evolution: agregando columna '{col}' al histórico")
                dtype = df_new[col].dtype
                df_existing = df_existing.with_columns(
                    pl.lit(None).cast(dtype).alias(col)
                )

        # También al revés: columnas del existente que no estén en el nuevo
        for col in df_existing.columns:
            if col not in df_new.columns:
                print(f"  Schema evolution: agregando columna '{col}' al delta")
                dtype = df_existing[col].dtype
                df_new = df_new.with_columns(
                    pl.lit(None).cast(dtype).alias(col)
                )

        # Comparar hashes
        existing_hashes = set(df_existing["_row_hash"].to_list())
        new_hashes = set(df_new["_row_hash"].to_list())

        hashes_nuevos = new_hashes - existing_hashes
        hashes_duplicados = new_hashes & existing_hashes

        metrics["rows_new"] = len(hashes_nuevos)
        metrics["rows_skipped"] = len(hashes_duplicados)

        print(f"  Registros nuevos: {metrics['rows_new']:,}")
        print(f"  Registros ya existentes (skip): {metrics['rows_skipped']:,}")

        if metrics["rows_new"] == 0:
            print("  No hay registros nuevos. Silver sin cambios.")
            return metrics

        # Filtrar solo registros nuevos
        df_to_append = df_new.filter(pl.col("_row_hash").is_in(list(hashes_nuevos)))

        # Asegurar mismo orden de columnas
        df_to_append = df_to_append.select(df_existing.columns)

        # Append
        df_final = pl.concat([df_existing, df_to_append], how="diagonal")

        # Backup del Silver anterior
        backup_path = Config.SILVER_DIR / f"vrm_consolidated_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        shutil.copy2(Config.SILVER_PARQUET, backup_path)
        print(f"  Backup: {backup_path.name}")

        # Escribir nuevo Silver
        df_final.write_parquet(Config.SILVER_PARQUET, compression="zstd")

        size_mb = Config.SILVER_PARQUET.stat().st_size / (1024 * 1024)
        print(f"  Silver actualizado: {df_final.shape[0]:,} filas | {size_mb:.1f} MB")

        return metrics


# ============================================================================
# ORQUESTADOR: Scripts de ejecución
# ============================================================================

def run_bootstrap(access_df: pl.DataFrame):
    """
    BOOTSTRAP: Ejecutar UNA SOLA VEZ para inicializar Silver desde Access.

    Uso:
        import pyodbc
        conn = pyodbc.connect(r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=ruta.accdb;")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM [nombre_tabla]")
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        data = [dict(zip(cols, row)) for row in rows]
        access_df = pl.DataFrame(data)
        conn.close()

        run_bootstrap(access_df)
    """
    print("=" * 60)
    print("BOOTSTRAP: Access → Silver Parquet")
    print("=" * 60)

    start = datetime.now()
    crear_estructura_directorios()

    df_silver = SilverLoader.bootstrap_from_access(access_df)

    duration = (datetime.now() - start).total_seconds()

    log_ingestion({
        "run_id": datetime.now().isoformat(),
        "tool_name": "vrm",
        "source_file": "ACCESS_BOOTSTRAP",
        "rows_read": df_silver.shape[0],
        "rows_new": df_silver.shape[0],
        "rows_updated": 0,
        "rows_skipped": 0,
        "errors": 0,
        "duration_sec": duration,
        "status": "SUCCESS",
    })

    print(f"\nBootstrap completado en {duration:.1f} segundos.")
    return df_silver


def run_daily(lista1_path: str, lista2_path: str, fecha_descarga: str = None):
    """
    CARGA DIARIA: Procesa las 2 listas del día y las agrega al Silver.

    Uso:
        run_daily(
            lista1_path=r"C:\Downloads\vrm_lista1_20260331.csv",
            lista2_path=r"C:\Downloads\vrm_lista2_20260331.csv",
            fecha_descarga="2026-03-31"
        )
    """
    print("=" * 60)
    print("CARGA INCREMENTAL DIARIA")
    print("=" * 60)

    start = datetime.now()
    crear_estructura_directorios()

    # 1. Guardar en Bronze (archivos originales sin modificar)
    print("\n1. BRONZE: Guardando archivos originales...")
    VRMExtractor.guardar_en_bronze(lista1_path, lista2_path, fecha_descarga)

    # 2. Leer y combinar las 2 listas
    print("\n2. EXTRACCIÓN: Leyendo listas...")
    df_raw = VRMExtractor.leer_listas(lista1_path, lista2_path)

    # 3. Transformar al schema Silver
    print("\n3. TRANSFORMACIÓN:")
    df_transformed = VRMTransformer.transformar(df_raw)

    # 4. Upsert incremental
    print("\n4. CARGA:")
    metrics = SilverLoader.upsert_incremental(df_transformed)

    # 5. Log
    duration = (datetime.now() - start).total_seconds()
    source = f"lista1+lista2_{fecha_descarga or date.today().isoformat()}"

    log_ingestion({
        "run_id": datetime.now().isoformat(),
        "tool_name": "vrm",
        "source_file": source,
        "rows_read": metrics["rows_read"],
        "rows_new": metrics["rows_new"],
        "rows_updated": metrics["rows_updated"],
        "rows_skipped": metrics["rows_skipped"],
        "errors": 0,
        "duration_sec": duration,
        "status": "SUCCESS",
    })

    print(f"\n{'=' * 60}")
    print(f"RESUMEN")
    print(f"{'=' * 60}")
    print(f"  Filas leídas:     {metrics['rows_read']:,}")
    print(f"  Nuevas:           {metrics['rows_new']:,}")
    print(f"  Ya existentes:    {metrics['rows_skipped']:,}")
    print(f"  Duración:         {duration:.1f} seg")
    print(f"{'=' * 60}")


# ============================================================================
# EJEMPLO DE USO COMPLETO
# ============================================================================

if __name__ == "__main__":
    """
    PASO 1 - BOOTSTRAP (ejecutar una sola vez):
    ============================================
    # Leer Access con pyodbc
    import pyodbc

    conn_str = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=C:\FRAUDES\HERRAMIENTAS\VRM\Datos\vrm_consolidado.accdb;"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM [TU_TABLA]")
    cols = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    data = [dict(zip(cols, row)) for row in rows]
    access_df = pl.DataFrame(data)
    conn.close()

    # Ejecutar bootstrap
    run_bootstrap(access_df)


    PASO 2 - CARGA DIARIA (ejecutar cada día):
    ============================================
    run_daily(
        lista1_path=r"C:\Downloads\vrm_lista1_20260331.csv",
        lista2_path=r"C:\Downloads\vrm_lista2_20260331.csv",
        fecha_descarga="2026-03-31"
    )
    """

    # --- Descomenta el paso que necesites ejecutar ---

    # BOOTSTRAP:
    # run_bootstrap(access_df)

    # CARGA DIARIA:
    # run_daily(
    #     lista1_path=r"C:\Downloads\vrm_lista1_20260331.csv",
    #     lista2_path=r"C:\Downloads\vrm_lista2_20260331.csv",
    #     fecha_descarga="2026-03-31"
    # )

    print("Pipeline listo. Descomenta el paso que necesites ejecutar.")
