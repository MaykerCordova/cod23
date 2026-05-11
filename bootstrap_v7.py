"""
╔══════════════════════════════════════════════════════════════════╗
║  BOOTSTRAP V7 — Migración Excel → SQLite                        ║
║  Ejecutar UNA SOLA VEZ antes de activar bitacoraV7.py           ║
╠══════════════════════════════════════════════════════════════════╣
║  Qué hace:                                                      ║
║  1. Lee tu Excel corregido manualmente (Bitacora_Master.xlsx)   ║
║  2. Crea las tablas en SQLite si no existen                     ║
║  3. Inserta SOLO los registros que no estén ya en SQLite         ║
║     (usa ID_Sistema como llave — no duplica nada)               ║
║  4. Muestra un resumen de cuántos se cargaron y cuáles ya       ║
║     estaban (para que puedas verificar todo quedó correcto)     ║
║  5. Verifica que el correlativo máximo sea correcto             ║
║                                                                  ║
║  Después de correr esto exitosamente → activar bitacoraV7.py    ║
╚══════════════════════════════════════════════════════════════════╝
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

# ══════════════════════════════════════════════════════════════════
# ▶  CONFIGURACIÓN — EDITAR ANTES DE CORRER
# ══════════════════════════════════════════════════════════════════

# RUTA_EXCEL → tu Excel corregido manualmente.
#   Puede estar en cualquier carpeta. Si lo tienes en el escritorio:
#   r"C:\Users\s4930359\Desktop\Bitacora_Master_corregido.xlsx"
#   Si ya está en OneDrive con el nombre original:
#   r"C:\Users\s4930359\OneDrive - The Bank of Nova Scotia\Bitacora_Reglas\Bitacora_Master.xlsx"
RUTA_EXCEL = (
    r"C:\Users\s4930359\OneDrive - The Bank of Nova Scotia"
    r"\Bitacora_Reglas\Bitacora_Master.xlsx"
    # ↑ Cambia esto si tu Excel corregido está en otra ruta
)

# RUTA_SQLITE → puede ser el mismo Respaldo_Blindado.db antiguo.
#   El modo RESET (abajo) lo limpia antes de cargar, así que
#   no necesitas crear un archivo nuevo.
RUTA_SQLITE = (
    r"C:\Users\s4930359\OneDrive - The Bank of Nova Scotia"
    r"\Bitacora_Reglas\Respaldo_Blindado.db"
)

# MODO_RESET → True  = borra los datos viejos del SQLite y carga
#                      todo desde el Excel corregido (recomendado
#                      cuando el SQLite tiene errores como el 300.0)
#            → False = solo agrega los que no existen (INSERT OR IGNORE)
#                      útil si el SQLite ya está limpio y solo quieres
#                      agregar los que faltan
MODO_RESET = True   # ← déjalo en True para la migración inicial

# ── COLUMNAS esperadas en el Excel (orden V7) ──────────────────────
COLUMNAS_ESPERADAS = [
    "Nro_Correlativo", "Fecha", "Hora", "Codigo_Generado",
    "Maker", "Solicitado_Por", "Herramienta", "Accion",
    "Institucion", "Codigo_Condicion", "Nombre_Condicion",
    "Estatus_Condicion", "Tipo_Condicion", "Consideraciones",
    "Objetivo", "Estimacion", "Sustento", "Checker",
    "Fecha_Revision", "Hora_Revision", "Mes_Revision",
    "Anio_Revision", "Enviado_Jefatura", "Conformidad",
    "Canal", "ID_Sistema",
]


def crear_tablas(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Bitacora (
            Nro_Correlativo  INTEGER,
            Fecha            TEXT,
            Hora             TEXT,
            Codigo_Generado  TEXT,
            Maker            TEXT,
            Solicitado_Por   TEXT,
            Herramienta      TEXT,
            Accion           TEXT,
            Institucion      TEXT,
            Codigo_Condicion TEXT,
            Nombre_Condicion TEXT,
            Estatus_Condicion TEXT,
            Tipo_Condicion   TEXT,
            Consideraciones  TEXT,
            Objetivo         TEXT,
            Estimacion       TEXT,
            Sustento         TEXT,
            Checker          TEXT,
            Fecha_Revision   TEXT,
            Hora_Revision    TEXT,
            Mes_Revision     TEXT,
            Anio_Revision    TEXT,
            Enviado_Jefatura TEXT,
            Conformidad      TEXT,
            Canal            TEXT,
            ID_Sistema       TEXT PRIMARY KEY
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Bitacora_Pendientes (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            Fecha_Captura    TEXT,
            ID_Sistema       TEXT,
            Asunto           TEXT,
            Remitente        TEXT,
            Campos_Faltantes TEXT,
            Cuerpo_Raw       TEXT,
            Procesado        INTEGER DEFAULT 0
        )
    """)
    conn.commit()


def leer_excel(ruta: str) -> pd.DataFrame:
    """
    Lee el Excel con dtype=str para evitar que pandas convierta
    correlativos a float (el problema del 300.0).
    """
    df = pd.read_excel(ruta, dtype=str)
    df = df.fillna("")          # reemplaza NaN por string vacío
    df.columns = [c.strip() for c in df.columns]   # quita espacios en nombres
    return df


def verificar_columnas(df: pd.DataFrame):
    """Avisa si faltan columnas o hay columnas extra respecto al esquema V7."""
    cols_excel   = set(df.columns)
    cols_v7      = set(COLUMNAS_ESPERADAS)
    faltantes    = cols_v7 - cols_excel
    extras       = cols_excel - cols_v7

    if faltantes:
        print(f"\n  ⚠️  Columnas faltantes en Excel (se rellenarán con '-'):")
        for c in sorted(faltantes):
            print(f"       - {c}")
    if extras:
        print(f"\n  ℹ️  Columnas extra en Excel (se ignorarán):")
        for c in sorted(extras):
            print(f"       + {c}")

    return faltantes


def normalizar_correlativo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte Nro_Correlativo a entero limpio.
    Maneja casos como '300.0', '300', '  300  '.
    """
    def _limpiar(val):
        try:
            return int(float(str(val).strip()))
        except Exception:
            return None

    df["Nro_Correlativo"] = df["Nro_Correlativo"].apply(_limpiar)
    return df


def main():
    sep = "═" * 60
    print(f"\n{sep}")
    print(f"  🔄  BOOTSTRAP V7 — Migración Excel → SQLite")
    print(f"  🕐  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{sep}\n")

    # ── Verificar que el Excel existe ──────────────────────────────
    if not os.path.exists(RUTA_EXCEL):
        print(f"❌ No se encontró el Excel en:\n   {RUTA_EXCEL}")
        return

    # ── Leer Excel ─────────────────────────────────────────────────
    print(f"  📂 Leyendo Excel: {os.path.basename(RUTA_EXCEL)}")
    df = leer_excel(RUTA_EXCEL)
    print(f"  ✔  Filas encontradas en Excel: {len(df)}")

    # ── Verificar columnas ─────────────────────────────────────────
    faltantes = verificar_columnas(df)
    for col in faltantes:
        df[col] = "-"   # rellenar columnas que faltan con '-'

    # ── Seleccionar solo las columnas del esquema V7 ───────────────
    df = df[[c for c in COLUMNAS_ESPERADAS if c in df.columns]]

    # ── Limpiar correlativo ────────────────────────────────────────
    df = normalizar_correlativo(df)

    filas_invalidas = df[df["Nro_Correlativo"].isna()]
    if not filas_invalidas.empty:
        print(f"\n  ⚠️  {len(filas_invalidas)} fila(s) con correlativo inválido (se omitirán):")
        print(filas_invalidas[["Nro_Correlativo", "ID_Sistema", "Fecha"]].to_string(index=False))
        df = df[df["Nro_Correlativo"].notna()]

    # ── Verificar ID_Sistema ────────────────────────────────────────
    sin_id = df[df["ID_Sistema"].str.strip() == ""]
    if not sin_id.empty:
        print(f"\n  ⚠️  {len(sin_id)} fila(s) sin ID_Sistema (se omitirán):")
        print(sin_id[["Nro_Correlativo", "Fecha", "Herramienta"]].to_string(index=False))
        df = df[df["ID_Sistema"].str.strip() != ""]

    print(f"\n  ✔  Filas válidas para migrar: {len(df)}")

    # ── Conectar/crear SQLite ──────────────────────────────────────
    os.makedirs(os.path.dirname(RUTA_SQLITE), exist_ok=True)
    print(f"\n  💾 Conectando a SQLite: {os.path.basename(RUTA_SQLITE)}")

    with sqlite3.connect(RUTA_SQLITE) as conn:
        crear_tablas(conn)

        # Estado actual del SQLite antes de migrar
        ya_en_db = conn.execute("SELECT COUNT(*) FROM Bitacora").fetchone()[0]
        print(f"  ℹ️  Registros en SQLite antes del bootstrap: {ya_en_db}")

        # ── MODO RESET: limpiar datos viejos antes de cargar ───────
        if MODO_RESET and ya_en_db > 0:
            print(f"\n  🗑️  MODO_RESET = True → borrando {ya_en_db} registros antiguos...")
            conn.execute("DELETE FROM Bitacora")
            conn.commit()
            print(f"  ✔  SQLite limpio. Cargando desde Excel corregido...")
        elif not MODO_RESET:
            print(f"  ℹ️  MODO_RESET = False → solo se agregarán registros nuevos.")

        # ── Insertar fila por fila con INSERT OR IGNORE ────────────
        insertados  = 0
        duplicados  = 0
        errores     = 0

        for _, row in df.iterrows():
            try:
                datos = {col: row.get(col, "-") for col in COLUMNAS_ESPERADAS}
                # Asegurar que correlativo sea int en SQLite
                datos["Nro_Correlativo"] = int(datos["Nro_Correlativo"])

                conn.execute("""
                    INSERT OR IGNORE INTO Bitacora VALUES (
                        :Nro_Correlativo, :Fecha, :Hora, :Codigo_Generado,
                        :Maker, :Solicitado_Por, :Herramienta, :Accion,
                        :Institucion, :Codigo_Condicion, :Nombre_Condicion,
                        :Estatus_Condicion, :Tipo_Condicion, :Consideraciones,
                        :Objetivo, :Estimacion, :Sustento, :Checker,
                        :Fecha_Revision, :Hora_Revision, :Mes_Revision,
                        :Anio_Revision, :Enviado_Jefatura, :Conformidad,
                        :Canal, :ID_Sistema
                    )
                """, datos)

                if conn.execute(
                    "SELECT changes()"
                ).fetchone()[0] > 0:
                    insertados += 1
                else:
                    duplicados += 1

            except Exception as e:
                errores += 1
                print(f"  ▲ Error en fila [{row.get('ID_Sistema','?')}]: {e}")

        conn.commit()

        # ── Verificación final ─────────────────────────────────────
        total_db       = conn.execute("SELECT COUNT(*) FROM Bitacora").fetchone()[0]
        max_correlativo = conn.execute(
            "SELECT MAX(Nro_Correlativo) FROM Bitacora"
        ).fetchone()[0]

    # ── Resumen ────────────────────────────────────────────────────
    print(f"\n{sep}")
    print(f"  ✅  BOOTSTRAP COMPLETADO")
    print(f"{'─'*60}")
    print(f"  Filas del Excel procesadas  : {len(df)}")
    print(f"  Insertadas en SQLite        : {insertados}")
    print(f"  Ya existían (ignoradas)     : {duplicados}")
    print(f"  Errores                     : {errores}")
    print(f"{'─'*60}")
    print(f"  Total registros en SQLite   : {total_db}")
    print(f"  Correlativo máximo actual   : {max_correlativo}")
    print(f"  Próximo correlativo (V7)    : {max_correlativo + 1}")
    print(f"{sep}")

    if errores == 0 and insertados > 0:
        print(f"\n  🟢 Todo OK. Ya puedes activar bitacoraV7.py el lunes.")
        print(f"     El próximo ticket recibirá el correlativo {max_correlativo + 1}.\n")
    elif errores > 0:
        print(f"\n  🟡 Hay {errores} error(es). Revisa las filas marcadas arriba.")
        print(f"     Corrige y vuelve a correr bootstrap_v7.py antes del lunes.\n")
    else:
        print(f"\n  ℹ️  No se insertaron registros nuevos.")
        print(f"     Si el SQLite ya tenía todos los datos, está correcto.\n")


if __name__ == "__main__":
    main()
