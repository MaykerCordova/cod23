# ─────────────────────────────────────────────────────────────────────────────
#  DICCIONARIO DE COLUMNAS  —  comercios_no_seguros
#  Cambia el VALOR de cada entrada con el nombre real de tu parquet.
#  No toques las CLAVES (lado izquierdo) porque el script las usa internamente.
# ─────────────────────────────────────────────────────────────────────────────

COLS = {
    # ── Identificadores ──────────────────────────────────────────────────────
    "tarjeta"      : "TARJETA",          # número de tarjeta (masked)
    "comercio_id"  : "COMERCIO_ID",      # ID o código del comercio
    "comercio_nom" : "COMERCIO_NOMBRE",  # nombre descriptivo del comercio (si existe, si no pon igual que comercio_id)

    # ── Fecha y hora (SEPARADAS en el parquet) ───────────────────────────────
    "fecha"        : "FECHA",            # formato YYYYMMDD  ej. 20240115
    "hora"         : "HORA",             # formato HH:MM:SS  ej. 14:30:00

    # ── Monto ─────────────────────────────────────────────────────────────────
    "monto"        : "IMPORTE",          # monto de la transacción (numérico)

    # ── Canal ─────────────────────────────────────────────────────────────────
    "canal"        : "CANAL",            # canal de la transacción

    # ── Calificación ──────────────────────────────────────────────────────────
    "calificacion" : "CALIFICACION",     # F / G / P / D  (en este script solo F)

    # ── Tipo de tarjeta ───────────────────────────────────────────────────────
    "tipo_tarjeta" : "TIPO_TARJETA",     # CREDITO / DEBITO

    # ── Segmento del cliente ──────────────────────────────────────────────────
    "segmento"     : "SEGMENTO",         # segmento del cliente / tarjetahabiente

    # ── Variables de seguridad (si no las tienes pon None en el valor) ────────
    "es_seguro"    : "ES_SEGURO",        # indicador de comercio seguro/no seguro (S/N o 1/0)
    "cvv_dinamico" : "CVV_DINAMICO",     # ¿CVV dinámico? (S/N o 1/0)
    "eci"          : "ECI",              # Electronic Commerce Indicator (si existe)
}

# ── Ruta del parquet de entrada ───────────────────────────────────────────────
PARQUET_INPUT  = "fraudes_comercios_no_seguros.parquet"   # <-- cambia aquí

# ── Ruta del parquet de salida (enriquecido) ──────────────────────────────────
PARQUET_OUTPUT = "fraudes_comercios_no_seguros_features.parquet"
