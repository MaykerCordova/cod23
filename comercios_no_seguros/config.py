# ─────────────────────────────────────────────────────────────────────────────
#  DICCIONARIO DE COLUMNAS  —  comercios_no_seguros
#  Cambia el VALOR de cada entrada con el nombre real de tu parquet.
#  No toques las CLAVES (lado izquierdo) porque el script las usa internamente.
# ─────────────────────────────────────────────────────────────────────────────

COLS = {
    # ── Identificadores ──────────────────────────────────────────────────────
    "tarjeta"          : "ACF-TARJETA REGISTRO 750",              # débito; crédito usa ACF-Tarjeta SHA256
    "bin"              : "ACF-BIN",
    "comercio_id"      : "ACF-NOMBRE/LOCALIZACION COMERCIO",      # Monitor no tiene ID separado
    "comercio_nom"     : "ACF-NOMBRE/LOCALIZACION COMERCIO",

    # ── Fechas ───────────────────────────────────────────────────────────────
    "fecha_hora_trx"   : "FECHA_HORA",        # columna construida por consolidar.py (YYYYMMDD + HH:MM:SS)
    "fecha_cierre"     : "FECHA_CIERRE",      # fecha de cierre del caso  formato YYYY-MM-DD

    # ── Montos ────────────────────────────────────────────────────────────────
    "monto"            : "ACF-MONTO EN MONEDA LOCAL",   # monto en moneda local (numérico)
    "monto_dolar"      : "ACF-MONTO DOLLAR",            # monto en dólares (numérico)

    # ── Tarjeta / cliente ─────────────────────────────────────────────────────
    "tipo_tarjeta"     : "ACF-TIPO PROD TC",  # tipo de producto (TC/TD)
    "nivel_tarjeta"    : "NIVEL_TARJETA",     # no disponible en Monitor — dejar como está
    "saldo_disponible" : "ACF-SALDO DISPONIBLE EN MONEDA TRX",
    "segmento"         : "VAA-EVENTO DE COMPROMISO OTRA FUENTE",
    "organizacion"     : "ACF-ORGANIZACION",  # SBP = Scotiabank  |  CSF = Santander
    "marca"            : "",                  # franquicia: 4 = Visa  |  5 = Mastercard — configurar manualmente

    # ── Comercio / transacción ────────────────────────────────────────────────
    "canal"            : "ACF-CANAL",
    "mcc"              : "ACF-MCC",           # débito usa "ACF-MCC +" (con espacio y +)
    "cvv_dinamico"     : "ACF-COD RED COMERCIO",  # S=Estático TD  D=Dinámico TC/TD  E=Estático TC  N=Sin CVV

    # ── Fraude ────────────────────────────────────────────────────────────────
    "modalidad_fraude" : "MODALIDAD_FRAUDE",  # modalidad del fraude (ej. CNP, SKIMMING, PHISHING…)
}

# ── Ruta del parquet de entrada ───────────────────────────────────────────────
PARQUET_INPUT  = "fraudes_comercios_no_seguros.parquet"   # <-- cambia aquí

# ── Ruta del parquet de salida (enriquecido) ──────────────────────────────────
PARQUET_OUTPUT = "fraudes_comercios_no_seguros_features.parquet"
