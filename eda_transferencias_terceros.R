# =============================================================================
#  📊 EDA — TRANSFERENCIAS A TERCEROS
#  Scotiabank Perú · Prevención de Fraude
#  Criterio Monitor: COD_TRX = 40 · TIPO_CUENTA = 1010 · MONTO >= S/1,200
#  Indicadores: F=Fraude · G=Buena · P=Pendiente · D=Descarte
# =============================================================================

# ── 0. PAQUETES ───────────────────────────────────────────────────────────────
paquetes <- c(
  # Data
  "readxl",       # leer .xlsx
  "data.table",   # equivalente a Polars — rápido, eficiente
  "dplyr",        # manipulación declarativa
  "tidyr",        # pivot, reshape
  "lubridate",    # fechas y horas
  "stringr",      # limpieza de strings
  # Visualización
  "ggplot2",      # gráficos base
  "ggridges",     # density ridgeline plots
  "ggdist",       # distribuciones estadísticas elegantes
  "patchwork",    # componer múltiples gráficos
  "scales",       # formateo de ejes
  "RColorBrewer", # paletas
  # Grafos (red de beneficiarios / ATO)
  "igraph",       # motor de análisis de grafos
  "tidygraph",    # API tidy para grafos
  "ggraph",       # visualización de grafos con gramática ggplot2
  # ML — Árbol de decisión para discretización
  "rpart",        # árbol de decisión
  "rpart.plot"    # visualización del árbol
)

instalar_si_falta <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg, quiet = TRUE)
}
invisible(lapply(paquetes, instalar_si_falta))
invisible(lapply(paquetes, library, character.only = TRUE))

cat("✅ Paquetes cargados\n")

# ── TEMA GLOBAL ───────────────────────────────────────────────────────────────
COLOR_FRAUD  <- "#ef5350"
COLOR_OK     <- "#4fc3f7"
COLOR_ACCENT <- "#ffa726"
COLOR_BG     <- "#0f1117"
COLOR_PANEL  <- "#1a1d27"
COLOR_GRID   <- "#2a2d3e"
COLOR_TEXT   <- "#e0e0e0"

tema_fraude <- theme_minimal(base_size = 11) +
  theme(
    plot.background    = element_rect(fill = COLOR_BG,    color = NA),
    panel.background   = element_rect(fill = COLOR_PANEL, color = NA),
    panel.grid.major   = element_line(color = COLOR_GRID, linetype = "dashed"),
    panel.grid.minor   = element_blank(),
    text               = element_text(color = COLOR_TEXT),
    axis.text          = element_text(color = COLOR_TEXT),
    axis.title         = element_text(color = COLOR_TEXT),
    plot.title         = element_text(color = COLOR_TEXT, face = "bold", size = 13),
    plot.subtitle      = element_text(color = "#aaaaaa", size = 10),
    legend.background  = element_rect(fill = COLOR_PANEL, color = NA),
    legend.text        = element_text(color = COLOR_TEXT),
    legend.title       = element_text(color = COLOR_TEXT),
    strip.text         = element_text(color = COLOR_TEXT, face = "bold")
  )
theme_set(tema_fraude)


# =============================================================================
#  1. INGESTA — LECTURA Y UNIÓN DE LOS EXCEL DE MONITOR
# =============================================================================

BASE_DIR <- r"(C:\Users\s4930359\Data_Herramientas\data\transferencias_terceros)"

ARCHIVOS <- list(
  Enero = file.path(BASE_DIR, "monitor_transferencias_enero.xlsx"),
  Marzo = file.path(BASE_DIR, "monitor_transferencias_marzo.xlsx"),
  Abril = file.path(BASE_DIR, "monitor_transferencias_abril.xlsx")
  # Mes4 = file.path(BASE_DIR, "monitor_transferencias_mes4.xlsx")
)

leer_monitor_excel <- function(path, mes) {
  df <- read_excel(
    path,
    skip      = 4,          # salta las 4 filas de header basura
    col_types = "text"      # todo como texto — equivalente a dtype=str
  )
  # Limpiar nombres de columnas
  names(df) <- names(df) |>
    str_trim() |>
    str_to_upper() |>
    str_replace_all("\\s+", "_") |>   # espacios → _
    str_replace_all("-", "_") |>      # guiones → _
    str_replace_all("[^A-Z0-9_]", "") # elimina caracteres raros

  df$MES_ORIGEN <- mes
  return(as.data.table(df))
}

frames <- mapply(leer_monitor_excel, ARCHIVOS, names(ARCHIVOS), SIMPLIFY = FALSE)
raw    <- rbindlist(frames, fill = TRUE)  # fill=TRUE = diagonal_relaxed de Polars

cat(sprintf("Shape total: %s filas × %s columnas\n",
            format(nrow(raw), big.mark=","), ncol(raw)))


# =============================================================================
#  2. INSPECCIÓN DE COLUMNAS
# =============================================================================

cat("\n── Columnas disponibles ──────────────────────────────────\n")
for (i in seq_along(names(raw))) cat(sprintf("%3d. %s\n", i, names(raw)[i]))

cat("\n── Nulos por columna (solo las que tienen) ───────────────\n")
nulos <- sapply(raw, function(x) sum(is.na(x) | x == ""))
nulos <- sort(nulos[nulos > 0], decreasing = TRUE)
pct   <- round(nulos / nrow(raw) * 100, 2)
print(data.frame(columna = names(nulos), nulos = nulos, pct_nulo = pct))


# =============================================================================
#  3. MAPEO DE COLUMNAS
#  ⚠️  COMPLETA LA CLAVE IZQUIERDA con el nombre exacto que apareció arriba
# =============================================================================

COL_MAP <- c(
  # Nombre exacto en Monitor             = Alias interno
  "NOMBRE_EXACTO_EN_MONITOR"             = "FECHA_TRX",
  "NOMBRE_EXACTO_EN_MONITOR"             = "HORA_TRX",
  "NOMBRE_EXACTO_EN_MONITOR"             = "MONTO",
  "NOMBRE_EXACTO_EN_MONITOR"             = "INDICADOR",
  "NOMBRE_EXACTO_EN_MONITOR"             = "ALERTA",
  "NOMBRE_EXACTO_EN_MONITOR"             = "CUENTA",
  "NOMBRE_EXACTO_EN_MONITOR"             = "PRIMER_NOMBRE_CLIENTE",
  "NOMBRE_EXACTO_EN_MONITOR"             = "APELLIDO_CLIENTE",
  "NOMBRE_EXACTO_EN_MONITOR"             = "NOMBRE_BENEFICIARIO",
  "NOMBRE_EXACTO_EN_MONITOR"             = "CUENTA_DESTINO",
  "NOMBRE_EXACTO_EN_MONITOR"             = "HASH_DISPOSITIVO",
  "NOMBRE_EXACTO_EN_MONITOR"             = "CANAL",
  "ACFTARJETA_RESGISTRO_750"             = "TARJETA_REG",
  "ACFTARJETA_POS_76_DIGITOS"            = "TARJETA_MID",
  "ACFSALDO_DISPONIBLE_EN_MONEDA_TRX"    = "SALDO_DISPONIBLE_RAW",
  "CC_K05_COUNTTMP_TAMANO_COMERCIO"      = "CNT_MES_PREVIO"
)

# Aplicar renombrado solo a columnas que existen
cols_existentes   <- names(COL_MAP)[names(COL_MAP) %in% names(raw)]
cols_no_encontradas <- names(COL_MAP)[!names(COL_MAP) %in% names(raw)]

setnames(raw, cols_existentes, COL_MAP[cols_existentes])

if (length(cols_no_encontradas) > 0) {
  cat("\n⚠️  No encontradas (revisa el nombre en Monitor):\n")
  cat(paste(" -", cols_no_encontradas, collapse = "\n"), "\n")
}
cat(sprintf("✅ Columnas renombradas: %d\n", length(cols_existentes)))


# =============================================================================
#  4. CASTEO DE TIPOS
# =============================================================================

df <- copy(raw)

# ── 4.1  MONTO → Numeric (viene como str, sin separadores) ───────────────────
df[, MONTO := as.numeric(MONTO)]

# ── 4.2  SALDO DISPONIBLE → Numeric (tiene punto como decimal) ───────────────
df[, SALDO_DISPONIBLE := as.numeric(SALDO_DISPONIBLE_RAW)]
df[, SALDO_DISPONIBLE_RAW := NULL]

# ── 4.3  CNT_MES_PREVIO → Integer ────────────────────────────────────────────
df[, CNT_MES_PREVIO := as.integer(CNT_MES_PREVIO)]

# ── 4.4  FECHA_TRX → Date (formato AAAAMMDD pegado) ──────────────────────────
df[, FECHA_TRX := as.Date(str_trim(FECHA_TRX), format = "%Y%m%d")]

# ── 4.5  HORA_TRX → hms (formato HH:MM:SS con dos puntos) ───────────────────
df[, HORA_TRX := hms::as_hms(str_trim(HORA_TRX))]

# ── 4.6  FECHA_HORA combinada → POSIXct ───────────────────────────────────────
df[, FECHA_HORA := as.POSIXct(
  paste(as.character(FECHA_TRX), as.character(HORA_TRX)),
  format = "%Y-%m-%d %H:%M:%S"
)]

# ── 4.7  TARJETA reconstruida ─────────────────────────────────────────────────
df[, `:=`(
  TARJETA_FULL  = paste0(str_sub(str_trim(TARJETA_REG), 1, 6),
                         str_trim(TARJETA_MID),
                         str_sub(str_trim(TARJETA_REG), -4)),
  BIN           = str_sub(str_trim(TARJETA_REG), 1, 6),
  TARJETA_LAST4 = str_sub(str_trim(TARJETA_REG), -4)
)]

# ── 4.8  Limpiar strings clave ────────────────────────────────────────────────
cols_str <- c("INDICADOR", "CANAL", "NOMBRE_BENEFICIARIO", "HASH_DISPOSITIVO")
cols_str <- cols_str[cols_str %in% names(df)]
df[, (cols_str) := lapply(.SD, function(x) str_to_upper(str_trim(x))),
   .SDcols = cols_str]

cat("✅ Casteo completado\n")
print(df[1:5, .(FECHA_TRX, HORA_TRX, MONTO, SALDO_DISPONIBLE,
                CNT_MES_PREVIO, TARJETA_FULL, BIN, CANAL)])


# =============================================================================
#  5. INGENIERÍA DE VARIABLES — FLAGS & FEATURES
# =============================================================================

# ── 5.1  Variable objetivo ────────────────────────────────────────────────────
df[, `:=`(
  FLAG_FRAUDE      = as.integer(INDICADOR == "F"),
  FLAG_CLASIFICADO = as.integer(INDICADOR %in% c("F","G","D")),
  FLAG_PENDIENTE   = as.integer(INDICADOR == "P")
)]

# ── 5.2  Variables temporales ─────────────────────────────────────────────────
df[, `:=`(
  MES         = month(FECHA_TRX),
  DIA         = mday(FECHA_TRX),
  DIA_SEMANA  = wday(FECHA_TRX, label = FALSE),   # 1=Dom … 7=Sáb
  HORA        = hour(FECHA_HORA),
  FRANJA_HORARIA = fcase(
    hour(FECHA_HORA) %between% c(0, 5),   "Madrugada",
    hour(FECHA_HORA) %between% c(6, 11),  "Mañana",
    hour(FECHA_HORA) %between% c(12, 17), "Tarde",
    default = "Noche"
  ),
  FLAG_FIN_SEMANA = as.integer(wday(FECHA_TRX) %in% c(1, 7)),
  FLAG_FIN_MES    = as.integer(mday(FECHA_TRX) >= 26)
)]

# ── 5.3  Flags de monto ───────────────────────────────────────────────────────
UMBRAL_ALTO    <- 5000
UMBRAL_CRITICO <- 10000

df[, `:=`(
  FLAG_MONTO_ALTO    = as.integer(MONTO >= UMBRAL_ALTO),
  FLAG_MONTO_CRITICO = as.integer(MONTO >= UMBRAL_CRITICO)
)]

# ── 5.4  Saldo disponible ─────────────────────────────────────────────────────
df[, RATIO_MONTO_SALDO := round(MONTO / (SALDO_DISPONIBLE + 1e-9), 4)]
df[, `:=`(
  FLAG_VACIADO_CUENTA = as.integer(RATIO_MONTO_SALDO >= 0.80),
  FLAG_SALDO_BAJO     = as.integer(SALDO_DISPONIBLE < 200)
)]

# ── 5.5  Cuenta nueva / sin historia ─────────────────────────────────────────
df[, `:=`(
  FLAG_SIN_HISTORIA = as.integer(is.na(CNT_MES_PREVIO) | CNT_MES_PREVIO == 0),
  FLAG_CUENTA_NUEVA = as.integer(!is.na(CNT_MES_PREVIO) & CNT_MES_PREVIO <= 3)
)]

# ── 5.6  Beneficiario nulo ────────────────────────────────────────────────────
df[, FLAG_BENE_NULO := as.integer(is.na(NOMBRE_BENEFICIARIO) |
                                    str_length(NOMBRE_BENEFICIARIO) == 0)]

# ── 5.7  Concentración de beneficiario ───────────────────────────────────────
conc_bene <- df[FLAG_BENE_NULO == 0,
                .(N_CUENTAS_ORIGEN_AL_BENE = uniqueN(CUENTA)),
                by = NOMBRE_BENEFICIARIO]
df <- merge(df, conc_bene, by = "NOMBRE_BENEFICIARIO", all.x = TRUE)
df[, FLAG_BENE_CONCENTRADO := as.integer(!is.na(N_CUENTAS_ORIGEN_AL_BENE) &
                                           N_CUENTAS_ORIGEN_AL_BENE >= 5)]

# ── 5.8  Hash dispositivo — ATO ───────────────────────────────────────────────
hash_multi <- df[!is.na(HASH_DISPOSITIVO),
                 .(N_CUENTAS_POR_DISPOSITIVO = uniqueN(CUENTA)),
                 by = HASH_DISPOSITIVO]
df <- merge(df, hash_multi, by = "HASH_DISPOSITIVO", all.x = TRUE)
df[, FLAG_DISPOSITIVO_MULTIUSUARIO := as.integer(!is.na(N_CUENTAS_POR_DISPOSITIVO) &
                                                   N_CUENTAS_POR_DISPOSITIVO >= 3)]

# ── 5.9  Velocidad por cuenta (ventana diaria) ───────────────────────────────
vel_cuenta <- df[, .(VEL_TRX_DIA_CUENTA   = .N,
                     VEL_MONTO_DIA_CUENTA = sum(MONTO, na.rm = TRUE)),
                 by = .(CUENTA, FECHA_TRX)]
df <- merge(df, vel_cuenta, by = c("CUENTA","FECHA_TRX"), all.x = TRUE)
df[, FLAG_VEL_ALTA := as.integer(VEL_TRX_DIA_CUENTA > 3)]

# ── 5.10 Velocidad por BIN ────────────────────────────────────────────────────
vel_bin <- df[, .(VEL_TRX_DIA_BIN   = .N,
                  VEL_MONTO_DIA_BIN = sum(MONTO, na.rm = TRUE)),
              by = .(BIN, FECHA_TRX)]
df <- merge(df, vel_bin, by = c("BIN","FECHA_TRX"), all.x = TRUE)

# ── 5.11 Z-score de monto por cuenta ─────────────────────────────────────────
df[, `:=`(
  ZSCORE_MONTO_CUENTA = round((MONTO - mean(MONTO, na.rm=TRUE)) /
                                (sd(MONTO, na.rm=TRUE) + 1e-9), 3)
), by = CUENTA]
df[, FLAG_ANOMALIA_MONTO := as.integer(abs(ZSCORE_MONTO_CUENTA) > 2)]

cat(sprintf("✅ Shape final con features: %s filas × %s columnas\n",
            format(nrow(df), big.mark=","), ncol(df)))

# Solo clasificadas para métricas limpias
df_cal <- df[FLAG_CLASIFICADO == 1]


# =============================================================================
#  6. KPIs EJECUTIVOS
# =============================================================================

total_trx   <- nrow(df)
total_cal   <- nrow(df_cal)
total_fraud <- nrow(df_cal[FLAG_FRAUDE == 1])
monto_fraud <- sum(df_cal[FLAG_FRAUDE == 1]$MONTO, na.rm = TRUE)
fraud_rate  <- total_fraud / total_cal * 100
severidad   <- monto_fraud / total_fraud
vaciados_f  <- nrow(df_cal[FLAG_VACIADO_CUENTA == 1 & FLAG_FRAUDE == 1])
ato_f       <- nrow(df_cal[FLAG_DISPOSITIVO_MULTIUSUARIO == 1 & FLAG_FRAUDE == 1])
nuevas_f    <- nrow(df_cal[FLAG_CUENTA_NUEVA == 1 & FLAG_FRAUDE == 1])

cat("\n", strrep("━", 65), "\n")
cat(sprintf("  Total transacciones (dataset)         : %15s\n", format(total_trx,   big.mark=",")))
cat(sprintf("  Transacciones clasificadas             : %15s\n", format(total_cal,   big.mark=",")))
cat(sprintf("  Fraudes confirmados (F)                : %15s\n", format(total_fraud, big.mark=",")))
cat(sprintf("  Monto total fraudulento (S/)           : %15s\n", format(round(monto_fraud,2), big.mark=",")))
cat(sprintf("  Fraud Rate (sobre clasificadas)        : %14.4f %%\n", fraud_rate))
cat(sprintf("  Severidad promedio (S/ por caso)       : %15s\n", format(round(severidad,2), big.mark=",")))
cat(strrep("─", 65), "\n")
cat(sprintf("  Fraudes con vaciado de cuenta (≥80%%)  : %15s\n", format(vaciados_f, big.mark=",")))
cat(sprintf("  Fraudes con dispositivo multiusuario  : %15s\n", format(ato_f,       big.mark=",")))
cat(sprintf("  Fraudes en cuentas nuevas (≤3 trx)    : %15s\n", format(nuevas_f,    big.mark=",")))
cat(strrep("━", 65), "\n\n")


# =============================================================================
#  7. EVOLUCIÓN MENSUAL
# =============================================================================

evol <- df_cal[, .(
  TOTAL_TRX            = .N,
  FRAUDES              = sum(FLAG_FRAUDE),
  MONTO_FRAUDE         = sum(MONTO[FLAG_FRAUDE==1], na.rm=TRUE),
  FRAUDES_VACIADO      = sum(FLAG_VACIADO_CUENTA[FLAG_FRAUDE==1]),
  FRAUDES_CUENTA_NUEVA = sum(FLAG_CUENTA_NUEVA[FLAG_FRAUDE==1])
), by = MES_ORIGEN]
evol[, `:=`(
  FRAUD_RATE = round(FRAUDES / TOTAL_TRX * 100, 4),
  SEVERIDAD  = round(MONTO_FRAUDE / FRAUDES, 2)
)]
evol <- evol[order(MES_ORIGEN)]

p_evol <- evol |>
  tidyr::pivot_longer(cols = c(FRAUDES, FRAUD_RATE, MONTO_FRAUDE),
                      names_to = "METRICA", values_to = "VALOR") |>
  ggplot(aes(x = MES_ORIGEN, y = VALOR, fill = METRICA)) +
  geom_col(alpha = 0.85, color = "white", linewidth = 0.3) +
  geom_text(aes(label = scales::comma(round(VALOR, 2))),
            vjust = -0.4, size = 3, color = COLOR_TEXT) +
  facet_wrap(~METRICA, scales = "free_y",
             labeller = labeller(METRICA = c(
               FRAUDES      = "# Fraudes",
               FRAUD_RATE   = "Fraud Rate (%)",
               MONTO_FRAUDE = "Monto Fraude (S/)"
             ))) +
  scale_fill_manual(values = c(FRAUDES=COLOR_FRAUD, FRAUD_RATE=COLOR_ACCENT,
                                MONTO_FRAUDE=COLOR_OK)) +
  scale_y_continuous(labels = scales::comma) +
  labs(title = "Evolución Mensual — Transferencias a Terceros",
       x = "Mes", y = NULL) +
  theme(legend.position = "none")

print(p_evol)


# =============================================================================
#  8. CANAL ATM vs MOT
# =============================================================================

por_canal <- df_cal[, .(
  TOTAL        = .N,
  FRAUDES      = sum(FLAG_FRAUDE),
  MONTO_FRAUDE = sum(MONTO[FLAG_FRAUDE==1], na.rm=TRUE),
  TICKET_PROM  = round(mean(MONTO, na.rm=TRUE), 2),
  VACIADOS_F   = sum(FLAG_VACIADO_CUENTA[FLAG_FRAUDE==1])
), by = CANAL]
por_canal[, FRAUD_RATE := round(FRAUDES / TOTAL * 100, 3)]

# Heatmap Canal × Franja
canal_hora <- df_cal[, .(
  TOTAL   = .N,
  FRAUDES = sum(FLAG_FRAUDE)
), by = .(CANAL, FRANJA_HORARIA)]
canal_hora[, FRAUD_RATE := round(FRAUDES / TOTAL * 100, 3)]

p_canal_heat <- ggplot(canal_hora,
                       aes(x = FRANJA_HORARIA, y = CANAL, fill = FRAUD_RATE)) +
  geom_tile(color = COLOR_BG, linewidth = 0.8) +
  geom_text(aes(label = paste0(FRAUD_RATE, "%")),
            color = "white", size = 3.5, fontface = "bold") +
  scale_fill_gradient(low = COLOR_PANEL, high = COLOR_FRAUD,
                      name = "Fraud Rate (%)") +
  labs(title = "Fraud Rate: Canal × Franja Horaria",
       x = "Franja", y = "Canal")

print(p_canal_heat)


# =============================================================================
#  9. TEMPORALIDAD — HORA Y DÍA DE SEMANA
# =============================================================================

por_hora <- df_cal[, .(
  TOTAL   = .N,
  FRAUDES = sum(FLAG_FRAUDE)
), by = HORA]
por_hora[, FRAUD_RATE := round(FRAUDES / TOTAL * 100, 3)]
por_hora <- por_hora[order(HORA)]

p_hora <- ggplot(por_hora, aes(x = HORA)) +
  geom_col(aes(y = TOTAL), fill = COLOR_OK, alpha = 0.4) +
  geom_line(aes(y = FRAUD_RATE * max(TOTAL) / max(FRAUD_RATE)),
            color = COLOR_FRAUD, linewidth = 2, linetype = "solid") +
  geom_point(aes(y = FRAUD_RATE * max(TOTAL) / max(FRAUD_RATE)),
             color = COLOR_FRAUD, size = 2.5) +
  scale_x_continuous(breaks = 0:23) +
  scale_y_continuous(
    name     = "Volumen transacciones",
    sec.axis = sec_axis(~ . * max(por_hora$FRAUD_RATE) / max(por_hora$TOTAL),
                        name = "Fraud Rate (%)")
  ) +
  labs(title   = "Volumen vs Fraud Rate por Hora del Día",
       subtitle = "Barras = volumen · Línea roja = Fraud Rate",
       x = "Hora")

DIAS_LABEL <- c("Dom","Lun","Mar","Mié","Jue","Vie","Sáb")
por_dia <- df_cal[, .(
  TOTAL   = .N,
  FRAUDES = sum(FLAG_FRAUDE)
), by = DIA_SEMANA]
por_dia[, `:=`(
  FRAUD_RATE = round(FRAUDES / TOTAL * 100, 3),
  NOMBRE_DIA = DIAS_LABEL[DIA_SEMANA]
)]

p_dia <- ggplot(por_dia, aes(x = reorder(NOMBRE_DIA, DIA_SEMANA), y = FRAUD_RATE,
                              fill = NOMBRE_DIA %in% c("Sáb","Dom"))) +
  geom_col(alpha = 0.85, color = "white") +
  geom_text(aes(label = paste0(FRAUD_RATE, "%")),
            vjust = -0.4, size = 3, color = COLOR_TEXT) +
  scale_fill_manual(values = c("FALSE" = COLOR_OK, "TRUE" = COLOR_FRAUD),
                    guide = "none") +
  labs(title = "Fraud Rate por Día de Semana",
       x = "Día", y = "Fraud Rate (%)")

print(p_hora / p_dia)


# =============================================================================
#  10. DISTRIBUCIÓN DE MONTOS — DENSITY PLOT SUPERPUESTO (ggdist)
# =============================================================================

df_plot_monto <- df_cal[!is.na(MONTO), .(
  MONTO,
  GRUPO = fifelse(FLAG_FRAUDE == 1, "Fraude", "No Fraude")
)]

p_density <- ggplot(df_plot_monto, aes(x = MONTO, fill = GRUPO, color = GRUPO)) +
  geom_density(alpha = 0.45, linewidth = 0.8) +
  scale_fill_manual(values  = c("Fraude" = COLOR_FRAUD, "No Fraude" = COLOR_OK)) +
  scale_color_manual(values = c("Fraude" = COLOR_FRAUD, "No Fraude" = COLOR_OK)) +
  scale_x_continuous(labels = scales::comma) +
  labs(title    = "Distribución de Montos — Fraude vs No Fraude",
       subtitle = "Densidades superpuestas con transparencia",
       x = "Monto (S/)", y = "Densidad", fill = NULL, color = NULL)

# Ridgeline por mes para ver evolución de la distribución
p_ridge <- ggplot(
  df_cal[!is.na(MONTO)],
  aes(x = MONTO, y = MES_ORIGEN, fill = after_stat(x))
) +
  geom_density_ridges_gradient(scale = 1.8, rel_min_height = 0.01) +
  scale_fill_gradient(low = COLOR_OK, high = COLOR_FRAUD,
                      name = "Monto (S/)") +
  scale_x_continuous(labels = scales::comma) +
  labs(title = "Distribución de Montos por Mes (Ridgeline)",
       x = "Monto (S/)", y = "Mes")

print(p_density / p_ridge)


# =============================================================================
#  11. SALDO DISPONIBLE — VACIADO DE CUENTA
# =============================================================================

tramos <- list(
  list(nombre="Nulo",    lo=NA,  hi=NA),
  list(nombre="0-20%",   lo=0,   hi=0.2),
  list(nombre="20-50%",  lo=0.2, hi=0.5),
  list(nombre="50-80%",  lo=0.5, hi=0.8),
  list(nombre="80-100%", lo=0.8, hi=1.0),
  list(nombre=">100%",   lo=1.0, hi=Inf)
)

res_tramos <- rbindlist(lapply(tramos, function(t) {
  if (is.na(t$lo)) {
    sub <- df_cal[is.na(RATIO_MONTO_SALDO)]
  } else {
    sub <- df_cal[!is.na(RATIO_MONTO_SALDO) &
                    RATIO_MONTO_SALDO >= t$lo & RATIO_MONTO_SALDO < t$hi]
  }
  n <- nrow(sub); f <- sum(sub$FLAG_FRAUDE)
  data.table(Tramo=t$nombre, Total=n, Fraudes=f,
             FraudRate=ifelse(n>0, round(f/n*100,3), 0))
}))
res_tramos[, Tramo := factor(Tramo, levels = Tramo)]

p_ratio <- ggplot(res_tramos, aes(x = Tramo, y = FraudRate,
                                   fill = FraudRate == max(FraudRate))) +
  geom_col(alpha = 0.85, color = "white") +
  geom_text(aes(label = paste0(FraudRate, "%")),
            vjust = -0.4, size = 3, color = COLOR_TEXT) +
  scale_fill_manual(values = c("FALSE"=COLOR_OK,"TRUE"=COLOR_FRAUD), guide="none") +
  labs(title    = "Fraud Rate por Tramo de Ratio Monto/Saldo",
       subtitle = "¿Los fraudes vacían la cuenta?",
       x = "Tramo", y = "Fraud Rate (%)")

print(p_ratio)


# =============================================================================
#  12. HISTORIA DE CUENTA — CC_K05
# =============================================================================

tramos_cnt <- list(
  list(n="Nulo",          lo=NA, hi=NA),
  list(n="Sin historia\n(0)", lo=0, hi=0),
  list(n="Muy baja\n(1-3)",   lo=1, hi=3),
  list(n="Baja\n(4-10)",      lo=4, hi=10),
  list(n="Media\n(11-30)",    lo=11, hi=30),
  list(n="Alta\n(>30)",       lo=31, hi=Inf)
)

res_cnt <- rbindlist(lapply(tramos_cnt, function(t) {
  if (is.na(t$lo)) {
    sub <- df_cal[is.na(CNT_MES_PREVIO)]
  } else {
    sub <- df_cal[!is.na(CNT_MES_PREVIO) &
                    CNT_MES_PREVIO >= t$lo & CNT_MES_PREVIO <= t$hi]
  }
  n <- nrow(sub); f <- sum(sub$FLAG_FRAUDE)
  data.table(Tramo=t$n, Total=n, Fraudes=f,
             FraudRate=ifelse(n>0, round(f/n*100,3), 0))
}))
res_cnt[, Tramo := factor(Tramo, levels = Tramo)]

p_cnt <- ggplot(res_cnt, aes(x = Tramo, y = FraudRate,
                               fill = FraudRate == max(FraudRate))) +
  geom_col(alpha = 0.85, color = "white") +
  geom_text(aes(label = paste0(FraudRate, "%")),
            vjust = -0.4, size = 3, color = COLOR_TEXT) +
  scale_fill_manual(values=c("FALSE"=COLOR_OK,"TRUE"=COLOR_FRAUD), guide="none") +
  labs(title    = "Fraud Rate por Historia de Transacciones (CC_K05)",
       subtitle = "Tramos del contador del mes previo — cuentas nuevas vs establecidas",
       x = "Tramo CC_K05 (trx mes anterior)", y = "Fraud Rate (%)")

print(p_cnt)


# =============================================================================
#  13. GRAFO DE RED — BENEFICIARIOS (igraph + ggraph)
# =============================================================================

# Construir edge list: CUENTA → NOMBRE_BENEFICIARIO (solo con nombre y con fraude)
edges_fraude <- df_cal[
  FLAG_FRAUDE == 1 & FLAG_BENE_NULO == 0,
  .(from = CUENTA, to = NOMBRE_BENEFICIARIO, monto = MONTO)
]

# Beneficiarios que reciben desde 2+ cuentas distintas
bene_multi <- edges_fraude[, .(n_cuentas = uniqueN(from)), by = to]
bene_multi <- bene_multi[n_cuentas >= 2]
edges_red  <- edges_fraude[to %in% bene_multi$to]

if (nrow(edges_red) > 0) {
  g <- graph_from_data_frame(
    d        = edges_red[, .(from, to, monto)],
    directed = TRUE
  )

  # Tipo de nodo: CUENTA o BENEFICIARIO
  V(g)$tipo <- ifelse(
    V(g)$name %in% edges_red$from, "Cuenta Origen", "Beneficiario"
  )
  V(g)$color_nodo <- ifelse(V(g)$tipo == "Beneficiario", COLOR_FRAUD, COLOR_OK)

  tg <- as_tbl_graph(g) |>
    activate(nodes) |>
    mutate(
      centralidad = centrality_degree(mode = "all"),
      tipo        = ifelse(name %in% edges_red$from, "Cuenta Origen", "Beneficiario")
    )

  p_grafo <- ggraph(tg, layout = "fr") +
    geom_edge_link(aes(edge_alpha = monto),
                   arrow = arrow(length = unit(3,"mm"), type = "closed"),
                   end_cap   = circle(3, "mm"),
                   color     = COLOR_ACCENT,
                   linewidth = 0.6) +
    geom_node_point(aes(color = tipo, size = centralidad)) +
    geom_node_text(aes(label = ifelse(tipo=="Beneficiario", name, "")),
                   repel = TRUE, size = 2.5, color = COLOR_TEXT) +
    scale_color_manual(values = c("Cuenta Origen"=COLOR_OK,
                                  "Beneficiario"  =COLOR_FRAUD),
                       name = "Tipo de nodo") +
    scale_size_continuous(range = c(2, 8), name = "Centralidad") +
    scale_edge_alpha_continuous(range = c(0.2, 0.9), name = "Monto (S/)") +
    labs(
      title    = "Red de Fraude — Beneficiarios Concentrados",
      subtitle = "Nodos rojos = beneficiarios · Azules = cuentas origen · Aristas = transferencias"
    ) +
    theme(legend.position = "right")

  print(p_grafo)
} else {
  cat("ℹ️  No hay suficientes fraudes con beneficiario conocido para el grafo.\n")
}


# =============================================================================
#  14. ANÁLISIS DE DISPOSITIVO — ATO
# =============================================================================

dispositivos_riesgo <- df_cal[
  !is.na(HASH_DISPOSITIVO),
  .(
    N_CUENTAS    = uniqueN(CUENTA),
    TOTAL_TRX    = .N,
    FRAUDES      = sum(FLAG_FRAUDE),
    MONTO_FRAUDE = sum(MONTO[FLAG_FRAUDE==1], na.rm=TRUE)
  ),
  by = HASH_DISPOSITIVO
][N_CUENTAS >= 2][order(-N_CUENTAS)]

dispositivos_riesgo[, FRAUD_RATE := round(FRAUDES/TOTAL_TRX*100, 3)]

cat(sprintf("Dispositivos que operan 2+ cuentas: %s\n",
            format(nrow(dispositivos_riesgo), big.mark=",")))
print(head(dispositivos_riesgo, 20))

p_ato <- ggplot(dispositivos_riesgo, aes(x = N_CUENTAS)) +
  geom_histogram(fill = COLOR_FRAUD, color = "white",
                 alpha = 0.85, bins = 20) +
  scale_x_continuous(breaks = scales::pretty_breaks()) +
  scale_y_continuous(labels = scales::comma) +
  labs(
    title    = "Distribución de Cuentas por Dispositivo — Riesgo ATO",
    subtitle = "Dispositivos con múltiples cuentas = posible Account Takeover",
    x = "N° de cuentas distintas por dispositivo",
    y = "N° de dispositivos"
  )

print(p_ato)


# =============================================================================
#  15. ÁRBOL DE DECISIÓN — DISCRETIZACIÓN DE VARIABLES
#  Objetivo: encontrar los cortes óptimos de variables continuas
#  que mejor discriminan fraude vs no fraude
# =============================================================================

vars_arbol <- c(
  "MONTO", "SALDO_DISPONIBLE", "RATIO_MONTO_SALDO",
  "CNT_MES_PREVIO", "VEL_TRX_DIA_CUENTA", "VEL_MONTO_DIA_CUENTA",
  "ZSCORE_MONTO_CUENTA", "N_CUENTAS_POR_DISPOSITIVO",
  "N_CUENTAS_ORIGEN_AL_BENE", "VEL_TRX_DIA_BIN"
)
vars_arbol <- vars_arbol[vars_arbol %in% names(df_cal)]

df_arbol <- df_cal[
  !is.na(FLAG_FRAUDE),
  c("FLAG_FRAUDE", vars_arbol),
  with = FALSE
] |> as.data.frame()

# Eliminar filas con todos NA en vars
df_arbol <- df_arbol[rowSums(is.na(df_arbol[, vars_arbol])) < length(vars_arbol), ]
df_arbol$FLAG_FRAUDE <- as.factor(df_arbol$FLAG_FRAUDE)

formula_arbol <- as.formula(paste("FLAG_FRAUDE ~", paste(vars_arbol, collapse = " + ")))

arbol <- rpart(
  formula_arbol,
  data    = df_arbol,
  method  = "class",
  control = rpart.control(
    cp       = 0.005,   # complejidad mínima — ajusta si el árbol es muy grande
    maxdepth = 5,       # profundidad máxima
    minsplit = 10       # mínimo de obs para hacer un split
  ),
  parms = list(prior = c(0.5, 0.5))  # balanceo manual dado el desbalance
)

cat("\n📋 Importancia de variables (árbol de decisión):\n")
imp <- data.frame(
  variable   = names(arbol$variable.importance),
  importancia = round(arbol$variable.importance / sum(arbol$variable.importance) * 100, 2)
)
print(imp[order(-imp$importancia), ])

# Visualización del árbol
rpart.plot(
  arbol,
  type    = 4,
  extra   = 106,    # % de clase + n observaciones
  fallen.leaves = TRUE,
  box.palette   = list(COLOR_OK, COLOR_FRAUD),
  shadow.col    = "gray30",
  main    = "Árbol de Decisión — Discretización de Variables de Fraude",
  cex     = 0.75
)

cat("\n📋 Reglas del árbol (cortes óptimos de discretización):\n")
print(arbol)


# =============================================================================
#  16. PROPUESTA DE REGLAS — EVALUACIÓN CON PRECISIÓN Y RECALL
# =============================================================================

evaluar_regla <- function(nombre, condicion) {
  sub       <- df_cal[eval(condicion)]
  n_alertas <- nrow(sub)
  n_fraudes <- sum(sub$FLAG_FRAUDE)
  precision <- ifelse(n_alertas > 0, n_fraudes / n_alertas * 100, 0)
  recall    <- ifelse(total_fraud > 0, n_fraudes / total_fraud * 100, 0)
  monto_cap <- sum(sub$MONTO[sub$FLAG_FRAUDE == 1], na.rm = TRUE)
  data.table(
    Regla      = nombre,
    Alertas    = n_alertas,
    Fraudes    = n_fraudes,
    Precision  = round(precision, 2),
    Recall     = round(recall, 2),
    Monto_Cap  = round(monto_cap, 2)
  )
}

reglas <- list(
  list("R01: Monto Alto (≥5,000)",             quote(FLAG_MONTO_ALTO == 1)),
  list("R02: Monto Crítico (≥10,000)",         quote(FLAG_MONTO_CRITICO == 1)),
  list("R03: Velocidad Alta (>3 trx/día)",     quote(FLAG_VEL_ALTA == 1)),
  list("R04: Anomalía Monto (|z|>2)",          quote(FLAG_ANOMALIA_MONTO == 1)),
  list("R05: Madrugada (0-5h)",                quote(FRANJA_HORARIA == "Madrugada")),
  list("R06: Vaciado cuenta (≥80%)",           quote(FLAG_VACIADO_CUENTA == 1)),
  list("R07: Cuenta nueva (≤3 trx previas)",   quote(FLAG_CUENTA_NUEVA == 1)),
  list("R08: Beneficiario nulo",               quote(FLAG_BENE_NULO == 1)),
  list("R09: Dispositivo multiusuario (ATO)",  quote(FLAG_DISPOSITIVO_MULTIUSUARIO == 1)),
  list("R10: Beneficiario concentrado (≥5c)",  quote(FLAG_BENE_CONCENTRADO == 1)),
  list("R11: Vaciado + Madrugada",             quote(FLAG_VACIADO_CUENTA==1 & FRANJA_HORARIA=="Madrugada")),
  list("R12: Vaciado + Vel Alta",              quote(FLAG_VACIADO_CUENTA==1 & FLAG_VEL_ALTA==1)),
  list("R13: Cuenta nueva + Monto Alto",       quote(FLAG_CUENTA_NUEVA==1 & FLAG_MONTO_ALTO==1)),
  list("R14: MOT + Madrugada + Vaciado",       quote(CANAL=="MOT" & FRANJA_HORARIA=="Madrugada" & FLAG_VACIADO_CUENTA==1)),
  list("R15: ATO + Monto Alto",                quote(FLAG_DISPOSITIVO_MULTIUSUARIO==1 & FLAG_MONTO_ALTO==1)),
  list("R16: Cuenta nueva + Vaciado + Madrugada", quote(FLAG_CUENTA_NUEVA==1 & FLAG_VACIADO_CUENTA==1 & FRANJA_HORARIA=="Madrugada"))
)

df_reglas <- rbindlist(lapply(reglas, function(r) evaluar_regla(r[[1]], r[[2]])))
df_reglas <- df_reglas[order(-Precision)]

cat("\n📋 Evaluación de Reglas:\n")
print(df_reglas)

# Visualización precision vs recall
p_reglas <- df_reglas |>
  tidyr::pivot_longer(cols = c(Precision, Recall),
                      names_to = "METRICA", values_to = "VALOR") |>
  ggplot(aes(x = reorder(Regla, VALOR), y = VALOR, fill = METRICA)) +
  geom_col(position = "dodge", alpha = 0.85, color = "white", linewidth = 0.3) +
  scale_fill_manual(values = c(Precision=COLOR_FRAUD, Recall=COLOR_ACCENT)) +
  coord_flip() +
  labs(
    title    = "Precisión vs Recall por Regla Propuesta",
    subtitle = "Ordenado por Precisión descendente",
    x = NULL, y = "%", fill = "Métrica"
  )

print(p_reglas)


# =============================================================================
#  17. EXPORT
# =============================================================================

OUTPUT_PATH <- file.path(BASE_DIR)
dir.create(OUTPUT_PATH, recursive = TRUE, showWarnings = FALSE)

# Dataset completo enriquecido
fwrite(df, file.path(OUTPUT_PATH, "transferencias_terceros_enriquecido.csv"))

# Solo fraudes
writexl::write_xlsx(
  as.data.frame(df_cal[FLAG_FRAUDE==1][order(-MONTO)]),
  file.path(OUTPUT_PATH, "fraudes_transferencias_terceros.xlsx")
)

# Tabla de reglas
writexl::write_xlsx(
  as.data.frame(df_reglas),
  file.path(OUTPUT_PATH, "evaluacion_reglas.xlsx")
)

# Dispositivos ATO
writexl::write_xlsx(
  as.data.frame(dispositivos_riesgo),
  file.path(OUTPUT_PATH, "dispositivos_ato.xlsx")
)

cat("✅ Archivos exportados en:", OUTPUT_PATH, "\n")
cat("   → transferencias_terceros_enriquecido.csv\n")
cat("   → fraudes_transferencias_terceros.xlsx\n")
cat("   → evaluacion_reglas.xlsx\n")
cat("   → dispositivos_ato.xlsx\n")
