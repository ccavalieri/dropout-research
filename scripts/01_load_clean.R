###############################################################################
# 01_load_clean.R
#
# Read, select project columns, and clean each BAS_ table.
#
# Inputs:  <INPUT_DIR>/{TABLE}_{YEAR}.csv  
# Outputs: data/clean/{TABLE}_{YEAR}.rds
#          data/clean/{TABLE}_{YEAR}.csv
#          data/clean/_quality_report.csv     
#          data/clean/_column_summary.csv     
###############################################################################

# ─── CONFIG ──────────────────────────────────────────────────────────────────
INPUT_DIR  <- "/Users/cc9/Documents/GitHub/dropout-research/data/synthetic"   
OUTPUT_DIR <- "/Users/cc9/Documents/GitHub/dropout-research/data/clean"
YEARS      <- 2019:2024

# Plausible age range
AGE_MIN <- 0L
AGE_MAX <- 120L

# Columns per table
COLS <- list(
  BAS_ESCOLA = c(
    "NU_ANO", "CO_ENTIDADE", "TP_SITUACAO_FUNCIONAMENTO", "TP_FALTANTE",
    "CO_UF", "CO_MUNICIPIO", "TP_DEPENDENCIA", "TP_LOCALIZACAO",
    "TP_LOCALIZACAO_DIFERENCIADA", "TP_CATEGORIA_ESCOLA_PRIVADA",
    "IN_LOCAL_FUNC_PREDIO_ESCOLAR", "IN_LOCAL_FUNC_SOCIOEDUCATIVO",
    "IN_LOCAL_FUNC_UNID_PRISIONAL", "IN_LOCAL_FUNC_PRISIONAL_SOCIO",
    "IN_LOCAL_FUNC_GALPAO", "IN_LOCAL_FUNC_SALAS_OUTRA_ESC",
    "IN_LOCAL_FUNC_OUTROS", "IN_PREDIO_COMPARTILHADO", "IN_AGUA_POTAVEL",
    "IN_AGUA_REDE_PUBLICA", "IN_AGUA_POCO_ARTESIANO", "IN_AGUA_CACIMBA",
    "IN_AGUA_FONTE_RIO", "IN_AGUA_INEXISTENTE", "IN_ENERGIA_REDE_PUBLICA",
    "IN_ENERGIA_GERADOR_FOSSIL", "IN_ENERGIA_RENOVAVEL", "IN_ENERGIA_INEXISTENTE",
    "IN_ESGOTO_REDE_PUBLICA", "IN_ESGOTO_FOSSA_SEPTICA", "IN_ESGOTO_FOSSA_COMUM",
    "IN_ESGOTO_FOSSA", "IN_ESGOTO_INEXISTENTE", "IN_LIXO_SERVICO_COLETA",
    "IN_LIXO_QUEIMA", "IN_LIXO_ENTERRA", "IN_LIXO_DESTINO_FINAL_PUBLICO",
    "IN_LIXO_DESCARTA_OUTRA_AREA", "IN_TRATAMENTO_LIXO_SEPARACAO",
    "IN_TRATAMENTO_LIXO_REUTILIZA", "IN_TRATAMENTO_LIXO_RECICLAGEM",
    "IN_TRATAMENTO_LIXO_INEXISTENTE", "IN_BANHEIRO", "IN_BANHEIRO_PNE",
    "IN_BIBLIOTECA_SALA_LEITURA", "IN_LABORATORIO_CIENCIAS",
    "IN_LABORATORIO_INFORMATICA", "IN_QUADRA_ESPORTES", "QT_SALAS_UTILIZADAS",
    "IN_COMPUTADOR", "IN_EQUIP_MULTIMIDIA", "QT_EQUIP_MULTIMIDIA",
    "IN_DESKTOP_ALUNO", "QT_DESKTOP_ALUNO", "IN_COMP_PORTATIL_ALUNO",
    "QT_COMP_PORTATIL_ALUNO", "IN_TABLET_ALUNO", "QT_TABLET_ALUNO",
    "IN_INTERNET", "IN_INTERNET_ALUNOS", "IN_ACESSO_INTERNET_COMPUTADOR",
    "IN_BANDA_LARGA", "QT_PROF_COORDENADOR", "QT_PROF_FONAUDIOLOGO",
    "QT_PROF_NUTRICIONISTA", "QT_PROF_PSICOLOGO", "QT_PROF_PEDAGOGIA",
    "IN_ALIMENTACAO", "IN_ORGAO_ASS_PAIS", "IN_ORGAO_ASS_PAIS_MESTRES",
    "IN_ORGAO_CONSELHO_ESCOLAR"
  ),
  BAS_TURMA = c(
    "NU_ANO", "ID_TURMA", "TP_MEDIACAO_DIDATICO_PEDAGO", "NU_DIAS_ATIVIDADE",
    "TP_ETAPA_ENSINO", "IN_REGULAR", "IN_EJA", "IN_PROFISSIONALIZANTE",
    "QT_MATRICULAS", "TX_HR_INICIAL", "CO_ENTIDADE"
  ),
  BAS_MATRICULA = c(
    "NU_ANO", "CO_PESSOA_FISICA", "ID_MATRICULA", "NU_IDADE_REFERENCIA",
    "TP_SEXO", "TP_COR_RACA", "TP_NACIONALIDADE", "CO_UF_NASC",
    "CO_MUNICIPIO_NASC", "CO_PAIS_RESIDENCIA", "CO_UF_END", "CO_MUNICIPIO_END",
    "TP_ZONA_RESIDENCIAL", "IN_NECESSIDADE_ESPECIAL", "IN_TRANSPORTE_PUBLICO",
    "IN_TRANSP_BICICLETA", "IN_TRANSP_MICRO_ONIBUS", "IN_TRANSP_ONIBUS",
    "IN_TRANSP_TR_ANIMAL", "IN_TRANSP_VANS_KOMBI", "IN_TRANSP_OUTRO_VEICULO",
    "IN_TRANSP_EMBAR_ATE5", "IN_TRANSP_EMBAR_5A15", "IN_TRANSP_EMBAR_15A35",
    "IN_TRANSP_EMBAR_35", "IN_ESPECIAL_EXCLUSIVA", "IN_REGULAR", "IN_EJA",
    "IN_PROFISSIONALIZANTE", "ID_TURMA", "CO_ENTIDADE"
  ),
  BAS_SITUACAO = c(
    "NU_ANO", "ID_MATRICULA", "CO_PESSOA_FISICA", "IN_REGULAR", "ID_TURMA",
    "CO_ENTIDADE", "TP_SITUACAO", "IN_CONCLUINTE", "IN_TRANSFERIDO"
  )
)

# Join keys per table
KEYS <- list(
  BAS_ESCOLA    = "CO_ENTIDADE",
  BAS_TURMA     = c("ID_TURMA", "CO_ENTIDADE"),
  BAS_MATRICULA = c("ID_MATRICULA", "CO_PESSOA_FISICA", "ID_TURMA", "CO_ENTIDADE"),
  BAS_SITUACAO  = c("ID_MATRICULA", "CO_PESSOA_FISICA", "ID_TURMA", "CO_ENTIDADE")
)

# Coded values to replace with NA, per table and column. Empty by default. Example:
#   SENTINELS <- list(
#     BAS_MATRICULA = list(TP_NACIONALIDADE = c(9L), TP_ZONA_RESIDENCIAL = c(9L)),
#     BAS_ESCOLA    = list(IN_ORGAO_ASS_PAIS = c(9L))
#   )
SENTINELS <- list(
  BAS_ESCOLA    = list(),
  BAS_TURMA     = list(),
  BAS_MATRICULA = list(),
  BAS_SITUACAO  = list()
)

# ─── PACKAGES ────────────────────────────────────────────────────────────────
suppressPackageStartupMessages(library(data.table))
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

`%||%` <- function(a, b) if (is.null(a)) b else a

# ─── TYPE COERCION ───────────────────────────────────────────────────────────
#   CO_*, ID_*  -> character
#   IN_*, TP_*  -> integer code
#   QT_*, NU_*  -> numeric
#   TX_HR_INICIAL is the class start hour and is numeric despite the TX_ prefix.
is_code <- function(col) grepl("^(CO_|ID_)", col)

coerce_col <- function(col, x) {
  if (col == "NU_ANO")        return(suppressWarnings(as.integer(x)))
  if (col == "TX_HR_INICIAL") return(suppressWarnings(as.integer(x)))
  if (is_code(col))    return(trimws(as.character(x)))
  if (startsWith(col, "IN_") || startsWith(col, "TP_"))
    return(suppressWarnings(as.integer(x)))
  if (startsWith(col, "QT_") || startsWith(col, "NU_"))
    return(suppressWarnings(as.numeric(x)))
  as.character(x)
}

# ─── SENTINELS ───────────────────────────────────────────────────────────────
# Replace configured coded values with NA. Returns the table plus a count of
# replacements per column.
apply_sentinels <- function(dt, tbl, config) {
  rules <- config[[tbl]]
  counts <- list()
  if (!is.null(rules)) {
    for (col in names(rules)) {
      if (!col %in% names(dt)) next
      mask <- !is.na(dt[[col]]) & dt[[col]] %in% rules[[col]]
      n <- sum(mask)
      if (n > 0) dt[mask, (col) := NA]
      counts[[col]] <- data.table(column = col, n_replaced = n)
    }
  }
  list(dt = dt,
       counts = if (length(counts)) rbindlist(counts)
                else data.table(column = character(), n_replaced = integer()))
}

# ─── COLUMN SUMMARY ──────────────────────────────────────────────────────────
# Missingness for every column; min/max/mean/median/p1/p99 for numeric columns
col_summary <- function(tbl, yr, col, x) {
  n <- length(x); nm <- sum(is.na(x))
  out <- data.table(table = tbl, year = yr, column = col, n = n,
                    n_missing = nm, pct_missing = round(100 * nm / max(n, 1), 3))
  if (is.numeric(x)) {
    v <- x[!is.na(x)]
    if (length(v) > 0) {
      qs <- quantile(v, c(0.01, 0.5, 0.99), names = FALSE)
      out[, `:=`(min = min(v), max = max(v), mean = round(mean(v), 4),
                 median = qs[2], p1 = qs[1], p99 = qs[3])]
    }
  }
  out
}

# ─── FILE DISCOVERY ──────────────────────────────────────────────────────────
find_file <- function(tbl, yr) {
  pat  <- sprintf("%s.*%d.*\\.csv$", tbl, yr)
  hits <- list.files(INPUT_DIR, pattern = pat, full.names = TRUE,
                     ignore.case = TRUE)
  if (length(hits)) hits[1] else NA_character_
}

# ─── CLEAN ONE TABLE-YEAR ────────────────────────────────────────────────────
clean_table_year <- function(tbl, yr, path) {
  cols <- COLS[[tbl]]

  # Read the header to request only columns that exist this year.
  present_all  <- names(fread(path, sep = ";", nrows = 0, showProgress = FALSE))
  present_cols <- intersect(cols, present_all)
  missing_cols <- setdiff(cols, present_all)

  # Force code columns to character at read so fread never infers numeric.
  code_present <- present_cols[is_code(present_cols)]
  dt <- fread(path, sep = ";", encoding = "UTF-8", na.strings = c("", "NA"),
              select = present_cols, showProgress = FALSE,
              colClasses = setNames(rep("character", length(code_present)),
                                    code_present))

  # Add absent project columns as NA so every year has the same schema.
  for (mc in missing_cols) dt[, (mc) := NA]
  dt <- dt[, ..cols]

  n_in <- nrow(dt)

  # Type coercion.
  for (col in cols) set(dt, j = col, value = coerce_col(col, dt[[col]]))

  # Sentinel replacement.
  sent <- apply_sentinels(dt, tbl, SENTINELS)
  dt <- sent$dt
  n_sentinel <- sum(sent$counts$n_replaced)

  # Age validity.
  n_age_invalid <- 0L
  if ("NU_IDADE_REFERENCIA" %in% cols) {
    bad <- !is.na(dt$NU_IDADE_REFERENCIA) &
           (dt$NU_IDADE_REFERENCIA < AGE_MIN | dt$NU_IDADE_REFERENCIA > AGE_MAX)
    n_age_invalid <- sum(bad)
    if (n_age_invalid > 0) dt[bad, NU_IDADE_REFERENCIA := NA_real_]
  }

  # Drop exact-duplicate rows. Multi-enrollment rows differ by ID_MATRICULA and are kept.
  dt <- unique(dt)
  n_dupes <- n_in - nrow(dt)

  # Check join keys presence.
  for (k in KEYS[[tbl]]) {
    n_key_na <- sum(is.na(dt[[k]]))
    if (n_key_na > 0)
      message(sprintf("    [WARN] %s %d: %d rows with missing key %s",
                      tbl, yr, n_key_na, k))
  }

  summary_rows <- rbindlist(
    lapply(cols, function(c) col_summary(tbl, yr, c, dt[[c]])), fill = TRUE)

  report <- data.table(
    table = tbl, year = yr, n_in = n_in, n_out = nrow(dt),
    n_dupes_removed = n_dupes, n_cols = length(cols),
    missing_cols = paste(missing_cols, collapse = ","),
    n_age_invalid = n_age_invalid, n_sentinel_replaced = n_sentinel)

  list(data = dt, report = report, summary = summary_rows)
}

# ─── MAIN ────────────────────────────────────────────────────────────────────
t0 <- Sys.time()
message("01_load_clean — input: ", normalizePath(INPUT_DIR, mustWork = FALSE))

reports <- list(); summaries <- list()
for (tbl in names(COLS)) {
  for (yr in YEARS) {
    if (tbl == "BAS_SITUACAO" && yr == 2024L) next  # no year-end outcome
    path <- find_file(tbl, yr)
    if (is.na(path)) {
      message(sprintf("  [%s %d] file not found — skipping", tbl, yr))
      next
    }
    message(sprintf("  [%s %d] cleaning %s", tbl, yr, basename(path)))
    res <- clean_table_year(tbl, yr, path)

    base <- file.path(OUTPUT_DIR, sprintf("%s_%d", tbl, yr))
    saveRDS(res$data, paste0(base, ".rds"))
    fwrite(res$data, paste0(base, ".csv"), sep = ";", bom = TRUE, na = "")
    message(sprintf("    -> %d rows x %d cols  (%d dupes removed, %d sentinels)",
                    res$report$n_out, res$report$n_cols,
                    res$report$n_dupes_removed, res$report$n_sentinel_replaced))

    reports[[length(reports) + 1]]     <- res$report
    summaries[[length(summaries) + 1]] <- res$summary
    rm(res); gc(verbose = FALSE)
  }
}

report_dt  <- rbindlist(reports, fill = TRUE)
summary_dt <- rbindlist(summaries, fill = TRUE)
fwrite(report_dt,  file.path(OUTPUT_DIR, "_quality_report.csv"),  sep = ";", bom = TRUE, na = "")
fwrite(summary_dt, file.path(OUTPUT_DIR, "_column_summary.csv"), sep = ";", bom = TRUE, na = "")

message("\n=== Summary ===")
print(report_dt[, .(table, year, n_in, n_out, n_dupes_removed)])
message(sprintf("\nDone in %.1f s. Output in %s",
                as.numeric(difftime(Sys.time(), t0, units = "secs")),
                normalizePath(OUTPUT_DIR, mustWork = FALSE)))
