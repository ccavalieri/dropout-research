###############################################################################
# 02_build_label.R
#
# Build the person-year analytical base with the dropout label, for the regular
# education population. One row per (CO_PESSOA_FISICA, year).
#
# Label: a regular student enrolled in year t is a dropout if the
# same person has no enrollment of any modality in t+1. 
#
#
# Inputs:  data/clean/{BAS_MATRICULA,BAS_SITUACAO,BAS_TURMA}_{YEAR}.rds
# Outputs: data/labels/person_year_{YEAR}.rds / .csv
#          data/labels/_label_report.csv
###############################################################################

# ─── CONFIG ──────────────────────────────────────────────────────────────────
CLEAN_DIR  <- "/Users/cc9/Documents/GitHub/dropout-research/data/clean"
OUTPUT_DIR <- "/Users/cc9/Documents/GitHub/dropout-research/data/labels"
YEARS      <- 2019:2024

# Final stage of "ensino médio".
FINAL_MEDIO_ETAPAS <- c(27L, 28L, 29L, 32L, 33L, 34L, 37L, 38L)

# ─── PACKAGES ────────────────────────────────────────────────────────────────
suppressPackageStartupMessages(library(data.table))
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

read_clean <- function(tbl, yr) {
  path <- file.path(CLEAN_DIR, sprintf("%s_%d.rds", tbl, yr))
  if (!file.exists(path)) stop("missing input: ", path)
  readRDS(path)
}

# ─── PRESENCE SETS ───────────────────────────────────────────────────────────
# Distinct students enrolled in each year, any modality.
message("Building presence sets (all modalities)")
present_any <- list()
for (yr in YEARS) {
  m <- read_clean("BAS_MATRICULA", yr)
  present_any[[as.character(yr)]] <- unique(m$CO_PESSOA_FISICA)
  rm(m); gc(verbose = FALSE)
}

# ─── ONE YEAR ────────────────────────────────────────────────────────────────
build_person_year <- function(t) {
  # Regular enrollments of year t, with year-end outcome and class stage.
  mat <- read_clean("BAS_MATRICULA", t)
  mat <- mat[IN_REGULAR == 1]

  #BAS_SITUACAO inexistence for the last year
  sit_path <- file.path(CLEAN_DIR, sprintf("BAS_SITUACAO_%d.rds", t))
  if (file.exists(sit_path)) {
    sit <- readRDS(sit_path)[
      , .(ID_MATRICULA, TP_SITUACAO, IN_CONCLUINTE, IN_TRANSFERIDO)]
  } else {
    sit <- data.table(ID_MATRICULA = character(), TP_SITUACAO = integer(),
                      IN_CONCLUINTE = integer(), IN_TRANSFERIDO = integer())
  }
  tur <- read_clean("BAS_TURMA", t)[
    , .(ID_TURMA, TP_ETAPA_ENSINO, NU_DIAS_ATIVIDADE)]

  base <- merge(mat, sit, by = "ID_MATRICULA", all.x = TRUE)
  base <- merge(base, tur, by = "ID_TURMA", all.x = TRUE)

  # Person-level aggregates over all regular enrollments in the year.
  agg <- base[, .(
    QT_ESCOLAS_ANO      = uniqueN(CO_ENTIDADE),
    IN_TRANSFERIDO      = as.integer(any(IN_TRANSFERIDO == 1, na.rm = TRUE)),
    IN_CONCLUINTE_FINAL = as.integer(any(IN_CONCLUINTE == 1 &
                                         TP_ETAPA_ENSINO %in% FINAL_MEDIO_ETAPAS,
                                         na.rm = TRUE)),
    IN_FALECIDO         = as.integer(any(TP_SITUACAO == 3, na.rm = TRUE)),
    N_MATRICULAS_REG    = .N
  ), by = CO_PESSOA_FISICA]
  agg[, IN_TROCA_ESCOLA := as.integer(QT_ESCOLAS_ANO > 1)]

  # Main enrollment: most weekly activity days; tiebreak by id.
  base[, .load := fifelse(is.na(NU_DIAS_ATIVIDADE), -1, NU_DIAS_ATIVIDADE)]
  setorder(base, CO_PESSOA_FISICA, -.load, ID_MATRICULA)
  main_idx <- base[, .I[1], by = CO_PESSOA_FISICA]$V1
  main <- base[main_idx]
  main[, .load := NULL]

  # Drop the per-enrollment outcome fields superseded by person-level aggregates.
  main[, c("IN_TRANSFERIDO", "IN_CONCLUINTE") := NULL]

  py <- merge(main, agg, by = "CO_PESSOA_FISICA")

  # Treat deceaseas.
  py <- py[IN_FALECIDO == 0]

  # Label.
  if (t < max(YEARS)) {
    nxt <- present_any[[as.character(t + 1L)]]
    py[, evadiu := as.integer(!(CO_PESSOA_FISICA %in% nxt))]
    py[IN_CONCLUINTE_FINAL == 1, evadiu := NA_integer_]
  } else {
    py[, evadiu := NA_integer_]
  }

  report <- data.table(
    year = t,
    n_regular_pop      = nrow(py) + sum(agg$IN_FALECIDO),
    n_deceased         = sum(agg$IN_FALECIDO),
    n_concluinte_final = sum(py$IN_CONCLUINTE_FINAL),
    n_labeled          = sum(!is.na(py$evadiu)),
    n_evadiu           = sum(py$evadiu, na.rm = TRUE),
    dropout_rate       = round(100 * mean(py$evadiu, na.rm = TRUE), 3))

  rm(mat, sit, tur, base, agg, main); gc(verbose = FALSE)
  list(data = py, report = report)
}

# ─── MAIN ────────────────────────────────────────────────────────────────────
t0 <- Sys.time()
reports <- list()
for (t in YEARS) {
  message(sprintf("  [%d] building person-year", t))
  res  <- build_person_year(t)
  base <- file.path(OUTPUT_DIR, sprintf("person_year_%d", t))
  saveRDS(res$data, paste0(base, ".rds"))
  fwrite(res$data, paste0(base, ".csv"), sep = ";", bom = TRUE, na = "")
  reports[[length(reports) + 1]] <- res$report
  message(sprintf("    -> %d students  (labeled=%d, dropout=%.2f%%)",
                  nrow(res$data), res$report$n_labeled, res$report$dropout_rate))
  rm(res); gc(verbose = FALSE)
}

report_dt <- rbindlist(reports, fill = TRUE)
fwrite(report_dt, file.path(OUTPUT_DIR, "_label_report.csv"), sep = ";", bom = TRUE, na = "")

message("\n=== Label report ===")
print(report_dt)
message(sprintf("\nDone in %.1f s. Output in %s",
                as.numeric(difftime(Sys.time(), t0, units = "secs")),
                normalizePath(OUTPUT_DIR, mustWork = FALSE)))
