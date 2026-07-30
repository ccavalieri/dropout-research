###############################################################################
# 02_build_label_ooc.R  (out-of-core version of 02_build_label.R)
#
# Person-year base + dropout label, computed PARTITION-LOCAL. Because 01_ooc
# partitions MATRICULA and SITUACAO by hash(CO_PESSOA_FISICA), every enrollment
# and situation row of a student is in the same partition, so the person-year
# collapse (group by student), the MATRICULA<->SITUACAO join (by ID_MATRICULA)
# and the "present in t+1" check are all correct within a partition. The union
# over partitions equals the in-core person_year_{t}.
#
# Iterates years DESCENDING within each partition so each MATRICULA partition is
# read once: its all-modality student set is reused as the next-year presence of
# the year below it.
#
# Inputs:  <CLEAN_DIR>/BAS_{MATRICULA,SITUACAO}_{YEAR}_p{K}_b*.rds, BAS_TURMA_{YEAR}.rds
# Outputs: <OUTPUT_DIR>/person_year_{YEAR}_p{K}.rds
###############################################################################

suppressPackageStartupMessages(library(data.table))
source(file.path("scripts", "ooc_common.R"))

CLEAN_DIR  <- Sys.getenv("LABEL_IN",  "data/clean_ooc")
OUTPUT_DIR <- Sys.getenv("LABEL_OUT", "data/labels_ooc")
YEARS      <- eval(parse(text = Sys.getenv("LABEL_YEARS", "2019:2024")))
FINAL_MEDIO_ETAPAS <- c(27L,28L,29L,32L,33L,34L,37L,38L)
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

read_turma <- function(t) {
  p <- file.path(CLEAN_DIR, sprintf("BAS_TURMA_%d.rds", t))
  if (file.exists(p)) readRDS(p)[, .(ID_TURMA, TP_ETAPA_ENSINO, NU_DIAS_ATIVIDADE)]
  else data.table(ID_TURMA=character(), TP_ETAPA_ENSINO=integer(), NU_DIAS_ATIVIDADE=numeric())
}

# person-year for one (year t, partition k). `nxt` = all-modality student ids of t+1.
build_py <- function(mat_k, t, k, nxt) {
  mat <- mat_k[IN_REGULAR == 1]
  if (!nrow(mat)) return(NULL)

  sit_k <- read_partition(CLEAN_DIR, "BAS_SITUACAO", t, k)
  sit <- if (!is.null(sit_k))
    sit_k[, .(ID_MATRICULA, TP_SITUACAO, IN_CONCLUINTE, IN_TRANSFERIDO)]
  else data.table(ID_MATRICULA=character(), TP_SITUACAO=integer(),
                  IN_CONCLUINTE=integer(), IN_TRANSFERIDO=integer())

  base <- merge(mat, sit, by = "ID_MATRICULA", all.x = TRUE)
  base <- merge(base, read_turma(t), by = "ID_TURMA", all.x = TRUE)

  agg <- base[, .(
    QT_ESCOLAS_ANO = uniqueN(CO_ENTIDADE),
    IN_TRANSFERIDO = as.integer(any(IN_TRANSFERIDO == 1, na.rm = TRUE)),
    IN_CONCLUINTE_FINAL = as.integer(any(IN_CONCLUINTE == 1 &
                                         TP_ETAPA_ENSINO %in% FINAL_MEDIO_ETAPAS, na.rm = TRUE)),
    IN_FALECIDO = as.integer(any(TP_SITUACAO == 3, na.rm = TRUE)),
    N_MATRICULAS_REG = .N), by = CO_PESSOA_FISICA]
  agg[, IN_TROCA_ESCOLA := as.integer(QT_ESCOLAS_ANO > 1)]

  base[, .load := fifelse(is.na(NU_DIAS_ATIVIDADE), -1, NU_DIAS_ATIVIDADE)]
  setorder(base, CO_PESSOA_FISICA, -.load, ID_MATRICULA)
  main <- base[base[, .I[1], by = CO_PESSOA_FISICA]$V1]
  main[, .load := NULL]
  main[, c("IN_TRANSFERIDO","IN_CONCLUINTE") := NULL]

  py <- merge(main, agg, by = "CO_PESSOA_FISICA")
  py <- py[IN_FALECIDO == 0]

  if (t < max(YEARS)) {
    py[, evadiu := as.integer(!(CO_PESSOA_FISICA %in% nxt))]
    py[IN_CONCLUINTE_FINAL == 1, evadiu := NA_integer_]
  } else py[, evadiu := NA_integer_]
  py
}

# ─── MAIN: outer partition, inner year descending ────────────────────────────
t0 <- Sys.time()
message("02_ooc — K=", K_PARTS, "  years=", paste(range(YEARS), collapse="-"))
tot <- data.table(year = YEARS, n = 0L, evad = 0L, lab = 0L)
setkey(tot, year)

for (k in 0:(K_PARTS - 1L)) {
  prev_present <- NULL                       # present set of the year just processed (t+1)
  for (t in rev(YEARS)) {
    mat_k <- read_partition(CLEAN_DIR, "BAS_MATRICULA", t, k)
    if (is.null(mat_k)) { prev_present <- character(); next }
    py <- build_py(mat_k, t, k, prev_present)
    prev_present <- unique(mat_k$CO_PESSOA_FISICA)   # all modalities, for t-1
    rm(mat_k)
    if (!is.null(py)) {
      save_rds(py, part_path(OUTPUT_DIR, "person_year", t, k))
      tot[.(t), `:=`(n = n + nrow(py),
                     evad = evad + sum(py$evadiu, na.rm = TRUE),
                     lab  = lab  + sum(!is.na(py$evadiu)))]
      rm(py)
    }
    gc(FALSE)
  }
  message(sprintf("  partition %d done  peak=%.2fGB", k, peak_rss_gb()))
}

tot[, dropout_rate := round(100 * evad / pmax(lab, 1), 2)]
fwrite(tot, file.path(OUTPUT_DIR, "_label_report.csv"), sep = ";", bom = TRUE, na = "")
message("\n=== label report (union over partitions) ===")
print(tot)
message(sprintf("\nDone in %.1f s (peak %.2f GB). Output in %s",
                as.numeric(difftime(Sys.time(), t0, units = "secs")),
                peak_rss_gb(), normalizePath(OUTPUT_DIR, mustWork = FALSE)))
