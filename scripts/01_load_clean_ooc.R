###############################################################################
# 01_load_clean_ooc.R  (out-of-core version of 01_load_clean.R)
#
# Same cleaning as 01, but the big tables are streamed in line-blocks and written
# as STUDENT PARTITIONS (hash of CO_PESSOA_FISICA) instead of one big .rds, so a
# full year never lives in memory. The union of a year's partition files equals
# what the in-core 01 would have produced. Small context tables (ESCOLA, TURMA)
# stay whole — they are tiny and read whole downstream.
#
# Inputs:  <INPUT_DIR>/{TABLE}_{YEAR}.csv
# Outputs: <OUTPUT_DIR>/BAS_{ESCOLA,TURMA}_{YEAR}.rds                (whole)
#          <OUTPUT_DIR>/BAS_{MATRICULA,SITUACAO}_{YEAR}_p{K}_b{B}.rds (partitions)
#          <OUTPUT_DIR>/_quality_report.csv
###############################################################################

suppressPackageStartupMessages(library(data.table))
source(file.path("scripts", "ooc_common.R"))

# ─── CONFIG ──────────────────────────────────────────────────────────────────
INPUT_DIR  <- Sys.getenv("CLEAN_IN",  "data/synthetic")
OUTPUT_DIR <- Sys.getenv("CLEAN_OUT", "data/clean_ooc")
YEARS      <- eval(parse(text = Sys.getenv("CLEAN_YEARS", "2017:2024")))
AGE_MIN <- 0L; AGE_MAX <- 120L

COLS <- list(
  BAS_ESCOLA = c(
    "NU_ANO","CO_ENTIDADE","TP_SITUACAO_FUNCIONAMENTO","TP_FALTANTE","CO_UF",
    "CO_MUNICIPIO","TP_DEPENDENCIA","TP_LOCALIZACAO","TP_LOCALIZACAO_DIFERENCIADA",
    "TP_CATEGORIA_ESCOLA_PRIVADA","IN_LOCAL_FUNC_PREDIO_ESCOLAR",
    "IN_LOCAL_FUNC_SOCIOEDUCATIVO","IN_LOCAL_FUNC_UNID_PRISIONAL",
    "IN_LOCAL_FUNC_PRISIONAL_SOCIO","IN_LOCAL_FUNC_GALPAO",
    "IN_LOCAL_FUNC_SALAS_OUTRA_ESC","IN_LOCAL_FUNC_OUTROS","IN_PREDIO_COMPARTILHADO",
    "IN_AGUA_POTAVEL","IN_AGUA_REDE_PUBLICA","IN_AGUA_POCO_ARTESIANO","IN_AGUA_CACIMBA",
    "IN_AGUA_FONTE_RIO","IN_AGUA_INEXISTENTE","IN_ENERGIA_REDE_PUBLICA",
    "IN_ENERGIA_GERADOR_FOSSIL","IN_ENERGIA_RENOVAVEL","IN_ENERGIA_INEXISTENTE",
    "IN_ESGOTO_REDE_PUBLICA","IN_ESGOTO_FOSSA_SEPTICA","IN_ESGOTO_FOSSA_COMUM",
    "IN_ESGOTO_FOSSA","IN_ESGOTO_INEXISTENTE","IN_LIXO_SERVICO_COLETA","IN_LIXO_QUEIMA",
    "IN_LIXO_ENTERRA","IN_LIXO_DESTINO_FINAL_PUBLICO","IN_LIXO_DESCARTA_OUTRA_AREA",
    "IN_TRATAMENTO_LIXO_SEPARACAO","IN_TRATAMENTO_LIXO_REUTILIZA",
    "IN_TRATAMENTO_LIXO_RECICLAGEM","IN_TRATAMENTO_LIXO_INEXISTENTE","IN_BANHEIRO",
    "IN_BANHEIRO_PNE","IN_BIBLIOTECA_SALA_LEITURA","IN_LABORATORIO_CIENCIAS",
    "IN_LABORATORIO_INFORMATICA","IN_QUADRA_ESPORTES","QT_SALAS_UTILIZADAS",
    "IN_COMPUTADOR","IN_EQUIP_MULTIMIDIA","QT_EQUIP_MULTIMIDIA","IN_DESKTOP_ALUNO",
    "QT_DESKTOP_ALUNO","IN_COMP_PORTATIL_ALUNO","QT_COMP_PORTATIL_ALUNO",
    "IN_TABLET_ALUNO","QT_TABLET_ALUNO","IN_INTERNET","IN_INTERNET_ALUNOS",
    "IN_ACESSO_INTERNET_COMPUTADOR","IN_BANDA_LARGA","QT_PROF_COORDENADOR",
    "QT_PROF_FONAUDIOLOGO","QT_PROF_NUTRICIONISTA","QT_PROF_PSICOLOGO",
    "QT_PROF_PEDAGOGIA","IN_ALIMENTACAO","IN_ORGAO_ASS_PAIS","IN_ORGAO_ASS_PAIS_MESTRES",
    "IN_ORGAO_CONSELHO_ESCOLAR"),
  BAS_TURMA = c("NU_ANO","ID_TURMA","TP_MEDIACAO_DIDATICO_PEDAGO","NU_DIAS_ATIVIDADE",
    "TP_ETAPA_ENSINO","IN_REGULAR","IN_EJA","IN_PROFISSIONALIZANTE","QT_MATRICULAS",
    "TX_HR_INICIAL","CO_ENTIDADE"),
  BAS_MATRICULA = c("NU_ANO","CO_PESSOA_FISICA","ID_MATRICULA","NU_IDADE_REFERENCIA",
    "TP_SEXO","TP_COR_RACA","TP_NACIONALIDADE","CO_UF_NASC","CO_MUNICIPIO_NASC",
    "CO_PAIS_RESIDENCIA","CO_UF_END","CO_MUNICIPIO_END","TP_ZONA_RESIDENCIAL",
    "IN_NECESSIDADE_ESPECIAL","IN_TRANSPORTE_PUBLICO","IN_TRANSP_BICICLETA",
    "IN_TRANSP_MICRO_ONIBUS","IN_TRANSP_ONIBUS","IN_TRANSP_TR_ANIMAL",
    "IN_TRANSP_VANS_KOMBI","IN_TRANSP_OUTRO_VEICULO","IN_TRANSP_EMBAR_ATE5",
    "IN_TRANSP_EMBAR_5A15","IN_TRANSP_EMBAR_15A35","IN_TRANSP_EMBAR_35",
    "IN_ESPECIAL_EXCLUSIVA","IN_REGULAR","IN_EJA","IN_PROFISSIONALIZANTE","ID_TURMA",
    "CO_ENTIDADE"),
  BAS_SITUACAO = c("NU_ANO","ID_MATRICULA","CO_PESSOA_FISICA","IN_REGULAR","ID_TURMA",
    "CO_ENTIDADE","TP_SITUACAO","IN_CONCLUINTE","IN_TRANSFERIDO"))

KEYS <- list(
  BAS_ESCOLA = "CO_ENTIDADE", BAS_TURMA = c("ID_TURMA","CO_ENTIDADE"),
  BAS_MATRICULA = c("ID_MATRICULA","CO_PESSOA_FISICA","ID_TURMA","CO_ENTIDADE"),
  BAS_SITUACAO  = c("ID_MATRICULA","CO_PESSOA_FISICA","ID_TURMA","CO_ENTIDADE"))

SENTINELS <- list(BAS_ESCOLA=list(), BAS_TURMA=list(), BAS_MATRICULA=list(), BAS_SITUACAO=list())
PART_TABLES <- c("BAS_MATRICULA","BAS_SITUACAO")   # partitioned by student
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ─── CLEANING PRIMITIVES (identical to 01) ───────────────────────────────────
is_code <- function(col) grepl("^(CO_|ID_)", col)
coerce_col <- function(col, x) {
  if (col == "NU_ANO")        return(suppressWarnings(as.integer(x)))
  if (col == "TX_HR_INICIAL") return(suppressWarnings(as.integer(x)))
  if (is_code(col))    return(as.character(x))
  if (startsWith(col,"IN_") || startsWith(col,"TP_")) return(suppressWarnings(as.integer(x)))
  if (startsWith(col,"QT_") || startsWith(col,"NU_")) return(suppressWarnings(as.numeric(x)))
  as.character(x)
}
apply_sentinels <- function(dt, tbl, config) {
  rules <- config[[tbl]]; if (is.null(rules)) return(dt)
  for (col in names(rules)) {
    if (!col %in% names(dt)) next
    mask <- !is.na(dt[[col]]) & dt[[col]] %in% rules[[col]]
    if (sum(mask) > 0) dt[mask, (col) := NA]
  }
  dt
}

# Clean one already-read block/table (row-independent: same result per block).
clean_block <- function(dt, tbl, cols, missing_cols) {
  for (mc in missing_cols) dt[, (mc) := NA]
  dt <- dt[, ..cols]
  for (col in cols) set(dt, j = col, value = coerce_col(col, dt[[col]]))
  dt <- apply_sentinels(dt, tbl, SENTINELS)
  if ("NU_IDADE_REFERENCIA" %in% cols) {
    bad <- !is.na(dt$NU_IDADE_REFERENCIA) &
           (dt$NU_IDADE_REFERENCIA < AGE_MIN | dt$NU_IDADE_REFERENCIA > AGE_MAX)
    if (any(bad)) dt[bad, NU_IDADE_REFERENCIA := NA_real_]
  }
  dt
}

# Lightweight per-column missingness, accumulated across blocks.
miss_accum <- function(acc, tbl, yr, dt) {
  m <- data.table(table = tbl, year = yr, column = names(dt),
                  n = nrow(dt), n_missing = vapply(dt, function(x) sum(is.na(x)), numeric(1)))
  rbindlist(list(acc, m))
}

find_file <- function(tbl, yr) {
  all_csv <- list.files(INPUT_DIR, pattern = "\\.csv$", recursive = TRUE,
                        full.names = TRUE, ignore.case = TRUE)
  hits <- all_csv[grepl(sprintf("%s.*%d", tbl, yr), all_csv, ignore.case = TRUE)]
  if (length(hits)) hits[1] else NA_character_
}

# ─── SMALL TABLE (whole) ─────────────────────────────────────────────────────
clean_small <- function(tbl, yr, path) {
  cols <- COLS[[tbl]]
  present_all  <- names(fread(path, sep = ";", nrows = 0, showProgress = FALSE))
  present_cols <- intersect(cols, present_all)
  code_present <- present_cols[is_code(present_cols)]
  dt <- fread(path, sep = ";", encoding = "UTF-8", na.strings = c("","NA"),
              select = present_cols, showProgress = FALSE,
              colClasses = setNames(rep("character", length(code_present)), code_present))
  dt <- clean_block(dt, tbl, cols, setdiff(cols, present_all))
  dt <- unique(dt)
  save_rds(dt, file.path(OUTPUT_DIR, sprintf("%s_%d.rds", tbl, yr)))
  data.table(table = tbl, year = yr, n_out = nrow(dt))
}

# ─── BIG TABLE (streamed -> student partitions) ──────────────────────────────
clean_big <- function(tbl, yr, path) {
  cols <- COLS[[tbl]]
  present_all  <- names(fread(path, sep = ";", nrows = 0, showProgress = FALSE))
  present_cols <- intersect(cols, present_all)
  missing_cols <- setdiff(cols, present_all)
  code_sel <- present_cols[is_code(present_cols)]
  cc <- if (length(code_sel)) setNames(rep("character", length(code_sel)), code_sel) else NULL

  # remove any stale partition files for this table-year
  file.remove(list.files(OUTPUT_DIR, pattern = sprintf("^%s_%d_p", tbl, yr),
                         full.names = TRUE))

  con <- file(path, "r"); on.exit(close(con))
  header <- sub("^\xef\xbb\xbf", "", sub("^﻿", "", readLines(con, n = 1L, warn = FALSE)))
  n_in <- 0L; blk <- 0L
  repeat {
    lines <- readLines(con, n = CHUNK_LINES, warn = FALSE)
    if (!length(lines)) break
    dt <- fread(text = c(header, lines), sep = ";", na.strings = c("","NA"),
                select = present_cols, colClasses = cc, showProgress = FALSE)
    dt <- clean_block(dt, tbl, cols, missing_cols)
    n_in <- n_in + nrow(dt)
    part <- hash_part(dt$CO_PESSOA_FISICA)
    for (k in 0:(K_PARTS - 1L)) {
      slice <- dt[part == k]
      if (nrow(slice))
        save_rds(slice, part_path(OUTPUT_DIR, tbl, yr, k, blk))
    }
    blk <- blk + 1L
    rm(dt, part); gc(FALSE)
    if (length(lines) < CHUNK_LINES) break
  }
  data.table(table = tbl, year = yr, n_out = n_in)
}

# ─── MAIN ────────────────────────────────────────────────────────────────────
t0 <- Sys.time()
message("01_ooc — input: ", normalizePath(INPUT_DIR, mustWork = FALSE),
        "  K=", K_PARTS, "  chunk=", CHUNK_LINES)
reports <- list()
for (tbl in names(COLS)) {
  for (yr in YEARS) {
    if (tbl == "BAS_SITUACAO" && yr == max(YEARS)) next
    path <- find_file(tbl, yr)
    if (is.na(path)) { message(sprintf("  [%s %d] not found", tbl, yr)); next }
    message(sprintf("  [%s %d] %s", tbl, yr, basename(path)))
    rep <- if (tbl %in% PART_TABLES) clean_big(tbl, yr, path) else clean_small(tbl, yr, path)
    reports[[length(reports) + 1L]] <- rep
    message(sprintf("    -> %s rows  peak=%.2fGB", format(rep$n_out, big.mark=","), peak_rss_gb()))
  }
}
fwrite(rbindlist(reports, fill = TRUE), file.path(OUTPUT_DIR, "_quality_report.csv"),
       sep = ";", bom = TRUE, na = "")
message(sprintf("\nDone in %.1f s. Output in %s (peak %.2f GB)",
                as.numeric(difftime(Sys.time(), t0, units = "secs")),
                normalizePath(OUTPUT_DIR, mustWork = FALSE), peak_rss_gb()))
