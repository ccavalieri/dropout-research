###############################################################################
# 00b_profile_real_data.R
#
# Purpose: Profile real SEDAP data to produce a structural fingerprint that
#          drives faithful synthetic-data generation outside the room. The
#          goal is structural (not statistical) realism: schemas, cardinalities,
#          missingness, categorical code lists, and numeric shape — enough so
#          that every downstream pipeline script that runs green on synthetic
#          also runs green on real.
#
# Inputs:  CSV files for BAS_ESCOLA, BAS_TURMA, BAS_MATRICULA, BAS_SITUACAO
#          partitioned by year (2019-2024, no BAS_SITUACAO for 2024).
#
# Outputs (in outputs/profile/, all ';'-separated UTF-8, long format):
#    schema.csv             table;year;column;dtype;position
#    row_counts.csv         table;year;n_rows
#    missingness.csv        table;year;column;n_missing;pct_missing
#    cardinality.csv        table;year;column;n_unique
#    cat_freqs.csv          table;year;column;value;n;pct    (n<3 -> "X")
#    numeric_summary.csv    table;year;column;mean;p10;p50;p90
#    join_overlap.csv       left_table;right_table;year;pct_match
#    crosstab_situacao.csv  year;tp_etapa_ensino;tp_situacao;n;pct
#    Leia-me.md / Leia-me.pdf
#
# SEDAP compliance:
#    - Cell suppression: any cell with n<3 -> "X"
#    - No min/max, no individual-level data, no school rankings
#    - Semicolon-separated CSV, UTF-8
#    - The PDF Leia-me is rendered at the end of this script.
###############################################################################

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
DATA_DIR       <- "."
OUT_DIR        <- "outputs/profile"
YEARS          <- 2019:2024
TABLES         <- c("BAS_ESCOLA", "BAS_TURMA", "BAS_MATRICULA", "BAS_SITUACAO")
N_MIN          <- 3                # SEDAP cell-suppression threshold
MAX_CAT_LEVELS <- 200              # stop treating as categorical above this
BIG_TABLES     <- "BAS_MATRICULA"  # tables read in column batches
COL_BATCH      <- 40               # columns per read for big tables
QUANTILES      <- c(0.10, 0.50, 0.90)

# Columns never profiled for frequencies (PII / join-key noise)
ID_COLS        <- c("ID_MATRICULA", "ID_TURMA", "CO_PESSOA_FISICA")

# Crosstab configuration
CROSSTAB_STRATA <- "TP_ETAPA_ENSINO"  # stratify TP_SITUACAO by this variable

# ─── PACKAGES ────────────────────────────────────────────────────────────────
stopifnot(requireNamespace("data.table", quietly = TRUE))
library(data.table)

HAS_RMD    <- requireNamespace("rmarkdown", quietly = TRUE)
HAS_TINYTEX <- requireNamespace("tinytex", quietly = TRUE)

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ─── HELPERS ─────────────────────────────────────────────────────────────────

safe <- function(expr, label = "") {
  tryCatch(expr, error = function(e) {
    message(sprintf("[ERROR %s] %s", label, conditionMessage(e)))
    NULL
  })
}

# Find CSVs matching TABLE_YEAR naming (case-insensitive, recursive)
find_files <- function(data_dir) {
  all_csv <- list.files(data_dir, pattern = "\\.csv$", recursive = TRUE,
                        full.names = TRUE, ignore.case = TRUE)
  matched <- list()
  for (tbl in TABLES) for (yr in YEARS) {
    if (tbl == "BAS_SITUACAO" && yr == 2024) next
    pat <- paste0(tbl, ".*", yr, "|", tolower(tbl), ".*", yr)
    hits <- all_csv[grepl(pat, basename(all_csv), ignore.case = TRUE)]
    matched[[paste0(tbl, "_", yr)]] <- if (length(hits)) hits[1] else NA_character_
  }
  matched
}

# Cheap row count via wc -l (fast on 10 GB files); falls back to fread
count_rows <- function(path) {
  n <- safe({
    res <- system2("wc", c("-l", shQuote(path)), stdout = TRUE, stderr = FALSE)
    as.integer(sub("\\D.*$", "", trimws(res[1])))
  }, label = paste0("wc ", path))
  if (!is.null(n) && !is.na(n)) return(n - 1L)
  safe({
    nrow(fread(path, select = 1L, sep = ";", showProgress = FALSE))
  }, label = paste0("fread-count ", path))
}

# Read header-only to get schema without loading data
read_schema <- function(path) {
  hdr <- safe(fread(path, nrows = 0, sep = ";", encoding = "UTF-8",
                    showProgress = FALSE),
              label = paste0("schema ", path))
  if (is.null(hdr) || ncol(hdr) == 0) return(NULL)
  data.table(column = names(hdr),
             dtype  = vapply(hdr, function(x) class(x)[1], character(1)),
             position = seq_len(ncol(hdr)))
}

# Load a file or a subset of columns into a data.table
load_cols <- function(path, cols = NULL) {
  args <- list(input = path, sep = ";", encoding = "UTF-8",
               showProgress = FALSE)
  if (!is.null(cols)) args$select <- cols
  safe(do.call(fread, args), label = paste0("load ", basename(path)))
}

# Treat empty strings as NA for missingness counting
is_missing <- function(x) {
  if (is.character(x)) is.na(x) | x == "" | x == "NA"
  else is.na(x)
}

# Decide profiling strategy for a column based on name + values
#   "skip"     = PII / ID — only count missing + cardinality
#   "highcard" = high-cardinality code (CO_) — only count missing + cardinality
#   "cat"      = categorical — frequency table
#   "num"      = numeric — mean + quantiles
classify_col <- function(colname, x) {
  if (colname %in% ID_COLS)          return("skip")
  if (startsWith(colname, "CO_"))    return("highcard")
  if (colname == "NU_ANO")           return("skip")
  nu <- length(unique(x[!is_missing(x)]))
  if (nu <= MAX_CAT_LEVELS &&
      (is.character(x) || is.integer(x) || is.logical(x) ||
       startsWith(colname, "IN_") || startsWith(colname, "TP_"))) {
    return("cat")
  }
  if (is.numeric(x)) return("num")
  if (nu <= MAX_CAT_LEVELS) return("cat")
  "highcard"
}

# Profile one in-memory column; returns named list of long-format rows
profile_column <- function(tbl, yr, colname, x) {
  n_total <- length(x)
  miss    <- sum(is_missing(x))
  nu      <- length(unique(x[!is_missing(x)]))

  out <- list(
    missing     = data.table(table = tbl, year = yr, column = colname,
                             n_missing = miss,
                             pct_missing = round(100 * miss / max(n_total, 1), 3)),
    cardinality = data.table(table = tbl, year = yr, column = colname,
                             n_unique = nu),
    cat_freqs   = NULL,
    numeric     = NULL
  )

  strat <- classify_col(colname, x)
  if (strat == "cat") {
    vals <- x[!is_missing(x)]
    tb <- table(vals)
    tot <- sum(tb)
    out$cat_freqs <- data.table(
      table  = tbl, year = yr, column = colname,
      value  = as.character(names(tb)),
      n      = as.integer(tb),
      pct    = round(100 * as.integer(tb) / max(tot, 1), 4)
    )
  } else if (strat == "num") {
    vals <- as.numeric(x[!is_missing(x)])
    if (length(vals) > 0) {
      qs <- quantile(vals, probs = QUANTILES, names = FALSE, na.rm = TRUE)
      out$numeric <- data.table(
        table = tbl, year = yr, column = colname,
        mean  = round(mean(vals, na.rm = TRUE), 6),
        p10   = round(qs[1], 6),
        p50   = round(qs[2], 6),
        p90   = round(qs[3], 6)
      )
    }
  }
  out
}

# Accumulate per-column profile rows into shared result lists
merge_results <- function(acc, col_result) {
  for (k in names(col_result)) {
    if (!is.null(col_result[[k]])) acc[[k]] <- c(acc[[k]], list(col_result[[k]]))
  }
  acc
}

# Profile one file. For big tables, read in column batches.
profile_file <- function(path, tbl, yr, acc) {
  schema <- read_schema(path)
  if (is.null(schema)) {
    message(sprintf("  [%s %d] schema read failed — skipping", tbl, yr))
    return(acc)
  }
  schema[, `:=`(table = tbl, year = yr)]
  acc$schema <- c(acc$schema, list(schema[, .(table, year, column, dtype, position)]))

  all_cols <- schema$column
  if (tbl %in% BIG_TABLES) {
    batches <- split(all_cols, ceiling(seq_along(all_cols) / COL_BATCH))
  } else {
    batches <- list(all_cols)
  }

  for (b in seq_along(batches)) {
    cols <- batches[[b]]
    message(sprintf("  [%s %d] batch %d/%d  (%d cols)",
                    tbl, yr, b, length(batches), length(cols)))
    dt <- load_cols(path, cols = cols)
    if (is.null(dt)) next
    for (cn in cols) {
      if (!cn %in% names(dt)) next
      res <- profile_column(tbl, yr, cn, dt[[cn]])
      acc <- merge_results(acc, res)
    }
    rm(dt); gc(verbose = FALSE)
  }
  acc
}

# Apply cell suppression: n<N_MIN -> "X" in frequency tables
suppress_cells <- function(dt, n_col = "n", pct_col = "pct") {
  if (is.null(dt) || nrow(dt) == 0) return(dt)
  dt <- copy(dt)
  mask <- dt[[n_col]] < N_MIN
  # Cast to character so "X" is valid
  if (any(mask)) {
    dt[, (n_col)   := as.character(get(n_col))]
    dt[, (pct_col) := as.character(get(pct_col))]
    dt[mask, (n_col)   := "X"]
    dt[mask, (pct_col) := "X"]
  }
  dt
}

# ─── MAIN ────────────────────────────────────────────────────────────────────

t0 <- Sys.time()
message(sprintf("Profiling started %s", format(t0, "%Y-%m-%d %H:%M:%S")))
message(sprintf("Data dir: %s", normalizePath(DATA_DIR, mustWork = FALSE)))
message(sprintf("Output:   %s", normalizePath(OUT_DIR, mustWork = FALSE)))

matched <- find_files(DATA_DIR)
n_missing_files <- sum(is.na(unlist(matched)))
message(sprintf("Files matched: %d / %d (missing: %d)",
                length(matched) - n_missing_files, length(matched),
                n_missing_files))

acc <- list(schema = list(), missing = list(), cardinality = list(),
            cat_freqs = list(), numeric = list())
row_counts <- list()

for (tbl in TABLES) {
  for (yr in YEARS) {
    if (tbl == "BAS_SITUACAO" && yr == 2024) next
    key  <- paste0(tbl, "_", yr)
    path <- matched[[key]]
    if (is.na(path)) {
      message(sprintf("  [%s] file not found — skipping", key))
      next
    }
    message(sprintf("Profiling %s  (%s)", key, basename(path)))
    nr <- count_rows(path)
    row_counts <- c(row_counts, list(data.table(table = tbl, year = yr,
                                                 n_rows = nr)))
    acc <- profile_file(path, tbl, yr, acc)
  }
}

message("Computing join overlaps...")
join_rows <- list()
for (yr in YEARS) {
  if (yr == 2024) next
  mat_path <- matched[[paste0("BAS_MATRICULA_", yr)]]
  sit_path <- matched[[paste0("BAS_SITUACAO_", yr)]]
  turma_path <- matched[[paste0("BAS_TURMA_", yr)]]
  esc_path   <- matched[[paste0("BAS_ESCOLA_", yr)]]

  if (!is.na(mat_path) && !is.na(sit_path)) {
    sit_ids <- load_cols(sit_path, "ID_MATRICULA")
    mat_ids <- load_cols(mat_path, "ID_MATRICULA")
    if (!is.null(sit_ids) && !is.null(mat_ids)) {
      s <- unique(sit_ids$ID_MATRICULA); s <- s[!is.na(s)]
      m <- unique(mat_ids$ID_MATRICULA); m <- m[!is.na(m)]
      join_rows <- c(join_rows, list(data.table(
        left_table = "BAS_SITUACAO", right_table = "BAS_MATRICULA",
        year = yr, pct_match = round(100 * mean(s %in% m), 3))))
      rm(sit_ids, mat_ids, s, m); gc(verbose = FALSE)
    }
  }
  if (!is.na(mat_path) && !is.na(turma_path)) {
    a <- load_cols(mat_path, "ID_TURMA")
    b <- load_cols(turma_path, "ID_TURMA")
    if (!is.null(a) && !is.null(b)) {
      ua <- unique(a$ID_TURMA); ua <- ua[!is.na(ua)]
      ub <- unique(b$ID_TURMA); ub <- ub[!is.na(ub)]
      join_rows <- c(join_rows, list(data.table(
        left_table = "BAS_MATRICULA", right_table = "BAS_TURMA",
        year = yr, pct_match = round(100 * mean(ua %in% ub), 3))))
      rm(a, b, ua, ub); gc(verbose = FALSE)
    }
  }
  if (!is.na(mat_path) && !is.na(esc_path)) {
    a <- load_cols(mat_path, "CO_ENTIDADE")
    b <- load_cols(esc_path, "CO_ENTIDADE")
    if (!is.null(a) && !is.null(b)) {
      ua <- unique(a$CO_ENTIDADE); ua <- ua[!is.na(ua)]
      ub <- unique(b$CO_ENTIDADE); ub <- ub[!is.na(ub)]
      join_rows <- c(join_rows, list(data.table(
        left_table = "BAS_MATRICULA", right_table = "BAS_ESCOLA",
        year = yr, pct_match = round(100 * mean(ua %in% ub), 3))))
      rm(a, b, ua, ub); gc(verbose = FALSE)
    }
  }
}

message("Computing TP_SITUACAO x TP_ETAPA_ENSINO crosstab...")
ct_rows <- list()
for (yr in YEARS) {
  if (yr == 2024) next
  sit_path <- matched[[paste0("BAS_SITUACAO_", yr)]]
  mat_path <- matched[[paste0("BAS_MATRICULA_", yr)]]
  if (is.na(sit_path) || is.na(mat_path)) next

  sit <- load_cols(sit_path, c("ID_MATRICULA", "TP_SITUACAO"))
  mat <- load_cols(mat_path, c("ID_MATRICULA", CROSSTAB_STRATA))
  if (is.null(sit) || is.null(mat)) next

  setkey(sit, ID_MATRICULA); setkey(mat, ID_MATRICULA)
  m <- merge(sit, mat, by = "ID_MATRICULA")
  if (nrow(m) == 0) { rm(sit, mat, m); gc(verbose = FALSE); next }

  setnames(m, CROSSTAB_STRATA, "tp_etapa_ensino")
  agg <- m[, .(n = .N), by = .(tp_etapa_ensino, TP_SITUACAO)]
  agg[, pct := round(100 * n / sum(n), 4), by = tp_etapa_ensino]
  agg[, year := yr]
  setnames(agg, "TP_SITUACAO", "tp_situacao")
  setcolorder(agg, c("year", "tp_etapa_ensino", "tp_situacao", "n", "pct"))
  ct_rows <- c(ct_rows, list(agg))

  rm(sit, mat, m, agg); gc(verbose = FALSE)
}

# ─── ASSEMBLE AND WRITE ──────────────────────────────────────────────────────
write_csv <- function(dt, name) {
  path <- file.path(OUT_DIR, name)
  fwrite(dt, path, sep = ";", bom = TRUE, na = "")
  message(sprintf("  wrote %-25s %8d rows", name, nrow(dt)))
}

bind <- function(lst) if (length(lst)) rbindlist(lst, fill = TRUE) else data.table()

schema_dt      <- bind(acc$schema)
missing_dt     <- bind(acc$missing)
cardinality_dt <- bind(acc$cardinality)
cat_freqs_dt   <- suppress_cells(bind(acc$cat_freqs), "n", "pct")
numeric_dt     <- bind(acc$numeric)
row_counts_dt  <- bind(row_counts)
join_dt        <- bind(join_rows)
crosstab_dt    <- suppress_cells(bind(ct_rows), "n", "pct")

write_csv(schema_dt,      "schema.csv")
write_csv(row_counts_dt,  "row_counts.csv")
write_csv(missing_dt,     "missingness.csv")
write_csv(cardinality_dt, "cardinality.csv")
write_csv(cat_freqs_dt,   "cat_freqs.csv")
write_csv(numeric_dt,     "numeric_summary.csv")
write_csv(join_dt,        "join_overlap.csv")
write_csv(crosstab_dt,    "crosstab_situacao.csv")

# ─── LEIA-ME ─────────────────────────────────────────────────────────────────
message("Writing Leia-me...")
elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "mins")), 1)

md <- c(
  "---",
  "title: \"Leia-me — Perfil estrutural dos microdados\"",
  paste0("date: \"", format(Sys.time(), "%Y-%m-%d"), "\""),
  "output: pdf_document",
  "---",
  "",
  "## Objetivo da extração",
  "",
  "Conjunto de tabelas de **perfil estrutural** dos microdados do Censo",
  "da Educação Básica (BAS_ESCOLA, BAS_TURMA, BAS_MATRICULA, BAS_SITUACAO,",
  paste0("anos ", min(YEARS), "–", max(YEARS), "). O objetivo é documentar"),
  "o esquema, a cardinalidade, a ausência de valores e a distribuição",
  "marginal de variáveis categóricas/numéricas — permitindo gerar, fora",
  "da SEDAP, um dataset sintético com a mesma estrutura e, assim, testar",
  "o pipeline completo antes de rodá-lo com dados reais.",
  "",
  "## Bases utilizadas",
  "",
  "- BAS_ESCOLA (anos 2019–2024)",
  "- BAS_TURMA (anos 2019–2024)",
  "- BAS_MATRICULA (anos 2019–2024)",
  "- BAS_SITUACAO (anos 2019–2023; não há arquivo para 2024)",
  "",
  "## Cruzamentos realizados",
  "",
  "- BAS_SITUACAO × BAS_MATRICULA por `ID_MATRICULA` (cobertura de join)",
  "- BAS_MATRICULA × BAS_TURMA por `ID_TURMA` (cobertura de join)",
  "- BAS_MATRICULA × BAS_ESCOLA por `CO_ENTIDADE` (cobertura de join)",
  "- Cruzamento `TP_SITUACAO × TP_ETAPA_ENSINO` por ano",
  "",
  paste0("## Supressão de células (n < ", N_MIN, ")"),
  "",
  paste0("Qualquer célula com menos de ", N_MIN, " observações é substituída por"),
  "`\"X\"` em `cat_freqs.csv` e `crosstab_situacao.csv`, conforme regras SEDAP.",
  "Valores mínimos e máximos não são reportados em nenhuma tabela.",
  "",
  "## Arquivos incluídos",
  "",
  "| Arquivo | Conteúdo |",
  "|---|---|",
  "| `schema.csv` | Colunas e tipos por tabela/ano |",
  "| `row_counts.csv` | Número de linhas por tabela/ano |",
  "| `missingness.csv` | Contagem e % de ausentes por coluna |",
  "| `cardinality.csv` | Número de valores únicos por coluna |",
  "| `cat_freqs.csv` | Frequências de valores para colunas categóricas |",
  "| `numeric_summary.csv` | Média e quantis (p10, p50, p90) para colunas numéricas |",
  "| `join_overlap.csv` | % de match entre chaves de join entre tabelas |",
  "| `crosstab_situacao.csv` | Distribuição de `TP_SITUACAO` por `TP_ETAPA_ENSINO` e ano |",
  "",
  "Todos os arquivos estão em **CSV separado por `;`, codificação UTF-8**,",
  "em **formato longo** (uma linha por fato).",
  "",
  "## Observações metodológicas",
  "",
  paste0("- Colunas classificadas como categóricas quando têm ≤ ", MAX_CAT_LEVELS,
         " valores únicos"),
  "  ou quando o nome começa com `IN_` / `TP_`.",
  "- Colunas `CO_*` (exceto `CO_PESSOA_FISICA`) são tratadas como códigos de alta",
  "  cardinalidade: reportamos apenas cardinalidade e ausência, sem listagem",
  "  de valores.",
  "- `ID_MATRICULA`, `ID_TURMA` e `CO_PESSOA_FISICA` não têm valores individuais",
  "  reportados — apenas cardinalidade e ausência.",
  "- Nenhuma tabela contém dados em nível individual, valores mínimos ou máximos,",
  "  ou rankings de escolas.",
  "",
  paste0("Tempo de geração: ", elapsed, " minutos."),
  paste0("Gerado em: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
)

md_path  <- file.path(OUT_DIR, "Leia-me.md")
pdf_path <- file.path(OUT_DIR, "Leia-me.pdf")
writeLines(md, md_path, useBytes = TRUE)
message(sprintf("  wrote %s", md_path))

if (HAS_RMD && HAS_TINYTEX && tinytex::is_tinytex()) {
  safe(rmarkdown::render(md_path, output_file = basename(pdf_path),
                         output_dir = OUT_DIR, quiet = TRUE),
       label = "pdf-render")
  if (file.exists(pdf_path)) {
    message(sprintf("  wrote %s", pdf_path))
  }
} else {
  message("  rmarkdown/tinytex not available — Leia-me.pdf NOT rendered.")
  message("  Install once inside SEDAP with:  tinytex::install_tinytex()")
  message("  Then re-run this script (or render Leia-me.md manually).")
}

message(sprintf("Done in %.1f minutes.", elapsed))
