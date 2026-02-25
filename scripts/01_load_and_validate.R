# =============================================================================
# 01_load_and_validate.R
# Loads data tables (CSV or SAS), validates expected columns and types,
# converts SAS labelled columns, and saves RDS checkpoints.
# =============================================================================

if (!exists("log_msg")) source("scripts/00_config.R")

library(data.table)

log_msg("==================================================")
log_msg("STEP 01: Load and Validate")
log_msg("==================================================")
log_msg(sprintf("Data directory: %s", DATA_DIR))
log_msg(sprintf("Data format:    %s", DATA_FORMAT))
log_msg(sprintf("Tables:         %s", paste(TABLES, collapse = ", ")))


# =============================================================================
# 1. LOAD FUNCTIONS
# =============================================================================

load_csv <- function(path) {
  fread(
    path,
    sep        = CSV_PARAMS$sep,
    encoding   = CSV_PARAMS$encoding,
    na.strings = CSV_PARAMS$na.strings,
    integer64  = "integer64"
  )
}

load_sas <- function(path) {
  if (!requireNamespace("haven", quietly = TRUE)) {
    stop("Package 'haven' is required to read SAS files but is not installed.")
  }
  dt <- as.data.table(haven::read_sas(path))

  # Convert haven_labelled columns to plain R types
  lab_cols <- names(dt)[sapply(dt, inherits, "haven_labelled")]
  if (length(lab_cols) > 0) {
    for (col in lab_cols) {
      vals <- unclass(dt[[col]])
      if (is.double(vals) && all(vals == floor(vals), na.rm = TRUE)) {
        set(dt, j = col, value = as.integer(vals))
      } else {
        set(dt, j = col, value = vals)
      }
    }
    log_msg(sprintf("  Converted %d labelled columns", length(lab_cols)))
  }
  dt
}

load_table <- function(table_name) {
  path <- get_table_path(table_name)

  if (!file.exists(path)) {
    stop(sprintf("File not found: %s", path))
  }

  log_msg(sprintf("Loading %s from %s ...", table_name, basename(path)))

  dt <- if (DATA_FORMAT == "csv") load_csv(path) else load_sas(path)

  log_msg(sprintf("  %s: %d rows x %d cols", table_name, nrow(dt), ncol(dt)))
  dt
}


# =============================================================================
# 2. LOAD ALL TABLES
# =============================================================================

DATA <- list()
for (tbl in TABLES) {
  DATA[[tbl]] <- load_table(tbl)
}


# =============================================================================
# 3. VALIDATE EXPECTED COLUMNS
# =============================================================================

log_msg("--- Column validation ---")
validation_ok <- TRUE

for (tbl in names(EXPECTED_COLS)) {
  expected <- EXPECTED_COLS[[tbl]]
  actual   <- names(DATA[[tbl]])
  missing  <- setdiff(expected, actual)

  if (length(missing) > 0) {
    log_msg(sprintf("  [FAIL] %s — missing columns: %s", tbl, paste(missing, collapse = ", ")), "ERROR")
    validation_ok <- FALSE
  } else {
    log_msg(sprintf("  [OK]   %s — all %d required columns present", tbl, length(expected)))
  }

  extra <- setdiff(actual, expected)
  if (length(extra) > 0) {
    log_msg(sprintf("         %s — %d extra columns available", tbl, length(extra)))
  }
}

if (!validation_ok) {
  stop("Column validation failed. Fix data or update EXPECTED_COLS in 00_config.R.")
}


# =============================================================================
# 4. VALIDATE YEAR COVERAGE
# =============================================================================

log_msg("--- Year coverage ---")

for (tbl in TABLES) {
  if (!"NU_ANO" %in% names(DATA[[tbl]])) {
    log_msg(sprintf("  [WARN] %s has no NU_ANO column", tbl), "WARN")
    next
  }

  years_found <- sort(unique(DATA[[tbl]]$NU_ANO))

  if (tbl == "BAS_SITUACAO") {
    expected_years <- SITUACAO_YEARS
  } else {
    expected_years <- YEARS
  }

  missing_years <- setdiff(expected_years, years_found)
  if (length(missing_years) > 0) {
    log_msg(sprintf("  [WARN] %s — missing years: %s", tbl, paste(missing_years, collapse = ", ")), "WARN")
  } else {
    log_msg(sprintf("  [OK]   %s — years: %s", tbl, paste(years_found, collapse = ", ")))
  }
}


# =============================================================================
# 5. VALIDATE KEY COLUMNS AND TYPES
# =============================================================================

log_msg("--- Key column checks ---")

# CO_PESSOA_FISICA must be present and non-empty in MATRICULA and SITUACAO
for (tbl in c("BAS_MATRICULA", "BAS_SITUACAO")) {
  cpf_col <- DATA[[tbl]]$CO_PESSOA_FISICA
  n_missing <- sum(is.na(cpf_col) | cpf_col == "")
  if (n_missing > 0) {
    log_msg(sprintf("  [WARN] %s — %d rows with missing CO_PESSOA_FISICA (%.2f%%)",
                    tbl, n_missing, 100 * n_missing / nrow(DATA[[tbl]])), "WARN")
  } else {
    log_msg(sprintf("  [OK]   %s — CO_PESSOA_FISICA complete", tbl))
  }
}

# CO_ENTIDADE must be non-missing in ESCOLA, TURMA, MATRICULA
for (tbl in c("BAS_ESCOLA", "BAS_TURMA", "BAS_MATRICULA")) {
  n_missing <- sum(is.na(DATA[[tbl]]$CO_ENTIDADE))
  if (n_missing > 0) {
    log_msg(sprintf("  [WARN] %s — %d rows with missing CO_ENTIDADE", tbl, n_missing), "WARN")
  }
}

# TP_SITUACAO valid values check
sit_vals <- unique(DATA[["BAS_SITUACAO"]]$TP_SITUACAO)
known_vals <- c(SIT_DROPOUT, SIT_DECEASED, SIT_FAILED, SIT_APPROVED, SIT_NO_INFO)
unknown <- setdiff(sit_vals, known_vals)
if (length(unknown) > 0) {
  log_msg(sprintf("  [WARN] BAS_SITUACAO — unexpected TP_SITUACAO values: %s",
                  paste(unknown, collapse = ", ")), "WARN")
} else {
  log_msg("  [OK]   BAS_SITUACAO — TP_SITUACAO values valid")
}


# =============================================================================
# 6. ROW COUNT SUMMARY PER YEAR
# =============================================================================

log_msg("--- Row counts per year ---")
for (tbl in TABLES) {
  if ("NU_ANO" %in% names(DATA[[tbl]])) {
    counts <- DATA[[tbl]][, .N, by = NU_ANO][order(NU_ANO)]
    for (i in seq_len(nrow(counts))) {
      log_msg(sprintf("  %s  %d: %s rows", tbl, counts$NU_ANO[i],
                      format(counts$N[i], big.mark = ",")))
    }
  }
}


# =============================================================================
# 7. SAVE CHECKPOINTS
# =============================================================================

log_msg("--- Saving checkpoints ---")
for (tbl in TABLES) {
  ckpt_path <- file.path(CHECKPOINT_DIR, paste0("01_", tbl, ".rds"))
  saveRDS(DATA[[tbl]], ckpt_path)
  log_msg(sprintf("  Saved %s -> %s", tbl, basename(ckpt_path)))
}

log_msg("Step 01 complete.")