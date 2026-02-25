# =============================================================================
# 04_data_quality.R
# Analyzes missing data patterns, detects logical inconsistencies,
# applies imputation, and produces a data quality report.
#
# Inputs:  outputs/checkpoints/03_features.rds
# Outputs: outputs/checkpoints/04_clean.rds
#          outputs/Resultados/data_quality_report.csv
# =============================================================================

if (!exists("log_msg")) source("scripts/00_config.R")

library(data.table)

log_msg("==================================================")
log_msg("STEP 04: Data Quality")
log_msg("==================================================")


# =============================================================================
# 1. LOAD DATA
# =============================================================================

ckpt_path <- file.path(CHECKPOINT_DIR, "03_features.rds")
if (!file.exists(ckpt_path)) stop("Checkpoint not found: 03_features.rds")
dt <- readRDS(ckpt_path)
log_msg(sprintf("Loaded: %s rows x %d cols",
                format(nrow(dt), big.mark = ","), ncol(dt)))


# =============================================================================
# 2. MISSING DATA ANALYSIS
# =============================================================================

log_msg("--- Missing data analysis ---")

# 2a. Overall missingness per column
miss_summary <- data.table(
  column    = names(dt),
  n_total   = nrow(dt),
  n_missing = sapply(dt, function(x) sum(is.na(x))),
  pct_missing = sapply(dt, function(x) round(100 * mean(is.na(x)), 2))
)
miss_summary <- miss_summary[order(-pct_missing)]

n_cols_with_na <- sum(miss_summary$n_missing > 0)
log_msg(sprintf("  Columns with any missing: %d / %d", n_cols_with_na, ncol(dt)))

# Log top missing columns
top_miss <- miss_summary[pct_missing > 0][head(order(-pct_missing), 15)]
for (i in seq_len(nrow(top_miss))) {
  log_msg(sprintf("    %-35s %6.2f%% missing (%s rows)",
                  top_miss$column[i], top_miss$pct_missing[i],
                  format(top_miss$n_missing[i], big.mark = ",")))
}

# 2b. Missingness by year (detect year-specific patterns)
log_msg("  Missingness by year for key columns:")
key_cols <- c("TP_COR_RACA", "TP_ZONA_RESIDENCIAL", "TP_SITUACAO",
              "IN_TRANSPORTE_PUBLICO", "school_dropout_rate_prev")
key_cols <- intersect(key_cols, names(dt))

miss_by_year <- dt[, lapply(.SD, function(x) round(100 * mean(is.na(x)), 1)),
                   .SDcols = key_cols, by = NU_ANO]
for (yr in sort(unique(miss_by_year$NU_ANO))) {
  row <- miss_by_year[NU_ANO == yr]
  vals <- paste(sprintf("%s=%.1f%%", key_cols, unlist(row[, -1, with = FALSE])),
                collapse = ", ")
  log_msg(sprintf("    %d: %s", yr, vals))
}

# 2c. MAR test: is missingness in TP_COR_RACA associated with dropout?
if ("TP_COR_RACA" %in% names(dt) & "dropout" %in% names(dt)) {
  dt[, raca_missing := as.integer(is.na(TP_COR_RACA))]
  mar_table <- dt[!is.na(dropout), .(dropout_rate = mean(dropout), n = .N),
                  by = raca_missing]
  log_msg("  MAR check — dropout rate by TP_COR_RACA missingness:")
  for (i in seq_len(nrow(mar_table))) {
    log_msg(sprintf("    raca_missing=%d: dropout=%.2f%% (n=%s)",
                    mar_table$raca_missing[i],
                    100 * mar_table$dropout_rate[i],
                    format(mar_table$n[i], big.mark = ",")))
  }
  dt[, raca_missing := NULL]
}


# =============================================================================
# 3. LOGICAL INCONSISTENCY CHECKS
# =============================================================================

log_msg("--- Inconsistency detection ---")
issues <- list()

# 3a. Age out of range for etapa
log_msg("  Checking age-etapa consistency...")
dt[, expected_age := get_expected_age(TP_ETAPA_ENSINO)]

# Extreme age: students younger than expected_age - 2 or older than expected_age + 10
dt[, age_too_young := as.integer(!is.na(expected_age) &
                                  NU_IDADE_REFERENCIA < (expected_age - 2))]
dt[, age_too_old := as.integer(!is.na(expected_age) &
                                NU_IDADE_REFERENCIA > (expected_age + 10))]

n_young <- sum(dt$age_too_young, na.rm = TRUE)
n_old   <- sum(dt$age_too_old, na.rm = TRUE)
log_msg(sprintf("    Too young for etapa (age < expected-2): %s (%.2f%%)",
                format(n_young, big.mark = ","),
                100 * n_young / nrow(dt)))
log_msg(sprintf("    Too old for etapa (age > expected+10):  %s (%.2f%%)",
                format(n_old, big.mark = ","),
                100 * n_old / nrow(dt)))
issues[["age_too_young"]] <- n_young
issues[["age_too_old"]]   <- n_old

# 3b. Disability sub-flags without main flag
disability_subs <- intersect(
  c("IN_BAIXA_VISAO", "IN_CEGUEIRA", "IN_DEF_AUDITIVA", "IN_DEF_FISICA",
    "IN_DEF_INTELECTUAL", "IN_SURDEZ", "IN_SURDOCEGUEIRA", "IN_DEF_MULTIPLA",
    "IN_AUTISMO", "IN_SUPERDOTACAO"),
  names(dt)
)
if (length(disability_subs) > 0 & "IN_NECESSIDADE_ESPECIAL" %in% names(dt)) {
  log_msg("  Checking disability flag consistency...")
  dt[, any_sub_flag := as.integer(rowSums(.SD, na.rm = TRUE) > 0),
     .SDcols = disability_subs]
  n_orphan_disability <- sum(dt$any_sub_flag == 1L & dt$IN_NECESSIDADE_ESPECIAL == 0L,
                             na.rm = TRUE)
  log_msg(sprintf("    Sub-flag without IN_NECESSIDADE_ESPECIAL=1: %s",
                  format(n_orphan_disability, big.mark = ",")))
  issues[["orphan_disability"]] <- n_orphan_disability
  dt[, any_sub_flag := NULL]
}

# 3c. Transport details without IN_TRANSPORTE_PUBLICO
if (all(c("TP_RESPONSAVEL_TRANSPORTE", "IN_TRANSPORTE_PUBLICO") %in% names(dt))) {
  log_msg("  Checking transport flag consistency...")
  n_orphan_transp <- sum(!is.na(dt$TP_RESPONSAVEL_TRANSPORTE) &
                          dt$TP_RESPONSAVEL_TRANSPORTE > 0L &
                          dt$IN_TRANSPORTE_PUBLICO == 0L, na.rm = TRUE)
  log_msg(sprintf("    Transport detail without IN_TRANSPORTE_PUBLICO=1: %s",
                  format(n_orphan_transp, big.mark = ",")))
  issues[["orphan_transport"]] <- n_orphan_transp
}

# 3d. EJA flag vs etapa code
if (all(c("IN_EJA", "is_eja") %in% names(dt))) {
  log_msg("  Checking EJA flag consistency...")
  n_eja_mismatch <- sum(dt$IN_EJA != dt$is_eja, na.rm = TRUE)
  log_msg(sprintf("    IN_EJA vs etapa-derived is_eja mismatch: %s",
                  format(n_eja_mismatch, big.mark = ",")))
  issues[["eja_mismatch"]] <- n_eja_mismatch
}

# 3e. Duplicate enrollments (same student, same year, same school)
log_msg("  Checking duplicate enrollments...")
dup_check <- dt[, .N, by = .(CO_PESSOA_FISICA, NU_ANO, CO_ENTIDADE)]
n_dup <- sum(dup_check$N > 1)
log_msg(sprintf("    Student-year-school duplicates: %s",
                format(n_dup, big.mark = ",")))
issues[["duplicate_enrollments"]] <- n_dup

# Clean temp columns
for (col in c("expected_age", "age_too_young", "age_too_old")) {
  if (col %in% names(dt)) dt[, (col) := NULL]
}


# =============================================================================
# 4. IMPUTATION
# =============================================================================

log_msg("--- Imputation ---")

# 4a. TP_COR_RACA: recode NA to 0 (Não declarada) — matches INEP convention
if ("TP_COR_RACA" %in% names(dt)) {
  n_before <- sum(is.na(dt$TP_COR_RACA))
  dt[is.na(TP_COR_RACA), TP_COR_RACA := 0L]
  log_msg(sprintf("  TP_COR_RACA: %s NA -> 0 (Não declarada)", format(n_before, big.mark = ",")))
  # Update derived flag
  dt[, is_preta_parda := as.integer(TP_COR_RACA %in% c(2L, 3L))]
}

# 4b. TP_ZONA_RESIDENCIAL: impute from school location
if (all(c("TP_ZONA_RESIDENCIAL", "TP_LOCALIZACAO") %in% names(dt))) {
  n_before <- sum(is.na(dt$TP_ZONA_RESIDENCIAL))
  dt[is.na(TP_ZONA_RESIDENCIAL), TP_ZONA_RESIDENCIAL := TP_LOCALIZACAO]
  n_after <- sum(is.na(dt$TP_ZONA_RESIDENCIAL))
  log_msg(sprintf("  TP_ZONA_RESIDENCIAL: %s NA imputed from school TP_LOCALIZACAO (%s remain)",
                  format(n_before - n_after, big.mark = ","),
                  format(n_after, big.mark = ",")))
  dt[, is_rural := as.integer(TP_ZONA_RESIDENCIAL == 2L)]
}

# 4c. school_dropout_rate_prev: fill NA with global mean (first year has no prior)
if ("school_dropout_rate_prev" %in% names(dt)) {
  n_before <- sum(is.na(dt$school_dropout_rate_prev))
  global_rate <- mean(dt$dropout, na.rm = TRUE)
  dt[is.na(school_dropout_rate_prev), school_dropout_rate_prev := global_rate]
  log_msg(sprintf("  school_dropout_rate_prev: %s NA -> global mean (%.3f)",
                  format(n_before, big.mark = ","), global_rate))
}

# 4d. Numeric features: fill remaining NA with 0 (safe for binary indicators)
indicator_cols <- grep("^IN_|^is_|^was_", names(dt), value = TRUE)
for (col in indicator_cols) {
  n_na <- sum(is.na(dt[[col]]))
  if (n_na > 0) {
    dt[is.na(get(col)), (col) := 0L]
    log_msg(sprintf("  %s: %s NA -> 0", col, format(n_na, big.mark = ",")))
  }
}


# =============================================================================
# 5. POST-IMPUTATION MISSING SUMMARY
# =============================================================================

log_msg("--- Post-imputation summary ---")
miss_after <- data.table(
  column    = names(dt),
  n_missing = sapply(dt, function(x) sum(is.na(x))),
  pct_missing = sapply(dt, function(x) round(100 * mean(is.na(x)), 2))
)
still_missing <- miss_after[n_missing > 0][order(-pct_missing)]
if (nrow(still_missing) > 0) {
  log_msg(sprintf("  %d columns still have missing values:", nrow(still_missing)))
  for (i in seq_len(min(10, nrow(still_missing)))) {
    log_msg(sprintf("    %-35s %6.2f%%", still_missing$column[i],
                    still_missing$pct_missing[i]))
  }
} else {
  log_msg("  No missing values remain in any column")
}


# =============================================================================
# 6. DATA QUALITY REPORT
# =============================================================================

log_msg("--- Generating quality report ---")

# Combine into a report table
report_rows <- list()

# Missingness section
for (i in seq_len(nrow(miss_summary))) {
  if (miss_summary$pct_missing[i] > 0) {
    report_rows[[length(report_rows) + 1]] <- data.table(
      section = "missingness",
      item    = miss_summary$column[i],
      value   = miss_summary$pct_missing[i],
      detail  = sprintf("%s / %s rows",
                        format(miss_summary$n_missing[i], big.mark = ","),
                        format(miss_summary$n_total[i], big.mark = ","))
    )
  }
}

# Inconsistency section
for (nm in names(issues)) {
  report_rows[[length(report_rows) + 1]] <- data.table(
    section = "inconsistency",
    item    = nm,
    value   = issues[[nm]],
    detail  = sprintf("%.2f%% of rows", 100 * issues[[nm]] / nrow(dt))
  )
}

# Dataset dimensions
report_rows[[length(report_rows) + 1]] <- data.table(
  section = "dimensions", item = "rows",
  value = nrow(dt), detail = "")
report_rows[[length(report_rows) + 1]] <- data.table(
  section = "dimensions", item = "columns",
  value = ncol(dt), detail = "")
report_rows[[length(report_rows) + 1]] <- data.table(
  section = "dimensions", item = "unique_students",
  value = uniqueN(dt$CO_PESSOA_FISICA), detail = "")
report_rows[[length(report_rows) + 1]] <- data.table(
  section = "dimensions", item = "years",
  value = length(unique(dt$NU_ANO)),
  detail = paste(sort(unique(dt$NU_ANO)), collapse = ","))

report <- rbindlist(report_rows)
report_path <- file.path(RESULTADOS_DIR, "data_quality_report.csv")
fwrite(report, report_path, sep = SEDAP$csv_separator)
log_msg(sprintf("  Report saved: %s (%d entries)", basename(report_path), nrow(report)))


# =============================================================================
# 7. SAVE CHECKPOINT
# =============================================================================

ckpt_out <- file.path(CHECKPOINT_DIR, "04_clean.rds")
saveRDS(dt, ckpt_out)
log_msg(sprintf("Checkpoint saved: %s", basename(ckpt_out)))

rm(miss_summary, miss_after, miss_by_year, top_miss, still_missing,
   report_rows, report, dup_check, mar_table)
gc()

log_msg("Step 04 complete.")