# =============================================================================
# 03_feature_engineering.R
# Creates all model features from the merged flat file.
# Enforces strict temporal logic: only data from year t or earlier.
#
# Features:
#   - Age-grade distortion
#   - Etapa classification
#   - School infrastructure composite score
#   - School-level prior-year dropout rate
#   - Longitudinal lag features (prior outcome, retention count, school changes)
#   - Binary dropout target variable
#
# Inputs:  outputs/checkpoints/02_merged.rds
# Outputs: outputs/checkpoints/03_features.rds
# =============================================================================

if (!exists("log_msg")) source("scripts/00_config.R")

library(data.table)

log_msg("==================================================")
log_msg("STEP 03: Feature Engineering")
log_msg("==================================================")


# =============================================================================
# 1. LOAD MERGED DATA
# =============================================================================

ckpt_path <- file.path(CHECKPOINT_DIR, "02_merged.rds")
if (!file.exists(ckpt_path)) stop("Checkpoint not found: 02_merged.rds")
dt <- readRDS(ckpt_path)
log_msg(sprintf("Loaded merged data: %s rows x %d cols",
                format(nrow(dt), big.mark = ","), ncol(dt)))


# =============================================================================
# 2. AGE-GRADE DISTORTION
# =============================================================================

log_msg("--- Age-grade distortion ---")

dt[, age_grade_distortion := compute_age_distortion(NU_IDADE_REFERENCIA, TP_ETAPA_ENSINO)]
dt[, is_overage := as.integer(age_grade_distortion >= 2)]

log_msg(sprintf("  Mean distortion: %.2f years", mean(dt$age_grade_distortion, na.rm = TRUE)))
log_msg(sprintf("  Overage (2+ years): %.1f%%", 100 * mean(dt$is_overage, na.rm = TRUE)))


# =============================================================================
# 3. ETAPA CLASSIFICATION
# =============================================================================

log_msg("--- Etapa classification ---")

dt[, etapa_level := classify_etapa(TP_ETAPA_ENSINO)]
dt[, is_eja := as.integer(etapa_level %in% c("eja_fund", "eja_medio"))]
dt[, is_medio := as.integer(etapa_level %in% c("medio", "medio_tecnico"))]

log_msg(sprintf("  Levels: %s",
                paste(names(table(dt$etapa_level)), collapse = ", ")))


# =============================================================================
# 4. SCHOOL INFRASTRUCTURE SCORE
# =============================================================================

log_msg("--- Infrastructure score ---")

infra_available <- intersect(INFRA_COLS, names(dt))
if (length(infra_available) > 0) {
  dt[, infra_score := rowMeans(.SD, na.rm = TRUE), .SDcols = infra_available]
  log_msg(sprintf("  Computed from %d/%d columns, mean=%.2f",
                  length(infra_available), length(INFRA_COLS),
                  mean(dt$infra_score, na.rm = TRUE)))
} else {
  dt[, infra_score := NA_real_]
  log_msg("  [WARN] No infrastructure columns found", "WARN")
}


# =============================================================================
# 5. BINARY TARGET VARIABLE
# =============================================================================

log_msg("--- Target variable ---")
log_msg(sprintf("  Dropout definition: %s", DROPOUT_DEFINITION))

# Strict: dropout = TP_SITUACAO == 2
dt[, dropout := NA_integer_]
dt[!is.na(TP_SITUACAO) & TP_SITUACAO != SIT_DECEASED & TP_SITUACAO != SIT_NO_INFO,
   dropout := as.integer(TP_SITUACAO == SIT_DROPOUT)]

n_labelled <- sum(!is.na(dt$dropout))
n_dropout  <- sum(dt$dropout == 1L, na.rm = TRUE)
log_msg(sprintf("  Labelled: %s (%.1f%% dropout)",
                format(n_labelled, big.mark = ","),
                if (n_labelled > 0) 100 * n_dropout / n_labelled else 0))
log_msg(sprintf("  Unlabelled (no situacao): %s",
                format(sum(is.na(dt$dropout)), big.mark = ",")))


# =============================================================================
# 6. LONGITUDINAL LAG FEATURES
# =============================================================================

log_msg("--- Longitudinal features ---")

# Sort by student and year
setkey(dt, CO_PESSOA_FISICA, NU_ANO)

# 6a. Prior-year outcome (TP_SITUACAO from t-1)
log_msg("  Computing prior-year outcome...")
prior <- dt[!is.na(TP_SITUACAO),
            .(CO_PESSOA_FISICA, NU_ANO, TP_SITUACAO, CO_ENTIDADE)]

prior[, NU_ANO_NEXT := NU_ANO + 1L]
setnames(prior,
         c("TP_SITUACAO", "CO_ENTIDADE"),
         c("sit_prev", "school_prev"))

dt <- merge(dt, prior[, .(CO_PESSOA_FISICA, NU_ANO_NEXT, sit_prev, school_prev)],
            by.x = c("CO_PESSOA_FISICA", "NU_ANO"),
            by.y = c("CO_PESSOA_FISICA", "NU_ANO_NEXT"),
            all.x = TRUE)

dt[, was_failed_prev := as.integer(sit_prev == SIT_FAILED)]
dt[is.na(was_failed_prev), was_failed_prev := 0L]

dt[, was_dropout_prev := as.integer(sit_prev == SIT_DROPOUT)]
dt[is.na(was_dropout_prev), was_dropout_prev := 0L]

log_msg(sprintf("  Prior outcome matched: %s / %s rows",
                format(sum(!is.na(dt$sit_prev)), big.mark = ","),
                format(nrow(dt), big.mark = ",")))

# 6b. School change flag
dt[, school_change := as.integer(!is.na(school_prev) & school_prev != CO_ENTIDADE)]
dt[is.na(school_change), school_change := 0L]

log_msg(sprintf("  School changes: %s (%.1f%%)",
                format(sum(dt$school_change), big.mark = ","),
                100 * mean(dt$school_change)))

# 6c. Cumulative retention count (how many times failed up to year t)
log_msg("  Computing cumulative retention count...")
fail_history <- dt[!is.na(TP_SITUACAO) & TP_SITUACAO == SIT_FAILED,
                   .(CO_PESSOA_FISICA, NU_ANO)]

if (nrow(fail_history) > 0) {
  fail_history <- fail_history[order(CO_PESSOA_FISICA, NU_ANO)]
  fail_history[, cum_fail := seq_len(.N), by = CO_PESSOA_FISICA]

  # For year t, count failures strictly before t
  fail_history[, NU_ANO_NEXT := NU_ANO + 1L]
  setnames(fail_history, "cum_fail", "retentions_before")

  dt <- merge(dt,
              fail_history[, .(CO_PESSOA_FISICA, NU_ANO_NEXT, retentions_before)],
              by.x = c("CO_PESSOA_FISICA", "NU_ANO"),
              by.y = c("CO_PESSOA_FISICA", "NU_ANO_NEXT"),
              all.x = TRUE)
} else {
  dt[, retentions_before := NA_integer_]
}
dt[is.na(retentions_before), retentions_before := 0L]

log_msg(sprintf("  Max retentions: %d, mean: %.2f",
                max(dt$retentions_before), mean(dt$retentions_before)))

# 6d. Years in census (proxy for enrollment persistence)
log_msg("  Computing enrollment history length...")
enroll_count <- dt[, .(years_enrolled = .N), by = CO_PESSOA_FISICA]

# For temporal safety: count only years strictly before t
enroll_by_year <- dt[, .(CO_PESSOA_FISICA, NU_ANO)]
enroll_by_year <- enroll_by_year[order(CO_PESSOA_FISICA, NU_ANO)]
enroll_by_year[, prior_enrollments := seq_len(.N) - 1L, by = CO_PESSOA_FISICA]

dt <- merge(dt,
            enroll_by_year[, .(CO_PESSOA_FISICA, NU_ANO, prior_enrollments)],
            by = c("CO_PESSOA_FISICA", "NU_ANO"),
            all.x = TRUE)
dt[is.na(prior_enrollments), prior_enrollments := 0L]


# =============================================================================
# 7. SCHOOL-LEVEL PRIOR-YEAR DROPOUT RATE
# =============================================================================

log_msg("--- School-level dropout rate (prior year) ---")

school_rates <- dt[!is.na(dropout),
                   .(school_dropout_rate = mean(dropout),
                     school_n_students = .N),
                   by = .(CO_ENTIDADE, NU_ANO)]

# Shift forward: rate from year t becomes the feature for year t+1
school_rates[, NU_ANO_NEXT := NU_ANO + 1L]
setnames(school_rates, "school_dropout_rate", "school_dropout_rate_prev")
school_rates[, school_n_students := NULL]

dt <- merge(dt,
            school_rates[, .(CO_ENTIDADE, NU_ANO_NEXT, school_dropout_rate_prev)],
            by.x = c("CO_ENTIDADE", "NU_ANO"),
            by.y = c("CO_ENTIDADE", "NU_ANO_NEXT"),
            all.x = TRUE)

n_with_rate <- sum(!is.na(dt$school_dropout_rate_prev))
log_msg(sprintf("  Matched: %s / %s rows (first year has no prior rate)",
                format(n_with_rate, big.mark = ","),
                format(nrow(dt), big.mark = ",")))


# =============================================================================
# 8. DEMOGRAPHIC INDICATORS
# =============================================================================

log_msg("--- Demographic indicators ---")

dt[, is_male   := as.integer(TP_SEXO == 1L)]
dt[, is_preta_parda := as.integer(TP_COR_RACA %in% c(2L, 3L))]

if ("TP_LOCALIZACAO" %in% names(dt)) {
  dt[, is_rural := as.integer(TP_LOCALIZACAO == 2L)]
} else if ("TP_ZONA_RESIDENCIAL" %in% names(dt)) {
  dt[, is_rural := as.integer(TP_ZONA_RESIDENCIAL == 2L)]
} else {
  dt[, is_rural := NA_integer_]
}


# =============================================================================
# 9. CLEAN UP TEMPORARY COLUMNS
# =============================================================================

log_msg("--- Cleanup ---")

temp_cols <- c("sit_prev", "school_prev")
for (col in temp_cols) {
  if (col %in% names(dt)) dt[, (col) := NULL]
}


# =============================================================================
# 10. FINAL SUMMARY AND SAVE
# =============================================================================

feature_cols <- c("age_grade_distortion", "is_overage", "etapa_level",
                  "is_eja", "is_medio", "infra_score", "dropout",
                  "was_failed_prev", "was_dropout_prev", "school_change",
                  "retentions_before", "prior_enrollments",
                  "school_dropout_rate_prev",
                  "is_male", "is_preta_parda", "is_rural")
present <- intersect(feature_cols, names(dt))

log_msg(sprintf("  Created %d feature columns", length(present)))
log_msg(sprintf("  Final dataset: %s rows x %d cols",
                format(nrow(dt), big.mark = ","), ncol(dt)))

ckpt_path <- file.path(CHECKPOINT_DIR, "03_features.rds")
saveRDS(dt, ckpt_path)
log_msg(sprintf("Checkpoint saved: %s", basename(ckpt_path)))

rm(prior, fail_history, enroll_by_year, enroll_count, school_rates)
gc()

log_msg("Step 03 complete.")