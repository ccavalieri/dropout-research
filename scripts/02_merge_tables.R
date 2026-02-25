# =============================================================================
# 02_merge_tables.R
# Joins BAS_ESCOLA + BAS_TURMA + BAS_MATRICULA + BAS_SITUACAO into a single
# analytical flat file keyed by student-year (NU_ANO, ID_MATRICULA).
# Validates referential integrity and saves an RDS checkpoint.
#
# Inputs:  outputs/checkpoints/01_*.rds (from 01_load_and_validate.R)
# Outputs: outputs/checkpoints/02_merged.rds
# =============================================================================

if (!exists("log_msg")) source("scripts/00_config.R")

library(data.table)

log_msg("==================================================")
log_msg("STEP 02: Merge Tables")
log_msg("==================================================")


# =============================================================================
# 1. LOAD CHECKPOINTS
# =============================================================================

load_checkpoint <- function(table_name) {
  path <- file.path(CHECKPOINT_DIR, paste0("01_", table_name, ".rds"))
  if (!file.exists(path)) stop(sprintf("Checkpoint not found: %s", path))
  readRDS(path)
}

escola    <- load_checkpoint("BAS_ESCOLA")
turma     <- load_checkpoint("BAS_TURMA")
matricula <- load_checkpoint("BAS_MATRICULA")
situacao  <- load_checkpoint("BAS_SITUACAO")

log_msg(sprintf("Loaded: ESCOLA=%s, TURMA=%s, MATRICULA=%s, SITUACAO=%s",
                format(nrow(escola), big.mark = ","),
                format(nrow(turma), big.mark = ","),
                format(nrow(matricula), big.mark = ","),
                format(nrow(situacao), big.mark = ",")))


# =============================================================================
# 2. REFERENTIAL INTEGRITY CHECKS
# =============================================================================

log_msg("--- Referential integrity ---")

# MATRICULA -> ESCOLA: every (NU_ANO, CO_ENTIDADE) must exist
mat_keys  <- unique(matricula[, .(NU_ANO, CO_ENTIDADE)])
esc_keys  <- unique(escola[, .(NU_ANO, CO_ENTIDADE)])
orphan_escola <- fsetdiff(mat_keys, esc_keys)
if (nrow(orphan_escola) > 0) {
  log_msg(sprintf("  [WARN] %d MATRICULA (year, school) pairs not in ESCOLA",
                  nrow(orphan_escola)), "WARN")
} else {
  log_msg("  [OK]   All MATRICULA schools found in ESCOLA")
}

# MATRICULA -> TURMA: every (NU_ANO, ID_TURMA) must exist
mat_turma <- unique(matricula[, .(NU_ANO, ID_TURMA)])
tur_keys  <- unique(turma[, .(NU_ANO, ID_TURMA)])
orphan_turma <- fsetdiff(mat_turma, tur_keys)
if (nrow(orphan_turma) > 0) {
  log_msg(sprintf("  [WARN] %d MATRICULA (year, turma) pairs not in TURMA",
                  nrow(orphan_turma)), "WARN")
} else {
  log_msg("  [OK]   All MATRICULA turmas found in TURMA")
}

# SITUACAO -> MATRICULA: every (NU_ANO, ID_MATRICULA) must exist
sit_keys <- unique(situacao[, .(NU_ANO, ID_MATRICULA)])
mat_keys2 <- unique(matricula[, .(NU_ANO, ID_MATRICULA)])
orphan_sit <- fsetdiff(sit_keys, mat_keys2)
if (nrow(orphan_sit) > 0) {
  log_msg(sprintf("  [WARN] %d SITUACAO records not matched in MATRICULA",
                  nrow(orphan_sit)), "WARN")
} else {
  log_msg("  [OK]   All SITUACAO records matched in MATRICULA")
}


# =============================================================================
# 3. PREPARE TABLES FOR JOIN (select non-overlapping columns)
# =============================================================================

log_msg("--- Preparing joins ---")

# -- ESCOLA: select school-level features not already in MATRICULA -----------
escola_join_cols <- c("NU_ANO", "CO_ENTIDADE")
escola_feature_cols <- setdiff(names(escola), names(matricula))
escola_feature_cols <- setdiff(escola_feature_cols, c("NU_ANO", "CO_ENTIDADE"))
escola_sel <- escola[, c(escola_join_cols, escola_feature_cols), with = FALSE]
log_msg(sprintf("  ESCOLA: bringing %d feature columns", length(escola_feature_cols)))

# -- TURMA: select turma-level features not already in MATRICULA -------------
turma_join_cols <- c("NU_ANO", "ID_TURMA")
turma_feature_cols <- setdiff(names(turma), names(matricula))
turma_feature_cols <- setdiff(turma_feature_cols, c("NU_ANO", "ID_TURMA"))
turma_sel <- turma[, c(turma_join_cols, turma_feature_cols), with = FALSE]
log_msg(sprintf("  TURMA:  bringing %d feature columns", length(turma_feature_cols)))

# -- SITUACAO: select outcome columns not already in MATRICULA ---------------
sit_join_cols <- c("NU_ANO", "ID_MATRICULA")
sit_feature_cols <- setdiff(names(situacao), names(matricula))
sit_feature_cols <- setdiff(sit_feature_cols, c("NU_ANO", "ID_MATRICULA"))
situacao_sel <- situacao[, c(sit_join_cols, sit_feature_cols), with = FALSE]
log_msg(sprintf("  SITUACAO: bringing %d feature columns", length(sit_feature_cols)))


# =============================================================================
# 4. MERGE
# =============================================================================

log_msg("--- Merging ---")
n_start <- nrow(matricula)

# 4a. MATRICULA + ESCOLA (left join on year + school)
setkey(matricula, NU_ANO, CO_ENTIDADE)
setkey(escola_sel, NU_ANO, CO_ENTIDADE)
merged <- escola_sel[matricula, on = .(NU_ANO, CO_ENTIDADE)]

n_after_escola <- nrow(merged)
log_msg(sprintf("  + ESCOLA:   %s rows (was %s)",
                format(n_after_escola, big.mark = ","),
                format(n_start, big.mark = ",")))

# 4b. + TURMA (left join on year + turma)
setkey(turma_sel, NU_ANO, ID_TURMA)
merged <- turma_sel[merged, on = .(NU_ANO, ID_TURMA)]

n_after_turma <- nrow(merged)
log_msg(sprintf("  + TURMA:    %s rows (was %s)",
                format(n_after_turma, big.mark = ","),
                format(n_after_escola, big.mark = ",")))

# 4c. + SITUACAO (left join on year + matricula — only SITUACAO_YEARS have data)
setkey(situacao_sel, NU_ANO, ID_MATRICULA)
merged <- situacao_sel[merged, on = .(NU_ANO, ID_MATRICULA)]

n_final <- nrow(merged)
log_msg(sprintf("  + SITUACAO: %s rows (was %s)",
                format(n_final, big.mark = ","),
                format(n_after_turma, big.mark = ",")))


# =============================================================================
# 5. POST-MERGE VALIDATION
# =============================================================================

log_msg("--- Post-merge validation ---")

# Row count should equal MATRICULA
if (n_final != n_start) {
  log_msg(sprintf("  [WARN] Row count changed: %d -> %d (check for duplicates in join keys)",
                  n_start, n_final), "WARN")
} else {
  log_msg(sprintf("  [OK]   Row count preserved: %s", format(n_final, big.mark = ",")))
}

# TP_SITUACAO coverage
if ("TP_SITUACAO" %in% names(merged)) {
  n_with_sit    <- sum(!is.na(merged$TP_SITUACAO))
  n_without_sit <- sum(is.na(merged$TP_SITUACAO))
  log_msg(sprintf("  TP_SITUACAO: %s with outcome, %s without (expected for %d)",
                  format(n_with_sit, big.mark = ","),
                  format(n_without_sit, big.mark = ","),
                  TEST_YEAR))
}

# Final dimensions
log_msg(sprintf("  Final dataset: %s rows x %d columns",
                format(nrow(merged), big.mark = ","), ncol(merged)))


# =============================================================================
# 6. SAVE CHECKPOINT
# =============================================================================

ckpt_path <- file.path(CHECKPOINT_DIR, "02_merged.rds")
saveRDS(merged, ckpt_path)
log_msg(sprintf("Checkpoint saved: %s", basename(ckpt_path)))

# Free memory
rm(escola, turma, matricula, situacao, escola_sel, turma_sel, situacao_sel)
gc()

log_msg("Step 02 complete.")