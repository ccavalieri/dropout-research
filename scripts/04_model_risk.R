###############################################################################
# 04_model_risk.R
#
# Train and compare three dropout-risk models on the person-year feature matrix
# and emit a calibrated risk score r per student-year (the input to the
# optimization stage). Temporal split, no resampling.
#
#   - Logistic + LASSO (glmnet)   transparent baseline
#   - GAM (mgcv)                  glass-box: smooths on key continuous predictors
#   - LightGBM                    black-box; SHAP from native predcontrib
#
# Evaluation: AUC-ROC, AUC-PR, recall@k / precision@k, Brier + calibration.
# Threshold chosen on validation by F-beta (beta>1). Probabilities are kept on
# the natural class distribution so they stay calibrated for the optimizer.
#
# Inputs:  data/features/features_{YEAR}.rds   (2019-2024)
# Outputs: data/model/risk_{YEAR}.rds          (keys + evadiu + r per model)
#          data/model/_metrics.csv, _calibration.csv, _threshold.csv
#          data/model/_lgb_importance.csv, _gam_summary.txt
###############################################################################

# ─── CONFIG ──────────────────────────────────────────────────────────────────
FEATURES_DIR <- "/Users/cc9/Documents/GitHub/dropout-research/data/features"
OUTPUT_DIR   <- "/Users/cc9/Documents/GitHub/dropout-research/data/model"
TARGET       <- "evadiu"
TRAIN_YEARS  <- 2019:2021
VAL_YEAR     <- 2022L
TEST_YEAR    <- 2023L
SCORE_YEARS  <- 2019:2024          # produce r for every year (2024 unlabeled)

RECALL_KS    <- c(0.05, 0.10, 0.20)
FBETA        <- 2                   # recall weighted over precision for the EWS

# Identifiers and high-cardinality codes never used as predictors.
DROP_COLS <- c("CO_PESSOA_FISICA", "ID_MATRICULA", "ID_TURMA", "CO_ENTIDADE",
               "CO_UF_NASC", "CO_MUNICIPIO_NASC", "CO_PAIS_RESIDENCIA",
               "CO_UF_END", "CO_MUNICIPIO_END", "NU_ANO")

# Curated GAM specification (kept interpretable, not the full feature space).
GAM_SMOOTH  <- c("NU_IDADE_REFERENCIA", "defasagem_idade_serie",
                 "n_reprovacoes_prev", "anos_observados", "indice_infra",
                 "tamanho_escola", "tamanho_turma", "apoio_por_aluno")
GAM_PARAM   <- c("TP_SEXO", "TP_COR_RACA", "TP_DEPENDENCIA", "TP_LOCALIZACAO",
                 "turno", "flag_defasado", "reprovou_ano_ant", "abandono_previo",
                 "mudou_municipio", "IN_NECESSIDADE_ESPECIAL", "IN_TROCA_ESCOLA",
                 "IN_TRANSFERIDO")

# ─── PACKAGES ────────────────────────────────────────────────────────────────
suppressPackageStartupMessages({
  library(data.table); library(Matrix); library(mgcv); library(glmnet)
  library(lightgbm)
})
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)
set.seed(42)

# ─── METRICS ─────────────────────────────────────────────────────────────────
auc_roc <- function(y, p) {
  n1 <- sum(y == 1); n0 <- sum(y == 0)
  if (n1 == 0 || n0 == 0) return(NA_real_)
  (sum(rank(p)[y == 1]) - n1 * (n1 + 1) / 2) / (n1 * n0)
}
auc_pr <- function(y, p) {
  o <- order(p, decreasing = TRUE); y <- y[o]
  tp <- cumsum(y); fp <- cumsum(1 - y)
  prec <- tp / (tp + fp); rec <- tp / sum(y)
  sum(prec * c(rec[1], diff(rec)))
}
rec_prec_at_k <- function(y, p, k) {
  top <- order(p, decreasing = TRUE)[seq_len(ceiling(k * length(y)))]
  tp <- sum(y[top] == 1)
  c(recall = tp / sum(y == 1), precision = tp / length(top))
}
brier <- function(y, p) mean((p - y)^2)

calibration_bins <- function(y, p, model, year, nb = 10) {
  b <- cut(p, breaks = seq(0, 1, length.out = nb + 1), include.lowest = TRUE)
  data.table(model = model, year = year, bin = b, p_pred = p, y = y)[
    , .(n = .N, mean_pred = mean(p_pred), obs_rate = mean(y)), by = .(model, year, bin)]
}

eval_model <- function(y, p, model, year) {
  rk <- sapply(RECALL_KS, function(k) rec_prec_at_k(y, p, k))
  out <- data.table(model = model, year = year,
                    auc_roc = auc_roc(y, p), auc_pr = auc_pr(y, p),
                    brier = brier(y, p))
  for (i in seq_along(RECALL_KS)) {
    out[[sprintf("recall@%d", round(RECALL_KS[i] * 100))]]    <- rk["recall", i]
    out[[sprintf("precision@%d", round(RECALL_KS[i] * 100))]] <- rk["precision", i]
  }
  out
}

# ─── LOAD PANEL ──────────────────────────────────────────────────────────────
panel <- rbindlist(lapply(SCORE_YEARS, function(y)
  readRDS(file.path(FEATURES_DIR, sprintf("features_%d.rds", y)))), fill = TRUE)

keys     <- panel[, .(CO_PESSOA_FISICA, NU_ANO, evadiu)]
feat_all <- setdiff(names(panel), c(DROP_COLS, TARGET))

# Drop degenerate columns (constant or all-NA over the panel).
nu <- panel[, lapply(.SD, function(x) length(unique(x[!is.na(x)]))), .SDcols = feat_all]
feat_all <- feat_all[as.integer(nu[1]) > 1]

# Classify: factors = character or TP_ codes; the rest numeric.
is_factor <- function(col) is.character(panel[[col]]) || startsWith(col, "TP_")
factor_cols  <- feat_all[vapply(feat_all, is_factor, logical(1))]
numeric_cols <- setdiff(feat_all, factor_cols)

train_idx <- panel$NU_ANO %in% TRAIN_YEARS & !is.na(panel$evadiu)
val_idx   <- panel$NU_ANO == VAL_YEAR     & !is.na(panel$evadiu)
test_idx  <- panel$NU_ANO == TEST_YEAR    & !is.na(panel$evadiu)

# ─── PREPROCESSING ───────────────────────────────────────────────────────────
# Factors: NA becomes an explicit "MISSING" level (levels fixed on full panel).
# Numerics: median imputation (from training) plus a missingness flag.
D <- copy(panel[, ..feat_all])
for (col in factor_cols)
  set(D, j = col, value = addNA(factor(D[[col]]), ifany = TRUE))
for (col in factor_cols)
  levels(D[[col]])[is.na(levels(D[[col]]))] <- "MISSING"

for (col in numeric_cols) {
  x <- D[[col]]
  if (anyNA(x)) {
    D[[paste0(col, "_isna")]] <- as.integer(is.na(x))
    med <- median(x[train_idx], na.rm = TRUE)
    set(D, which(is.na(x)), col, med)
  }
}

# ─── GLMNET (logistic + LASSO) ───────────────────────────────────────────────
X <- sparse.model.matrix(~ . - 1, data = D)
y_train <- panel$evadiu[train_idx]

cvfit <- cv.glmnet(X[train_idx, ], y_train, family = "binomial",
                   type.measure = "auc")
p_logit <- as.numeric(predict(cvfit, X, s = "lambda.min", type = "response"))

# ─── GAM (mgcv) ──────────────────────────────────────────────────────────────
gam_smooth <- intersect(GAM_SMOOTH, numeric_cols)
gam_param  <- intersect(GAM_PARAM, feat_all)
k_for <- function(col) min(10L, length(unique(D[[col]][train_idx])) - 1L)
sm <- vapply(gam_smooth, function(v)
  if (k_for(v) >= 3) sprintf("s(%s, k=%d)", v, k_for(v)) else v, character(1))
form <- as.formula(paste("y ~", paste(c(sm, sprintf("`%s`", gam_param)),
                                      collapse = " + ")))
Dg <- copy(D); Dg[, y := panel$evadiu]
gam_fit <- bam(form, family = binomial, data = Dg[train_idx], discrete = TRUE)
p_gam <- as.numeric(predict(gam_fit, newdata = Dg, type = "response"))

# ─── LIGHTGBM ────────────────────────────────────────────────────────────────
# Native categoricals (integer-coded, NA kept) and native missing handling.
Dl <- copy(panel[, ..feat_all])
for (col in factor_cols) set(Dl, j = col, value = as.integer(factor(Dl[[col]])))
Ml <- as.matrix(Dl)

dtrain <- lgb.Dataset(Ml[train_idx, ], label = panel$evadiu[train_idx],
                      categorical_feature = factor_cols)
dval   <- lgb.Dataset.create.valid(dtrain, Ml[val_idx, ],
                                   label = panel$evadiu[val_idx])
lgb_fit <- lgb.train(
  params = list(objective = "binary", metric = "auc", learning_rate = 0.05,
                num_leaves = 31, min_data_in_leaf = 20, verbose = -1),
  data = dtrain, valids = list(val = dval), nrounds = 500,
  early_stopping_rounds = 30)
p_lgb <- predict(lgb_fit, Ml, type = "response")

# ─── EVALUATION (test year) ──────────────────────────────────────────────────
preds <- list(logit = p_logit, gam = p_gam, lgb = p_lgb)
yte   <- panel$evadiu[test_idx]

metrics <- rbindlist(lapply(names(preds), function(m)
  eval_model(yte, preds[[m]][test_idx], m, TEST_YEAR)), fill = TRUE)
calib <- rbindlist(lapply(names(preds), function(m)
  calibration_bins(yte, preds[[m]][test_idx], m, TEST_YEAR)), fill = TRUE)

# ─── THRESHOLD (F-beta on validation, applied to test) ───────────────────────
pick_threshold <- function(y, p, beta) {
  ths <- sort(unique(p))
  best <- 0; best_f <- -1
  for (th in ths) {
    pred <- p >= th
    tp <- sum(pred & y == 1); fp <- sum(pred & y == 0); fn <- sum(!pred & y == 1)
    prec <- if (tp + fp > 0) tp / (tp + fp) else 0
    rec  <- if (tp + fn > 0) tp / (tp + fn) else 0
    f <- if (prec + rec > 0) (1 + beta^2) * prec * rec / (beta^2 * prec + rec) else 0
    if (!is.na(f) && f > best_f) { best_f <- f; best <- th }
  }
  best
}
thr <- rbindlist(lapply(names(preds), function(m) {
  th <- pick_threshold(panel$evadiu[val_idx], preds[[m]][val_idx], FBETA)
  pr <- preds[[m]][test_idx] >= th
  data.table(model = m, threshold = th,
             recall = sum(pr & yte == 1) / sum(yte == 1),
             precision = if (sum(pr) > 0) sum(pr & yte == 1) / sum(pr) else NA,
             flagged = sum(pr))
}))

# ─── EXPLAINABILITY ──────────────────────────────────────────────────────────
# LightGBM: global importance from native SHAP (mean |contribution|).
shap <- predict(lgb_fit, Ml[test_idx, ], type = "contrib")
shap_imp <- data.table(feature = c(colnames(Ml), "BIAS"),
                       mean_abs_shap = colMeans(abs(shap)))[feature != "BIAS"][
                       order(-mean_abs_shap)]
fwrite(shap_imp, file.path(OUTPUT_DIR, "_lgb_importance.csv"), sep = ";", bom = TRUE)
capture.output(summary(gam_fit), file = file.path(OUTPUT_DIR, "_gam_summary.txt"))

# ─── OUTPUTS ─────────────────────────────────────────────────────────────────
risk <- cbind(keys, r_logit = p_logit, r_gam = p_gam, r_lgb = p_lgb)
for (y in SCORE_YEARS)
  saveRDS(risk[NU_ANO == y], file.path(OUTPUT_DIR, sprintf("risk_%d.rds", y)))

fwrite(metrics, file.path(OUTPUT_DIR, "_metrics.csv"),     sep = ";", bom = TRUE, na = "")
fwrite(calib,   file.path(OUTPUT_DIR, "_calibration.csv"), sep = ";", bom = TRUE, na = "")
fwrite(thr,     file.path(OUTPUT_DIR, "_threshold.csv"),   sep = ";", bom = TRUE, na = "")

message("\n=== Test-year metrics (", TEST_YEAR, ") ===")
print(metrics[, .(model, auc_roc = round(auc_roc, 3), auc_pr = round(auc_pr, 3),
                  brier = round(brier, 4))])
message("\nOutputs in ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
