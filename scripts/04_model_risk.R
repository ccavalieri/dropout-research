###############################################################################
# 04_model_risk.R
#
# Train and compare three dropout-risk models on the person-year feature matrix
# and emit a calibrated risk score r per student-year
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
SCORE_YEARS  <- 2019:2024          

RECALL_KS    <- c(0.05, 0.10, 0.20)
FBETA        <- 2                   # recall weighted over precision for the EWS

# Identifiers and high-cardinality codes never used as predictors.
DROP_COLS <- c("CO_PESSOA_FISICA", "ID_MATRICULA", "ID_TURMA", "CO_ENTIDADE",
               "CO_UF_NASC", "CO_MUNICIPIO_NASC", "CO_PAIS_RESIDENCIA",
               "CO_UF_END", "CO_MUNICIPIO_END", "NU_ANO")

# Curated GAM specification.
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

# Memory strategy: train once on the labeled years, then score one year at a
# time (never binds all years, never builds a full-panel matrix). glmnet is
# sparse; LightGBM keeps native NA/categorical (dense, bounded to labeled years).

# ─── LOAD LABELED YEARS + PREPROCESSING SPEC ─────────────────────────────────
LABELED_YEARS <- sort(unique(c(TRAIN_YEARS, VAL_YEAR, TEST_YEAR)))
lab <- rbindlist(lapply(LABELED_YEARS, function(y)
  readRDS(file.path(FEATURES_DIR, sprintf("features_%d.rds", y)))), fill = TRUE)

feat_all <- setdiff(names(lab), c(DROP_COLS, TARGET))
nu <- lab[, lapply(.SD, function(x) length(unique(x[!is.na(x)]))), .SDcols = feat_all]
feat_all <- feat_all[as.integer(nu[1]) > 1]
is_factor <- function(col) is.character(lab[[col]]) || startsWith(col, "TP_")
factor_cols  <- feat_all[vapply(feat_all, is_factor, logical(1))]
numeric_cols <- setdiff(feat_all, factor_cols)

train_mask <- lab$NU_ANO %in% TRAIN_YEARS & !is.na(lab$evadiu)
val_mask   <- lab$NU_ANO == VAL_YEAR     & !is.na(lab$evadiu)
test_mask  <- lab$NU_ANO == TEST_YEAR    & !is.na(lab$evadiu)

# Fixed factor levels (with MISSING) and train medians, so any year preprocesses
# to the same columns.
lvls <- lapply(factor_cols, function(c) {
  L <- levels(addNA(factor(lab[[c]]), ifany = TRUE)); L[is.na(L)] <- "MISSING"; L })
names(lvls) <- factor_cols
meds <- vapply(numeric_cols, function(c) median(lab[[c]][train_mask], na.rm = TRUE), numeric(1))
miss_cols <- numeric_cols[vapply(numeric_cols, function(c) anyNA(lab[[c]]), logical(1))]

# Imputed design (fixed factor levels + isna flags) for glmnet / GAM.
apply_prep <- function(dt) {
  D <- copy(dt[, ..feat_all])
  for (c in factor_cols) {
    v <- as.character(D[[c]]); v[is.na(v) | !v %in% lvls[[c]]] <- "MISSING"
    set(D, j = c, value = factor(v, levels = lvls[[c]]))
  }
  for (c in numeric_cols) {
    x <- D[[c]]
    if (c %in% miss_cols) set(D, j = paste0(c, "_isna"), value = as.integer(is.na(x)))
    if (anyNA(x)) set(D, which(is.na(x)), c, meds[[c]])
  }
  D
}
# LightGBM matrix: raw values (native NA), factors as consistent integer codes.
lgb_matrix <- function(dt) {
  Dl <- copy(dt[, ..feat_all])
  for (c in factor_cols)
    set(Dl, j = c, value = as.integer(factor(as.character(Dl[[c]]), levels = lvls[[c]])))
  as.matrix(Dl)
}

# ─── TRAIN (labeled years only) ──────────────────────────────────────────────
Dlab <- apply_prep(lab)
Xlab <- sparse.model.matrix(~ . - 1, data = Dlab)
Mlab <- lgb_matrix(lab)
xcols <- colnames(Xlab)

cvfit <- cv.glmnet(Xlab[train_mask, ], lab$evadiu[train_mask],
                   family = "binomial", type.measure = "auc")

gam_smooth <- intersect(GAM_SMOOTH, numeric_cols)
gam_param  <- intersect(GAM_PARAM, feat_all)
k_for <- function(col) min(10L, length(unique(Dlab[[col]][train_mask])) - 1L)
sm <- vapply(gam_smooth, function(v)
  if (k_for(v) >= 3) sprintf("s(%s, k=%d)", v, k_for(v)) else v, character(1))
form <- as.formula(paste("evadiu ~", paste(c(sm, sprintf("`%s`", gam_param)), collapse = " + ")))
Dg <- copy(Dlab); Dg[, evadiu := lab$evadiu]
gam_fit <- bam(form, family = binomial, data = Dg[train_mask], discrete = TRUE)

dtrain <- lgb.Dataset(Mlab[train_mask, ], label = lab$evadiu[train_mask],
                      categorical_feature = factor_cols)
dval <- lgb.Dataset.create.valid(dtrain, Mlab[val_mask, ], label = lab$evadiu[val_mask])
lgb_fit <- lgb.train(
  params = list(objective = "binary", metric = "auc", learning_rate = 0.05,
                num_leaves = 31, min_data_in_leaf = 20, verbose = -1),
  data = dtrain, valids = list(val = dval), nrounds = 500, early_stopping_rounds = 30)

# ─── EVALUATE (test) + THRESHOLD (val) + SHAP ────────────────────────────────
pv <- list(logit = as.numeric(predict(cvfit, Xlab[val_mask, ], s = "lambda.min", type = "response")),
           gam   = as.numeric(predict(gam_fit, Dlab[val_mask], type = "response")),
           lgb   = predict(lgb_fit, Mlab[val_mask, ], type = "response"))
pt <- list(logit = as.numeric(predict(cvfit, Xlab[test_mask, ], s = "lambda.min", type = "response")),
           gam   = as.numeric(predict(gam_fit, Dlab[test_mask], type = "response")),
           lgb   = predict(lgb_fit, Mlab[test_mask, ], type = "response"))
yte <- lab$evadiu[test_mask]; yval <- lab$evadiu[val_mask]

metrics <- rbindlist(lapply(names(pt), function(m) eval_model(yte, pt[[m]], m, TEST_YEAR)), fill = TRUE)
calib <- rbindlist(lapply(names(pt), function(m) calibration_bins(yte, pt[[m]], m, TEST_YEAR)), fill = TRUE)

pick_threshold <- function(y, p, beta) {
  best <- 0; best_f <- -1
  for (th in sort(unique(p))) {
    pred <- p >= th
    tp <- sum(pred & y == 1); fp <- sum(pred & y == 0); fn <- sum(!pred & y == 1)
    prec <- if (tp + fp > 0) tp/(tp+fp) else 0; rec <- if (tp + fn > 0) tp/(tp+fn) else 0
    f <- if (prec + rec > 0) (1+beta^2)*prec*rec/(beta^2*prec+rec) else 0
    if (!is.na(f) && f > best_f) { best_f <- f; best <- th }
  }
  best
}
thr <- rbindlist(lapply(names(pt), function(m) {
  th <- pick_threshold(yval, pv[[m]], FBETA); pr <- pt[[m]] >= th
  data.table(model = m, threshold = th, recall = sum(pr & yte==1)/sum(yte==1),
             precision = if (sum(pr) > 0) sum(pr & yte==1)/sum(pr) else NA, flagged = sum(pr)) }))

shap <- predict(lgb_fit, Mlab[test_mask, ], type = "contrib")
shap_imp <- data.table(feature = c(colnames(Mlab), "BIAS"),
                       mean_abs_shap = colMeans(abs(shap)))[feature != "BIAS"][order(-mean_abs_shap)]
fwrite(shap_imp, file.path(OUTPUT_DIR, "_lgb_importance.csv"), sep = ";", bom = TRUE)
capture.output(summary(gam_fit), file = file.path(OUTPUT_DIR, "_gam_summary.txt"))
fwrite(metrics, file.path(OUTPUT_DIR, "_metrics.csv"),     sep = ";", bom = TRUE, na = "")
fwrite(calib,   file.path(OUTPUT_DIR, "_calibration.csv"), sep = ";", bom = TRUE, na = "")
fwrite(thr,     file.path(OUTPUT_DIR, "_threshold.csv"),   sep = ";", bom = TRUE, na = "")
saveRDS(gam_fit, file.path(OUTPUT_DIR, "model_gam.rds"))   # report_A plots its shapes

rm(lab, Dlab, Xlab, Mlab, Dg, dtrain, dval); gc(verbose = FALSE)

# ─── SCORE ONE YEAR AT A TIME ────────────────────────────────────────────────
for (y in SCORE_YEARS) {
  fy <- readRDS(file.path(FEATURES_DIR, sprintf("features_%d.rds", y)))
  Dy <- apply_prep(fy)
  Xy <- sparse.model.matrix(~ . - 1, data = Dy)
  miss <- setdiff(xcols, colnames(Xy))
  if (length(miss))
    Xy <- cbind(Xy, Matrix(0, nrow(Xy), length(miss), sparse = TRUE, dimnames = list(NULL, miss)))
  Xy <- Xy[, xcols, drop = FALSE]
  My <- lgb_matrix(fy)
  saveRDS(data.table(CO_PESSOA_FISICA = fy$CO_PESSOA_FISICA, NU_ANO = y, evadiu = fy$evadiu,
                     r_logit = as.numeric(predict(cvfit, Xy, s = "lambda.min", type = "response")),
                     r_gam   = as.numeric(predict(gam_fit, Dy, type = "response")),
                     r_lgb   = predict(lgb_fit, My, type = "response")),
          file.path(OUTPUT_DIR, sprintf("risk_%d.rds", y)))
  rm(fy, Dy, Xy, My); gc(verbose = FALSE)
  message("  scored ", y)
}

message("\n=== Test-year metrics (", TEST_YEAR, ") ===")
print(metrics[, .(model, auc_roc = round(auc_roc, 3), auc_pr = round(auc_pr, 3),
                  brier = round(brier, 4))])
message("\nOutputs in ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
