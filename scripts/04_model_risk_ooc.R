###############################################################################
# 04_model_risk_ooc.R  (out-of-core version of 04_model_risk.R)
#
# Memory-bounded training + scoring. The three models are trained without ever
# holding all labeled years at once:
#   * glmnet (LASSO) and GAM are inherently in-memory; at national scale 120M
#     train rows cannot fit 12 GB, so they train on a STREAMED, capped, per-year
#     training sample (SAMPLE_MAX). For linear/smooth models the sampling error
#     at millions of rows is negligible. The preprocessing spec (factor levels,
#     train medians) is derived from this sample; unseen factor levels at score
#     time map to "MISSING", exactly as in-core.
#   * LightGBM (the model feeding Half B) can use ALL train rows when LGB_FULL=1
#     via a file-backed lgb.Dataset (binned in C++, ~1 byte/value); otherwise it
#     also trains on the sample.
# SCORING is always over EVERY student-year, streamed one (year, partition) at a
# time, so the full risk table is produced within a bounded footprint.
#
# Inputs:  <FEATURES_DIR>/features_{YEAR}_p{K}.rds, <LABELS_DIR>/_label_report.csv
# Outputs: <OUTPUT_DIR>/risk_{YEAR}_p{K}.rds  +  _metrics/_calibration/_threshold/…
###############################################################################

suppressPackageStartupMessages({
  library(data.table); library(Matrix); library(mgcv); library(glmnet); library(lightgbm)
})
source(file.path("scripts", "ooc_common.R"))

FEATURES_DIR <- Sys.getenv("MODEL_FEAT",   "data/features_ooc")
LABELS_DIR   <- Sys.getenv("MODEL_LABELS", "data/labels_ooc")
OUTPUT_DIR   <- Sys.getenv("MODEL_OUT",    "data/model_ooc")
TARGET      <- "evadiu"
TRAIN_YEARS <- eval(parse(text = Sys.getenv("MODEL_TRAIN", "2019:2021")))
VAL_YEAR    <- as.integer(Sys.getenv("MODEL_VAL",  "2022"))
TEST_YEAR   <- as.integer(Sys.getenv("MODEL_TEST", "2023"))
SCORE_YEARS <- eval(parse(text = Sys.getenv("MODEL_SCORE", "2019:2024")))
SAMPLE_MAX  <- as.numeric(Sys.getenv("MODEL_SAMPLE", "5e6"))   # cap for glmnet/GAM (and lgb if !LGB_FULL)
# LGB_FULL=1 trains LightGBM on ALL train rows via a file-backed Dataset, but the
# C++ text parser is very slow at scale (10 GB CSV -> >1h just to construct), so
# the default is 0: LightGBM trains on the same large sample as glmnet/GAM, which
# is memory-safe, fast, and statistically ample for a boosted model at millions
# of rows. Set LGB_FULL=1 only when training time is not a concern.
LGB_FULL    <- as.integer(Sys.getenv("LGB_FULL", "0"))
RECALL_KS <- c(0.05,0.10,0.20); FBETA <- 2
DROP_COLS <- c("CO_PESSOA_FISICA","ID_MATRICULA","ID_TURMA","CO_ENTIDADE","CO_UF_NASC",
  "CO_MUNICIPIO_NASC","CO_PAIS_RESIDENCIA","CO_UF_END","CO_MUNICIPIO_END","NU_ANO")
GAM_SMOOTH <- c("NU_IDADE_REFERENCIA","defasagem_idade_serie","n_reprovacoes_prev",
  "anos_observados","indice_infra","tamanho_escola","tamanho_turma","apoio_por_aluno")
GAM_PARAM  <- c("TP_SEXO","TP_COR_RACA","TP_DEPENDENCIA","TP_LOCALIZACAO","turno","flag_defasado",
  "reprovou_ano_ant","abandono_previo","mudou_municipio","IN_NECESSIDADE_ESPECIAL",
  "IN_TROCA_ESCOLA","IN_TRANSFERIDO")
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)
set.seed(42)
try(mem.maxVSize(1024 * 1024), silent = TRUE)   # lift macOS R vector cap (no-op on Windows)

# ─── METRICS (identical to in-core) ──────────────────────────────────────────
auc_roc <- function(y,p){n1<-as.numeric(sum(y==1));n0<-as.numeric(sum(y==0))
  if(n1==0||n0==0)return(NA_real_)
  (sum(rank(p)[y==1])-n1*(n1+1)/2)/(n1*n0)}   # doubles: avoid int overflow at scale
auc_pr <- function(y,p){o<-order(p,decreasing=TRUE);y<-y[o];tp<-cumsum(y);fp<-cumsum(1-y)
  prec<-tp/(tp+fp);rec<-tp/sum(y);sum(prec*c(rec[1],diff(rec)))}
rec_prec_at_k <- function(y,p,k){top<-order(p,decreasing=TRUE)[seq_len(ceiling(k*length(y)))]
  c(recall=sum(y[top]==1)/sum(y==1),precision=sum(y[top]==1)/length(top))}
brier <- function(y,p) mean((p-y)^2)
calibration_bins <- function(y,p,model,year,nb=10){
  b<-cut(p,breaks=seq(0,1,length.out=nb+1),include.lowest=TRUE)
  data.table(model=model,year=year,bin=b,p_pred=p,y=y)[,.(n=.N,mean_pred=mean(p_pred),
    obs_rate=mean(y)),by=.(model,year,bin)]}
eval_model <- function(y,p,model,year){rk<-sapply(RECALL_KS,function(k)rec_prec_at_k(y,p,k))
  out<-data.table(model=model,year=year,auc_roc=auc_roc(y,p),auc_pr=auc_pr(y,p),brier=brier(y,p))
  for(i in seq_along(RECALL_KS)){out[[sprintf("recall@%d",round(RECALL_KS[i]*100))]]<-rk["recall",i]
    out[[sprintf("precision@%d",round(RECALL_KS[i]*100))]]<-rk["precision",i]};out}
# O(n log n): sort by score once, sweep cutpoints (top-i as positive). The old
# O(u*n) loop over unique thresholds was catastrophic at millions of rows.
pick_threshold <- function(y,p,beta){P<-sum(y==1);if(P==0)return(0)
  o<-order(p,decreasing=TRUE);ys<-y[o];ps<-p[o]
  tp<-cumsum(ys);fp<-cumsum(1-ys)
  prec<-tp/(tp+fp);rec<-tp/P
  f<-(1+beta^2)*prec*rec/(beta^2*prec+rec);f[is.na(f)]<-0
  ps[which.max(f)]}

# ─── STREAM HELPERS ──────────────────────────────────────────────────────────
part_files_year <- function(y) Filter(file.exists,
  lapply(0:(K_PARTS-1L), function(k) part_path(FEATURES_DIR, "features", y, k)))

# labeled-train row count (from 02's report) -> per-year sampling fraction
lab_rep <- fread(file.path(LABELS_DIR, "_label_report.csv"))
n_train <- sum(lab_rep[year %in% TRAIN_YEARS]$lab)
frac <- min(1, SAMPLE_MAX / max(n_train, 1))
message(sprintf("train rows(labeled)~%s  sample frac=%.4f  (SAMPLE_MAX=%s)",
                format(n_train, big.mark=","), frac, format(SAMPLE_MAX, scientific=FALSE)))

# read one labeled year, keep labeled rows, optionally Bernoulli-subsample
read_labeled <- function(y, fr) rbindlist(lapply(part_files_year(y), function(f){
  d <- readRDS(f); d <- d[!is.na(get(TARGET))]
  if (fr < 1 && nrow(d)) d <- d[sample.int(.N, round(.N * fr))]
  drop <- intersect(DROP_COLS, names(d)); if (length(drop)) d[, (drop) := NULL]  # solta IDs pesados
  d}), fill = TRUE)

# ─── BUILD TRAINING SAMPLE + SPEC ────────────────────────────────────────────
train_s <- rbindlist(lapply(TRAIN_YEARS, function(y) read_labeled(y, frac)), fill = TRUE)
val_s   <- read_labeled(VAL_YEAR,  frac)
message(sprintf("  train sample=%s  val sample=%s  peak=%.2fGB",
                format(nrow(train_s), big.mark=","), format(nrow(val_s), big.mark=","), peak_rss_gb()))

feat_all <- setdiff(names(train_s), c(DROP_COLS, TARGET))
nu <- train_s[, lapply(.SD, function(x) length(unique(x[!is.na(x)]))), .SDcols = feat_all]
feat_all <- feat_all[as.integer(nu[1]) > 1]
is_factor <- function(col) is.character(train_s[[col]]) || startsWith(col, "TP_")
factor_cols  <- feat_all[vapply(feat_all, is_factor, logical(1))]
numeric_cols <- setdiff(feat_all, factor_cols)
lvls <- lapply(factor_cols, function(c){L<-levels(addNA(factor(train_s[[c]]),ifany=TRUE))
  L[is.na(L)]<-"MISSING";L}); names(lvls) <- factor_cols
meds <- vapply(numeric_cols, function(c) median(train_s[[c]], na.rm = TRUE), numeric(1))
miss_cols <- numeric_cols[vapply(numeric_cols, function(c) anyNA(train_s[[c]]), logical(1))]

apply_prep <- function(dt){D<-copy(dt[,..feat_all])
  for(c in factor_cols){v<-as.character(D[[c]]);v[is.na(v)|!v%in%lvls[[c]]]<-"MISSING"
    set(D,j=c,value=factor(v,levels=lvls[[c]]))}
  for(c in numeric_cols){x<-D[[c]]
    if(c%in%miss_cols)set(D,j=paste0(c,"_isna"),value=as.integer(is.na(x)))
    if(anyNA(x))set(D,which(is.na(x)),c,meds[[c]])};D}
lgb_matrix <- function(dt){Dl<-copy(dt[,..feat_all])
  for(c in factor_cols)set(Dl,j=c,value=as.integer(factor(as.character(Dl[[c]]),levels=lvls[[c]])))
  as.matrix(Dl)}

# ─── TRAIN glmnet + GAM (on sample) ──────────────────────────────────────────
Dtr <- apply_prep(train_s); Xtr <- sparse.model.matrix(~ . - 1, data = Dtr); xcols <- colnames(Xtr)
cvfit <- cv.glmnet(Xtr, train_s$evadiu, family = "binomial", type.measure = "auc")

gam_smooth <- intersect(GAM_SMOOTH, numeric_cols); gam_param <- intersect(GAM_PARAM, feat_all)
k_for <- function(col) min(10L, length(unique(Dtr[[col]])) - 1L)
sm <- vapply(gam_smooth, function(v) if (k_for(v) >= 3) sprintf("s(%s, k=%d)", v, k_for(v)) else v, character(1))
form <- as.formula(paste("evadiu ~", paste(c(sm, sprintf("`%s`", gam_param)), collapse = " + ")))
Dg <- copy(Dtr); Dg[, evadiu := train_s$evadiu]
gam_fit <- bam(form, family = binomial, data = Dg, discrete = TRUE)
# Free the big sparse design + prepped table before LightGBM builds its Dataset
# (that was the >16GB spike: sample matrices + models + lgb construct at once).
rm(Dg, Dtr, Xtr); gc(FALSE)

# ─── TRAIN LightGBM (full via file, or sample) ───────────────────────────────
# LightGBM: ALL train rows via a file-backed Dataset (C++ bins ~1 byte/value).
# Falls back to the in-memory sample if the file path API misbehaves.
build_lgb_full <- function() {
  txt <- file.path(OUTPUT_DIR, "lgb_train.csv"); if (file.exists(txt)) file.remove(txt)
  first <- TRUE
  for (y in TRAIN_YEARS) for (f in part_files_year(y)) {
    d <- readRDS(f); d <- d[!is.na(evadiu)]; if (!nrow(d)) next
    fwrite(cbind(data.table(label = d$evadiu), as.data.table(lgb_matrix(d))),
           txt, sep = ",", append = !first, col.names = first); first <- FALSE; rm(d)
  }
  # file-backed: categorical_feature must be 0-based FEATURE indices (label excluded)
  lgb.Dataset(txt, params = list(label_column = "name:label", header = TRUE),
              categorical_feature = as.integer(match(factor_cols, feat_all) - 1L))
}
dtrain <- NULL
if (LGB_FULL == 1L)
  dtrain <- tryCatch({ ds <- build_lgb_full(); ds$construct(); ds },
              error = function(e){ message("  [lgb] file-backed failed (",
                conditionMessage(e), "); using sample"); NULL })
if (is.null(dtrain))
  dtrain <- lgb.Dataset(lgb_matrix(train_s), label = train_s$evadiu, categorical_feature = factor_cols)
val_lgbm <- lgb.Dataset.create.valid(dtrain, lgb_matrix(val_s), label = val_s$evadiu)
lgb_fit <- lgb.train(params = list(objective="binary", metric="auc", learning_rate=0.05,
                     num_leaves=31, min_data_in_leaf=20, verbose=-1),
                     data = dtrain, valids = list(val = val_lgbm),
                     nrounds = 500, early_stopping_rounds = 30)
rm(val_lgbm, dtrain, train_s, val_s); gc(FALSE)   # only the fitted models are needed now

# ─── SCORE EVERY (year, partition) + collect val/test predictions ────────────
# Score one partition in SCORE_BLK-row sub-blocks so the design matrix / GAM
# prediction frame never scale with the (large) partition size.
SCORE_BLK <- as.integer(Sys.getenv("MODEL_SCORE_BLK", "1000000"))
score_dt <- function(d){D<-apply_prep(d);X<-sparse.model.matrix(~ . -1,data=D)
  miss<-setdiff(xcols,colnames(X))
  if(length(miss))X<-cbind(X,Matrix(0,nrow(X),length(miss),sparse=TRUE,dimnames=list(NULL,miss)))
  X<-X[,xcols,drop=FALSE];M<-lgb_matrix(d)
  list(logit=as.numeric(predict(cvfit,X,s="lambda.min",type="response")),
       gam=as.numeric(predict(gam_fit,D,type="response")),
       lgb=predict(lgb_fit,M,type="response"))}

collect <- list()   # for val/test metrics
for (y in SCORE_YEARS) {
  for (k in 0:(K_PARTS-1L)) {
    f <- part_path(FEATURES_DIR, "features", y, k); if (!file.exists(f)) next
    d <- readRDS(f)
    keep <- intersect(c(feat_all, "CO_PESSOA_FISICA", "evadiu"), names(d)); d <- d[, ..keep]
    nb <- nrow(d); res <- list()
    for (s in seq(1L, nb, by = SCORE_BLK)) {
      e <- min(s + SCORE_BLK - 1L, nb); db <- d[s:e]; pr <- score_dt(db)
      res[[length(res)+1L]] <- data.table(CO_PESSOA_FISICA=db$CO_PESSOA_FISICA, NU_ANO=y,
        evadiu=db$evadiu, r_logit=pr$logit, r_gam=pr$gam, r_lgb=pr$lgb)
      rm(db, pr)
    }
    out <- rbindlist(res); rm(res, d)
    save_rds(out, part_path(OUTPUT_DIR, "risk", y, k))
    if (y %in% c(VAL_YEAR, TEST_YEAR)) {
      lab <- !is.na(out$evadiu)
      collect[[length(collect)+1L]] <- data.table(year=y, y=out$evadiu[lab],
        logit=out$r_logit[lab], gam=out$r_gam[lab], lgb=out$r_lgb[lab])
    }
    rm(out); gc(FALSE)
  }
  message(sprintf("  scored %d  peak=%.2fGB", y, peak_rss_gb()))
}
C <- rbindlist(collect); rm(collect)
pv <- lapply(c("logit","gam","lgb"), function(m) C[year==VAL_YEAR][[m]]); names(pv)<-c("logit","gam","lgb")
pt <- lapply(c("logit","gam","lgb"), function(m) C[year==TEST_YEAR][[m]]); names(pt)<-c("logit","gam","lgb")
yval <- C[year==VAL_YEAR]$y; yte <- C[year==TEST_YEAR]$y

metrics <- rbindlist(lapply(names(pt), function(m) eval_model(yte, pt[[m]], m, TEST_YEAR)), fill=TRUE)
calib   <- rbindlist(lapply(names(pt), function(m) calibration_bins(yte, pt[[m]], m, TEST_YEAR)), fill=TRUE)
thr <- rbindlist(lapply(names(pt), function(m){th<-pick_threshold(yval,pv[[m]],FBETA);pr<-pt[[m]]>=th
  data.table(model=m,threshold=th,recall=sum(pr&yte==1)/sum(yte==1),
    precision=if(sum(pr)>0)sum(pr&yte==1)/sum(pr)else NA,flagged=sum(pr))}))
# SHAP importance on a capped sample of the test year (re-read one partition).
ftest <- part_path(FEATURES_DIR, "features", TEST_YEAR, 0L)
if (file.exists(ftest)) {
  ds <- readRDS(ftest); ds <- ds[!is.na(evadiu)]
  if (nrow(ds) > 200000L) ds <- ds[sample.int(.N, 200000L)]
  Ms <- lgb_matrix(ds); sh <- predict(lgb_fit, Ms, type = "contrib")
  imp <- data.table(feature = c(colnames(Ms), "BIAS"), mean_abs_shap = colMeans(abs(sh)))[
    feature != "BIAS"][order(-mean_abs_shap)]
  fwrite(imp, file.path(OUTPUT_DIR, "_lgb_importance.csv"), sep = ";", bom = TRUE)
  rm(ds, Ms, sh)
}
fwrite(metrics, file.path(OUTPUT_DIR,"_metrics.csv"), sep=";", bom=TRUE, na="")
fwrite(calib,   file.path(OUTPUT_DIR,"_calibration.csv"), sep=";", bom=TRUE, na="")
fwrite(thr,     file.path(OUTPUT_DIR,"_threshold.csv"), sep=";", bom=TRUE, na="")
capture.output(summary(gam_fit), file=file.path(OUTPUT_DIR,"_gam_summary.txt"))
saveRDS(gam_fit, file.path(OUTPUT_DIR,"model_gam.rds"))

message("\n=== test metrics (", TEST_YEAR, ") ===")
print(metrics[, .(model, auc_roc=round(auc_roc,3), auc_pr=round(auc_pr,3), brier=round(brier,4))])
message(sprintf("Done (peak %.2f GB). Output in %s", peak_rss_gb(),
                normalizePath(OUTPUT_DIR, mustWork=FALSE)))
