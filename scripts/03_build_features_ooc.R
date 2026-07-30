###############################################################################
# 03_build_features_ooc.R  (out-of-core version of 03_build_features.R)
#
# Same features as 03, but memory-bounded:
#   * PER-YEAR context that needs a global reduce (tamanho_escola = enrollment
#     count by school) is precomputed once by streaming MATRICULA partitions;
#     the resulting school/class feature tables are tiny and cached.
#   * The history backbone and every per-student feature are PARTITION-LOCAL:
#     a student's whole history is in one partition, so temporal features,
#     defasagem and mudou_municipio match the in-core result exactly.
#
# Inputs:  <LABELS_DIR>/person_year_{YEAR}_p{K}.rds
#          <CLEAN_DIR>/BAS_{MATRICULA,SITUACAO}_{YEAR}_p{K}_b*.rds, BAS_{ESCOLA,TURMA}_{YEAR}.rds
# Outputs: <OUTPUT_DIR>/features_{YEAR}_p{K}.rds
###############################################################################

suppressPackageStartupMessages(library(data.table))
source(file.path("scripts", "ooc_common.R"))

CLEAN_DIR     <- Sys.getenv("FEAT_CLEAN",  "data/clean_ooc")
LABELS_DIR    <- Sys.getenv("FEAT_LABELS", "data/labels_ooc")
OUTPUT_DIR    <- Sys.getenv("FEAT_OUT",    "data/features_ooc")
FEATURE_YEARS <- eval(parse(text = Sys.getenv("FEAT_YEARS", "2019:2024")))
HISTORY_START <- as.integer(Sys.getenv("FEAT_HIST", "2017"))
DEFASAGEM_FLAG_MIN <- 2L
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

AGE_BY_ETAPA <- c("14"=6L,"15"=7L,"16"=8L,"17"=9L,"18"=10L,"19"=11L,"20"=12L,"21"=13L,
  "41"=14L,"25"=15L,"26"=16L,"27"=17L,"28"=18L,"30"=15L,"31"=16L,"32"=17L,"33"=18L,
  "35"=15L,"36"=16L,"37"=17L,"38"=18L)
turno_from_hour <- function(h)
  fifelse(is.na(h), NA_character_, fifelse(h < 12, "manha", fifelse(h < 18, "tarde", "noite")))
INFRA_ITEMS <- c("IN_AGUA_POTAVEL","IN_AGUA_REDE_PUBLICA","IN_ENERGIA_REDE_PUBLICA",
  "IN_ESGOTO_REDE_PUBLICA","IN_BANHEIRO","IN_BANHEIRO_PNE","IN_BIBLIOTECA_SALA_LEITURA",
  "IN_LABORATORIO_CIENCIAS","IN_LABORATORIO_INFORMATICA","IN_QUADRA_ESPORTES","IN_COMPUTADOR",
  "IN_EQUIP_MULTIMIDIA","IN_DESKTOP_ALUNO","IN_INTERNET","IN_BANDA_LARGA","IN_INTERNET_ALUNOS",
  "IN_ACESSO_INTERNET_COMPUTADOR","IN_ALIMENTACAO")
SUPPORT_PROF <- c("QT_PROF_COORDENADOR","QT_PROF_FONAUDIOLOGO","QT_PROF_NUTRICIONISTA",
                  "QT_PROF_PSICOLOGO","QT_PROF_PEDAGOGIA")
ESCOLA_FEATURES <- c("TP_DEPENDENCIA","TP_LOCALIZACAO","TP_CATEGORIA_ESCOLA_PRIVADA",
                     INFRA_ITEMS, SUPPORT_PROF)
DROP_FROM_BASE <- c("TP_SITUACAO","IN_FALECIDO")

read_clean_whole <- function(tbl, yr) {
  p <- file.path(CLEAN_DIR, sprintf("%s_%d.rds", tbl, yr))
  if (file.exists(p)) readRDS(p) else NULL
}

# ─── GLOBAL REDUCE: school size (stream MATRICULA partitions) ─────────────────
school_size <- function(t) {
  acc <- NULL
  for (k in 0:(K_PARTS - 1L)) {
    m <- read_partition(CLEAN_DIR, "BAS_MATRICULA", t, k)
    if (is.null(m)) next
    s <- m[, .(n = .N), by = CO_ENTIDADE]
    acc <- if (is.null(acc)) s else rbindlist(list(acc, s))[, .(n = sum(n)), by = CO_ENTIDADE]
    rm(m, s)
  }
  if (is.null(acc)) data.table(CO_ENTIDADE = character(), tamanho_escola = integer())
  else setnames(acc, "n", "tamanho_escola")[]
}

# ─── PER-YEAR CONTEXT (small; cached) ────────────────────────────────────────
school_feat <- function(t) {
  esc <- read_clean_whole("BAS_ESCOLA", t)
  esc <- esc[, c("CO_ENTIDADE", intersect(ESCOLA_FEATURES, names(esc))), with = FALSE]
  esc[, indice_infra := rowMeans(as.matrix(.SD), na.rm = TRUE),
      .SDcols = intersect(INFRA_ITEMS, names(esc))]
  esc[, apoio_total := rowSums(as.matrix(.SD), na.rm = TRUE),
      .SDcols = intersect(SUPPORT_PROF, names(esc))]
  esc <- merge(esc, school_size(t), by = "CO_ENTIDADE", all.x = TRUE)
  esc[, apoio_por_aluno := fifelse(tamanho_escola > 0, apoio_total / tamanho_escola, NA_real_)]
  esc[, apoio_total := NULL]
  esc
}
class_feat <- function(t) {
  tur <- read_clean_whole("BAS_TURMA", t)
  tur <- tur[, .(ID_TURMA, TX_HR_INICIAL, tamanho_turma = QT_MATRICULAS)]
  tur[, turno := turno_from_hour(TX_HR_INICIAL)][]
}

# ─── PARTITION-LOCAL HISTORY BACKBONE ────────────────────────────────────────
build_backbone_k <- function(k) {
  years <- HISTORY_START:max(FEATURE_YEARS); out <- list()
  for (y in years) {
    mat <- read_partition(CLEAN_DIR, "BAS_MATRICULA", y, k)
    if (is.null(mat)) next
    pm <- mat[, .(municipio = CO_MUNICIPIO_END[1], present = 1L), by = CO_PESSOA_FISICA]
    sit <- read_partition(CLEAN_DIR, "BAS_SITUACAO", y, k)
    if (!is.null(sit)) {
      ps <- sit[, .(reprovou  = as.integer(any(TP_SITUACAO == 4, na.rm = TRUE)),
                    abandonou = as.integer(any(TP_SITUACAO == 2, na.rm = TRUE))),
                by = CO_PESSOA_FISICA]
      pm <- merge(pm, ps, by = "CO_PESSOA_FISICA", all.x = TRUE)
    } else pm[, `:=`(reprovou = NA_integer_, abandonou = NA_integer_)]
    pm[, year := y]; out[[length(out) + 1L]] <- pm
    rm(mat, sit)
  }
  rbindlist(out, fill = TRUE)
}
temporal_features <- function(backbone, t) {
  prior <- backbone[year >= HISTORY_START & year < t]
  cum <- prior[, .(n_reprovacoes_prev = sum(reprovou, na.rm = TRUE),
                   abandono_previo = as.integer(any(abandonou == 1, na.rm = TRUE)),
                   anos_observados = uniqueN(year)), by = CO_PESSOA_FISICA]
  lag <- backbone[year == t - 1L,
                  .(CO_PESSOA_FISICA, reprovou_ano_ant = reprovou, municipio_ant = municipio)]
  feats <- merge(cum, lag, by = "CO_PESSOA_FISICA", all = TRUE)
  feats[is.na(n_reprovacoes_prev), n_reprovacoes_prev := 0L]
  feats[is.na(abandono_previo),    abandono_previo := 0L]
  feats[is.na(anos_observados),    anos_observados := 0L]
  feats[, tem_historico := as.integer(anos_observados > 0L)]
  feats[]
}

# ─── ONE (year, partition) ───────────────────────────────────────────────────
build_features <- function(t, k, backbone_k, esc_t, cls_t) {
  f <- part_path(LABELS_DIR, "person_year", t, k)
  if (!file.exists(f)) return(0L)
  py <- readRDS(f)
  py[, (intersect(DROP_FROM_BASE, names(py))) := NULL]

  py[, idade_esperada := AGE_BY_ETAPA[as.character(TP_ETAPA_ENSINO)]]
  py[, defasagem_idade_serie := NU_IDADE_REFERENCIA - idade_esperada]
  py[, flag_defasado := as.integer(defasagem_idade_serie >= DEFASAGEM_FLAG_MIN)]
  py[, idade_esperada := NULL]

  py <- merge(py, temporal_features(backbone_k, t), by = "CO_PESSOA_FISICA", all.x = TRUE)
  py[is.na(n_reprovacoes_prev), n_reprovacoes_prev := 0L]
  py[is.na(abandono_previo),    abandono_previo := 0L]
  py[is.na(anos_observados),    anos_observados := 0L]
  py[is.na(tem_historico),      tem_historico := 0L]
  py[, mudou_municipio := fifelse(is.na(municipio_ant), NA_integer_,
                                  as.integer(CO_MUNICIPIO_END != municipio_ant))]
  py[, municipio_ant := NULL]

  py <- merge(py, esc_t, by = "CO_ENTIDADE", all.x = TRUE)
  py <- merge(py, cls_t, by = "ID_TURMA",    all.x = TRUE)
  save_rds(py, part_path(OUTPUT_DIR, "features", t, k))
  nrow(py)
}

# ─── MAIN ────────────────────────────────────────────────────────────────────
t0 <- Sys.time()
message("03_ooc — K=", K_PARTS, "  years=", paste(range(FEATURE_YEARS), collapse="-"),
        "  history from ", HISTORY_START)

message("Precomputing per-year context (school size reduce + escola/turma) ...")
ESC <- list(); CLS <- list()
for (t in FEATURE_YEARS) { ESC[[as.character(t)]] <- school_feat(t); CLS[[as.character(t)]] <- class_feat(t) }

nrows <- 0L
for (k in 0:(K_PARTS - 1L)) {
  bk <- build_backbone_k(k)
  for (t in FEATURE_YEARS)
    nrows <- nrows + build_features(t, k, bk, ESC[[as.character(t)]], CLS[[as.character(t)]])
  rm(bk); gc(FALSE)
  message(sprintf("  partition %d done  peak=%.2fGB", k, peak_rss_gb()))
}
message(sprintf("\nDone in %.1f s (peak %.2f GB). %s feature rows in %s",
                as.numeric(difftime(Sys.time(), t0, units = "secs")), peak_rss_gb(),
                format(nrows, big.mark = ","), normalizePath(OUTPUT_DIR, mustWork = FALSE)))
