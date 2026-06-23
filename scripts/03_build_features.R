###############################################################################
# 03_build_features.R
#
# Build the modeling feature matrix on top of the person-year base from 02.
#
# Inputs:  data/labels/person_year_{YEAR}.rds   (base, 2019-2024)
#          data/clean/{BAS_ESCOLA,BAS_TURMA,BAS_MATRICULA,BAS_SITUACAO}_{YEAR}.rds
# Outputs: data/features/features_{YEAR}.rds / .csv
#          data/features/_feature_summary.csv   (missingness + numeric stats)
#          data/features/_feature_catalog.csv   (name, group, origin)
###############################################################################

# ─── CONFIG ──────────────────────────────────────────────────────────────────
CLEAN_DIR     <- "/Users/cc9/Documents/GitHub/dropout-research/data/clean"
LABELS_DIR    <- "/Users/cc9/Documents/GitHub/dropout-research/data/labels"
OUTPUT_DIR    <- "/Users/cc9/Documents/GitHub/dropout-research/data/features"
FEATURE_YEARS <- 2019:2024
HISTORY_START <- 2017L          # lookback history
USE_IBGE      <- FALSE          # municipal SES join
IBGE_PATH     <- NULL

DEFASAGEM_FLAG_MIN <- 2L         # over-age threshold

# Official expected age per TP_ETAPA_ENSINO
AGE_BY_ETAPA <- c(
  "14" = 6L,  "15" = 7L,  "16" = 8L,  "17" = 9L,  "18" = 10L, # EF 9 anos 1º-5º
  "19" = 11L, "20" = 12L, "21" = 13L, "41" = 14L,             # EF 9 anos 6º-9º
  "25" = 15L, "26" = 16L, "27" = 17L, "28" = 18L,             # EM regular
  "30" = 15L, "31" = 16L, "32" = 17L, "33" = 18L,             # EM integrado
  "35" = 15L, "36" = 16L, "37" = 17L, "38" = 18L              # EM normal/magistério
)

# Turno cutoffs on TX_HR_INICIAL (class start hour).
turno_from_hour <- function(h) {
  fifelse(is.na(h), NA_character_,
    fifelse(h < 12, "manha", fifelse(h < 18, "tarde", "noite")))
}

# Infrastructure items that compose indice_infra.
INFRA_ITEMS <- c(
  "IN_AGUA_POTAVEL", "IN_AGUA_REDE_PUBLICA", "IN_ENERGIA_REDE_PUBLICA",
  "IN_ESGOTO_REDE_PUBLICA", "IN_BANHEIRO", "IN_BANHEIRO_PNE",
  "IN_BIBLIOTECA_SALA_LEITURA", "IN_LABORATORIO_CIENCIAS",
  "IN_LABORATORIO_INFORMATICA", "IN_QUADRA_ESPORTES", "IN_COMPUTADOR",
  "IN_EQUIP_MULTIMIDIA", "IN_DESKTOP_ALUNO", "IN_INTERNET", "IN_BANDA_LARGA",
  "IN_INTERNET_ALUNOS", "IN_ACESSO_INTERNET_COMPUTADOR", "IN_ALIMENTACAO"
)

# Support professionals counted in apoio_por_aluno.
SUPPORT_PROF <- c("QT_PROF_COORDENADOR", "QT_PROF_FONAUDIOLOGO",
                  "QT_PROF_NUTRICIONISTA", "QT_PROF_PSICOLOGO",
                  "QT_PROF_PEDAGOGIA")

# School columns carried as features.
ESCOLA_FEATURES <- c("TP_DEPENDENCIA", "TP_LOCALIZACAO",
                     "TP_CATEGORIA_ESCOLA_PRIVADA", INFRA_ITEMS, SUPPORT_PROF)

# Year-t outcome columns dropped from the base to avoid label leakage.
DROP_FROM_BASE <- c("TP_SITUACAO", "IN_FALECIDO")

# ─── PACKAGES ────────────────────────────────────────────────────────────────
suppressPackageStartupMessages(library(data.table))
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

read_clean <- function(tbl, yr) {
  path <- file.path(CLEAN_DIR, sprintf("%s_%d.rds", tbl, yr))
  if (file.exists(path)) readRDS(path) else NULL
}

# ─── HISTORY BACKBONE ────────────────────────────────────────────────────────
build_backbone <- function() {
  years <- HISTORY_START:max(FEATURE_YEARS)
  out <- vector("list", length(years))
  for (i in seq_along(years)) {
    y   <- years[i]
    mat <- read_clean("BAS_MATRICULA", y)
    if (is.null(mat)) next
    pm <- mat[, .(municipio = CO_MUNICIPIO_END[1], present = 1L),
              by = CO_PESSOA_FISICA]

    sit <- read_clean("BAS_SITUACAO", y)
    if (!is.null(sit)) {
      ps <- sit[, .(reprovou  = as.integer(any(TP_SITUACAO == 4, na.rm = TRUE)),
                    abandonou = as.integer(any(TP_SITUACAO == 2, na.rm = TRUE))),
                by = CO_PESSOA_FISICA]
      pm <- merge(pm, ps, by = "CO_PESSOA_FISICA", all.x = TRUE)
    } else {
      pm[, `:=`(reprovou = NA_integer_, abandonou = NA_integer_)]
    }
    pm[, year := y]
    out[[i]] <- pm
  }
  rbindlist(out, fill = TRUE)
}

# Temporal features for modeling year t from the backbone.
temporal_features <- function(backbone, t) {
  prior <- backbone[year >= HISTORY_START & year < t]
  cum <- prior[, .(
    n_reprovacoes_prev = sum(reprovou, na.rm = TRUE),
    abandono_previo    = as.integer(any(abandonou == 1, na.rm = TRUE)),
    anos_observados    = uniqueN(year)
  ), by = CO_PESSOA_FISICA]

  lag <- backbone[year == t - 1L,
                  .(CO_PESSOA_FISICA, reprovou_ano_ant = reprovou,
                    municipio_ant = municipio)]

  feats <- merge(cum, lag, by = "CO_PESSOA_FISICA", all = TRUE)
  feats[is.na(n_reprovacoes_prev), n_reprovacoes_prev := 0L]
  feats[is.na(abandono_previo),    abandono_previo := 0L]
  feats[is.na(anos_observados),    anos_observados := 0L]
  feats[, tem_historico := as.integer(anos_observados > 0L)]
  feats[]
}

# ─── SCHOOL / CLASS CONTEXT ──────────────────────────────────────────────────
school_features <- function(t) {
  esc <- read_clean("BAS_ESCOLA", t)
  esc <- esc[, c("CO_ENTIDADE", intersect(ESCOLA_FEATURES, names(esc))),
             with = FALSE]
  esc[, indice_infra := rowMeans(as.matrix(.SD), na.rm = TRUE),
      .SDcols = intersect(INFRA_ITEMS, names(esc))]
  esc[, apoio_total := rowSums(as.matrix(.SD), na.rm = TRUE),
      .SDcols = intersect(SUPPORT_PROF, names(esc))]

  # School size = all enrollments in the school that year.
  mat  <- read_clean("BAS_MATRICULA", t)
  size <- mat[, .(tamanho_escola = .N), by = CO_ENTIDADE]
  esc  <- merge(esc, size, by = "CO_ENTIDADE", all.x = TRUE)
  esc[, apoio_por_aluno := fifelse(tamanho_escola > 0,
                                   apoio_total / tamanho_escola, NA_real_)]
  esc[, apoio_total := NULL]
  esc
}

class_features <- function(t) {
  tur <- read_clean("BAS_TURMA", t)
  tur <- tur[, .(ID_TURMA, TX_HR_INICIAL, tamanho_turma = QT_MATRICULAS)]
  tur[, turno := turno_from_hour(TX_HR_INICIAL)]
  tur
}

# ─── MUNICIPAL SES ──────────────────────────────────────────────────
# Joins external IBGE municipal indicators by CO_MUNICIPIO_END.
add_municipal_ses <- function(dt) {
  if (!USE_IBGE || is.null(IBGE_PATH) || !file.exists(IBGE_PATH)) return(dt)
  ibge <- fread(IBGE_PATH, sep = ";", encoding = "UTF-8",
                colClasses = list(character = "CO_MUNICIPIO"))
  setnames(ibge, "CO_MUNICIPIO", "CO_MUNICIPIO_END")
  merge(dt, ibge, by = "CO_MUNICIPIO_END", all.x = TRUE)
}

# ─── ONE YEAR ────────────────────────────────────────────────────────────────
build_features_year <- function(t, backbone) {
  py <- readRDS(file.path(LABELS_DIR, sprintf("person_year_%d.rds", t)))
  py[, (intersect(DROP_FROM_BASE, names(py))) := NULL]

  # Defasagem idade-série.
  py[, idade_esperada := AGE_BY_ETAPA[as.character(TP_ETAPA_ENSINO)]]
  py[, defasagem_idade_serie := NU_IDADE_REFERENCIA - idade_esperada]
  py[, flag_defasado := as.integer(defasagem_idade_serie >= DEFASAGEM_FLAG_MIN)]
  py[, idade_esperada := NULL]

  # Temporal history.
  py <- merge(py, temporal_features(backbone, t), by = "CO_PESSOA_FISICA",
              all.x = TRUE)
  py[is.na(n_reprovacoes_prev), n_reprovacoes_prev := 0L]
  py[is.na(abandono_previo),    abandono_previo := 0L]
  py[is.na(anos_observados),    anos_observados := 0L]
  py[is.na(tem_historico),      tem_historico := 0L]
  py[, mudou_municipio := fifelse(is.na(municipio_ant), NA_integer_,
                                  as.integer(CO_MUNICIPIO_END != municipio_ant))]
  py[, municipio_ant := NULL]

  # School and class context.
  py <- merge(py, school_features(t), by = "CO_ENTIDADE", all.x = TRUE)
  py <- merge(py, class_features(t),  by = "ID_TURMA",    all.x = TRUE)

  py <- add_municipal_ses(py)
  py[]
}

# Missingness + numeric stats per feature.
feature_summary <- function(dt, t) {
  rbindlist(lapply(names(dt), function(col) {
    x <- dt[[col]]; n <- length(x); nm <- sum(is.na(x))
    out <- data.table(year = t, feature = col, n = n, n_missing = nm,
                      pct_missing = round(100 * nm / max(n, 1), 3))
    if (is.numeric(x)) {
      v <- x[!is.na(x)]
      if (length(v) > 0) {
        qs <- quantile(v, c(0.01, 0.5, 0.99), names = FALSE)
        out[, `:=`(min = min(v), max = max(v), median = qs[2],
                   p1 = qs[1], p99 = qs[3])]
      }
    }
    out
  }), fill = TRUE)
}

# ─── MAIN ────────────────────────────────────────────────────────────────────
t0 <- Sys.time()
message("Building history backbone (", HISTORY_START, "-", max(FEATURE_YEARS), ")")
backbone <- build_backbone()

summaries <- list()
for (t in FEATURE_YEARS) {
  message(sprintf("  [%d] building features", t))
  feats <- build_features_year(t, backbone)
  base  <- file.path(OUTPUT_DIR, sprintf("features_%d", t))
  saveRDS(feats, paste0(base, ".rds"))
  fwrite(feats, paste0(base, ".csv"), sep = ";", bom = TRUE, na = "")
  summaries[[length(summaries) + 1]] <- feature_summary(feats, t)
  message(sprintf("    -> %d rows x %d features", nrow(feats), ncol(feats)))
  rm(feats); gc(verbose = FALSE)
}

fwrite(rbindlist(summaries, fill = TRUE),
       file.path(OUTPUT_DIR, "_feature_summary.csv"), sep = ";", bom = TRUE, na = "")

# Static catalog (name -> group, origin) for the dissertation dictionary.
catalog <- rbindlist(list(
  data.table(feature = "defasagem_idade_serie", grupo = "individual", origem = "gerada"),
  data.table(feature = "flag_defasado",         grupo = "individual", origem = "gerada"),
  data.table(feature = "n_reprovacoes_prev",    grupo = "temporal",   origem = "gerada"),
  data.table(feature = "reprovou_ano_ant",      grupo = "temporal",   origem = "gerada"),
  data.table(feature = "abandono_previo",       grupo = "temporal",   origem = "gerada"),
  data.table(feature = "anos_observados",       grupo = "temporal",   origem = "gerada"),
  data.table(feature = "tem_historico",         grupo = "temporal",   origem = "gerada"),
  data.table(feature = "mudou_municipio",       grupo = "comunitario",origem = "gerada"),
  data.table(feature = "IN_TROCA_ESCOLA",       grupo = "temporal",   origem = "gerada"),
  data.table(feature = "QT_ESCOLAS_ANO",        grupo = "temporal",   origem = "gerada"),
  data.table(feature = "turno",                 grupo = "escolar",    origem = "gerada"),
  data.table(feature = "tamanho_turma",         grupo = "escolar",    origem = "original"),
  data.table(feature = "tamanho_escola",        grupo = "escolar",    origem = "gerada"),
  data.table(feature = "indice_infra",          grupo = "escolar",    origem = "gerada"),
  data.table(feature = "apoio_por_aluno",       grupo = "escolar",    origem = "gerada")
))
fwrite(catalog, file.path(OUTPUT_DIR, "_feature_catalog.csv"), sep = ";", bom = TRUE)

message(sprintf("\nDone in %.1f s. Output in %s",
                as.numeric(difftime(Sys.time(), t0, units = "secs")),
                normalizePath(OUTPUT_DIR, mustWork = FALSE)))
