# =============================================================================
# 00_config.R
# This file defines paths, parameters, and constants used across all scripts.
# =============================================================================


# =============================================================================
# 1. PROJECT ROOT AND PATHS
# =============================================================================

# Locate project root
.find_project_root <- function() {

  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg) > 0) {
    script_dir <- dirname(normalizePath(sub("^--file=", "", file_arg[1])))
    return(dirname(script_dir))  # go up from scripts/
  }
  # Try sys.frame
  for (i in seq_len(sys.nframe())) {
    env <- sys.frame(i)
    if (!is.null(env$ofile)) {
      return(dirname(dirname(normalizePath(env$ofile))))
    }
  }
  # Fallback: assume working directory is project root
  return(getwd())
}

PROJECT_ROOT <- .find_project_root()

# -- Toggle: synthetic vs real data -------------------------------------
USE_SYNTHETIC <- TRUE

# -- Data paths ---------------------------------------------------------------
if (USE_SYNTHETIC) {
  DATA_DIR     <- file.path(PROJECT_ROOT, "data", "synthetic")
  DATA_FORMAT  <- "csv"  # "csv" or "sas"
} else {
  # >>> EDIT THESE <<<
  DATA_DIR     <- "/path/to/sedap/sas/library"
  DATA_FORMAT  <- "sas"
}

EXTERNAL_DIR   <- file.path(PROJECT_ROOT, "data", "external")
SCHEMAS_DIR    <- file.path(PROJECT_ROOT, "data", "schemas")

# -- Output paths -------------------------------------------------------------
OUTPUTS_DIR    <- file.path(PROJECT_ROOT, "outputs")
RESULTADOS_DIR <- file.path(OUTPUTS_DIR, "Resultados")

# -- Intermediate checkpoints -------------------------------------------------
CHECKPOINT_DIR <- file.path(OUTPUTS_DIR, "checkpoints")

# -- Logs ---------------------------------------------------------------------
LOG_DIR        <- file.path(OUTPUTS_DIR, "logs")

# -- Utils path ---------------------------------------------------------------
UTILS_DIR      <- file.path(PROJECT_ROOT, "scripts", "utils")

# Create directories
for (d in c(RESULTADOS_DIR, CHECKPOINT_DIR, LOG_DIR)) {
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
}


# =============================================================================
# 2. SOURCE UTILITIES
# =============================================================================

source(file.path(UTILS_DIR, "feature_functions.R"))


# =============================================================================
# 3. YEAR RANGES
# =============================================================================

YEARS          <- 2019L:2024L
SITUACAO_YEARS <- 2019L:2023L   # BAS_SITUACAO not available for 2024

# Temporal split
TRAIN_YEARS <- 2019L:2022L
VAL_YEAR    <- 2023L
TEST_YEAR   <- 2024L


# =============================================================================
# 4. TABLE NAMES AND FILE MAPPING
# =============================================================================

TABLES <- c("BAS_ESCOLA", "BAS_TURMA", "BAS_MATRICULA", "BAS_SITUACAO")

# Build file paths per table
get_table_path <- function(table_name, format = DATA_FORMAT, dir = DATA_DIR) {
  ext <- switch(format, csv = ".csv", sas = ".sas7bdat")
  file.path(dir, paste0(table_name, ext))
}

# CSV read parameters 
CSV_PARAMS <- list(
  sep        = ";",
  encoding   = "UTF-8",
  na.strings = c("", "NA")
)


# =============================================================================
# 5. EXPECTED SCHEMA 
# =============================================================================

EXPECTED_COLS <- list(
  BAS_ESCOLA = c(
    "NU_ANO", "CO_ENTIDADE", "CO_MUNICIPIO", "CO_UF",
    "TP_DEPENDENCIA", "TP_LOCALIZACAO",
    "IN_AGUA_POTAVEL", "IN_ENERGIA_REDE_PUBLICA", "IN_ESGOTO_REDE_PUBLICA",
    "IN_LIXO_SERVICO_COLETA", "IN_SALA_ATENDIMENTO_ESPECIAL",
    "IN_LABORATORIO_INFORMATICA", "IN_LABORATORIO_CIENCIAS",
    "IN_BIBLIOTECA", "IN_INTERNET",
    "IN_COMUM_FUND_AI", "IN_COMUM_FUND_AF",
    "IN_COMUM_MEDIO_MEDIO", "IN_EJA"
  ),
  BAS_TURMA = c(
    "NU_ANO", "CO_ENTIDADE", "ID_TURMA",
    "TP_ETAPA_ENSINO", "TP_MEDIACAO_DIDATICO_PEDAGOGICA",
    "NU_DURACAO_TURMA", "NU_MATRICULAS"
  ),
  BAS_MATRICULA = c(
    "NU_ANO", "CO_PESSOA_FISICA", "ID_MATRICULA", "CO_ENTIDADE", "ID_TURMA",
    "TP_ETAPA_ENSINO", "NU_IDADE_REFERENCIA", "TP_SEXO", "TP_COR_RACA",
    "TP_NACIONALIDADE", "TP_ZONA_RESIDENCIAL",
    "IN_NECESSIDADE_ESPECIAL", "IN_TRANSPORTE_PUBLICO"
  ),
  BAS_SITUACAO = c(
    "NU_ANO", "CO_PESSOA_FISICA", "ID_MATRICULA", "TP_SITUACAO"
  )
)


# =============================================================================
# 6. TARGET VARIABLE
# =============================================================================

# TP_SITUACAO codes
SIT_DROPOUT    <- 2L  # Abandono
SIT_DECEASED   <- 3L  # Falecido
SIT_FAILED     <- 4L  # Reprovado
SIT_APPROVED   <- 5L  # Aprovado
SIT_NO_INFO    <- 9L  # Sem informação

# Dropout definitions (used in 03_feature_engineering.R)
# "strict": only TP_SITUACAO == 2
# "broad":  strict + student absent next year without graduating
# "intermediate": strict + absent next year (excluding transfers)
DROPOUT_DEFINITION <- "strict"


# =============================================================================
# 7. SEDAP COMPLIANCE PARAMETERS
# =============================================================================

SEDAP <- list(
  min_cell_municipio = 10L,
  min_cell_estado    = 3L,
  min_cell_nacional  = 1L,
  suppression_char   = "X",
  zero_absolute      = "-",
  zero_rounding      = "0",
  not_applicable     = "..",
  not_available      = "...",
  decimal_places     = 1L,
  csv_separator      = ";"
)


# =============================================================================
# 8. FEATURE ENGINEERING PARAMETERS
# =============================================================================

# Infrastructure columns for composite score
INFRA_COLS <- c(
  "IN_AGUA_POTAVEL", "IN_ENERGIA_REDE_PUBLICA", "IN_ESGOTO_REDE_PUBLICA",
  "IN_LIXO_SERVICO_COLETA", "IN_SALA_ATENDIMENTO_ESPECIAL",
  "IN_LABORATORIO_INFORMATICA", "IN_LABORATORIO_CIENCIAS",
  "IN_BIBLIOTECA", "IN_INTERNET"
)

# Intervention proxy variables (for uplift modeling)
INTERVENTION_VARS <- c(
  "IN_TRANSPORTE_PUBLICO",
  "IN_NECESSIDADE_ESPECIAL"
)


# =============================================================================
# 9. MODEL HYPERPARAMETERS
# =============================================================================

# Logistic regression (baseline) — no tuning needed

# GAM (mgcv::bam)
HYPER_GAM <- list(
  family = "binomial",
  method = "fREML"
)

# Random Forest (ranger)
HYPER_RF <- list(
  num.trees     = c(500L, 1000L),
  mtry_fraction = c(0.3, 0.5),
  min.node.size = c(20L, 50L),
  sample.fraction = 0.8
)

# Gradient Boosting (xgboost)
HYPER_XGB <- list(
  nrounds     = c(200L, 500L, 1000L),
  max_depth   = c(4L, 6L, 8L),
  eta         = c(0.01, 0.05, 0.1),
  subsample   = 0.8,
  colsample_bytree = 0.8,
  min_child_weight  = c(10L, 50L),
  objective   = "binary:logistic",
  eval_metric = "auc"
)

# Cross-validation
CV_FOLDS <- 5L
CV_SEED  <- 42L


# =============================================================================
# 10. OPTIMIZATION PARAMETERS (MILP)
# =============================================================================

OPTIM <- list(
  solver          = "glpk",
  time_limit_sec  = 300L,
  gap_tolerance   = 0.01,
  budget_levels   = c(0.5, 1.0, 2.0, 5.0),   # budget multipliers for sensitivity
  intervention_costs = list(
    transporte  = 1.0,
    apoio_escolar = 1.5,
    bolsa       = 2.0
  )
)


# =============================================================================
# 11. RANDOM SEED
# =============================================================================

GLOBAL_SEED <- 42L


# =============================================================================
# 12. LOGGING UTILITY
# =============================================================================

LOG_FILE <- file.path(LOG_DIR, paste0("pipeline_", format(Sys.Date(), "%Y%m%d"), ".log"))

log_msg <- function(msg, level = "INFO") {
  entry <- sprintf("[%s] [%s] %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), level, msg)
  cat(entry, "\n")
  cat(entry, "\n", file = LOG_FILE, append = TRUE)
}


# =============================================================================
# 13. PACKAGE AVAILABILITY CHECK
# =============================================================================

check_packages <- function(packages) {
  available <- sapply(packages, requireNamespace, quietly = TRUE)
  if (!all(available)) {
    missing <- packages[!available]
    log_msg(sprintf("Missing packages: %s", paste(missing, collapse = ", ")), "WARN")
  }
  return(available)
}

REQUIRED_PACKAGES <- c("data.table", "haven")
OPTIONAL_PACKAGES <- c("mgcv", "ranger", "xgboost", "pROC",
                        "DALEX", "iml", "ompr", "ompr.roi",
                        "ROI.plugin.glpk", "ggplot2", "Matrix")

PKG_STATUS <- check_packages(c(REQUIRED_PACKAGES, OPTIONAL_PACKAGES))


# =============================================================================
# STARTUP MESSAGE
# =============================================================================

log_msg("=== Pipeline Configuration Loaded ===")
log_msg(sprintf("Project root:  %s", PROJECT_ROOT))
log_msg(sprintf("Data source:   %s (%s)", DATA_DIR, DATA_FORMAT))
log_msg(sprintf("Synthetic:     %s", USE_SYNTHETIC))
log_msg(sprintf("Years:         %s", paste(YEARS, collapse = ", ")))
log_msg(sprintf("Dropout def:   %s", DROPOUT_DEFINITION))