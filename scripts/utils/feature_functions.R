# utils/feature_functions.R
# Reusable functions for feature construction.

# -- Etapa de Ensino mapping tables -------------------------------------------

# TP_ETAPA_ENSINO codes used in 2019-2024 (Fundamental 9 anos + Médio + EJA)
ETAPA_CODES <- list(
  fund_ai = c(14L, 15L, 16L, 17L, 18L),       # Fund. Anos Iniciais (1-5 ano)
  fund_af = c(19L, 20L, 21L, 41L),             # Fund. Anos Finais (6-9 ano)
  fund_multi = c(22L, 23L, 24L),               # Fund. Multi/Correção
  medio = c(25L, 26L, 27L, 28L),               # Ensino Médio (1-4 série)
  medio_tecnico = c(30L, 31L, 32L, 33L),       # Técnico Integrado
  eja_fund = c(69L, 70L, 72L),                 # EJA Fundamental
  eja_medio = c(71L)                            # EJA Médio
)

# All codes relevant for dropout study (excludes Educação Infantil)
ETAPA_STUDY <- unlist(ETAPA_CODES, use.names = FALSE)

# Expected age for each seriado etapa code
# Returns NA for non-seriado codes (Multi, Correção, EJA)
get_expected_age <- function(tp_etapa) {
  mapping <- c(
    "14" = 6,  "15" = 7,  "16" = 8,  "17" = 9,  "18" = 10,
    "19" = 11, "20" = 12, "21" = 13, "41" = 14,
    "25" = 15, "26" = 16, "27" = 17, "28" = 18,
    "30" = 15, "31" = 16, "32" = 17, "33" = 18
  )
  result <- mapping[as.character(tp_etapa)]
  result[is.na(result)] <- NA_real_
  return(as.numeric(result))
}

# Compute age-grade distortion (years above expected)
# Positive = student is older than expected (risk factor)
compute_age_distortion <- function(age, tp_etapa) {
  expected <- get_expected_age(tp_etapa)
  distortion <- age - expected
  distortion[is.na(distortion)] <- 0
  return(distortion)
}

# Get the next etapa code after approval (grade progression)
get_next_etapa <- function(tp_etapa) {
  progression <- c(
    "14" = 15L, "15" = 16L, "16" = 17L, "17" = 18L, "18" = 19L,
    "19" = 20L, "20" = 21L, "21" = 41L, "41" = 25L,
    "25" = 26L, "26" = 27L, "27" = NA_integer_, "28" = NA_integer_,
    "30" = 31L, "31" = 32L, "32" = NA_integer_, "33" = NA_integer_
  )
  result <- progression[as.character(tp_etapa)]
  result[is.na(names(result))] <- NA_integer_
  return(as.integer(result))
}

# Classify etapa into broad level
classify_etapa <- function(tp_etapa) {
  ifelse(tp_etapa %in% ETAPA_CODES$fund_ai, "fund_ai",
  ifelse(tp_etapa %in% ETAPA_CODES$fund_af, "fund_af",
  ifelse(tp_etapa %in% ETAPA_CODES$fund_multi, "fund_multi",
  ifelse(tp_etapa %in% ETAPA_CODES$medio, "medio",
  ifelse(tp_etapa %in% ETAPA_CODES$medio_tecnico, "medio_tecnico",
  ifelse(tp_etapa %in% ETAPA_CODES$eja_fund, "eja_fund",
  ifelse(tp_etapa %in% ETAPA_CODES$eja_medio, "eja_medio",
         "other")))))))
}

# -- Municipality / Region lookups --------------------------------------------

# Sample of real IBGE municipality codes grouped by region
SAMPLE_MUNICIPIOS <- data.frame(
  CO_MUNICIPIO = c(
    # Norte (region 1)
    1100205L, 1302603L, 1501402L, 1200401L, 1600303L,
    # Nordeste (region 2)
    2111300L, 2211001L, 2304400L, 2408102L, 2507507L,
    2611606L, 2704302L, 2800308L, 2927408L,
    # Sudeste (region 3)
    3106200L, 3205309L, 3304557L, 3509502L, 3550308L,
    # Sul (region 4)
    4106902L, 4205407L, 4314902L,
    # Centro-Oeste (region 5)
    5002704L, 5103403L, 5208707L, 5300108L
  ),
  NO_MUNICIPIO = c(
    "Porto Velho", "Manaus", "Belém", "Rio Branco", "Macapá",
    "São Luís", "Teresina", "Fortaleza", "Mossoró", "João Pessoa",
    "Recife", "Maceió", "Aracaju", "Salvador",
    "Belo Horizonte", "Vitória", "Rio de Janeiro", "Campinas", "São Paulo",
    "Curitiba", "Florianópolis", "Porto Alegre",
    "Campo Grande", "Cuiabá", "Goiânia", "Brasília"
  ),
  stringsAsFactors = FALSE
)
SAMPLE_MUNICIPIOS$CO_UF    <- as.integer(SAMPLE_MUNICIPIOS$CO_MUNICIPIO %/% 1e5L)
SAMPLE_MUNICIPIOS$CO_REGIAO <- as.integer(SAMPLE_MUNICIPIOS$CO_UF %/% 10L)

# Region population weights
REGION_WEIGHTS <- c("1" = 0.09, "2" = 0.27, "3" = 0.42, "4" = 0.14, "5" = 0.08)

# Lookup CO_UF from CO_MUNICIPIO
get_uf_from_municipio <- function(co_municipio) {
  as.integer(co_municipio %/% 1e5L)
}

# Lookup CO_REGIAO from CO_UF
get_regiao_from_uf <- function(co_uf) {
  as.integer(co_uf %/% 10L)
}