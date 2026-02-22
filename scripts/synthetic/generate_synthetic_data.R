# =============================================================================
# generate_synthetic_data.R
# Generates structurally faithful synthetic datasets mirroring INEP's Censo
# Escolar schema (BAS_ESCOLA, BAS_TURMA, BAS_MATRICULA, BAS_SITUACAO).
#
# Embeds realistic dropout signal via logistic model.
# Supports 3 size modes: "small" (1K), "medium" (50K), "large" (500K+).
# Outputs semicolon-separated CSV (UTF-8) and .sas7bdat files.
#
# Run: Rscript generate_synthetic_data.R [small|medium|large]
# =============================================================================

library(data.table)
library(haven)

# -- Configuration ------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)
SIZE_MODE <- if (length(args) >= 1) tolower(args[1]) else "small"
stopifnot(SIZE_MODE %in% c("small", "medium", "large"))

get_script_dir <- function() {
  # Works with Rscript and source()
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg) > 0) {
    return(dirname(normalizePath(sub("^--file=", "", file_arg[1]))))
  }
  if (sys.nframe() > 0) {
    frame <- sys.frame(1)
    if (!is.null(frame$ofile)) return(dirname(normalizePath(frame$ofile)))
  }
  return(".")
}
SCRIPT_DIR <- get_script_dir()
source(file.path(SCRIPT_DIR, "..", "utils", "feature_functions.R"))

YEARS          <- 2019L:2024L
SITUACAO_YEARS <- 2019L:2023L   # BAS_SITUACAO not available for 2024

OUTPUT_DIR <- file.path(SCRIPT_DIR, "..", "..", "data", "synthetic")
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# Size parameters
PARAMS <- switch(SIZE_MODE,
  small  = list(n_schools = 200,  n_students = 1000,   classes_per_school = c(2, 6)),
  medium = list(n_schools = 800,  n_students = 50000,  classes_per_school = c(3, 12)),
  large  = list(n_schools = 3000, n_students = 500000, classes_per_school = c(5, 20))
)

# Dropout logistic model coefficients (from literature)
BETA <- list(
  intercept       = -3.5,   # base log-odds (~3% baseline)
  age_distortion  =  0.30,  # per year of distortion
  male            =  0.25,
  rural           =  0.20,
  preta_parda     =  0.15,
  no_transport    =  0.20,
  eja             =  0.50,
  medio           =  0.35,
  low_infra       =  0.25,
  disability      = -0.10   # inclusive schools may retain
)

SEED <- 42L
set.seed(SEED)

cat(sprintf("[%s] Generating synthetic data — mode: %s, students: %d, schools: %d\n",
            Sys.time(), SIZE_MODE, PARAMS$n_students, PARAMS$n_schools))


# =============================================================================
# LAYER 1: BAS_ESCOLA (School Universe)
# =============================================================================

generate_schools <- function(n_schools) {
  cat(sprintf("[%s] Layer 1: Generating %d schools...\n", Sys.time(), n_schools))

  # Sample municipalities weighted by region population
  muni_pool <- SAMPLE_MUNICIPIOS
  region_w  <- REGION_WEIGHTS[as.character(muni_pool$CO_REGIAO)]
  muni_idx  <- sample(seq_len(nrow(muni_pool)), n_schools, replace = TRUE, prob = region_w)
  muni_sel  <- muni_pool[muni_idx, ]

  schools <- data.table(
    CO_ENTIDADE   = 10000000L + seq_len(n_schools),
    CO_MUNICIPIO  = muni_sel$CO_MUNICIPIO,
    CO_UF         = muni_sel$CO_UF,
    CO_REGIAO     = muni_sel$CO_REGIAO,
    CO_DISTRITO   = muni_sel$CO_MUNICIPIO * 100L + sample(1:5, n_schools, replace = TRUE)
  )

  # Administrative dependency: ~2% federal, ~20% state, ~48% municipal, ~30% private
  schools[, TP_DEPENDENCIA := sample(1:4, .N, replace = TRUE,
                                     prob = c(0.02, 0.20, 0.48, 0.30))]

  # Location: urban/rural varies by dependency
  schools[, TP_LOCALIZACAO := fifelse(
    TP_DEPENDENCIA %in% c(1L, 4L),
    sample(1:2, .N, replace = TRUE, prob = c(0.95, 0.05)),
    sample(1:2, .N, replace = TRUE, prob = c(0.70, 0.30))
  )]

  schools[, TP_LOCALIZACAO_DIFERENCIADA := sample(0:3, .N, replace = TRUE,
                                                   prob = c(0.90, 0.04, 0.03, 0.03))]
  schools[, TP_SITUACAO_FUNCIONAMENTO := 1L]  # all active
  schools[, NO_ENTIDADE := paste0("ESCOLA SINTETICA ", CO_ENTIDADE)]

  # Infrastructure flags (correlated with dependency and location)
  is_better <- schools$TP_DEPENDENCIA %in% c(1L, 4L) | schools$TP_LOCALIZACAO == 1L
  p_good <- ifelse(is_better, 0.85, 0.45)

  infra_cols <- c(
    "IN_AGUA_POTAVEL", "IN_ENERGIA_REDE_PUBLICA", "IN_ESGOTO_REDE_PUBLICA",
    "IN_LIXO_SERVICO_COLETA", "IN_BIBLIOTECA", "IN_BIBLIOTECA_SALA_LEITURA",
    "IN_LABORATORIO_CIENCIAS", "IN_LABORATORIO_INFORMATICA",
    "IN_QUADRA_ESPORTES", "IN_INTERNET", "IN_BANDA_LARGA",
    "IN_COMPUTADOR", "IN_ALIMENTACAO",
    "IN_BANHEIRO", "IN_BANHEIRO_PNE",
    "IN_SALA_DIRETORIA", "IN_SALA_PROFESSOR", "IN_SECRETARIA",
    "IN_COZINHA", "IN_REFEITORIO"
  )
  for (col in infra_cols) {
    schools[, (col) := as.integer(rbinom(.N, 1, p_good))]
  }

  # Energy/water absence flags (inverse of presence)
  schools[, IN_ENERGIA_INEXISTENTE := fifelse(IN_ENERGIA_REDE_PUBLICA == 0L,
                                               as.integer(rbinom(.N, 1, 0.3)), 0L)]
  schools[, IN_AGUA_INEXISTENTE := fifelse(IN_AGUA_POTAVEL == 0L,
                                            as.integer(rbinom(.N, 1, 0.2)), 0L)]

  # Classrooms
  schools[, QT_SALAS_UTILIZADAS := as.integer(
    ifelse(TP_DEPENDENCIA %in% c(1L, 4L),
           sample(5:30, .N, replace = TRUE),
           sample(2:20, .N, replace = TRUE))
  )]
  schools[, QT_SALAS_UTILIZADAS_DENTRO := as.integer(QT_SALAS_UTILIZADAS * runif(.N, 0.8, 1.0))]
  schools[, QT_SALAS_UTILIZADAS_FORA   := QT_SALAS_UTILIZADAS - QT_SALAS_UTILIZADAS_DENTRO]
  schools[, QT_SALAS_UTILIZA_CLIMATIZADAS := as.integer(
    QT_SALAS_UTILIZADAS * runif(.N, 0, ifelse(CO_REGIAO %in% c(1L, 2L), 0.8, 0.4))
  )]

  # Modality flags
  schools[, IN_REGULAR := 1L]
  schools[, IN_EJA := as.integer(rbinom(.N, 1, 0.30))]
  schools[, IN_PROFISSIONALIZANTE := as.integer(rbinom(.N, 1, 0.10))]
  schools[, IN_ESPECIAL_EXCLUSIVA := as.integer(rbinom(.N, 1, 0.03))]
  schools[, IN_ESCOLARIZACAO := 1L]
  schools[, IN_MEDIACAO_PRESENCIAL := 1L]
  schools[, IN_MEDIACAO_SEMIPRESENCIAL := 0L]
  schools[, IN_MEDIACAO_EAD := 0L]

  # Offer flags
  schools[, IN_COMUM_FUND_AI := as.integer(rbinom(.N, 1, 0.70))]
  schools[, IN_COMUM_FUND_AF := as.integer(rbinom(.N, 1, 0.60))]
  schools[, IN_COMUM_MEDIO_MEDIO := as.integer(rbinom(.N, 1, 0.35))]

  # School-level infrastructure score (internal, for dropout model)
  schools[, .infra_score := rowMeans(.SD),
          .SDcols = c("IN_AGUA_POTAVEL", "IN_ENERGIA_REDE_PUBLICA",
                       "IN_BIBLIOTECA", "IN_LABORATORIO_INFORMATICA",
                       "IN_INTERNET", "IN_ALIMENTACAO")]

  return(schools)
}


# =============================================================================
# LAYER 2: BAS_TURMA (Classes)
# =============================================================================

generate_turmas <- function(schools, year) {
  cat(sprintf("[%s] Layer 2: Generating turmas for %d...\n", Sys.time(), year))

  n_classes <- as.integer(runif(nrow(schools),
                                PARAMS$classes_per_school[1],
                                PARAMS$classes_per_school[2]))

  turma_list <- lapply(seq_len(nrow(schools)), function(i) {
    s <- schools[i]
    nc <- n_classes[i]

    # Assign etapas based on what this school offers
    available_etapas <- c()
    if (s$IN_COMUM_FUND_AI == 1L) available_etapas <- c(available_etapas, ETAPA_CODES$fund_ai)
    if (s$IN_COMUM_FUND_AF == 1L) available_etapas <- c(available_etapas, ETAPA_CODES$fund_af)
    if (s$IN_COMUM_MEDIO_MEDIO == 1L) available_etapas <- c(available_etapas, ETAPA_CODES$medio)
    if (s$IN_EJA == 1L) available_etapas <- c(available_etapas, 69L, 70L, 71L)
    if (length(available_etapas) == 0) available_etapas <- ETAPA_CODES$fund_ai

    etapas <- sample(available_etapas, nc, replace = TRUE)

    data.table(
      NU_ANO        = year,
      ID_TURMA      = paste0(year, s$CO_ENTIDADE, sprintf("%03d", seq_len(nc))),
      CO_ENTIDADE   = s$CO_ENTIDADE,
      TP_ETAPA_ENSINO = etapas,
      TP_MEDIACAO_DIDATICO_PEDAGO = 1L,  # presencial
      TP_TIPO_ATENDIMENTO_TURMA = 1L,    # escolarização
      NU_DURACAO_TURMA = as.integer(sample(200:300, nc, replace = TRUE)),
      NU_DIAS_ATIVIDADE = sample(4:5, nc, replace = TRUE)
    )
  })

  turmas <- rbindlist(turma_list)
  return(turmas)
}


# =============================================================================
# LAYER 3 & 4: Students + Longitudinal Panel
# =============================================================================

generate_student_pool <- function(n_students, schools) {
  cat(sprintf("[%s] Layer 3: Generating %d student profiles...\n", Sys.time(), n_students))

  # Assign students to schools (weighted by school size)
  school_weights <- schools$QT_SALAS_UTILIZADAS / sum(schools$QT_SALAS_UTILIZADAS)
  school_idx <- sample(seq_len(nrow(schools)), n_students, replace = TRUE, prob = school_weights)

  students <- data.table(
    CO_PESSOA_FISICA = 100000000L + seq_len(n_students),
    CO_ENTIDADE_HOME = schools$CO_ENTIDADE[school_idx],
    TP_SEXO          = sample(1:2, n_students, replace = TRUE, prob = c(0.50, 0.50)),
    TP_COR_RACA      = sample(0:5, n_students, replace = TRUE,
                               prob = c(0.03, 0.43, 0.09, 0.47, 0.01, 0.01)),
    TP_NACIONALIDADE = sample(1:3, n_students, replace = TRUE,
                               prob = c(0.97, 0.015, 0.015)),
    IN_NECESSIDADE_ESPECIAL = as.integer(rbinom(n_students, 1, 0.04))
  )

  # Disability sub-flags (conditional on IN_NECESSIDADE_ESPECIAL)
  disability_cols <- c("IN_BAIXA_VISAO", "IN_CEGUEIRA", "IN_DEF_AUDITIVA",
                        "IN_DEF_FISICA", "IN_DEF_INTELECTUAL", "IN_SURDEZ",
                        "IN_SURDOCEGUEIRA", "IN_DEF_MULTIPLA",
                        "IN_AUTISMO", "IN_SUPERDOTACAO")
  for (col in disability_cols) {
    students[, (col) := fifelse(IN_NECESSIDADE_ESPECIAL == 1L,
                                 as.integer(rbinom(.N, 1, 0.15)), 0L)]
  }

  # Initial etapa assignment
  school_etapas <- merge(students[, .(CO_PESSOA_FISICA, CO_ENTIDADE_HOME)],
                          schools[, .(CO_ENTIDADE, IN_COMUM_FUND_AI, IN_COMUM_FUND_AF,
                                      IN_COMUM_MEDIO_MEDIO, IN_EJA)],
                          by.x = "CO_ENTIDADE_HOME", by.y = "CO_ENTIDADE", all.x = TRUE)

  # Assign starting etapa code weighted toward lower grades
  starting_etapas <- sapply(seq_len(n_students), function(i) {
    avail <- c()
    if (school_etapas$IN_COMUM_FUND_AI[i] == 1L) avail <- c(avail, ETAPA_CODES$fund_ai)
    if (school_etapas$IN_COMUM_FUND_AF[i] == 1L) avail <- c(avail, ETAPA_CODES$fund_af)
    if (school_etapas$IN_COMUM_MEDIO_MEDIO[i] == 1L) avail <- c(avail, ETAPA_CODES$medio)
    if (school_etapas$IN_EJA[i] == 1L) avail <- c(avail, 69L, 70L, 71L)
    if (length(avail) == 0) avail <- ETAPA_CODES$fund_ai
    sample(avail, 1)
  })

  students[, TP_ETAPA_ENSINO_START := as.integer(starting_etapas)]

  # Assign coherent starting age (expected + small distortion)
  expected_ages <- get_expected_age(students$TP_ETAPA_ENSINO_START)
  expected_ages[is.na(expected_ages)] <- sample(15:40, sum(is.na(expected_ages)), replace = TRUE)
  students[, NU_IDADE_START := as.integer(pmax(5, expected_ages + sample(-1:3, .N, replace = TRUE,
                                                                          prob = c(0.10, 0.50, 0.25, 0.10, 0.05))))]

  # Residence location (from school's municipality by default)
  students <- merge(students,
                    schools[, .(CO_ENTIDADE, CO_MUNICIPIO, CO_UF, CO_REGIAO,
                                TP_LOCALIZACAO, .infra_score)],
                    by.x = "CO_ENTIDADE_HOME", by.y = "CO_ENTIDADE", all.x = TRUE)

  students[, CO_MUNICIPIO_END := CO_MUNICIPIO]
  students[, CO_UF_END := CO_UF]
  students[, TP_ZONA_RESIDENCIAL := TP_LOCALIZACAO]

  # Transport (correlated with rural)
  students[, IN_TRANSPORTE_PUBLICO := fifelse(
    TP_ZONA_RESIDENCIAL == 2L,
    as.integer(rbinom(.N, 1, 0.60)),
    as.integer(rbinom(.N, 1, 0.15))
  )]
  students[, TP_RESPONSAVEL_TRANSPORTE := fifelse(
    IN_TRANSPORTE_PUBLICO == 1L,
    sample(1:2, .N, replace = TRUE, prob = c(0.40, 0.60)),
    NA_integer_
  )]

  return(students)
}


# =============================================================================
# LAYER 5 & 6: Longitudinal Simulation + Outcomes
# =============================================================================

simulate_longitudinal <- function(students, schools, turmas_by_year) {
  cat(sprintf("[%s] Layers 4-6: Simulating longitudinal trajectories...\n", Sys.time()))

  all_matriculas <- list()
  all_situacoes  <- list()

  # Current state per student
  state <- data.table(
    CO_PESSOA_FISICA  = students$CO_PESSOA_FISICA,
    CO_ENTIDADE       = students$CO_ENTIDADE_HOME,
    TP_ETAPA_ENSINO   = students$TP_ETAPA_ENSINO_START,
    NU_IDADE          = students$NU_IDADE_START,
    active            = TRUE,
    graduated         = FALSE,
    dropped_years_ago = 0L
  )

  id_counter <- 0L

  for (yr in YEARS) {
    cat(sprintf("[%s]   Year %d: %d active students\n", Sys.time(), yr, sum(state$active)))

    # Filter active students
    active <- state[active == TRUE]
    n_active <- nrow(active)
    if (n_active == 0) break

    id_counter <- id_counter + n_active
    id_range   <- (id_counter - n_active + 1L):id_counter

    # Assign to turmas (match etapa to available turmas for that year)
    turmas_yr <- turmas_by_year[[as.character(yr)]]

    assigned_turmas <- sapply(seq_len(n_active), function(i) {
      candidates <- turmas_yr[CO_ENTIDADE == active$CO_ENTIDADE[i] &
                                TP_ETAPA_ENSINO == active$TP_ETAPA_ENSINO[i], ID_TURMA]
      if (length(candidates) == 0) {
        # Fallback: any turma in the school
        candidates <- turmas_yr[CO_ENTIDADE == active$CO_ENTIDADE[i], ID_TURMA]
      }
      if (length(candidates) == 0) {
        # Fallback: random turma
        candidates <- turmas_yr[, ID_TURMA]
      }
      sample(candidates, 1)
    })

    # Merge school info for each active student
    sch_info <- schools[match(active$CO_ENTIDADE, schools$CO_ENTIDADE)]

    # Build BAS_MATRICULA
    age_ref <- active$NU_IDADE
    is_eja  <- active$TP_ETAPA_ENSINO %in% c(ETAPA_CODES$eja_fund, ETAPA_CODES$eja_medio)

    stud_info <- students[match(active$CO_PESSOA_FISICA, students$CO_PESSOA_FISICA)]

    mat <- data.table(
      NU_ANO             = yr,
      CO_PESSOA_FISICA   = active$CO_PESSOA_FISICA,
      ID_MATRICULA       = 200000000L + id_range,
      ID_TURMA           = assigned_turmas,
      CO_ENTIDADE        = active$CO_ENTIDADE,
      NU_IDADE_REFERENCIA = age_ref,
      NU_IDADE           = age_ref,
      TP_SEXO            = stud_info$TP_SEXO,
      TP_COR_RACA        = stud_info$TP_COR_RACA,
      TP_NACIONALIDADE   = stud_info$TP_NACIONALIDADE,
      CO_UF_END          = stud_info$CO_UF_END,
      CO_MUNICIPIO_END   = stud_info$CO_MUNICIPIO_END,
      TP_ZONA_RESIDENCIAL = stud_info$TP_ZONA_RESIDENCIAL,
      IN_NECESSIDADE_ESPECIAL = stud_info$IN_NECESSIDADE_ESPECIAL,
      IN_BAIXA_VISAO     = stud_info$IN_BAIXA_VISAO,
      IN_CEGUEIRA        = stud_info$IN_CEGUEIRA,
      IN_DEF_AUDITIVA    = stud_info$IN_DEF_AUDITIVA,
      IN_DEF_FISICA      = stud_info$IN_DEF_FISICA,
      IN_DEF_INTELECTUAL = stud_info$IN_DEF_INTELECTUAL,
      IN_SURDEZ          = stud_info$IN_SURDEZ,
      IN_SURDOCEGUEIRA   = stud_info$IN_SURDOCEGUEIRA,
      IN_DEF_MULTIPLA    = stud_info$IN_DEF_MULTIPLA,
      IN_AUTISMO         = stud_info$IN_AUTISMO,
      IN_SUPERDOTACAO    = stud_info$IN_SUPERDOTACAO,
      IN_TRANSPORTE_PUBLICO    = stud_info$IN_TRANSPORTE_PUBLICO,
      TP_RESPONSAVEL_TRANSPORTE = stud_info$TP_RESPONSAVEL_TRANSPORTE,
      TP_ETAPA_ENSINO    = active$TP_ETAPA_ENSINO,
      IN_REGULAR         = as.integer(!is_eja),
      IN_EJA             = as.integer(is_eja),
      IN_PROFISSIONALIZANTE = 0L,
      IN_ESPECIAL_EXCLUSIVA = 0L,
      TP_MEDIACAO_DIDATICO_PEDAGO = 1L,
      # School context columns (denormalized in real data)
      CO_REGIAO          = sch_info$CO_REGIAO,
      CO_UF              = sch_info$CO_UF,
      CO_MUNICIPIO       = sch_info$CO_MUNICIPIO,
      CO_DISTRITO        = sch_info$CO_DISTRITO,
      TP_DEPENDENCIA     = sch_info$TP_DEPENDENCIA,
      TP_LOCALIZACAO     = sch_info$TP_LOCALIZACAO
    )

    all_matriculas[[as.character(yr)]] <- mat

    # -- Generate outcomes (BAS_SITUACAO) for years with data --
    if (yr %in% SITUACAO_YEARS) {
      # Compute dropout probability via logistic model
      age_dist <- compute_age_distortion(age_ref, active$TP_ETAPA_ENSINO)
      is_male  <- as.integer(stud_info$TP_SEXO == 1L)
      is_rural <- as.integer(stud_info$TP_ZONA_RESIDENCIAL == 2L)
      is_preta_parda <- as.integer(stud_info$TP_COR_RACA %in% c(2L, 3L))
      no_transp <- as.integer(stud_info$IN_TRANSPORTE_PUBLICO == 0L & is_rural == 1L)
      is_eja_flag <- as.integer(is_eja)
      is_medio <- as.integer(active$TP_ETAPA_ENSINO %in% c(ETAPA_CODES$medio,
                                                             ETAPA_CODES$medio_tecnico))
      low_infra <- as.integer(sch_info$.infra_score < 0.5)

      logit_p <- BETA$intercept +
        BETA$age_distortion * pmin(age_dist, 5) +
        BETA$male           * is_male +
        BETA$rural          * is_rural +
        BETA$preta_parda    * is_preta_parda +
        BETA$no_transport   * no_transp +
        BETA$eja            * is_eja_flag +
        BETA$medio          * is_medio +
        BETA$low_infra      * low_infra +
        BETA$disability     * stud_info$IN_NECESSIDADE_ESPECIAL

      p_dropout <- 1 / (1 + exp(-logit_p))

      # Assign outcomes
      # TP_SITUACAO: 2=Abandono, 4=Reprovado, 5=Aprovado
      # Small fractions: 3=Falecido, 9=Sem info
      u <- runif(n_active)
      p_fail <- 0.08  # ~8% failure among non-dropouts
      p_dead <- 0.0002

      tp_situacao <- fifelse(
        u < p_dead, 3L,
        fifelse(u < p_dead + p_dropout, 2L,
        fifelse(u < p_dead + p_dropout + p_fail, 4L,
                5L)))

      # Concluintes: last grade (9º Ano Fund or 3ª Série Médio) + approved
      is_final_grade <- active$TP_ETAPA_ENSINO %in% c(41L, 27L, 32L, 71L)
      in_concluinte <- as.integer(is_final_grade & tp_situacao == 5L)

      sit <- data.table(
        NU_ANO           = yr,
        ID_MATRICULA     = mat$ID_MATRICULA,
        CO_PESSOA_FISICA = active$CO_PESSOA_FISICA,
        ID_TURMA         = assigned_turmas,
        CO_ENTIDADE      = active$CO_ENTIDADE,
        CO_UF            = sch_info$CO_UF,
        CO_MUNICIPIO     = sch_info$CO_MUNICIPIO,
        TP_DEPENDENCIA   = sch_info$TP_DEPENDENCIA,
        IN_REGULAR       = mat$IN_REGULAR,
        TP_SITUACAO      = tp_situacao,
        IN_CONCLUINTE    = in_concluinte,
        IN_TRANSFERIDO   = 0L
      )

      all_situacoes[[as.character(yr)]] <- sit

      # -- Update state for next year (use CO_PESSOA_FISICA for safe matching) --
      active_ids <- active$CO_PESSOA_FISICA

      # Approved (non-concluinte): advance grade
      mask_approved <- tp_situacao == 5L & in_concluinte == 0L
      if (any(mask_approved)) {
        approved_dt <- data.table(
          CO_PESSOA_FISICA = active_ids[mask_approved],
          new_etapa = get_next_etapa(active$TP_ETAPA_ENSINO[mask_approved])
        )
        state[approved_dt, TP_ETAPA_ENSINO := i.new_etapa, on = "CO_PESSOA_FISICA"]
      }

      # Graduated concluintes
      grad_ids <- active_ids[in_concluinte == 1L]
      if (length(grad_ids) > 0) {
        state[CO_PESSOA_FISICA %in% grad_ids, `:=`(active = FALSE, graduated = TRUE)]
      }

      # Dropped out
      drop_ids <- active_ids[tp_situacao == 2L]
      if (length(drop_ids) > 0) {
        state[CO_PESSOA_FISICA %in% drop_ids, `:=`(active = FALSE, dropped_years_ago = 1L)]
      }

      # Deceased
      dead_ids <- active_ids[tp_situacao == 3L]
      if (length(dead_ids) > 0) {
        state[CO_PESSOA_FISICA %in% dead_ids, active := FALSE]
      }

      # Handle students whose next_etapa is NA (completed progression)
      state[active == TRUE & is.na(TP_ETAPA_ENSINO), active := FALSE]

    } else {
      # Year 2024: no outcome data, just enrollment
    }

    # Age everyone by 1 year
    state[, NU_IDADE := NU_IDADE + 1L]

    # Dropout re-entry: ~15% of dropouts return after 1-2 years (some via EJA)
    returning <- state[active == FALSE & dropped_years_ago %in% 1:2]
    if (nrow(returning) > 0) {
      n_return <- max(1, as.integer(nrow(returning) * 0.15))
      return_idx <- sample(seq_len(nrow(returning)), min(n_return, nrow(returning)))
      return_ids <- returning$CO_PESSOA_FISICA[return_idx]
      # Some return to regular, some to EJA
      state[CO_PESSOA_FISICA %in% return_ids, `:=`(
        active = TRUE,
        TP_ETAPA_ENSINO = fifelse(NU_IDADE >= 18L, 71L, TP_ETAPA_ENSINO)
      )]
      # Fix NA etapas for returnees
      state[CO_PESSOA_FISICA %in% return_ids & is.na(TP_ETAPA_ENSINO),
            TP_ETAPA_ENSINO := 70L]
    }
    state[active == FALSE & dropped_years_ago > 0, dropped_years_ago := dropped_years_ago + 1L]

    # Add new cohort inflow (~5% of original size)
    n_new <- max(1L, as.integer(PARAMS$n_students * 0.05))
    if (yr < max(YEARS)) {
      new_ids <- max(state$CO_PESSOA_FISICA) + seq_len(n_new)
      new_schools <- sample(schools$CO_ENTIDADE, n_new, replace = TRUE,
                            prob = schools$QT_SALAS_UTILIZADAS / sum(schools$QT_SALAS_UTILIZADAS))
      new_etapa <- sample(c(14L, 25L, 69L), n_new, replace = TRUE, prob = c(0.60, 0.30, 0.10))
      new_age <- as.integer(get_expected_age(new_etapa))
      new_age[is.na(new_age)] <- sample(16:25, sum(is.na(new_age)), replace = TRUE)

      new_state <- data.table(
        CO_PESSOA_FISICA = new_ids,
        CO_ENTIDADE      = new_schools,
        TP_ETAPA_ENSINO  = new_etapa,
        NU_IDADE         = new_age,
        active           = TRUE,
        graduated        = FALSE,
        dropped_years_ago = 0L
      )
      state <- rbindlist(list(state, new_state), fill = TRUE)

      # Add new students to the student pool
      new_sch_info <- schools[match(new_schools, schools$CO_ENTIDADE)]
      new_students <- data.table(
        CO_PESSOA_FISICA = new_ids,
        CO_ENTIDADE_HOME = new_schools,
        TP_SEXO = sample(1:2, n_new, replace = TRUE),
        TP_COR_RACA = sample(0:5, n_new, replace = TRUE, prob = c(0.03, 0.43, 0.09, 0.47, 0.01, 0.01)),
        TP_NACIONALIDADE = 1L,
        IN_NECESSIDADE_ESPECIAL = as.integer(rbinom(n_new, 1, 0.04)),
        CO_UF_END = new_sch_info$CO_UF,
        CO_MUNICIPIO_END = new_sch_info$CO_MUNICIPIO,
        TP_ZONA_RESIDENCIAL = new_sch_info$TP_LOCALIZACAO,
        IN_TRANSPORTE_PUBLICO = as.integer(rbinom(n_new, 1,
            ifelse(new_sch_info$TP_LOCALIZACAO == 2L, 0.60, 0.15))),
        TP_RESPONSAVEL_TRANSPORTE = NA_integer_,
        TP_ETAPA_ENSINO_START = new_etapa,
        NU_IDADE_START = new_age,
        CO_MUNICIPIO = new_sch_info$CO_MUNICIPIO,
        CO_UF = new_sch_info$CO_UF,
        CO_REGIAO = new_sch_info$CO_REGIAO,
        TP_LOCALIZACAO = new_sch_info$TP_LOCALIZACAO,
        .infra_score = new_sch_info$.infra_score
      )
      # Disability sub-flags
      for (col in c("IN_BAIXA_VISAO", "IN_CEGUEIRA", "IN_DEF_AUDITIVA", "IN_DEF_FISICA",
                     "IN_DEF_INTELECTUAL", "IN_SURDEZ", "IN_SURDOCEGUEIRA", "IN_DEF_MULTIPLA",
                     "IN_AUTISMO", "IN_SUPERDOTACAO")) {
        new_students[, (col) := fifelse(IN_NECESSIDADE_ESPECIAL == 1L,
                                         as.integer(rbinom(.N, 1, 0.15)), 0L)]
      }
      new_students[IN_TRANSPORTE_PUBLICO == 1L,
                    TP_RESPONSAVEL_TRANSPORTE := sample(1:2, .N, replace = TRUE, prob = c(0.4, 0.6))]
      students <- rbindlist(list(students, new_students), fill = TRUE)
    }
  }

  list(
    matriculas = rbindlist(all_matriculas),
    situacoes  = rbindlist(all_situacoes)
  )
}


# =============================================================================
# LAYER 7: Inject Corruption (Missingness)
# =============================================================================

inject_missingness <- function(dt, cols, rate = 0.05) {
  dt <- copy(dt)
  for (col in cols) {
    if (col %in% names(dt)) {
      mask <- rbinom(nrow(dt), 1, rate) == 1L
      if (is.integer(dt[[col]])) {
        dt[mask, (col) := NA_integer_]
      } else if (is.numeric(dt[[col]])) {
        dt[mask, (col) := NA_real_]
      } else {
        dt[mask, (col) := NA_character_]
      }
    }
  }
  return(dt)
}


# =============================================================================
# MAIN EXECUTION
# =============================================================================

# Layer 1: Schools
schools <- generate_schools(PARAMS$n_schools)

# Layer 2: Turmas for each year
turmas_by_year <- list()
for (yr in YEARS) {
  turmas_by_year[[as.character(yr)]] <- generate_turmas(schools, yr)
}

# Layer 3: Student pool
students <- generate_student_pool(PARAMS$n_students, schools)

# Layers 4-6: Longitudinal simulation
result <- simulate_longitudinal(students, schools, turmas_by_year)
matriculas <- result$matriculas
situacoes  <- result$situacoes

# Combine all turmas
all_turmas <- rbindlist(turmas_by_year)

# Expand schools across years (one row per school-year)
bas_escola <- rbindlist(lapply(YEARS, function(yr) {
  dt <- copy(schools)
  dt[, NU_ANO := yr]
  dt[, .infra_score := NULL]
  dt
}))

# Clean internal columns
all_turmas[, CO_ENTIDADE := as.integer(CO_ENTIDADE)]

# Inject ~5% missingness in selected columns
matriculas <- inject_missingness(matriculas,
  cols = c("TP_COR_RACA", "TP_ZONA_RESIDENCIAL", "TP_RESPONSAVEL_TRANSPORTE"),
  rate = 0.05)


# =============================================================================
# SAVE OUTPUTS
# =============================================================================

save_table <- function(dt, table_name) {
  csv_path <- file.path(OUTPUT_DIR, paste0(table_name, ".csv"))
  sas_path <- file.path(OUTPUT_DIR, paste0(table_name, ".sas7bdat"))

  fwrite(dt, csv_path, sep = ";", na = "", bom = TRUE)
  cat(sprintf("  CSV: %s (%d rows, %d cols)\n", csv_path, nrow(dt), ncol(dt)))

  # SAS format (.sas7bdat — the format used in SEDAP's secure room)
  tryCatch({
    suppressWarnings(write_sas(as.data.frame(dt), sas_path))
    cat(sprintf("  SAS: %s\n", sas_path))
  }, error = function(e) {
    cat(sprintf("  SAS export warning: %s\n", e$message))
  })
}

cat(sprintf("\n[%s] Saving outputs to %s\n", Sys.time(), OUTPUT_DIR))

save_table(bas_escola, "BAS_ESCOLA")
save_table(all_turmas, "BAS_TURMA")
save_table(matriculas, "BAS_MATRICULA")
save_table(situacoes,  "BAS_SITUACAO")

# Summary stats
cat(sprintf("\n=== Generation Summary (%s mode) ===\n", SIZE_MODE))
cat(sprintf("Schools:      %d (across %d years = %d rows)\n",
            PARAMS$n_schools, length(YEARS), nrow(bas_escola)))
cat(sprintf("Turmas:       %d total rows\n", nrow(all_turmas)))
cat(sprintf("Matrículas:   %d total rows\n", nrow(matriculas)))
cat(sprintf("Situações:    %d total rows\n", nrow(situacoes)))
cat(sprintf("Unique students: %d\n", uniqueN(matriculas$CO_PESSOA_FISICA)))
cat(sprintf("Dropout rate (TP_SITUACAO==2): %.1f%%\n",
            100 * mean(situacoes$TP_SITUACAO == 2L, na.rm = TRUE)))
cat(sprintf("Approval rate (TP_SITUACAO==5): %.1f%%\n",
            100 * mean(situacoes$TP_SITUACAO == 5L, na.rm = TRUE)))
cat(sprintf("Years with situacao: %s\n", paste(SITUACAO_YEARS, collapse = ", ")))
cat(sprintf("[%s] Done.\n", Sys.time()))