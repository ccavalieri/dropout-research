###############################################################################
# generate_synthetic_data.R
#
# Build a structurally faithful synthetic Censo da Educação Básica dataset
# from the profile extracted by 00b_profile_real_data.R. The goal is *not*
# statistical realism — it is to exercise the full pipeline outside SEDAP so
# that scripts that run green on synthetic also run green on real data.
#
# Longitudinal attrition: unlike the first version (a fixed cohort present in
# every year, which made the dropout label degenerate), students now enter and
# leave over time. Each year's enrolled set = survivors of the previous year +
# new entrants, holding the per-year size at TARGET_N. A student who is present
# in year t but absent in t+1 is a dropout — exactly the label the project uses
# (present in t, absent in t+1, deceased excluded). Attrition follows a logistic
# hazard on age plus per-student heterogeneity, so the label carries learnable
# signal (age is observable) rather than pure noise.
#
# Inputs:  data/1st_extraction/{1..8}_*.csv  (the profile bundle)
# Outputs: data/synthetic/{TABLE}_{YEAR}.csv  (23 files)
###############################################################################

# ─── CONFIGURATION ───────────────────────────────────────────────────────────
PROFILE_DIR          <- "data/1st_extraction"
OUTPUT_DIR           <- "data/synthetic"
YEARS                <- 2019:2024
TARGET_N             <- 1000L      # enrolled students per year
N_SCHOOLS            <- max(10L, ceiling(TARGET_N / 33L))
N_TURMAS_PER_SCHOOL  <- 7L
SEED                 <- 42L

# Longitudinal attrition (annual non-return hazard, calibrate later from a
# transition extraction; these are plausible placeholders for plumbing).
ATTRITION_BASE            <- 0.08  # baseline annual prob of not returning
ATTRITION_AGE_SLOPE       <- 0.80  # logit shift per +10 years of age
ATTRITION_FRAILTY_SD      <- 0.50  # unobserved per-student heterogeneity (logit)
DECEASED_SHARE_OF_LEAVERS <- 0.01  # of those who leave, fraction deceased (=3)

# ─── PACKAGES + HELPERS ──────────────────────────────────────────────────────
suppressPackageStartupMessages({
  library(data.table)
})
source(file.path("scripts", "synth_helpers.R"))

set.seed(SEED)
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ─── LOAD PROFILE ────────────────────────────────────────────────────────────
message("Loading profile from ", PROFILE_DIR, " ...")
profile <- load_profile(PROFILE_DIR)
message(sprintf("  schema: %d rows, %d table-year combos",
                nrow(profile$schema),
                uniqueN(profile$schema, by = c("table", "year"))))

# ─── UNIVERSE + ENROLLMENT PANEL ─────────────────────────────────────────────
# Build, in one forward pass over the years, (a) a growing universe of students
# with time-invariant attributes drawn at entry, and (b) the per-year enrolled
# set under the attrition hazard. Returns the panel plus, per year, the ids that
# leave (dropouts) and the subset that die (deceased), so the downstream label
# and its deceased-exclusion can both be exercised.
build_universe_and_panel <- function(profile, years, target_n,
                                     base, age_slope, frailty_sd,
                                     deceased_share, id_start = 1e9) {
  invariant_cols <- c("TP_SEXO", "TP_COR_RACA", "TP_NACIONALIDADE",
                      "CO_PAIS_ORIGEM", "CO_UF_NASC")
  base_logit <- qlogis(base)

  U       <- data.table()
  counter <- 0L

  # Append n students entering in `entry_year`; returns their ids.
  add_students <- function(n, entry_year) {
    if (n <= 0L) return(integer(0))
    ids  <- as.integer(id_start) + counter + seq_len(n) - 1L
    counter <<- counter + n
    age0 <- pmax(0L, round(sample_num(profile, "BAS_MATRICULA", entry_year,
                                      "NU_IDADE_REFERENCIA", n)))
    dt <- data.table(CO_PESSOA_FISICA = ids,
                     birth_year       = entry_year - age0,
                     frailty          = rnorm(n, 0, frailty_sd))
    for (col in invariant_cols) {
      dt[, (col) := sample_cat(profile, "BAS_MATRICULA", entry_year, col, n)]
    }
    U <<- rbind(U, dt, fill = TRUE)
    ids
  }

  enrolled <- list(); leavers <- list(); deceased <- list()

  y0 <- years[1]
  enrolled[[as.character(y0)]] <- add_students(target_n, y0)
  setkey(U, CO_PESSOA_FISICA)

  for (i in seq_len(length(years) - 1L)) {
    yr <- years[i]; ny <- years[i + 1L]
    cur <- enrolled[[as.character(yr)]]

    m      <- U[.(cur)]
    age_yr <- yr - m$birth_year
    p      <- inv_logit(base_logit + age_slope * (age_yr - 15) / 10 + m$frailty)

    leave_mask <- runif(length(cur)) < p
    leave_ids  <- cur[leave_mask]

    dec_mask <- runif(length(leave_ids)) < deceased_share
    deceased[[as.character(yr)]] <- leave_ids[dec_mask]
    leavers[[as.character(yr)]]  <- leave_ids[!dec_mask]

    survivors <- cur[!leave_mask]
    new_ids   <- add_students(target_n - length(survivors), ny)
    setkey(U, CO_PESSOA_FISICA)
    enrolled[[as.character(ny)]] <- c(survivors, new_ids)
  }

  list(U = U, enrolled = enrolled, leavers = leavers, deceased = deceased)
}

build_school_pool <- function(n) {
  data.table(CO_ENTIDADE = gen_id_seq(11000000L, n))
}

message("Building universe and enrollment panel ...")
panel   <- build_universe_and_panel(profile, YEARS, TARGET_N,
                                    ATTRITION_BASE, ATTRITION_AGE_SLOPE,
                                    ATTRITION_FRAILTY_SD,
                                    DECEASED_SHARE_OF_LEAVERS)
schools <- build_school_pool(N_SCHOOLS)
message(sprintf("  universe: %d distinct students across %d years",
                nrow(panel$U), length(YEARS)))

# ─── COLUMN ROUTER ───────────────────────────────────────────────────────────
# Given a schema column, return `n` generated values for it. Join-key,
# longitudinal, and cross-table-consistent columns are supplied via `ctx`.
gen_column <- function(col, dtype, tbl, yr, n, ctx) {
  # Generic ctx override — any column whose name appears in `ctx` is
  # taken from there. Used for join keys, longitudinal pools, and
  # time-invariant student attributes.
  if (!is.null(ctx[[col]])) return(ctx[[col]])

  if (col == "NU_ANO")   return(rep(yr, n))
  if (col == "CPF_MASC") return(rep(NA_character_, n))
  if (col == "NU_IDADE" && !is.null(ctx$NU_IDADE_REFERENCIA))
    return(as.integer(round(ctx$NU_IDADE_REFERENCIA)))

  if (grepl("^NU_CNPJ", col)) {
    return(apply_missing(sample_num(profile, tbl, yr, col, n),
                         profile, tbl, yr, col))
  }
  if (nrow(profile$cat_freqs[.(tbl, yr, col), nomatch = NULL]) > 0) {
    return(apply_missing(sample_cat(profile, tbl, yr, col, n),
                         profile, tbl, yr, col))
  }
  if (nrow(profile$numeric[.(tbl, yr, col), nomatch = NULL]) > 0) {
    return(apply_missing(sample_num(profile, tbl, yr, col, n),
                         profile, tbl, yr, col))
  }
  if (startsWith(col, "CO_") || startsWith(col, "ID_")) {
    card <- profile$cardinality[.(tbl, yr, col), nomatch = NULL]
    nu   <- if (nrow(card) > 0) card$n_unique else max(50L, as.integer(n / 10))
    return(apply_missing(gen_codes(n, nu), profile, tbl, yr, col))
  }
  rep(NA, n)
}

# ─── TABLE BUILDER ───────────────────────────────────────────────────────────
gen_table_year <- function(tbl, yr, n, ctx) {
  schema_rows <- profile$schema[.(tbl, yr), nomatch = NULL][order(position)]
  if (nrow(schema_rows) == 0)
    stop(sprintf("No schema rows for %s %d", tbl, yr))

  out <- data.table(.row_id = seq_len(n))
  for (i in seq_len(nrow(schema_rows))) {
    col <- schema_rows$column[i]
    set(out, j = col, value = gen_column(col, schema_rows$dtype[i],
                                         tbl, yr, n, ctx))
  }
  out[, .row_id := NULL]
  setcolorder(out, schema_rows$column)
  out
}

write_year <- function(dt, tbl, yr) {
  path <- file.path(OUTPUT_DIR, sprintf("%s_%d.csv", tbl, yr))
  fwrite(dt, path, sep = ";", bom = TRUE, na = "")
  message(sprintf("  wrote %-25s  %d rows x %d cols",
                  basename(path), nrow(dt), ncol(dt)))
}

# ─── MAIN: ONE YEAR ──────────────────────────────────────────────────────────
generate_year <- function(yr) {
  message(sprintf("=== Year %d ===", yr))

  ids      <- panel$enrolled[[as.character(yr)]]
  attr     <- panel$U[.(ids)]          # ordered to match `ids` (enrolled order)
  n_matric <- length(ids)

  n_schools <- N_SCHOOLS
  n_turmas  <- n_schools * N_TURMAS_PER_SCHOOL

  # 1. BAS_ESCOLA
  ctx_esc <- list(CO_ENTIDADE = schools$CO_ENTIDADE)
  escola  <- gen_table_year("BAS_ESCOLA", yr, n_schools, ctx_esc)
  write_year(escola, "BAS_ESCOLA", yr)

  # 2. BAS_TURMA
  turma_id_pool <- gen_id_seq((yr - 2019L) * 1e7 + 1L, n_turmas)
  turma_school  <- rep(schools$CO_ENTIDADE, each = N_TURMAS_PER_SCHOOL)
  ctx_trm <- list(ID_TURMA = turma_id_pool, CO_ENTIDADE = turma_school)
  turma   <- gen_table_year("BAS_TURMA", yr, n_turmas, ctx_trm)
  write_year(turma, "BAS_TURMA", yr)

  # 3. BAS_MATRICULA — one row per enrolled student
  matric_id_pool  <- gen_id_seq((yr - 2019L) * 1e8 + 1L, n_matric)
  matric_turma    <- sample(turma_id_pool, n_matric, replace = TRUE)
  turma_to_school <- setNames(turma_school, as.character(turma_id_pool))
  matric_school   <- unname(turma_to_school[as.character(matric_turma)])

  age_in_year <- pmax(4L, pmin(80L, yr - attr$birth_year))

  ctx_mat <- list(
    CO_PESSOA_FISICA    = ids,
    ID_MATRICULA        = matric_id_pool,
    ID_TURMA            = matric_turma,
    CO_ENTIDADE         = matric_school,
    NU_IDADE_REFERENCIA = age_in_year,
    TP_SEXO             = attr$TP_SEXO,
    TP_COR_RACA         = attr$TP_COR_RACA,
    TP_NACIONALIDADE    = attr$TP_NACIONALIDADE,
    CO_UF_NASC          = attr$CO_UF_NASC
  )
  matric <- gen_table_year("BAS_MATRICULA", yr, n_matric, ctx_mat)
  write_year(matric, "BAS_MATRICULA", yr)

  # 4. BAS_SITUACAO (skip 2024 — no year-end outcome / no t+1)
  if (yr <= 2023L) {
    is_leaver   <- ids %in% panel$leavers[[as.character(yr)]]
    is_deceased <- ids %in% panel$deceased[[as.character(yr)]]
    sit_situacao <- sample_situacao_panel(profile, yr, matric$TP_ETAPA_ENSINO,
                                          is_leaver, is_deceased)
    ctx_sit <- list(
      CO_PESSOA_FISICA = matric$CO_PESSOA_FISICA,
      ID_MATRICULA     = matric$ID_MATRICULA,
      ID_TURMA         = matric$ID_TURMA,
      CO_ENTIDADE      = matric$CO_ENTIDADE,
      TP_ETAPA_ENSINO  = matric$TP_ETAPA_ENSINO,
      IN_REGULAR       = matric$IN_REGULAR
    )
    sit <- gen_table_year("BAS_SITUACAO", yr, n_matric, ctx_sit)
    if ("TP_SITUACAO" %in% names(sit)) {
      sit[, TP_SITUACAO := suppressWarnings(as.integer(sit_situacao))]
    }
    write_year(sit, "BAS_SITUACAO", yr)
  }

  invisible(NULL)
}

for (yr in YEARS) generate_year(yr)

# ─── VALIDATION ──────────────────────────────────────────────────────────────
message("\n=== Validation ===")

read_synth <- function(tbl, yr) {
  fread(file.path(OUTPUT_DIR, sprintf("%s_%d.csv", tbl, yr)),
        sep = ";", encoding = "UTF-8", showProgress = FALSE)
}

for (yr in YEARS) {
  esc <- read_synth("BAS_ESCOLA",    yr)
  trm <- read_synth("BAS_TURMA",     yr)
  mat <- read_synth("BAS_MATRICULA", yr)

  exp_esc <- profile$schema[.("BAS_ESCOLA",    yr)][order(position)]$column
  exp_trm <- profile$schema[.("BAS_TURMA",     yr)][order(position)]$column
  exp_mat <- profile$schema[.("BAS_MATRICULA", yr)][order(position)]$column

  stopifnot(identical(names(esc), exp_esc))
  stopifnot(identical(names(trm), exp_trm))
  stopifnot(identical(names(mat), exp_mat))

  stopifnot(all(trm$CO_ENTIDADE %in% esc$CO_ENTIDADE))
  stopifnot(all(mat$ID_TURMA    %in% trm$ID_TURMA))
  stopifnot(all(mat$CO_ENTIDADE %in% esc$CO_ENTIDADE))

  if (yr <= 2023L) {
    sit <- read_synth("BAS_SITUACAO", yr)
    exp_sit <- profile$schema[.("BAS_SITUACAO", yr)][order(position)]$column
    stopifnot(identical(names(sit), exp_sit))
    stopifnot(all(sit$ID_MATRICULA %in% mat$ID_MATRICULA))

    dist <- prop.table(table(sit$TP_SITUACAO))
    message(sprintf("  %d  TP_SITUACAO: %s", yr,
                    paste(sprintf("%s=%.1f%%", names(dist), 100 * dist),
                          collapse = "  ")))
  } else {
    message(sprintf("  %d  schemas + joins OK (no BAS_SITUACAO)", yr))
  }
}

# Longitudinal label check: present in t, absent in t+1 (deceased excluded).
message("\n=== Dropout label (present in t, absent in t+1) ===")
for (i in seq_len(length(YEARS) - 1L)) {
  yr <- YEARS[i]; ny <- YEARS[i + 1L]
  cur      <- panel$enrolled[[as.character(yr)]]
  nxt      <- panel$enrolled[[as.character(ny)]]
  dec      <- panel$deceased[[as.character(yr)]]
  absent   <- setdiff(cur, nxt)
  dropouts <- setdiff(absent, dec)
  message(sprintf("  %d->%d  enrolled=%d  dropouts=%d (%.1f%%)  deceased=%d",
                  yr, ny, length(cur), length(dropouts),
                  100 * length(dropouts) / length(cur), length(dec)))
}

message(sprintf("\nDone. Files written to %s",
                normalizePath(OUTPUT_DIR, mustWork = FALSE)))
