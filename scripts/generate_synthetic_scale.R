###############################################################################
# generate_synthetic_scale.R
#
# Self-contained, STREAMING synthetic Censo generator that does not need the
# profile bundle (which is gitignored and absent here). Goal is plumbing +
# memory validation, not statistical realism: it produces the columns the
# pipeline actually consumes (join keys, age, etapa, modality flags, year-end
# situation) with a non-degenerate longitudinal dropout label, plus filler
# columns so the raw CSVs have a realistic width. Everything is written in
# row-chunks with fwrite(append=TRUE) so a year never lives fully in memory.
#
# Scale knobs (env overridable):
#   SCALE_N        enrolled students per year          (tiny=2000 ... real~48e6)
#   SCALE_YEARS    e.g. "2017:2024"
#   SCALE_CHUNK    students written per fwrite append   (memory of the write step)
#   SCALE_FILL     filler columns added to each table   (CSV width realism)
#
# Output: data/synthetic/{TABLE}_{YEAR}.csv   (semicolon, BOM)
###############################################################################

suppressPackageStartupMessages(library(data.table))

# ─── CONFIG ──────────────────────────────────────────────────────────────────
OUTPUT_DIR  <- Sys.getenv("SCALE_OUT", "data/synthetic")
TARGET_N    <- as.numeric(Sys.getenv("SCALE_N",     "2000"))
YEARS       <- eval(parse(text = Sys.getenv("SCALE_YEARS", "2017:2024")))
CHUNK_STU   <- as.integer(Sys.getenv("SCALE_CHUNK", "2000000"))
N_FILL      <- as.integer(Sys.getenv("SCALE_FILL",  "20"))
SEED        <- as.integer(Sys.getenv("SCALE_SEED",  "42"))

N_SCHOOLS   <- max(20L, as.integer(ceiling(TARGET_N / 250)))
EJA_FRAC    <- 0.06        # of enrolled, non-regular EJA share
PROF_FRAC   <- 0.03        # profissionalizante share
MULTI_FRAC  <- 0.05        # students with a 2nd (special) enrollment row

ATTR_BASE   <- 0.08; ATTR_AGE_SLOPE <- 0.80; ATTR_FRAILTY_SD <- 0.50
DECEASED_SHARE <- 0.01

# Etapa -> official expected age (subset of AGE_BY_ETAPA, EF1..9 then EM1..4).
ETAPA_AGE <- c(`14`=6L,`15`=7L,`16`=8L,`17`=9L,`18`=10L,`19`=11L,`20`=12L,
               `21`=13L,`41`=14L,`25`=15L,`26`=16L,`27`=17L,`28`=18L)
ETAPAS    <- as.integer(names(ETAPA_AGE))
EAGE      <- as.integer(ETAPA_AGE)

inv_logit <- function(x) 1 / (1 + exp(-x))
set.seed(SEED)
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ─── SCHOOL POOL (stable across years) ───────────────────────────────────────
message(sprintf("Config: N/year=%s  schools=%d  years=%s  chunk=%d  fill=%d",
                format(TARGET_N, scientific = FALSE), N_SCHOOLS,
                paste(range(YEARS), collapse = "-"), CHUNK_STU, N_FILL))

schools <- data.table(
  CO_ENTIDADE  = 11000000L + seq_len(N_SCHOOLS),
  CO_UF        = sample(c(11:17,21:29,31:35,41:43,50:53), N_SCHOOLS, TRUE),
  TP_DEPENDENCIA = sample(1:4, N_SCHOOLS, TRUE, c(.05,.35,.55,.05)),
  TP_LOCALIZACAO = sample(1:2, N_SCHOOLS, TRUE, c(.8,.2)))
schools[, CO_MUNICIPIO := CO_UF * 100000L + sample(1:900, N_SCHOOLS, TRUE)]

# ─── UNIVERSE + ENROLLMENT PANEL (attrition) ─────────────────────────────────
# One forward pass; entrants accumulated in a list (no rbind-in-loop).
message("Building universe + enrollment panel ...")
INVAR <- c("TP_SEXO","TP_COR_RACA","TP_NACIONALIDADE","CO_UF_NASC",
           "CO_MUNICIPIO_NASC","CO_PAIS_RESIDENCIA","home_school")
base_logit <- qlogis(ATTR_BASE)
entrants <- list(); counter <- 0L
enrolled <- list(); leavers <- list(); deceased <- list()

add_students <- function(n, entry_year) {
  n <- as.integer(n); if (n <= 0L) return(integer(0))
  ids <- 1000000000L + counter + seq_len(n)
  counter <<- counter + n
  age0 <- pmax(4L, pmin(17L, as.integer(round(rnorm(n, 9, 3)))))
  hs   <- sample.int(N_SCHOOLS, n, replace = TRUE)
  dt <- data.table(
    CO_PESSOA_FISICA = ids, birth_year = entry_year - age0,
    frailty = rnorm(n, 0, ATTR_FRAILTY_SD),
    TP_SEXO = sample(1:2, n, TRUE), TP_COR_RACA = sample(0:5, n, TRUE),
    TP_NACIONALIDADE = sample(1:3, n, TRUE, c(.97,.02,.01)),
    CO_UF_NASC = schools$CO_UF[hs],
    CO_MUNICIPIO_NASC = schools$CO_MUNICIPIO[hs],
    CO_PAIS_RESIDENCIA = 76L, home_school = hs)
  entrants[[length(entrants) + 1L]] <<- dt
  ids
}

y0 <- YEARS[1]
enrolled[[as.character(y0)]] <- add_students(TARGET_N, y0)
for (i in seq_len(length(YEARS) - 1L)) {
  yr <- YEARS[i]; ny <- YEARS[i + 1L]
  cur <- enrolled[[as.character(yr)]]
  U   <- rbindlist(entrants)                    # current universe (grows slowly)
  setkey(U, CO_PESSOA_FISICA)
  m   <- U[.(cur)]
  age_yr <- yr - m$birth_year
  p <- inv_logit(base_logit + ATTR_AGE_SLOPE * (age_yr - 15) / 10 + m$frailty)
  leave <- runif(length(cur)) < p
  lv <- cur[leave]
  dec <- lv[runif(length(lv)) < DECEASED_SHARE]
  deceased[[as.character(yr)]] <- dec
  leavers[[as.character(yr)]]  <- setdiff(lv, dec)
  surv <- cur[!leave]
  new  <- add_students(TARGET_N - length(surv), ny)
  enrolled[[as.character(ny)]] <- c(surv, new)
}
U <- rbindlist(entrants); setkey(U, CO_PESSOA_FISICA); rm(entrants); gc(FALSE)
message(sprintf("  universe: %s distinct students", format(nrow(U), big.mark = ",")))

# ─── FILLER COLUMNS ──────────────────────────────────────────────────────────
add_fillers <- function(dt, n) {
  if (N_FILL <= 0L) return(dt)
  for (j in seq_len(N_FILL))
    set(dt, j = sprintf("NU_FILL_%03d", j), value = sample.int(1000L, n, TRUE))
  dt
}

# ─── ESCOLA (whole; small) ───────────────────────────────────────────────────
# Only the columns 01 selects need real names; the rest are 0/1 infra flags.
ESCOLA_IN <- c("IN_AGUA_POTAVEL","IN_AGUA_REDE_PUBLICA","IN_ENERGIA_REDE_PUBLICA",
  "IN_ESGOTO_REDE_PUBLICA","IN_BANHEIRO","IN_BANHEIRO_PNE","IN_BIBLIOTECA_SALA_LEITURA",
  "IN_LABORATORIO_CIENCIAS","IN_LABORATORIO_INFORMATICA","IN_QUADRA_ESPORTES",
  "IN_COMPUTADOR","IN_EQUIP_MULTIMIDIA","IN_DESKTOP_ALUNO","IN_INTERNET",
  "IN_BANDA_LARGA","IN_INTERNET_ALUNOS","IN_ACESSO_INTERNET_COMPUTADOR","IN_ALIMENTACAO")
ESCOLA_QT <- c("QT_PROF_COORDENADOR","QT_PROF_FONAUDIOLOGO","QT_PROF_NUTRICIONISTA",
               "QT_PROF_PSICOLOGO","QT_PROF_PEDAGOGIA","QT_SALAS_UTILIZADAS")

write_escola <- function(yr) {
  n <- N_SCHOOLS
  dt <- data.table(NU_ANO = yr, CO_ENTIDADE = schools$CO_ENTIDADE,
    TP_SITUACAO_FUNCIONAMENTO = 1L, TP_FALTANTE = 0L,
    CO_UF = schools$CO_UF, CO_MUNICIPIO = schools$CO_MUNICIPIO,
    TP_DEPENDENCIA = schools$TP_DEPENDENCIA, TP_LOCALIZACAO = schools$TP_LOCALIZACAO,
    TP_CATEGORIA_ESCOLA_PRIVADA = fifelse(schools$TP_DEPENDENCIA == 4L,
                                          sample(1:3, n, TRUE), NA_integer_))
  for (c in ESCOLA_IN) set(dt, j = c, value = rbinom(n, 1, 0.6))
  for (c in ESCOLA_QT) set(dt, j = c, value = rpois(n, 2))
  add_fillers(dt, n)
  fwrite(dt, file.path(OUTPUT_DIR, sprintf("BAS_ESCOLA_%d.csv", yr)),
         sep = ";", bom = TRUE, na = "")
}

# ─── TURMA (whole; one per school x etapa) ───────────────────────────────────
# turma id encodes (school_index, etapa) so matricula can join deterministically.
turma_id  <- function(si, et) si * 100L + match(et, ETAPAS)
write_turma <- function(yr) {
  grid <- CJ(si = seq_len(N_SCHOOLS), et = ETAPAS)
  n <- nrow(grid)
  dt <- data.table(NU_ANO = yr, ID_TURMA = turma_id(grid$si, grid$et),
    TP_MEDIACAO_DIDATICO_PEDAGO = 1L, NU_DIAS_ATIVIDADE = sample(180:220, n, TRUE),
    TP_ETAPA_ENSINO = grid$et, IN_REGULAR = 1L, IN_EJA = 0L, IN_PROFISSIONALIZANTE = 0L,
    QT_MATRICULAS = sample(12:35, n, TRUE),
    TX_HR_INICIAL = sample(c(7L,8L,13L,14L,19L), n, TRUE),
    CO_ENTIDADE = schools$CO_ENTIDADE[grid$si])
  add_fillers(dt, n)
  fwrite(dt, file.path(OUTPUT_DIR, sprintf("BAS_TURMA_%d.csv", yr)),
         sep = ";", bom = TRUE, na = "")
}

# ─── MATRICULA + SITUACAO (streamed in student chunks) ───────────────────────
MAT_IN_FLAGS <- c("IN_NECESSIDADE_ESPECIAL","IN_TRANSPORTE_PUBLICO","IN_TRANSP_BICICLETA",
  "IN_TRANSP_MICRO_ONIBUS","IN_TRANSP_ONIBUS","IN_TRANSP_TR_ANIMAL","IN_TRANSP_VANS_KOMBI",
  "IN_TRANSP_OUTRO_VEICULO","IN_TRANSP_EMBAR_ATE5","IN_TRANSP_EMBAR_5A15",
  "IN_TRANSP_EMBAR_15A35","IN_TRANSP_EMBAR_35","IN_ESPECIAL_EXCLUSIVA")

# etapa nearest to (age - defasagem draw); returns etapa code + expected age.
assign_etapa <- function(age) {
  delta <- rpois(length(age), 0.4)                       # over-age -> defasagem
  target <- pmax(6L, pmin(18L, age - delta))
  idx <- max.col(-abs(outer(target, EAGE, "-")), ties.method = "first")
  ETAPAS[idx]
}

write_matricula_situacao <- function(yr, subU) {
  ids <- enrolled[[as.character(yr)]]
  is_leaver <- ids %in% leavers[[as.character(yr)]]
  is_dec    <- ids %in% deceased[[as.character(yr)]]
  n_all <- length(ids)
  matpath <- file.path(OUTPUT_DIR, sprintf("BAS_MATRICULA_%d.csv", yr))
  sitpath <- file.path(OUTPUT_DIR, sprintf("BAS_SITUACAO_%d.csv", yr))
  if (file.exists(matpath)) file.remove(matpath)
  if (file.exists(sitpath)) file.remove(sitpath)
  first <- TRUE

  for (s in seq(1L, n_all, by = CHUNK_STU)) {
    e   <- min(s + CHUNK_STU - 1L, n_all)
    idx <- s:e
    cid <- ids[idx]
    a   <- subU[.(cid), on = "CO_PESSOA_FISICA"]
    age <- pmax(4L, pmin(80L, yr - a$birth_year))
    et  <- assign_etapa(age)
    si  <- a$home_school
    n   <- length(cid)

    modality <- sample(c("REG","EJA","PROF"), n, TRUE,
                       c(1 - EJA_FRAC - PROF_FRAC, EJA_FRAC, PROF_FRAC))
    reg  <- as.integer(modality == "REG")
    eja  <- as.integer(modality == "EJA")
    prof <- as.integer(modality == "PROF")
    # base enrollment rows
    mat <- data.table(
      NU_ANO = yr, CO_PESSOA_FISICA = cid,
      ID_MATRICULA = as.character((yr %% 100) * 1e9 + (s - 1) + seq_len(n)),
      NU_IDADE_REFERENCIA = age,
      TP_SEXO = a$TP_SEXO, TP_COR_RACA = a$TP_COR_RACA,
      TP_NACIONALIDADE = a$TP_NACIONALIDADE, CO_UF_NASC = a$CO_UF_NASC,
      CO_MUNICIPIO_NASC = a$CO_MUNICIPIO_NASC, CO_PAIS_RESIDENCIA = a$CO_PAIS_RESIDENCIA,
      CO_UF_END = schools$CO_UF[si], CO_MUNICIPIO_END = schools$CO_MUNICIPIO[si],
      TP_ZONA_RESIDENCIAL = sample(1:2, n, TRUE),
      IN_REGULAR = reg, IN_EJA = eja, IN_PROFISSIONALIZANTE = prof,
      ID_TURMA = turma_id(si, et), CO_ENTIDADE = schools$CO_ENTIDADE[si])
    for (c in MAT_IN_FLAGS) set(mat, j = c, value = rbinom(n, 1, 0.1))
    setcolorder(mat, c("NU_ANO","CO_PESSOA_FISICA","ID_MATRICULA","NU_IDADE_REFERENCIA",
      "TP_SEXO","TP_COR_RACA","TP_NACIONALIDADE","CO_UF_NASC","CO_MUNICIPIO_NASC",
      "CO_PAIS_RESIDENCIA","CO_UF_END","CO_MUNICIPIO_END","TP_ZONA_RESIDENCIAL",
      MAT_IN_FLAGS, "IN_REGULAR","IN_EJA","IN_PROFISSIONALIZANTE","ID_TURMA","CO_ENTIDADE"))

    # a fraction of students get a 2nd special (AEE) enrollment row -> collapse>1
    extra_mask <- runif(n) < MULTI_FRAC
    if (any(extra_mask)) {
      ex <- mat[extra_mask]
      ex[, ID_MATRICULA := paste0("9", ID_MATRICULA)]
      ex[, `:=`(IN_REGULAR = 1L, IN_EJA = 0L, IN_PROFISSIONALIZANTE = 0L,
                IN_ESPECIAL_EXCLUSIVA = 1L,
                CO_ENTIDADE = schools$CO_ENTIDADE[sample.int(N_SCHOOLS, .N, TRUE)])]
      mat <- rbind(mat, ex)
    }
    add_fillers(mat, nrow(mat))
    fwrite(mat, matpath, sep = ";", bom = first, na = "", append = !first)

    # SITUACAO (skip last year: no year-end outcome)
    if (yr < max(YEARS)) {
      lv <- is_leaver[idx][match(mat$CO_PESSOA_FISICA, cid)]
      dc <- is_dec[idx][match(mat$CO_PESSOA_FISICA, cid)]
      tp <- fifelse(dc, 3L,
             fifelse(lv, sample(c(2L,4L), nrow(mat), TRUE, c(.6,.4)),
                     sample(c(4L,5L,9L), nrow(mat), TRUE, c(.15,.8,.05))))
      sit <- data.table(NU_ANO = yr, ID_MATRICULA = mat$ID_MATRICULA,
        CO_PESSOA_FISICA = mat$CO_PESSOA_FISICA, IN_REGULAR = mat$IN_REGULAR,
        ID_TURMA = mat$ID_TURMA, CO_ENTIDADE = mat$CO_ENTIDADE,
        TP_SITUACAO = tp, IN_CONCLUINTE = 0L,
        IN_TRANSFERIDO = rbinom(nrow(mat), 1, 0.02))
      # concluinte only for final-EM etapa (28), not a leaver/deceased
      sit[, IN_CONCLUINTE := as.integer(!lv & !dc & (mat$ID_TURMA %% 100L) == match(28L, ETAPAS))]
      add_fillers(sit, nrow(sit))
      fwrite(sit, sitpath, sep = ";", bom = first, na = "", append = !first)
    }
    first <- FALSE
  }
}

# ─── MAIN ────────────────────────────────────────────────────────────────────
t0 <- Sys.time()
for (yr in YEARS) {
  message(sprintf("=== %d  (enrolled=%s) ===", yr,
                  format(length(enrolled[[as.character(yr)]]), big.mark = ",")))
  write_escola(yr); write_turma(yr)
  write_matricula_situacao(yr, U)
}
message(sprintf("\nDone in %.1f min. Output: %s",
                as.numeric(difftime(Sys.time(), t0, units = "mins")),
                normalizePath(OUTPUT_DIR, mustWork = FALSE)))

# label sanity (present in t, absent in t+1, deceased excluded)
message("\n=== label (panel truth) ===")
for (i in seq_len(length(YEARS) - 1L)) {
  yr <- YEARS[i]; ny <- YEARS[i + 1L]
  cur <- enrolled[[as.character(yr)]]; nxt <- enrolled[[as.character(ny)]]
  dec <- deceased[[as.character(yr)]]
  drop <- setdiff(setdiff(cur, nxt), dec)
  message(sprintf("  %d->%d enrolled=%s dropouts=%s (%.1f%%)",
                  yr, ny, format(length(cur), big.mark=","),
                  format(length(drop), big.mark=","), 100*length(drop)/length(cur)))
}
