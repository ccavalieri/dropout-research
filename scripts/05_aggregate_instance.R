###############################################################################
# 05_aggregate_instance.R
#
# Aggregate the per-student risk into the optimization instance at the turma and
# school levels, and write the SEDAP extraction bundle (CSV tables + Leia-me).
#
# The optimizer needs r only through R_t = sum of risk over a turma's students,
# so nothing individual leaves: rows are aggregates (sums/counts), ids are
# anonymized, and turmas/schools with fewer than MIN_CELL students are dropped.
#
# Inputs:  data/model/risk_{YEAR}.rds       (r per student-year)
#          data/features/features_{YEAR}.rds (turma/school keys per student)
#          data/clean/{BAS_ESCOLA,BAS_TURMA}_{YEAR}.rds
# Outputs: data/instance/turmas.csv, escolas.csv, Leia-me.txt
###############################################################################

# ─── CONFIG ──────────────────────────────────────────────────────────────────
RISK_DIR     <- "data/model"
FEATURES_DIR <- "data/features"
CLEAN_DIR    <- "data/clean"
OUTPUT_DIR   <- "data/instance"
YEAR         <- 2024L          # allocation year (risk-only; parameterize freely)
MIN_CELL     <- 10L            # SEDAP minimum informants per cell (school/coorte)
MODELS       <- c("logit", "gam", "lgb")

# Scope presets (filter on the SCHOOL's location/network). Real INEP/IBGE codes,
# used inside the room; "nacional" (no filter) is the default and runs anywhere.
SCOPE_NAME <- "nacional"
SCOPES <- list(
  nacional          = list(uf = NULL, municipio = NULL,      dependencia = NULL),
  campinas_municipal= list(uf = NULL, municipio = "3509502", dependencia = 3L),
  sp_estadual       = list(uf = "35", municipio = NULL,      dependencia = 2L)
)

# Positive infrastructure items composing indice_infra (same as 03).
INFRA_ITEMS <- c(
  "IN_AGUA_POTAVEL", "IN_AGUA_REDE_PUBLICA", "IN_ENERGIA_REDE_PUBLICA",
  "IN_ESGOTO_REDE_PUBLICA", "IN_BANHEIRO", "IN_BANHEIRO_PNE",
  "IN_BIBLIOTECA_SALA_LEITURA", "IN_LABORATORIO_CIENCIAS",
  "IN_LABORATORIO_INFORMATICA", "IN_QUADRA_ESPORTES", "IN_COMPUTADOR",
  "IN_EQUIP_MULTIMIDIA", "IN_DESKTOP_ALUNO", "IN_INTERNET", "IN_BANDA_LARGA",
  "IN_INTERNET_ALUNOS", "IN_ACESSO_INTERNET_COMPUTADOR", "IN_ALIMENTACAO"
)

REGIAO <- c("1" = "Norte", "2" = "Nordeste", "3" = "Sudeste",
            "4" = "Sul", "5" = "Centro-Oeste")

# ─── PACKAGES ────────────────────────────────────────────────────────────────
suppressPackageStartupMessages(library(data.table))
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)
scope <- SCOPES[[SCOPE_NAME]]

# ─── LOAD + JOIN RISK TO KEYS ────────────────────────────────────────────────
risk <- readRDS(file.path(RISK_DIR, sprintf("risk_%d.rds", YEAR)))
feat <- readRDS(file.path(FEATURES_DIR, sprintf("features_%d.rds", YEAR)))
stu  <- merge(risk[, .(CO_PESSOA_FISICA, r_logit, r_gam, r_lgb)],
              feat[, .(CO_PESSOA_FISICA, ID_TURMA, CO_ENTIDADE)],
              by = "CO_PESSOA_FISICA")

esc_raw <- readRDS(file.path(CLEAN_DIR, sprintf("BAS_ESCOLA_%d.rds", YEAR)))
tur_raw <- readRDS(file.path(CLEAN_DIR, sprintf("BAS_TURMA_%d.rds", YEAR)))

# ─── SCHOOL TABLE ────────────────────────────────────────────────────────────
esc_att <- esc_raw[, c("CO_ENTIDADE", "CO_UF", "CO_MUNICIPIO", "TP_DEPENDENCIA",
                       "TP_LOCALIZACAO", intersect(INFRA_ITEMS, names(esc_raw))),
                   with = FALSE]
esc_att[, indice_infra := rowMeans(as.matrix(.SD), na.rm = TRUE),
        .SDcols = intersect(INFRA_ITEMS, names(esc_att))]
esc_att[, regiao := REGIAO[substr(CO_UF, 1, 1)]]

esc_agg <- stu[, .(n_sigma = .N,
                   R_sigma_logit = sum(r_logit), R_sigma_gam = sum(r_gam),
                   R_sigma_lgb = sum(r_lgb)), by = CO_ENTIDADE]
escolas <- merge(esc_agg, esc_att, by = "CO_ENTIDADE", all.x = TRUE)
escolas[, group_g := paste(regiao, TP_DEPENDENCIA, sep = "_")]

# ─── TURMA TABLE ─────────────────────────────────────────────────────────────
tur_agg <- stu[, .(n_t = .N, CO_ENTIDADE = CO_ENTIDADE[1],
                   R_t_logit = sum(r_logit), R_t_gam = sum(r_gam),
                   R_t_lgb = sum(r_lgb)), by = ID_TURMA]
turmas <- merge(tur_agg, tur_raw[, .(ID_TURMA, TP_ETAPA_ENSINO)],
                by = "ID_TURMA", all.x = TRUE)

# ─── SCOPE FILTER (on schools) ───────────────────────────────────────────────
in_scope <- rep(TRUE, nrow(escolas))
if (!is.null(scope$uf))          in_scope <- in_scope & escolas$CO_UF == scope$uf
if (!is.null(scope$municipio))   in_scope <- in_scope & escolas$CO_MUNICIPIO == scope$municipio
if (!is.null(scope$dependencia)) in_scope <- in_scope & escolas$TP_DEPENDENCIA == scope$dependencia
escolas <- escolas[in_scope]
turmas  <- turmas[CO_ENTIDADE %in% escolas$CO_ENTIDADE]

# ─── SUPPRESSION (< MIN_CELL informants) ─────────────────────────────────────
n_turma_pre <- nrow(turmas); n_esc_pre <- nrow(escolas)
turmas  <- turmas[n_t >= MIN_CELL]
escolas <- escolas[n_sigma >= MIN_CELL]
turmas  <- turmas[CO_ENTIDADE %in% escolas$CO_ENTIDADE]   # keep referential link

# ─── ANONYMIZE IDS ───────────────────────────────────────────────────────────
esc_map <- setNames(seq_len(nrow(escolas)), escolas$CO_ENTIDADE)
escolas[, escola_id := esc_map[CO_ENTIDADE]]
turmas[, escola_id := esc_map[CO_ENTIDADE]]
turmas[, turma_id := seq_len(.N)]

esc_out <- escolas[, .(escola_id, n_sigma, R_sigma_logit, R_sigma_gam,
                       R_sigma_lgb, TP_DEPENDENCIA, regiao, group_g,
                       TP_LOCALIZACAO, indice_infra)]
tur_out <- turmas[, .(turma_id, escola_id, n_t, R_t_logit, R_t_gam, R_t_lgb,
                      TP_ETAPA_ENSINO)]

fwrite(tur_out, file.path(OUTPUT_DIR, "turmas.csv"),  sep = ";", bom = TRUE, na = "")
fwrite(esc_out, file.path(OUTPUT_DIR, "escolas.csv"), sep = ";", bom = TRUE, na = "")

# ─── LEIA-ME (extraction justification) ──────────────────────────────────────
leia <- c(
  "LEIA-ME — Instancia agregada de alocacao de intervencoes (PEE)",
  sprintf("Gerado em: %s", format(Sys.time(), "%Y-%m-%d %H:%M")),
  sprintf("Ano de referencia: %d   |   Escopo: %s", YEAR, SCOPE_NAME),
  "",
  "OBJETIVO",
  "Tabelas agregadas por turma e por escola com a massa de risco de evasao",
  "(soma do risco estimado dos alunos), para uso como instancia do problema de",
  "alocacao de intervencoes. Nenhum dado individual e extraido.",
  "",
  "BASES E CRUZAMENTOS",
  "Censo Escolar (BAS_MATRICULA, BAS_SITUACAO, BAS_TURMA, BAS_ESCOLA) ->",
  "rotulo de evasao e features por aluno-ano -> modelo de risco (r por aluno) ->",
  "agregacao por ID_TURMA e por CO_ENTIDADE.",
  "",
  "CONTEUDO",
  "turmas.csv : uma linha por turma; n_t (n alunos), R_t = soma do risco",
  "  (3 modelos: logit/gam/lgb), etapa. Vinculada a escola por escola_id.",
  "escolas.csv: uma linha por escola; n_sigma, R_sigma (3 modelos), dependencia,",
  "  regiao, grupo de equidade, localizacao, indice de infraestrutura.",
  "",
  "PROTECAO DE DADOS",
  sprintf("- Unidade amostral minima: %d informantes por celula (turma/escola).", MIN_CELL),
  sprintf("- Turmas suprimidas (n_t < %d): %d de %d.", MIN_CELL,
          n_turma_pre - nrow(tur_out), n_turma_pre),
  sprintf("- Escolas suprimidas (n_sigma < %d): %d de %d.", MIN_CELL,
          n_esc_pre - nrow(esc_out), n_esc_pre),
  "- Identificadores de escola e turma sao anonimizados (codigos sequenciais).",
  "- Somente agregados (somas e contagens); sem valores individuais, maximos,",
  "  minimos ou rankings que identifiquem instituicoes.",
  "",
  sprintf("Tabelas: turmas (%d linhas x %d cols), escolas (%d linhas x %d cols).",
          nrow(tur_out), ncol(tur_out), nrow(esc_out), ncol(esc_out))
)
writeLines(leia, file.path(OUTPUT_DIR, "Leia-me.txt"))

# ─── REPORT ──────────────────────────────────────────────────────────────────
message(sprintf("Escopo '%s', ano %d", SCOPE_NAME, YEAR))
message(sprintf("  turmas:  %d -> %d (suprimidas %d por n<%d)",
                n_turma_pre, nrow(tur_out), n_turma_pre - nrow(tur_out), MIN_CELL))
message(sprintf("  escolas: %d -> %d (suprimidas %d por n<%d)",
                n_esc_pre, nrow(esc_out), n_esc_pre - nrow(esc_out), MIN_CELL))
message("Saida em ", normalizePath(OUTPUT_DIR, mustWork = FALSE))
