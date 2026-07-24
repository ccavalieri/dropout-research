###############################################################################
# report_A.R
#
# Output: outputs/report_A/{instance,tables,figures}/ + Leia-me.(txt|pdf)
###############################################################################

# ─── CONFIG ──────────────────────────────────────────────────────────────────
RISK_DIR     <- "data/model"
FEATURES_DIR <- "data/features"
CLEAN_DIR    <- "data/clean"
OUT          <- "outputs/report_A"
YEAR         <- 2024L           # instance / scoring year
TEST_YEAR    <- 2023L           # last labeled year, for model evaluation
LABEL_YEARS  <- 2019:2023       # labeled years, for descriptives
MIN_CELL     <- 10L             # conservative suppression everywhere
MODELS       <- c("logit", "gam", "lgb")
EMIT_MODEL_DEPENDENT <- TRUE

SCOPE_NAME <- "nacional"
SCOPES <- list(
  nacional           = list(uf = NULL, municipio = NULL,      dependencia = NULL),
  campinas_municipal = list(uf = NULL, municipio = "3509502", dependencia = 3L),
  sp_estadual        = list(uf = "35", municipio = NULL,      dependencia = 2L)
)

PROJETO <- list(
  titulo     = "Predicao e Otimizacao de Intervencoes para Reducao da Evasao Escolar no Brasil",
  autor      = "Carlos Eduardo Cavalieri Furtado",
  orientador = "Prof. Dr. Santiago Valdes Ravelo",
  instituicao= "Universidade Estadual de Campinas (UNICAMP) - Instituto de Computacao",
  processo   = "23036.001376/2026-77")

INFRA_ITEMS <- c(
  "IN_AGUA_POTAVEL", "IN_AGUA_REDE_PUBLICA", "IN_ENERGIA_REDE_PUBLICA",
  "IN_ESGOTO_REDE_PUBLICA", "IN_BANHEIRO", "IN_BANHEIRO_PNE",
  "IN_BIBLIOTECA_SALA_LEITURA", "IN_LABORATORIO_CIENCIAS",
  "IN_LABORATORIO_INFORMATICA", "IN_QUADRA_ESPORTES", "IN_COMPUTADOR",
  "IN_EQUIP_MULTIMIDIA", "IN_DESKTOP_ALUNO", "IN_INTERNET", "IN_BANDA_LARGA",
  "IN_INTERNET_ALUNOS", "IN_ACESSO_INTERNET_COMPUTADOR", "IN_ALIMENTACAO")
REGIAO <- c("1"="Norte","2"="Nordeste","3"="Sudeste","4"="Sul","5"="Centro-Oeste")
GAM_SMOOTH <- c("NU_IDADE_REFERENCIA","defasagem_idade_serie","n_reprovacoes_prev",
                "anos_observados","indice_infra","tamanho_escola","tamanho_turma",
                "apoio_por_aluno")
GAM_PARAM  <- c("TP_SEXO","TP_COR_RACA","TP_DEPENDENCIA","TP_LOCALIZACAO","turno",
                "flag_defasado","reprovou_ano_ant","abandono_previo",
                "mudou_municipio","IN_NECESSIDADE_ESPECIAL","IN_TROCA_ESCOLA",
                "IN_TRANSFERIDO")

# ─── PACKAGES + IO ───────────────────────────────────────────────────────────
suppressPackageStartupMessages({ library(data.table); library(ggplot2); library(mgcv) })
for (d in c("instance", "tables", "figures"))
  dir.create(file.path(OUT, d), showWarnings = FALSE, recursive = TRUE)
scope <- SCOPES[[SCOPE_NAME]]
notes <- character(0)                       # collected for the Leia-me

rd_clean <- function(t, y) { p <- file.path(CLEAN_DIR, sprintf("%s_%d.rds", t, y))
  if (file.exists(p)) readRDS(p) else NULL }
save_tbl <- function(dt, name)
  fwrite(dt, file.path(OUT, "tables", paste0(name, ".csv")), sep = ";", bom = TRUE, na = "")
save_fig <- function(p, name, w = 7, h = 4.5)
  tryCatch(ggsave(file.path(OUT, "figures", paste0(name, ".pdf")), p, width = w, height = h),
           error = function(e) message("  [fig ", name, "] ", conditionMessage(e)))
suppress <- function(dt) dt[n >= MIN_CELL]   # tables carry a column 'n'
guard <- function(label, expr) tryCatch(expr,
  error = function(e) message("  [SKIP ", label, "] ", conditionMessage(e)))

# ─── METRICS ─────────────────────────────────────────────────────────────────
auc_roc <- function(y, p) { n1 <- sum(y==1); n0 <- sum(y==0)
  if (n1==0||n0==0) NA_real_ else (sum(rank(p)[y==1]) - n1*(n1+1)/2)/(n1*n0) }
auc_pr <- function(y, p) { o <- order(p, decreasing=TRUE); y <- y[o]
  tp <- cumsum(y); prec <- tp/seq_along(y); rec <- tp/sum(y)
  sum(prec * c(rec[1], diff(rec))) }
rec_at_k <- function(y, p, k) { top <- order(p, decreasing=TRUE)[seq_len(ceiling(k*length(y)))]
  sum(y[top]==1)/sum(y==1) }

# ═══ BLOCK 1 — INSTANCE (absorbed 05) ════════════════════════════════════════
guard("instancia", {
  risk <- readRDS(file.path(RISK_DIR, sprintf("risk_%d.rds", YEAR)))
  feat <- readRDS(file.path(FEATURES_DIR, sprintf("features_%d.rds", YEAR)))
  stu  <- merge(risk[, .(CO_PESSOA_FISICA, r_logit, r_gam, r_lgb)],
                feat[, .(CO_PESSOA_FISICA, ID_TURMA, CO_ENTIDADE)], by = "CO_PESSOA_FISICA")
  esc_raw <- rd_clean("BAS_ESCOLA", YEAR); tur_raw <- rd_clean("BAS_TURMA", YEAR)

  esc_att <- esc_raw[, c("CO_ENTIDADE","CO_UF","CO_MUNICIPIO","TP_DEPENDENCIA",
                         "TP_LOCALIZACAO", intersect(INFRA_ITEMS, names(esc_raw))), with = FALSE]
  esc_att[, indice_infra := rowMeans(as.matrix(.SD), na.rm = TRUE),
          .SDcols = intersect(INFRA_ITEMS, names(esc_att))]
  esc_att[, regiao := REGIAO[substr(CO_UF, 1, 1)]]
  escolas <- merge(stu[, .(n = .N, R_sigma_logit = sum(r_logit), R_sigma_gam = sum(r_gam),
                           R_sigma_lgb = sum(r_lgb)), by = CO_ENTIDADE],
                   esc_att, by = "CO_ENTIDADE", all.x = TRUE)
  escolas[, group_g := paste(regiao, TP_DEPENDENCIA, sep = "_")]
  turmas <- merge(stu[, .(n = .N, CO_ENTIDADE = CO_ENTIDADE[1], R_t_logit = sum(r_logit),
                          R_t_gam = sum(r_gam), R_t_lgb = sum(r_lgb)), by = ID_TURMA],
                  tur_raw[, .(ID_TURMA, TP_ETAPA_ENSINO)], by = "ID_TURMA", all.x = TRUE)

  ins <- rep(TRUE, nrow(escolas))
  if (!is.null(scope$uf))          ins <- ins & escolas$CO_UF == scope$uf
  if (!is.null(scope$municipio))   ins <- ins & escolas$CO_MUNICIPIO == scope$municipio
  if (!is.null(scope$dependencia)) ins <- ins & escolas$TP_DEPENDENCIA == scope$dependencia
  escolas <- escolas[ins]; turmas <- turmas[CO_ENTIDADE %in% escolas$CO_ENTIDADE]

  n_t_pre <- nrow(turmas); n_e_pre <- nrow(escolas)
  escolas <- suppress(escolas); turmas <- suppress(turmas)
  turmas  <- turmas[CO_ENTIDADE %in% escolas$CO_ENTIDADE]
  emap <- setNames(seq_len(nrow(escolas)), escolas$CO_ENTIDADE)
  escolas[, escola_id := emap[CO_ENTIDADE]]; turmas[, escola_id := emap[CO_ENTIDADE]]
  turmas[, turma_id := seq_len(.N)]

  fwrite(turmas[, .(turma_id, escola_id, n_t = n, R_t_logit, R_t_gam, R_t_lgb, TP_ETAPA_ENSINO)],
         file.path(OUT, "instance", "turmas.csv"), sep = ";", bom = TRUE, na = "")
  fwrite(escolas[, .(escola_id, n_sigma = n, R_sigma_logit, R_sigma_gam, R_sigma_lgb,
                     TP_DEPENDENCIA, regiao, group_g, TP_LOCALIZACAO, indice_infra)],
         file.path(OUT, "instance", "escolas.csv"), sep = ";", bom = TRUE, na = "")
  notes <<- c(notes,
    sprintf("Instancia (ano %d, escopo %s): turmas %d->%d, escolas %d->%d (suprimidas por n<%d).",
            YEAR, SCOPE_NAME, n_t_pre, nrow(turmas), n_e_pre, nrow(escolas), MIN_CELL))
  message("Bloco 1 (instancia): turmas ", nrow(turmas), ", escolas ", nrow(escolas))
})

# ─── loader de 1 ano (features + regiao), rotulados ──────────────────────────
load_year <- function(y) {
  f <- readRDS(file.path(FEATURES_DIR, sprintf("features_%d.rds", y)))
  e <- rd_clean("BAS_ESCOLA", y)
  if (!is.null(e)) f <- merge(f, e[, .(CO_ENTIDADE, regiao = REGIAO[substr(CO_UF, 1, 1)])],
                              by = "CO_ENTIDADE", all.x = TRUE)
  f <- f[!is.na(evadiu)]
  f[, faixa_idade := cut(NU_IDADE_REFERENCIA, c(-1,6,10,14,17,200),
                         labels = c("<=6","7-10","11-14","15-17","18+"))]
  f
}

# ═══ BLOCK 2 — DESCRIPTIVE / EDA (streamed year by year, no pooling) ═════════
guard("descritivas", {
  strata <- c("NU_ANO","TP_ETAPA_ENSINO","TP_DEPENDENCIA","regiao","faixa_idade",
              "flag_defasado","TP_SEXO","TP_COR_RACA","TP_ZONA_RESIDENCIAL")
  acc <- setNames(vector("list", length(strata)), strata)
  for (y in LABEL_YEARS) {                       # one year in memory at a time
    f <- load_year(y)
    for (s in intersect(strata, names(f))) {
      a <- f[, .(n = .N, ev = sum(evadiu)), by = s]; setnames(a, s, "categoria")
      acc[[s]] <- rbind(acc[[s]], a)
    }
    rm(f); gc(verbose = FALSE)
  }
  agg <- lapply(acc, function(a) if (!is.null(a))
    a[, .(n = sum(n), taxa_evasao = round(100*sum(ev)/sum(n),3)), by = categoria])

  save_tbl(suppress(agg$NU_ANO[order(categoria)]), "desc_evasao_por_ano")
  save_fig(ggplot(suppress(agg$NU_ANO), aes(as.integer(as.character(categoria)), taxa_evasao)) +
             geom_line() + geom_point() + labs(x = "Ano", y = "Taxa de evasao (%)") + theme_minimal(),
           "desc_evasao_por_ano")
  for (s in setdiff(names(agg), "NU_ANO"))
    if (!is.null(agg[[s]])) save_tbl(suppress(agg[[s]]), paste0("desc_evasao_por_", tolower(s)))
  if (!is.null(agg$flag_defasado))
    save_fig(ggplot(suppress(agg$flag_defasado), aes(factor(categoria), taxa_evasao)) + geom_col() +
               labs(x = "Defasado (0/1)", y = "Taxa de evasao (%)") + theme_minimal(),
             "desc_evasao_por_defasado")
  message("Bloco 2 (descritivas): ok")
})

# ═══ BLOCKS 3-5 — MODEL-DEPENDENT ════════════════════════════════════════════
if (EMIT_MODEL_DEPENDENT) {
  risk_te <- readRDS(file.path(RISK_DIR, sprintf("risk_%d.rds", TEST_YEAR)))
  risk_te <- risk_te[!is.na(evadiu)]        # drop unlabeled (concluinte-final) rows
  yte <- risk_te$evadiu
  preds <- list(logit = risk_te$r_logit, gam = risk_te$r_gam, lgb = risk_te$r_lgb)

  # BLOCK 3 — evaluation
  guard("avaliacao", {
    met <- rbindlist(lapply(names(preds), function(m) data.table(
      model = m, auc_roc = round(auc_roc(yte, preds[[m]]),3),
      auc_pr = round(auc_pr(yte, preds[[m]]),3),
      brier = round(mean((preds[[m]]-yte)^2),4),
      recall_at_10 = round(rec_at_k(yte, preds[[m]], .10),3))))
    save_tbl(met, "eval_metrics")

    roc <- rbindlist(lapply(names(preds), function(m) { p <- preds[[m]]
      o <- order(p, decreasing=TRUE); yy <- yte[o]
      data.table(model = m, fpr = cumsum(yy==0)/sum(yy==0), tpr = cumsum(yy==1)/sum(yy==1)) }))
    save_fig(ggplot(roc, aes(fpr, tpr, color = model)) + geom_line() +
               geom_abline(lty = 2, color = "grey") + labs(x="FPR", y="TPR") + theme_minimal(), "eval_roc")

    cal <- rbindlist(lapply(names(preds), function(m) { p <- preds[[m]]
      b <- cut(p, seq(0, max(p)+1e-9, length.out = 11), include.lowest = TRUE)
      data.table(model = m, p = p, y = yte, b = b)[, .(mean_pred = mean(p), obs = mean(y), n = .N),
        by = .(model, b)] }))
    save_tbl(cal[, .(model, mean_pred = round(mean_pred,4), obs = round(obs,4), n)][n >= MIN_CELL],
             "eval_calibracao")
    save_fig(ggplot(cal[n >= MIN_CELL], aes(mean_pred, obs, color = model)) + geom_line() + geom_point() +
               geom_abline(lty = 2, color = "grey") + labs(x="Risco previsto", y="Evasao observada") +
               theme_minimal(), "eval_calibracao")

    gains <- rbindlist(lapply(names(preds), function(m) { p <- preds[[m]]
      o <- order(p, decreasing=TRUE)
      data.table(model = m, frac_alvo = seq_along(o)/length(o),
                 frac_capturado = cumsum(yte[o]==1)/sum(yte==1)) }))
    save_fig(ggplot(gains, aes(frac_alvo, frac_capturado, color = model)) + geom_line() +
               geom_abline(lty = 2, color = "grey") + labs(x="Fracao alvejada", y="Evadidos capturados") +
               theme_minimal(), "eval_ganhos")
    message("Bloco 3 (avaliacao): ok")
  })

  # BLOCK 5 — fairness by stratum
  guard("fairness", {
    fte <- readRDS(file.path(FEATURES_DIR, sprintf("features_%d.rds", TEST_YEAR)))
    e   <- rd_clean("BAS_ESCOLA", TEST_YEAR)
    if (!is.null(e)) fte <- merge(fte, e[, .(CO_ENTIDADE, regiao = REGIAO[substr(CO_UF,1,1)])],
                                  by = "CO_ENTIDADE", all.x = TRUE)
    d <- cbind(risk_te, fte[match(risk_te$CO_PESSOA_FISICA, fte$CO_PESSOA_FISICA),
                            .(TP_DEPENDENCIA, TP_COR_RACA, regiao)])
    fair <- rbindlist(lapply(c("TP_DEPENDENCIA","TP_COR_RACA","regiao"), function(s)
      d[, .(dimensao = s, n = .N, auc_lgb = round(auc_roc(evadiu, r_lgb),3),
            recall10_lgb = round(rec_at_k(evadiu, r_lgb, .10),3)), by = s][
        , setnames(.SD, s, "categoria")]), fill = TRUE)
    save_tbl(suppress(fair), "fairness_por_estrato")
    message("Bloco 5 (fairness): ok")
  })

  # BLOCK 4 — interpretability
  guard("interpretabilidade", {
    imp <- fread(file.path(RISK_DIR, "_lgb_importance.csv"))
    top <- head(imp[order(-mean_abs_shap)], 15)
    save_fig(ggplot(top, aes(reorder(feature, mean_abs_shap), mean_abs_shap)) + geom_col() +
               coord_flip() + labs(x = NULL, y = "|SHAP| medio (LightGBM)") + theme_minimal(),
             "interp_shap_importance", h = 5)

    # GAM shape functions from the model trained in 04 (no refit, no data held).
    g <- readRDS(file.path(RISK_DIR, "model_gam.rds"))
    grDevices::pdf(file.path(OUT, "figures", "interp_gam_shapes.pdf"), width = 9, height = 7)
    plot(g, pages = 1, se = FALSE, scale = 0); grDevices::dev.off()
    message("Bloco 4 (interpretabilidade): ok")
  })
} else message("EMIT_MODEL_DEPENDENT=FALSE — blocos 3-5 pulados (extracao descritiva-cedo).")

# ═══ BLOCK 6 — LEIA-ME (solicitacao de extracao) ═════════════════════════════
# Documento formal conforme a secao 2.11 do Guia do Usuario do SEDAP: descreve os
# documentos, as tabelas utilizadas, os cruzamentos e a unidade amostral minima.
descr <- function(f) {
  s <- tools::file_path_sans_ext(f)
  if (grepl("^desc_evasao_por_", s))
    return(paste("Taxa de evasao agregada por", gsub("_", " ", sub("desc_evasao_por_", "", s))))
  switch(s,
    turmas = "Massa de risco (soma do risco estimado) e numero de alunos, por turma",
    escolas = "Massa de risco, numero de alunos e atributos agregados, por escola",
    eval_metrics = "Metricas de desempenho dos modelos no ano de teste",
    eval_calibracao = "Calibracao (risco previsto medio x evasao observada) por modelo",
    fairness_por_estrato = "Desempenho do modelo por estrato observavel (regiao, cor/raca, dependencia)",
    eval_roc = "Curva ROC por modelo", eval_ganhos = "Curva de ganhos por modelo",
    interp_shap_importance = "Importancia media de variaveis (SHAP, LightGBM)",
    interp_gam_shapes = "Funcoes de forma das variaveis continuas (GAM)",
    s)
}
inventory <- function(sub, ext) {
  fs <- sort(list.files(file.path(OUT, sub), pattern = ext))
  if (!length(fs)) return(character(0))
  c("| Nº | Arquivo | Conteúdo |", "|---|---|---|",
    sprintf("| %02d | `%s/%s` | %s |", seq_along(fs), sub, fs, vapply(fs, descr, character(1))))
}
tabs_inst <- inventory("instance", "\\.csv$")
tabs_out  <- inventory("tables",   "\\.csv$")
figs_out  <- inventory("figures",  "\\.pdf$")
n_tab <- length(list.files(file.path(OUT,"instance"),"\\.csv$")) + length(list.files(file.path(OUT,"tables"),"\\.csv$"))
n_fig <- length(list.files(file.path(OUT,"figures"),"\\.pdf$"))

body <- c(
  "# Leia-me — Solicitacao de extracao de resultados",
  "",
  sprintf("**Projeto:** %s  ", PROJETO$titulo),
  sprintf("**Pesquisador titular:** %s  ", PROJETO$autor),
  sprintf("**Orientador:** %s  ", PROJETO$orientador),
  sprintf("**Instituicao:** %s  ", PROJETO$instituicao),
  sprintf("**Processo SEI:** %s  ", PROJETO$processo),
  sprintf("**Data:** %s  ", format(Sys.time(), "%d/%m/%Y")),
  sprintf("**Ano de referencia da instancia:** %d  ", YEAR),
  sprintf("**Recorte:** %s", SCOPE_NAME),
  "",
  "## 1. Objetivo",
  "",
  "Solicita-se a liberacao de tabelas e figuras **agregadas**, produzidas a partir",
  "de modelagem estatistica sobre os microdados, para subsidiar as etapas de analise",
  "e de otimizacao previstas no projeto. Os documentos nao contem dados",
  "individualizados nem fragmentos das bases consultadas.",
  "",
  "## 2. Bases de dados e periodo de referencia",
  "",
  "Censo Escolar da Educacao Basica, edicoes **2017 a 2024**: BAS_ESCOLA, BAS_TURMA,",
  "BAS_MATRICULA e BAS_SITUACAO.",
  "",
  "## 3. Cruzamentos realizados",
  "",
  "- BAS_SITUACAO x BAS_MATRICULA (por ID_MATRICULA) e BAS_MATRICULA x BAS_TURMA",
  "  (por ID_TURMA) x BAS_ESCOLA (por CO_ENTIDADE), compondo o registro aluno-ano;",
  "- encadeamento longitudinal por CO_PESSOA_FISICA (ano t e t+1) para a definicao do",
  "  desfecho de evasao;",
  "- estimacao de um modelo de risco por aluno-ano e **agregacao** por turma",
  "  (ID_TURMA), por escola (CO_ENTIDADE) e por estratos observaveis (etapa,",
  "  dependencia, regiao, entre outros).",
  "",
  sprintf("## 4. Documentos solicitados (%d tabelas, %d figuras)", n_tab, n_fig),
  "",
  "### 4.1 Instancia de alocacao (.csv)", "", tabs_inst, "",
  "### 4.2 Tabelas de analise (.csv)", "", tabs_out, "",
  "### 4.3 Figuras (.pdf)", "", figs_out, "",
  "## 5. Protecao de dados e unidade amostral minima",
  "",
  "- Todas as celulas liberadas correspondem a agregacoes com **pelo menos 3",
  "  informantes** (tabulacoes ate o nivel de municipio) e **pelo menos 10",
  "  informantes** (niveis de escola, turma e coorte). As linhas que nao atingiam",
  "  esse minimo foram **omitidas**, de modo que nenhuma tabela liberada contem",
  "  celulas abaixo do limite.",
  sprintf("- Nesta geracao: %s", paste(notes, collapse = " ")),
  "- Os documentos contem **exclusivamente resultados agregados** (somas, contagens",
  "  e taxas) de modelagem estatistica; nao ha dados pessoais, dados individualizados,",
  "  valores maximos ou minimos de distribuicoes, nem rankings de unidades ou",
  "  variaveis que possibilitem identificacao.",
  "- Os identificadores de escola e de turma foram substituidos por codigos",
  "  sequenciais, sem correspondencia com os codigos de origem.",
  "- Arquivos nao comprimidos; tabelas em .csv e figuras em .pdf, individualizadas.")

writeLines(body, file.path(OUT, "Leia-me.txt"))

guard("leia-me-pdf", if (requireNamespace("rmarkdown", quietly = TRUE) &&
                         requireNamespace("tinytex", quietly = TRUE) && tinytex::is_tinytex()) {
  src <- file.path(OUT, "_leia_src.md")
  writeLines(c("---", "title: \"\"", "output: pdf_document", "geometry: margin=2.5cm", "---", "", body), src)
  rmarkdown::render(src, output_file = "Leia-me.pdf", output_dir = OUT, quiet = TRUE)
  unlink(src)
} else message("  tinytex/LaTeX ausente — Leia-me.pdf nao gerado (rodar tinytex::install_tinytex() na sala)."))

message("\nConcluido. Saida em ", normalizePath(OUT, mustWork = FALSE))
