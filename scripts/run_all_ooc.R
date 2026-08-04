###############################################################################
# run_all_ooc.R — roda a cadeia out-of-core em sequência no RStudio.
#
# Como usar: EDITE os caminhos abaixo (T:/ e B:/), ajuste OOC_K, e rode este
# arquivo inteiro (botão "Source"). Ou, para isolar memória e não travar o
# console, rode como "Source as Background Job" (aba Jobs do RStudio).
#
# Cada script é sourceado em um ambiente novo (local=new.env()) e seguido de
# gc(), então os objetos de um NÃO se acumulam no próximo (eles se comunicam por
# arquivos .rds, não por variáveis). Se algum script der erro, a sequência para
# ali e você vê qual foi.
###############################################################################

setwd("T:/CAMINHO/PARA/dropout-research")     # <-- raiz do projeto (onde está scripts/)

Sys.setenv(
  # 01  (LÊ do B:, ESCREVE no T:)
  CLEAN_IN    = "B:/CENSO ESCOLAR",           # <-- raiz com as subpastas BAS_*
  CLEAN_OUT   = "T:/dropout/clean",
  CLEAN_YEARS = "2017:2024",
  # 02
  LABEL_IN    = "T:/dropout/clean",
  LABEL_OUT   = "T:/dropout/labels",
  LABEL_YEARS = "2019:2024",
  # 03
  FEAT_CLEAN  = "T:/dropout/clean",
  FEAT_LABELS = "T:/dropout/labels",
  FEAT_OUT    = "T:/dropout/features",
  FEAT_YEARS  = "2019:2024",
  FEAT_HIST   = "2017",
  # 04
  MODEL_FEAT  = "T:/dropout/features",
  MODEL_LABELS= "T:/dropout/labels",
  MODEL_OUT   = "T:/dropout/model",
  MODEL_SCORE = "2019:2024",
  # report_A
  REPORT_RISK  = "T:/dropout/model",
  REPORT_FEAT  = "T:/dropout/features",
  REPORT_CLEAN = "T:/dropout/clean",
  REPORT_OUT   = "T:/dropout/outputs/report_A",
  REPORT_YEAR  = "2024",
  REPORT_TEST  = "2023",
  REPORT_LABELS= "2019:2023",
  # globais  (pico ≈ linhas_por_ano / OOC_K)
  OOC_K        = "16",         # <-- 48M/16 = 3M por partição -> pico esperado ~5-6GB
  OOC_CHUNK    = "2000000",
  OOC_COMPRESS = "TRUE",
  MODEL_SAMPLE = "5e6"
)

steps <- c("01_load_clean_ooc", "02_build_label_ooc", "03_build_features_ooc",
           "04_model_risk_ooc", "report_A_ooc")
for (s in steps) {
  message("\n########## ", s, "  (", format(Sys.time(), "%H:%M"), ") ##########")
  source(file.path("scripts", paste0(s, ".R")), local = new.env())
  gc()
}
message("\n########## PIPELINE COMPLETO ##########")
