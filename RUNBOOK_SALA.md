# Runbook — rodar o pipeline out-of-core na sala SEDAP

Pipeline chunked (particionado por aluno) para caber em pouca RAM. Validado em
densidade real (25M linhas/ano, 6M por partição = 48M reais com K=8):
picos **01=1,7 GB · 02=2,5 GB · 03=3,3 GB · 04≈6,4 GB** — folga sob 12 GB.

## 1. O que levar para a sala
Só a pasta `scripts/` com estes arquivos (o resto é de fora da sala):
- `scripts/ooc_common.R`
- `scripts/01_load_clean_ooc.R`
- `scripts/02_build_label_ooc.R`
- `scripts/03_build_features_ooc.R`
- `scripts/04_model_risk_ooc.R`
- `scripts/report_A_ooc.R`

NÃO precisa: gerador sintético, scripts 01–04 originais, dados.
Copie a pasta do projeto para o **T:** (drive gravável); rode com a working
directory na raiz do projeto no T:.

## 2. Pré-requisitos (uma vez)
- Pacotes: `data.table, Matrix, glmnet, mgcv, lightgbm` (já pedidos ao SEDAP).
- Desligar auto-restore do `.RData` (Tools → Global Options): "Restore .RData" OFF,
  "Save workspace on exit = Never". Workspace no **T:**.

## 3. Setup (cole no console do RStudio, ajustando os caminhos reais)
```r
setwd("T:/.../dropout-research")          # raiz do projeto (onde está scripts/)
Sys.setenv(
  # 01  (LÊ do B:, ESCREVE no T:)
  CLEAN_IN    = "B:/CENSO ESCOLAR",       # raiz onde ficam as subpastas BAS_* (busca recursiva)
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
  # report_A (extração)
  REPORT_RISK  = "T:/dropout/model",
  REPORT_FEAT  = "T:/dropout/features",
  REPORT_CLEAN = "T:/dropout/clean",
  REPORT_OUT   = "T:/dropout/outputs/report_A",
  REPORT_YEAR  = "2024",       # ano da instância
  REPORT_TEST  = "2023",
  REPORT_LABELS= "2019:2023",
  # globais
  OOC_K        = "8",          # 48M/8 = 6M por partição -> pico ~6 GB (validado)
  OOC_CHUNK    = "2000000",    # bloco de leitura do 01
  OOC_COMPRESS = "TRUE",       # .rds comprimido (economiza T:)
  MODEL_SAMPLE = "5e6"         # amostra de treino p/ glmnet/GAM/LightGBM
)
```

## 4. Pré-voo (recomendado): testar 1 ano antes do run completo
```r
Sys.setenv(CLEAN_YEARS = "2019")          # só 2019
source("scripts/01_load_clean_ooc.R")     # confirma que ACHA os arquivos no B: e o pico
```
Se aparecer `[BAS_XXX 2019] not found`, o padrão de nome/pasta não bateu — me
avise o nome real dos arquivos. Se rodar e imprimir o pico, restaure
`Sys.setenv(CLEAN_YEARS="2017:2024")` e siga.

## 5. Rodar (um de cada vez; confira o pico impresso ao fim de cada)
```r
source("scripts/01_load_clean_ooc.R")     # B: -> T:/dropout/clean (partições por aluno)
source("scripts/02_build_label_ooc.R")    # -> T:/dropout/labels
source("scripts/03_build_features_ooc.R") # -> T:/dropout/features
source("scripts/04_model_risk_ooc.R")     # -> T:/dropout/model (risk_{ano}, _metrics, etc.)
source("scripts/report_A_ooc.R")          # -> T:/dropout/outputs/report_A (instância + tabelas + figuras + Leia-me)
```
Cada script imprime `peak=X.XGB` por partição/estágio. Se algum passar de ~10 GB,
suba `OOC_K` (ver §6) e re-rode aquele script.
O `report_A_ooc` lê as MESMAS partições (usa `OOC_K`); é leve (agrega para turma/
escola/estratos por partição) e produz o pacote de extração + Leia-me do SEDAP.

## 6. Ajuste de memória (se precisar)
O pico ≈ nº de linhas por partição = (linhas/ano) / `OOC_K`. Para baixar o pico,
**aumente `OOC_K`** e re-rode do 01 (a partição precisa ser consistente em toda a
cadeia). Referência (validada a 6M/partição):
- `OOC_K=8`  → ~6M/partição → pico 04 ~6,4 GB (recomendado p/ 30 GB).
- `OOC_K=12` → ~4M/partição → pico ~4–5 GB (mais margem; ~50% mais arquivos).
- `OOC_K=16` → ~3M/partição → pico ~3–4 GB (se a RAM for realmente baixa).
Também dá para baixar `OOC_CHUNK` (ex.: `1000000`) se o **01** ficar alto.

## 7. Observações
- **Disco no T:**: intermediários comprimidos ~100 GB no nacional. Confira espaço
  antes. Se apertar, apague as partições de `clean` após rodar 02+03 (elas não são
  mais lidas pelo 04).
- **01 lê do B: (read-only), tudo o mais escreve no T:.** Não gravar no B:.
- **04**: LightGBM treina na amostra (`MODEL_SAMPLE`), igual a glmnet/GAM; escora
  TODOS os alunos. Para treinar o LightGBM em 100% dos dados use `LGB_FULL=1`
  (via arquivo) — mas é MUITO lento (>1h só a construção); não recomendado na sala.
- Saída para a Metade B: `T:/dropout/model/risk_{ano}_p*.rds` (r_logit, r_gam,
  r_lgb por aluno-ano). O `report_A_ooc` agrega isso para turma/escola + estratos e
  monta o pacote de extração (turmas.csv, escolas.csv, tabelas, figuras, Leia-me).
- O que sai da sala é SÓ o conteúdo de `T:/dropout/outputs/report_A/` (agregados,
  células suprimidas por n<10, ids anonimizados) — conferir contra as regras do
  SEDAP antes de solicitar a extração.
```
