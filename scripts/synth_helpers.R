###############################################################################
# synth_helpers.R
#
# Pure helpers for synthetic-data generation. Sourced by
# generate_synthetic_data.R. Reads the 8 profile CSVs produced by
# 00b_profile_real_data.R and provides sampling primitives that respect
# per-column marginals, missingness rates, and the conditional
# TP_SITUACAO | TP_ETAPA_ENSINO distribution.
###############################################################################

suppressPackageStartupMessages({
  library(data.table)
})

# ─── PROFILE LOADER ──────────────────────────────────────────────────────────
load_profile <- function(profile_dir) {
  files <- list(
    schema      = "1_schema.csv",
    row_counts  = "2_row_counts.csv",
    missingness = "3_missingness.csv",
    cardinality = "4_cardinality.csv",
    cat_freqs   = "5_cat_freqs.csv",
    numeric     = "6_numeric_summary.csv",
    join        = "7_join_overlap.csv",
    crosstab    = "8_crosstab_situacao.csv"
  )
  out <- lapply(files, function(f) {
    fread(file.path(profile_dir, f), sep = ";", encoding = "UTF-8",
          na.strings = c("", "NA"))
  })
  setkey(out$schema,      table, year, column)
  setkey(out$missingness, table, year, column)
  setkey(out$cardinality, table, year, column)
  setkey(out$cat_freqs,   table, year, column)
  setkey(out$numeric,     table, year, column)
  setkey(out$crosstab,    year, tp_etapa_ensino)
  out
}

# ─── CATEGORICAL SAMPLING ────────────────────────────────────────────────────
# Suppressed cells (n == "X") are imputed with `x_impute` so rare codes
# retain a small but positive sampling probability.
sample_cat <- function(profile, tbl, yr, col, n, x_impute = 2L) {
  d <- profile$cat_freqs[.(tbl, yr, col), nomatch = NULL]
  if (nrow(d) == 0) return(rep(NA_character_, n))
  ns <- suppressWarnings(as.numeric(d$n))
  ns[is.na(ns)] <- x_impute
  probs <- ns / sum(ns)
  sample(d$value, size = n, replace = TRUE, prob = probs)
}

# ─── NUMERIC SAMPLING (piecewise-linear inverse CDF) ─────────────────────────
sample_num <- function(profile, tbl, yr, col, n) {
  if (grepl("^NU_CNPJ", col)) {
    return(sprintf("%014.0f", runif(n, 0, 1e14 - 1)))
  }

  d <- profile$numeric[.(tbl, yr, col), nomatch = NULL]
  if (nrow(d) == 0) {
    # Profiler classified this as categorical (typical for small-cardinality
    # integers like NU_IDADE_REFERENCIA). Fall back to cat_freqs.
    if (nrow(profile$cat_freqs[.(tbl, yr, col), nomatch = NULL]) > 0) {
      return(suppressWarnings(as.numeric(sample_cat(profile, tbl, yr, col, n))))
    }
    return(rep(NA_real_, n))
  }

  p10 <- d$p10; p50 <- d$p50; p90 <- d$p90
  lower <- max(0, 2 * p10 - p50)
  upper <- 2 * p90 - p50

  u   <- runif(n)
  out <- numeric(n)

  m1 <- u <  0.10
  m2 <- u >= 0.10 & u <  0.50
  m3 <- u >= 0.50 & u <  0.90
  m4 <- u >= 0.90

  out[m1] <- lower + (p10 - lower) * (u[m1] / 0.10)
  out[m2] <- p10   + (p50 - p10)   * ((u[m2] - 0.10) / 0.40)
  out[m3] <- p50   + (p90 - p50)   * ((u[m3] - 0.50) / 0.40)
  out[m4] <- p90   + (upper - p90) * ((u[m4] - 0.90) / 0.10)

  out
}

# ─── MISSINGNESS ─────────────────────────────────────────────────────────────
apply_missing <- function(values, profile, tbl, yr, col) {
  d <- profile$missingness[.(tbl, yr, col), nomatch = NULL]
  if (nrow(d) == 0) return(values)
  p <- d$pct_missing / 100
  if (is.na(p) || p <= 0) return(values)
  if (p >= 1) return(rep(NA, length(values)))
  mask <- runif(length(values)) < p
  values[mask] <- NA
  values
}

# ─── CONDITIONAL TP_SITUACAO | TP_ETAPA_ENSINO ───────────────────────────────
# Vectorised: groups input by etapa and samples within each group.
sample_situacao_conditional <- function(profile, yr, etapa_vec, x_impute = 2L) {
  ct <- copy(profile$crosstab[year == yr])
  ct[, n_num := suppressWarnings(as.numeric(n))]
  ct[is.na(n_num), n_num := x_impute]
  ct[, etapa_chr := as.character(tp_etapa_ensino)]

  etapa_in <- as.character(etapa_vec)
  out      <- rep(NA_character_, length(etapa_in))

  for (e in unique(etapa_in)) {
    pos <- which(etapa_in == e)
    if (is.na(e) || nrow(ct[etapa_chr == e]) == 0) {
      out[pos] <- sample_cat(profile, "BAS_SITUACAO", yr, "TP_SITUACAO",
                             length(pos), x_impute)
      next
    }
    sub   <- ct[etapa_chr == e]
    probs <- sub$n_num / sum(sub$n_num)
    out[pos] <- sample(as.character(sub$tp_situacao), length(pos), TRUE, probs)
  }
  out
}

# ─── HIGH-CARDINALITY CODES ──────────────────────────────────────────────────
# For CO_* columns that the profiler did not enumerate (only cardinality
# is known), draw from a synthetic pool of the same size.
gen_codes <- function(n, pool_size, width = 8L) {
  pool <- sprintf(paste0("%0", width, "d"), seq_len(max(1L, pool_size)))
  sample(pool, n, replace = TRUE)
}

# ─── SEQUENTIAL ID HELPER ────────────────────────────────────────────────────
gen_id_seq <- function(start, n) seq.int(from = start, length.out = n)

# Null-coalescing helper used by the column-router.
`%||%` <- function(a, b) if (is.null(a)) b else a

# ─── LONGITUDINAL ATTRITION ──────────────────────────────────────────────────
# Inverse-logit for the per-student annual non-return hazard.
inv_logit <- function(x) 1 / (1 + exp(-x))

# TP_SITUACAO sampler aware of the enrollment panel. Three regimes:
#   - deceased rows           -> "3" (so the label builder's exclusion is testable)
#   - leavers (non-deceased)  -> biased toward abandono/reprovado/sir, giving
#                                TP_SITUACAO_t a real (observable) relationship
#                                with the dropout label
#   - stayers                 -> conditional-on-etapa, as in the marginal profile
# leaver_weights is a named vector over TP_SITUACAO codes; tune to taste.
sample_situacao_panel <- function(profile, yr, etapa_vec, is_leaver, is_deceased,
                                   leaver_weights = c("2" = 0.40, "4" = 0.30,
                                                      "9" = 0.20, "5" = 0.10)) {
  n   <- length(etapa_vec)
  out <- rep(NA_character_, n)

  stay <- !is_leaver & !is_deceased
  if (any(stay)) {
    out[stay] <- sample_situacao_conditional(profile, yr, etapa_vec[stay])
  }
  lv <- is_leaver & !is_deceased
  if (any(lv)) {
    out[lv] <- sample(names(leaver_weights), sum(lv), replace = TRUE,
                      prob = leaver_weights)
  }
  out[is_deceased] <- "3"
  out
}
