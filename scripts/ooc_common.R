###############################################################################
# ooc_common.R
#
# Shared configuration and helpers for the out-of-core (chunked) pipeline.
# All heavy tables are partitioned by a deterministic hash of CO_PESSOA_FISICA
# so that every enrollment/situation/history row of a student lands in the same
# partition. That makes the person-year collapse, the t->t+1 label and the
# per-student history all PARTITION-LOCAL: each partition can be processed on its
# own with a bounded memory footprint, and the union of partitions equals what
# the in-core pipeline would have produced.
#
# Knobs (env overridable so the same code runs tiny here and huge in the room):
#   OOC_K      number of student partitions   (more parts = smaller footprint)
#   OOC_CHUNK  CSV read block size in lines    (caps the read-time peak)
###############################################################################

suppressPackageStartupMessages(library(data.table))

K_PARTS      <- as.integer(Sys.getenv("OOC_K",     "8"))
CHUNK_LINES  <- as.integer(Sys.getenv("OOC_CHUNK", "2000000"))
OOC_COMPRESS <- as.logical(Sys.getenv("OOC_COMPRESS", "TRUE"))   # FALSE = faster, larger .rds

# saveRDS wrapper honoring the compression knob (disk vs speed tradeoff).
save_rds <- function(obj, path) saveRDS(obj, path, compress = OOC_COMPRESS)

# ─── PARTITION HASH ──────────────────────────────────────────────────────────
# Deterministic bucket in 0..K-1 from an id string. INEP CO_PESSOA_FISICA is a
# numeric identifier, so as.numeric() is exact for <=15 digits; for anything
# non-numeric or longer we fall back to a base-36 read of the tail. Correctness
# only needs determinism (same id -> same bucket), not perfect balance.
hash_part <- function(x, K = K_PARTS) {
  v   <- suppressWarnings(as.numeric(x))
  bad <- is.na(v) | !is.finite(v)
  if (any(bad)) {
    xb <- as.character(x[bad]); n <- nchar(xb)
    v[bad] <- suppressWarnings(strtoi(substr(xb, pmax(1L, n - 8L), n), 36L))
  }
  v[is.na(v)] <- 0
  as.integer(abs(v) %% K)
}

# ─── PARTITION FILE I/O ──────────────────────────────────────────────────────
# A logical partition may be written as one file (_pK.rds) or as several block
# files (_pK_bN.rds) when a table is streamed in blocks. read_partition() unions
# whatever is present, so writers and readers stay decoupled.
part_path <- function(dir, tbl, yr, k, blk = NULL)
  file.path(dir, if (is.null(blk)) sprintf("%s_%d_p%d.rds", tbl, yr, k)
                 else               sprintf("%s_%d_p%d_b%d.rds", tbl, yr, k, blk))

list_part_files <- function(dir, tbl, yr, k) {
  f <- list.files(dir, pattern = sprintf("^%s_%d_p%d(_b[0-9]+)?\\.rds$", tbl, yr, k),
                  full.names = TRUE)
  b <- suppressWarnings(as.integer(sub(".*_b([0-9]+)\\.rds$", "\\1", basename(f))))
  b[is.na(b)] <- -1L                       # numeric block order (b2 before b10)
  f[order(b)]
}

read_partition <- function(dir, tbl, yr, k) {
  f <- list_part_files(dir, tbl, yr, k)
  if (!length(f)) return(NULL)
  rbindlist(lapply(f, readRDS), use.names = TRUE, fill = TRUE)
}

has_partition <- function(dir, tbl, yr, k) length(list_part_files(dir, tbl, yr, k)) > 0L

# Peak RSS of this process in GB (Darwin: bytes, Linux: kB), for the memory log.
peak_rss_gb <- function() {
  v <- tryCatch({
    if (.Platform$OS.type != "unix") return(NA_real_)
    r <- system2("ps", c("-o", "rss=", "-p", Sys.getpid()), stdout = TRUE)
    as.numeric(r) / 1024 / 1024          # ps reports kB on both mac and linux
  }, error = function(e) NA_real_)
  round(v, 2)
}
