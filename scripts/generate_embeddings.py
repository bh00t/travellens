"""
generate_embeddings.py — Phase 3: Review Embedding Job
=======================================================
What this script does:
  Reads every row in reviews_raw where embedding IS NULL, converts the
  review_text into a 384-dimensional vector using all-MiniLM-L6-v2, and
  writes those vectors back into the embedding column.

  After all rows are embedded, it builds an IVFFlat index on the embedding
  column so that cosine similarity queries in Phase 4 are fast.

Why this matters:
  The embedding column is what powers the semantic search path. Without it,
  queries like "find reviews about noisy AC" would need exact keyword matches.
  With it, pgvector can find reviews that *mean* the same thing even if they
  use different words.

Idempotency:
  The job only processes rows where embedding IS NULL. Safe to re-run at any
  time — if it crashes halfway, restart it and it picks up where it left off.
  Future reviews ingested with NULL embeddings are handled automatically by
  the same WHERE clause, no code changes needed.
"""

import os
import logging
from typing import Optional

import psycopg2
import torch
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────

# The model that converts review text into vectors.
# all-MiniLM-L6-v2 produces 384-dimensional vectors. It's a 6-layer distilled
# model trained specifically on paraphrase tasks, meaning reviews that talk
# about the same thing (even in different words) end up geometrically close
# in vector space. ~90MB on disk, runs fully offline.
MODEL_NAME = "all-MiniLM-L6-v2"

# How many reviews to embed in one GPU pass.
# 128 is the sweet spot for RTX 3070 (8GB VRAM) at typical Indian hotel review
# lengths (80–150 chars). Going higher risks OOM on batches with unusually
# long reviews. Going lower wastes GPU throughput.
BATCH_SIZE = 128

# all-MiniLM-L6-v2 has a hard limit of 256 tokens (~1,000 characters).
# Text beyond this is silently truncated by the model internally. We truncate
# explicitly here so we can log how many reviews hit the ceiling — and so the
# behaviour is visible rather than hidden inside the model.
# We do NOT skip long reviews. Skipping = NULL embedding = invisible to search.
MAX_CHARS = 1000

# IVFFlat clustering parameter. pgvector rule of thumb: lists ≈ rows / 1000.
# For 30K reviews that's 30. The blueprint says 100 — that's too high for this
# row count and will slow down queries without improving recall.
# IVFFlat works by grouping all vectors into `lists` clusters at index build
# time. At query time it searches only the nearest clusters, not all 30K rows.
IVFFLAT_LISTS = 30

# Postgres connection — reads from .env file.
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB",   "travellens"),
    "user":     os.getenv("POSTGRES_USER", "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Helper functions ──────────────────────────────────────────────────────────

def prepare_text(text: Optional[str]) -> str:
    """
    Clean and truncate a single review text before embedding.

    Steps:
      1. Handle None/empty — return empty string (model handles it gracefully)
      2. Strip leading/trailing whitespace
      3. Truncate to MAX_CHARS if longer

    We do NOT do any other cleaning (lowercasing, punctuation removal etc.)
    because sentence-transformers handles that internally. Over-cleaning can
    actually hurt embedding quality.
    """
    if not text:
        return ""
    text = text.strip()
    if len(text) > MAX_CHARS:
        text = text[:MAX_CHARS]
    return text


def fetch_unembedded(conn) -> list[tuple[str, str]]:
    """
    Fetch all reviews that don't have an embedding yet.

    Returns a list of (review_id, review_text) tuples.

    ORDER BY review_id gives deterministic ordering — if the job crashes and
    restarts, it processes rows in the same sequence, which makes debugging
    easier. It also means progress bars are consistent across runs.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT review_id, review_text
            FROM reviews_raw
            WHERE embedding IS NULL
            ORDER BY review_id
        """)
        return cur.fetchall()


def write_batch(conn, batch: list[tuple]) -> None:
    """
    Write one batch of embeddings back to Postgres.

    batch is a list of (embedding_as_list, review_id) tuples.

    Why executemany instead of row-by-row UPDATE:
      executemany sends all 128 updates in a single round-trip to Postgres.
      Row-by-row would mean 128 separate network calls per batch — ~234 extra
      round-trips for the full 30K job. At localhost latency this costs ~2-3
      extra seconds, but the pattern matters more at scale or over a real
      network.

    Why commit inside this function:
      Committing per batch means if the job crashes, we only lose the current
      batch (at most 128 rows), not everything since the last commit. The next
      run picks up from the last committed row via WHERE embedding IS NULL.
    """
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE reviews_raw SET embedding = %s WHERE review_id = %s",
            batch,
        )
    conn.commit()


def build_ivfflat_index(conn) -> None:
    """
    Build the IVFFlat cosine similarity index on the embedding column.

    Why this must run AFTER all embeddings are written:
      IVFFlat works by running k-means clustering on the existing vectors at
      build time. If you build the index on an empty or partially-filled table,
      the clusters are wrong and recall degrades — you'll miss relevant results
      at query time. Always populate data first, index second.

    Why DROP IF EXISTS first:
      Makes the function safe to call multiple times (e.g. on re-runs after a
      model change where you wiped and re-embedded everything).

    vector_cosine_ops tells pgvector to optimise for cosine distance (the <=>
    operator). This is the right choice for semantic similarity — we care about
    the angle between vectors, not their magnitude.
    """
    log.info("Building IVFFlat index (lists=%d)...", IVFFLAT_LISTS)
    with conn.cursor() as cur:
        cur.execute("DROP INDEX IF EXISTS idx_reviews_embedding;")
        cur.execute(f"""
            CREATE INDEX idx_reviews_embedding
            ON reviews_raw
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = {IVFFLAT_LISTS});
        """)
    conn.commit()
    log.info("IVFFlat index built successfully.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # ── 1. Detect device ──────────────────────────────────────────────────────
    # sentence-transformers will use whatever device we pass here.
    # CUDA = RTX 3070 GPU (~17 seconds for 30K reviews)
    # CPU  = falls back gracefully but takes ~3 minutes
    device = "cuda" if torch.cuda.is_available() else "cpu"
    log.info("Device: %s", device.upper())
    if device == "cpu":
        log.warning(
            "CUDA not available — running on CPU. "
            "Expect ~3 min runtime. Check Nvidia drivers if this is unexpected."
        )

    # ── 2. Load model ─────────────────────────────────────────────────────────
    # First run downloads ~90MB from HuggingFace and caches it locally.
    # Subsequent runs load from cache — no internet needed.
    log.info("Loading model: %s", MODEL_NAME)
    model = SentenceTransformer(MODEL_NAME, device=device)

    # ── 3. Connect to Postgres ────────────────────────────────────────────────
    conn = psycopg2.connect(**DB_CONFIG)

    # register_vector tells psycopg2 how to serialise Python lists into the
    # pgvector `vector` type. Without this call, the UPDATE would fail with a
    # type error even though the data is correct.
    register_vector(conn)

    # ── 4. Fetch unembedded rows ──────────────────────────────────────────────
    rows = fetch_unembedded(conn)
    total = len(rows)
    log.info("Rows to embed: %d", total)

    if total == 0:
        log.info("Nothing to do — all embeddings already populated.")
        # Still rebuild the index in case it was dropped manually
        build_ivfflat_index(conn)
        conn.close()
        return

    # ── 5. Batch loop ─────────────────────────────────────────────────────────
    truncated = 0  # track how many reviews exceeded MAX_CHARS

    with tqdm(total=total, unit="reviews", desc="Embedding") as pbar:
        for i in range(0, total, BATCH_SIZE):
            batch_rows = rows[i : i + BATCH_SIZE]

            # Prepare texts — clean and truncate
            texts = []
            for _, text in batch_rows:
                if text and len(text) > MAX_CHARS:
                    truncated += 1
                texts.append(prepare_text(text))

            # Embed the batch.
            # convert_to_numpy=True gives us numpy arrays which .tolist()
            # converts to plain Python lists — the format psycopg2 + pgvector
            # expects for the UPDATE.
            # show_progress_bar=False because tqdm above already shows progress.
            embeddings = model.encode(
                texts,
                batch_size=BATCH_SIZE,
                show_progress_bar=False,
                convert_to_numpy=True,
            )

            # Build write payload: list of (embedding_list, review_id)
            # Order matters — must match the UPDATE's %s placeholders.
            write_payload = [
                (emb.tolist(), row[0])
                for emb, row in zip(embeddings, batch_rows)
            ]

            write_batch(conn, write_payload)
            pbar.update(len(batch_rows))

    log.info(
        "Embedding complete. Truncated: %d / %d reviews (%.1f%%)",
        truncated, total, 100 * truncated / total
    )

    # ── 6. Build IVFFlat index ────────────────────────────────────────────────
    # All rows are now embedded — safe to build the index.
    build_ivfflat_index(conn)

    conn.close()
    log.info("Phase 3 complete. Run acceptance tests from the spec.")


if __name__ == "__main__":
    main()
