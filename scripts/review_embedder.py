"""Continuous review-embedding micro-batch for TravelLens (B-030b).

Reads reviews_raw WHERE embedding IS NULL AND review_text IS NOT NULL in
bounded batches, encodes with all-MiniLM-L6-v2 (the same 384-d model used
by generate_embeddings.py and ai/semantic_search.py), and writes vectors back.
Idempotent — only processes NULL rows; safe to restart at any time.

Single-instance enforced via pg_try_advisory_lock(7400050).  A second
concurrent instance logs the conflict and exits with code 1.

After clearing the backlog for the first time the process logs a notice to
rebuild the IVFFlat index with the correct lists value for the current row
count (~121K rows ≈ lists=120).  The rebuild is intentionally NOT automated
so the operator can run a representative semantic query before and after to
confirm recall is unaffected (see Part B acceptance notes).

Run:
    python -m scripts.review_embedder

Config (all env-overridable):
    EMBED_INTERVAL_SECONDS   default 15   — sleep between caught-up polls
    EMBED_BATCH_SIZE         default 2000 — reviews per encode+write pass
"""

import os
import sys
import time
import logging

import psycopg2
import psycopg2.extras
import torch
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

EMBED_INTERVAL_SECONDS = int(os.getenv("EMBED_INTERVAL_SECONDS", "15"))
EMBED_BATCH_SIZE       = int(os.getenv("EMBED_BATCH_SIZE",       "2000"))

# Same model and truncation limit as generate_embeddings.py and semantic_search.py.
# If this ever changes, ALL three must change together AND the index must be rebuilt
# from scratch (old and new vectors are geometrically incompatible).
MODEL_NAME = "all-MiniLM-L6-v2"
MAX_CHARS  = 1000

# Advisory lock key — unique to this process, distinct from gold (7400040).
EMBED_ADVISORY_LOCK_KEY = 7_400_050

# Encode batch size passed to sentence_transformers. 128 is the safe ceiling
# for RTX 3070 (8GB VRAM) at typical Indian hotel review lengths.  The outer
# EMBED_BATCH_SIZE (500) is the DB fetch size; we may encode up to 128 at a
# time inside that.
_ENCODE_BATCH = 128

DB_PARAMS = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "travellens"),
    "user":     os.getenv("POSTGRES_USER",     "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _prepare(text) -> str:
    """Strip + truncate one review text before encoding."""
    if not text:
        return ""
    text = text.strip()
    return text[:MAX_CHARS] if len(text) > MAX_CHARS else text


def embed_batch(conn, model) -> int:
    """Fetch up to EMBED_BATCH_SIZE unembedded rows, encode, write back.

    Returns the number of rows embedded (0 = backlog clear).
    Commits per call so a crash loses at most one batch.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT review_id, review_text
            FROM reviews_raw
            WHERE embedding IS NULL AND review_text IS NOT NULL
            LIMIT %s
            """,
            (EMBED_BATCH_SIZE,),
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    texts = [_prepare(text) for _, text in rows]

    embeddings = model.encode(
        texts,
        batch_size=_ENCODE_BATCH,
        show_progress_bar=False,
        convert_to_numpy=True,
    )

    payload = [(emb.tolist(), rid) for emb, (rid, _) in zip(embeddings, rows)]
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE reviews_raw SET embedding = %s WHERE review_id = %s",
            payload,
        )
    conn.commit()
    return len(rows)


def _log_index_notice(log) -> None:
    """Remind the operator to rebuild the IVFFlat index after backlog clears."""
    log.info(
        "Backlog cleared — all embeddable reviews now have vectors. "
        "The IVFFlat index (idx_reviews_embedding) was built with lists=30 "
        "for 30K rows. With ~121K rows the correct value is lists=120. "
        "Part B (index re-tune) steps:\n"
        "  1. Run a representative query BEFORE the rebuild:\n"
        "       python -m ai.main 'complaints about AC not working'\n"
        "  2. Rebuild the index:\n"
        "       docker exec travellens-postgres psql -U travellens -d travellens -c "
        "\"DROP INDEX IF EXISTS idx_reviews_embedding; "
        "CREATE INDEX idx_reviews_embedding ON reviews_raw "
        "USING ivfflat (embedding vector_cosine_ops) WITH (lists = 120);\"\n"
        "  3. Run the SAME query AFTER and confirm results are sensibly similar.\n"
        "  If results look worse, keep the old index (restore with lists=30) and report."
    )


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [embedder] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("embedder")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    log.info(
        "Review embedder starting — model=%s device=%s interval=%ds batch=%d",
        MODEL_NAME, device.upper(), EMBED_INTERVAL_SECONDS, EMBED_BATCH_SIZE,
    )

    log.info("Loading model %s (first run may download ~90MB)...", MODEL_NAME)
    model = SentenceTransformer(MODEL_NAME, device=device)
    log.info("Model loaded.")

    conn = psycopg2.connect(**DB_PARAMS)
    register_vector(conn)

    # Advisory lock — prevents two embedder instances from racing on the
    # same NULL rows and writing incompatible vectors.
    with conn.cursor() as _cur:
        _cur.execute("SELECT pg_try_advisory_lock(%s)", (EMBED_ADVISORY_LOCK_KEY,))
        _lock_acquired = _cur.fetchone()[0]
    if not _lock_acquired:
        log.error(
            "Another review_embedder instance holds the advisory lock "
            "(key=%d) — exiting. Kill the stale instance first.",
            EMBED_ADVISORY_LOCK_KEY,
        )
        conn.close()
        sys.exit(1)
    log.info("Advisory lock acquired (key=%d).", EMBED_ADVISORY_LOCK_KEY)

    # Track whether we have already emitted the index re-tune notice so we
    # print it exactly once, on the first "caught up" event.
    _noticed_backlog_cleared = False

    try:
        while True:
            try:
                total_this_pass = 0
                while True:
                    n = embed_batch(conn, model)
                    if n == 0:
                        break
                    total_this_pass += n
                    log.info("Embedded %d reviews this batch (%d this pass)", n, total_this_pass)

                if total_this_pass:
                    log.info("Pass complete: %d reviews embedded", total_this_pass)
                else:
                    log.debug("No unembedded reviews — sleeping %ds", EMBED_INTERVAL_SECONDS)

                # Emit the index re-tune notice the first time the backlog clears
                # (i.e. this pass found zero new rows to embed AND we haven't
                # already noticed it). This fires on the very first "caught up"
                # pass after any earlier pass embedded at least one row — or on
                # startup if the backlog was already empty.
                if not _noticed_backlog_cleared and total_this_pass == 0:
                    _noticed_backlog_cleared = True
                    _log_index_notice(log)

                time.sleep(EMBED_INTERVAL_SECONDS)

            except KeyboardInterrupt:
                raise
            except Exception as exc:
                log.error(
                    "Error in embedding batch: %s — rolling back, retrying in 10s",
                    exc, exc_info=True,
                )
                try:
                    conn.rollback()
                except Exception:
                    pass
                time.sleep(10)

    except KeyboardInterrupt:
        log.info("Shutting down.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
