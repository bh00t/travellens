"""Per-review sentiment scorer for TravelLens (B-026 Stage 1).

Reads reviews_raw WHERE sentiment_label IS NULL AND review_text IS NOT NULL in
bounded batches, classifies each review with CardiffNLP
twitter-roberta-base-sentiment-latest (3-class: positive / negative / neutral),
and writes (sentiment_label, sentiment_score) back. Idempotent — only processes
unscored rows; safe to restart at any time.

WHY a real classifier, not the star rating (the B-062 mitigation this replaces):
    Rating is a proven-bad sentiment proxy (L-012). Genuine complaints live in
    mixed-sentiment 3★ reviews that the ≤2★ / ≥4★ rating filter never reaches.
    A dedicated 3-class transformer recovers those — see the B-026 audit.

WHY safetensors + a pinned revision (do NOT remove):
    The model's main revision ships only a PyTorch `.bin` checkpoint, which
    transformers 4.57.x refuses to load on the project's pinned torch 2.3.0
    (CVE-2025-32434 — would require torch >= 2.6). We do NOT upgrade torch
    (it would risk the cu121 / pgvector / sentence-transformers stack — L-008).
    Instead we pin MODEL_REVISION to the auto-converted safetensors commit and
    pass use_safetensors=True, which loads cleanly on torch 2.3.0 with no upgrade.
    Pinning the revision also makes the backfill reproducible.

Sentiment is INDEPENDENT of the embedding — this process never touches the
`embedding` column or the IVFFlat index. It only fills the two B-026 columns.

Single-instance enforced via pg_try_advisory_lock(7400070) — distinct from the
producer (7400030), gold (7400040), embedder (7400050), quarantine (7400060).
A second concurrent instance logs the conflict and exits with code 1.

Run:
    python -m scripts.review_sentiment_scorer --once   # one-time backfill, then exit
    python -m scripts.review_sentiment_scorer           # continuous micro-batch loop

Config (all env-overridable):
    SENTIMENT_INTERVAL_SECONDS  default 15   — sleep between caught-up polls (loop mode)
    SENTIMENT_BATCH_SIZE        default 2000 — reviews per DB fetch + score + write pass
"""

import os
import sys
import time
import argparse
import logging

import psycopg2
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

SENTIMENT_INTERVAL_SECONDS = int(os.getenv("SENTIMENT_INTERVAL_SECONDS", "15"))
SENTIMENT_BATCH_SIZE       = int(os.getenv("SENTIMENT_BATCH_SIZE",       "2000"))

# CardiffNLP 3-class sentiment (id2label: 0=negative, 1=neutral, 2=positive).
# MODEL_REVISION is the safetensors-bearing commit (see module docstring) — keep
# it pinned. If the model is ever changed, the full backfill must be re-run.
MODEL_NAME     = "cardiffnlp/twitter-roberta-base-sentiment-latest"
MODEL_REVISION = "d616e2bdfcdb0ca89d9b6efc4909e1db063af290"

# Same text-length cap as the embedder; the model also hard-truncates at 256
# tokens. Reviews are short (42–237 words) so this rarely bites.
MAX_CHARS  = 1000
MAX_TOKENS = 256

# GPU encode batch. 64 is comfortable for roberta-base on the RTX 3070 (8GB) at
# typical review lengths; the outer SENTIMENT_BATCH_SIZE is the DB fetch size.
_ENCODE_BATCH = 64

# Advisory lock key — unique to this process.
SENTIMENT_ADVISORY_LOCK_KEY = 7_400_070

DB_PARAMS = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB",       "travellens"),
    "user":     os.getenv("POSTGRES_USER",     "travellens"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}


# ── Model ─────────────────────────────────────────────────────────────────────

class SentimentModel:
    """Thin wrapper around the CardiffNLP classifier.

    Loaded once; classify() takes a list of texts and returns a list of
    (label, score) tuples where score is the softmax confidence of the chosen
    label, rounded to 3 decimals to fit reviews_raw.sentiment_score NUMERIC(4,3).
    """

    def __init__(self, device: str):
        self.device = device
        self.tokenizer = AutoTokenizer.from_pretrained(
            MODEL_NAME, revision=MODEL_REVISION
        )
        self.model = AutoModelForSequenceClassification.from_pretrained(
            MODEL_NAME, revision=MODEL_REVISION, use_safetensors=True
        ).to(device).eval()
        # {0: 'negative', 1: 'neutral', 2: 'positive'} — read from the model,
        # never hardcoded, so a model swap can't silently mislabel.
        self.id2label = self.model.config.id2label

    def classify(self, texts: list[str]) -> list[tuple[str, float]]:
        out: list[tuple[str, float]] = []
        for i in range(0, len(texts), _ENCODE_BATCH):
            batch = texts[i : i + _ENCODE_BATCH]
            enc = self.tokenizer(
                batch,
                return_tensors="pt",
                truncation=True,
                max_length=MAX_TOKENS,
                padding=True,
            ).to(self.device)
            with torch.no_grad():
                probs = torch.softmax(self.model(**enc).logits, dim=-1)
            scores, idxs = probs.max(dim=-1)
            out.extend(
                (self.id2label[int(j)], round(float(s), 3))
                for j, s in zip(idxs, scores)
            )
        return out


# ── Helpers ───────────────────────────────────────────────────────────────────

def _prepare(text) -> str:
    """Strip + truncate one review text before classification."""
    if not text:
        return ""
    text = text.strip()
    return text[:MAX_CHARS] if len(text) > MAX_CHARS else text


def score_batch(conn, model: SentimentModel) -> int:
    """Fetch up to SENTIMENT_BATCH_SIZE unscored rows, classify, write back.

    Returns the number of rows scored (0 = backlog clear).
    Commits per call so a crash loses at most one batch.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT review_id, review_text
            FROM reviews_raw
            WHERE sentiment_label IS NULL AND review_text IS NOT NULL
            LIMIT %s
            """,
            (SENTIMENT_BATCH_SIZE,),
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    texts = [_prepare(text) for _, text in rows]
    preds = model.classify(texts)

    # (label, score, review_id) — order matches the UPDATE placeholders.
    payload = [
        (label, score, rid)
        for (label, score), (rid, _) in zip(preds, rows)
    ]
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE reviews_raw SET sentiment_label = %s, sentiment_score = %s "
            "WHERE review_id = %s",
            payload,
        )
    conn.commit()
    return len(rows)


def _drain(conn, model, log) -> int:
    """Score every currently-unscored row. Returns the total scored this drain."""
    total = 0
    while True:
        n = score_batch(conn, model)
        if n == 0:
            break
        total += n
        log.info("Scored %d reviews this batch (%d this pass)", n, total)
    return total


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="TravelLens per-review sentiment scorer (B-026)")
    parser.add_argument(
        "--once",
        action="store_true",
        help="One-time backfill: score all unscored rows, then exit (no loop).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [sentiment] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("sentiment")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    log.info(
        "Sentiment scorer starting — model=%s rev=%s device=%s mode=%s",
        MODEL_NAME, MODEL_REVISION[:12], device.upper(),
        "once" if args.once else "loop",
    )

    log.info("Loading model (safetensors; first run may download ~500MB)...")
    model = SentimentModel(device)
    log.info("Model loaded. Labels=%s", model.id2label)

    conn = psycopg2.connect(**DB_PARAMS)

    # Advisory lock — prevents two scorer instances from racing on the same
    # NULL rows.
    with conn.cursor() as _cur:
        _cur.execute("SELECT pg_try_advisory_lock(%s)", (SENTIMENT_ADVISORY_LOCK_KEY,))
        _lock_acquired = _cur.fetchone()[0]
    if not _lock_acquired:
        log.error(
            "Another review_sentiment_scorer instance holds the advisory lock "
            "(key=%d) — exiting. Kill the stale instance first.",
            SENTIMENT_ADVISORY_LOCK_KEY,
        )
        conn.close()
        sys.exit(1)
    log.info("Advisory lock acquired (key=%d).", SENTIMENT_ADVISORY_LOCK_KEY)

    try:
        if args.once:
            t0 = time.time()
            total = _drain(conn, model, log)
            log.info(
                "Backfill complete: %d reviews scored in %.1fs.",
                total, time.time() - t0,
            )
            return

        # Continuous micro-batch loop (mirrors review_embedder.py).
        while True:
            try:
                total = _drain(conn, model, log)
                if total:
                    log.info("Pass complete: %d reviews scored", total)
                else:
                    log.debug("No unscored reviews — sleeping %ds", SENTIMENT_INTERVAL_SECONDS)
                time.sleep(SENTIMENT_INTERVAL_SECONDS)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                log.error(
                    "Error in sentiment batch: %s — rolling back, retrying in 10s",
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
