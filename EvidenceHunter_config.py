# -*- coding: utf-8 -*-

"""BTC HUNTER shared configuration - recent-trade canonical price + exchange-clock baseline V1.2.3."""

from pathlib import Path
import json
import uuid
import hashlib

BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = BASE_DIR / "runtime"
LOG_DIR = BASE_DIR / "logs"
RESEARCH_DIR = BASE_DIR / "research"
ARCHIVE_DIR = BASE_DIR / "archive"

# Research/version identity. The baseline name is unchanged; schema/collector
# versions are bumped because the stored field definitions change.
BASELINE_NAME = "EvidenceHunter_V1.2_BASELINE_FINAL"
SCHEMA_VERSION = "V1.2.3"
FEATURE_VERSION = "V1.2.1"
COLLECTOR_VERSION = "V1.2.3"
OUTCOME_VERSION = "V1.2.3"
MODEL_VERSION = "NONE"
RUN_ID = str(uuid.uuid4())[:12]

# V1 paths kept for compatibility.
STATE_FILE = RUNTIME_DIR / "EvidenceHunter_state.json"
EVENT_FILE = LOG_DIR / "EvidenceHunter_events.jsonl"
SHADOW_FILE = LOG_DIR / "EvidenceHunter_shadow_records.jsonl"

# Authoritative V2 baseline/research paths.
SHADOW_FILE_V2 = LOG_DIR / "v2" / "EvidenceHunter_shadow_records.jsonl"
OUTCOMES_FILE_V2 = LOG_DIR / "v2" / "EvidenceHunter_shadow_outcomes.jsonl"
ORDERFLOW_FILE_V2 = RUNTIME_DIR / "v2" / "orderflow_snapshot.json"
ANALYSIS_FILE_V2 = RUNTIME_DIR / "v2" / "shadow_analysis_v2.json"
EV_STATE_FILE_V2 = RUNTIME_DIR / "v2" / "ev_state.json"
DATASET_SESSION_FILE = RUNTIME_DIR / "v2" / "dataset_session.json"

SYMBOL = "BTCUSDT"
MARKET_TYPE = "USD_M_FUTURES"

PAPER_ONLY = True
AUTO_TRADE = False
AUTO_CANCEL = False
MANUAL_CONFIRMATION_REQUIRED = True

MARKET_REFRESH_SECONDS = 5
ORDERFLOW_REFRESH_SECONDS = 1
DASHBOARD_REFRESH_SECONDS = 2

STRATEGIC_TIMEFRAME = "1d"
CONTEXT_TIMEFRAMES = ("4h", "1h")
EXECUTION_TIMEFRAMES = ("15m", "5m")

# These remain conservative research defaults. They are NOT live permissions.
MAX_RISK_PER_TRADE = 0.01
MAX_DAILY_DRAWDOWN = 0.03
MAX_OPEN_POSITIONS = 1
MAX_EFFECTIVE_LEVERAGE = 1.0
MAX_DAILY_OPPORTUNITIES = 2

REQUIRE_CALIBRATED_EV = True
MIN_EV_SAMPLES = 30
MIN_REWARD_RISK = 1.5
EXECUTION_COST_BUFFER = 0.001

ORDERBOOK_DEPTH_LIMIT = 1000
ORDERFLOW_WINDOWS_SECONDS = (5, 15, 30, 60)

# Data-integrity thresholds.
DATA_DEGRADED_MAX_AGE = 60
ORDERFLOW_STATE_MAX_AGE = 15
# Legacy cross-source event-time spread threshold is retained for reporting only.
# It is NOT a hard coherence gate because ticker/funding/OI endpoints can update at
# different times even when the local clock is perfect.
SOURCE_TIME_SKEW_MAX_MS = 5000

# Binance exchange-clock synchronization.
CLOCK_SYNC_INTERVAL_SECONDS = 60
CLOCK_MAX_STALE_SECONDS = 600
CLOCK_MAX_RTT_MS = 2000
CLOCK_RETRY_AFTER_FAILURE_SECONDS = 10

# Source-specific freshness limits measured against Binance-adjusted time.
PRICE_EVENT_MAX_AGE_MS = 5000
FUNDING_EVENT_MAX_AGE_MS = 15000
OPEN_INTEREST_EVENT_MAX_AGE_MS = 15000

# Descriptive time-series bootstrap only. This is not OOS validation.
BLOCK_BOOTSTRAP_ROUNDS = 1000
BLOCK_BOOTSTRAP_BLOCK_SIZE = 5
BLOCK_BOOTSTRAP_SEED = 20260827


def ensure_directories():
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    (LOG_DIR / "v2").mkdir(parents=True, exist_ok=True)
    (RUNTIME_DIR / "v2").mkdir(parents=True, exist_ok=True)


def load_dataset_session():
    """Return the explicitly prepared dataset session, or None.

    Deliberately does not auto-create a dataset. This prevents accidentally
    appending a new clean run into archived/old V2 logs.
    """
    ensure_directories()
    if not DATASET_SESSION_FILE.exists():
        return None
    try:
        with DATASET_SESSION_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def get_dataset_id():
    session = load_dataset_session()
    if not session:
        return None
    value = session.get("dataset_id")
    return str(value) if value else None


LIFECYCLE_FILE = BASE_DIR / "dataset_lifecycle.json"


def dataset_lifecycle():
    """Missing or malformed governance state must never authorize a write."""
    data = json.loads(LIFECYCLE_FILE.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1 or not isinstance(data.get("frozen_datasets"), dict):
        raise RuntimeError("INVALID_DATASET_LIFECYCLE")
    return data["frozen_datasets"]


def assert_dataset_writable(dataset_id, path):
    target = path.resolve()
    for frozen_id, entry in dataset_lifecycle().items():
        if dataset_id == frozen_id:
            raise RuntimeError("FROZEN_DATASET_WRITE_FORBIDDEN: " + frozen_id)
        for relative in entry["protected_paths"]:
            protected = (BASE_DIR / relative).resolve()
            if target == protected or target in protected.parents:
                raise RuntimeError("FROZEN_ARTIFACT_WRITE_FORBIDDEN: " + str(target))


def research_outcomes_path(dataset_id, fallback):
    """Resolve a frozen dataset's canonical read input, never a write target."""
    entry = dataset_lifecycle().get(dataset_id)
    if entry is None:
        return fallback
    artifact = entry["canonical_outcome"]
    path = (BASE_DIR / artifact["path"]).resolve()
    if not path.is_relative_to(BASE_DIR.resolve()):
        raise RuntimeError("CANONICAL_OUTCOME_OUTSIDE_PROJECT")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != artifact["sha256"]:
        raise RuntimeError("CANONICAL_OUTCOME_HASH_MISMATCH")
    return path

