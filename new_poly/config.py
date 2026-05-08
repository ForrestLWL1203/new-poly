"""Configuration constants for reusable market and execution plumbing."""

CLOB_HOST: str = "https://clob.polymarket.com"
CHAIN_ID: int = 137
SIGNATURE_TYPE: int = 1

SLUG_STEP: int = 300
SERIES_SLUG_PREFIX: str = "btc-updown-5m"

WS_RECONNECT_DELAY: float = 1.0
WS_RECONNECT_MAX_DELAY: float = 30.0
WS_RECONNECT_MAX_RETRIES: int = 10
CLOB_WS_IDLE_RECONNECT_SEC: float = 20.0
CLOB_DEPTH_IDLE_RECONNECT_SEC: float = 0.0

PRICE_HINT_BUFFER_TICKS: float = 1.0
FAK_RETRY_PRICE_HINT_BUFFER_TICKS: float = 2.0
