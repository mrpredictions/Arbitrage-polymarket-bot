use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::Json;
use axum::Router;
use chrono::{DateTime, Timelike, Utc};
use ethers::providers::{Http, Provider};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::{transaction::eip2718::TypedTransaction, H160, TransactionRequest, U256};
use ethers::utils::{keccak256, to_checksum};
use parking_lot::RwLock;
use redis::AsyncCommands;
use reqwest::header::HeaderMap;
use reqwest::Client;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use serde::{Deserialize, Serialize};
use serde_json::{from_str, json, Value};
use telegram_bot::types::requests::SendMessage;
use telegram_bot::types::ChatId;
use telegram_bot::Api;
use tokio::sync::{broadcast, mpsc};
use tokio::time::sleep;
use tungstenite::{connect, Message};
use url::Url;

#[derive(Clone)]
struct Config {
    host: String,
    gamma_api: String,
    polygon_rpc: String,
    safe_address: H160,
    bot_address: H160,
    chain_id: u64,
    private_key: String,
    api_key: String,
    api_secret: String,
    passphrase: String,
    chainlink_username: String,
    chainlink_password: String,
    telegram_token: String,
    telegram_chat_id: i64,
    threshold_pct: f64,
    min_edge_pct: f64,
    sum_threshold: f64,
    max_size: f64,
    risk_pct: f64,
    dry_run: bool,
    cooldown_sec: u64,
    delay_to_add_1h: u64,
    max_daily_loss: f64,
    max_inventory: f64,
    min_liquidity: f64,
    token_cache_ttl_sec: u64,
    kill_switch: bool,
    enabled_assets: Vec<String>,
    enabled_timeframes: Vec<String>,
    starting_capital: f64,
    order_ttl_sec: u64,
    order_price_drift_pct: f64,
    feed_stale_sec: u64,
    orderbook_stale_sec: u64,
    order_id_sync_window_sec: u64,
    order_status_poll_sec: u64,
    safe_tx_confirm_timeout_sec: u64,
    safe_tx_confirm_poll_sec: u64,
    orderbook_max_fraction: f64,
    order_refresh_window_sec: u64,
    order_status_path: String,
    direct_order_submit_enabled: bool,
    direct_order_submit_mode: String,
    direct_order_submit_path: String,
    direct_order_submit_batch_path: String,
    direct_order_submit_fallback_safe: bool,
    direct_order_submit_market_field: String,
    direct_order_submit_expiration_sec: u64,
    market_maker_enabled: bool,
    market_maker_spread_pct: f64,
    market_maker_size_pct: f64,
    usdc_contract: H160,
    usdc_decimals: u32,
    clob_contract: H160,
    multi_send_contract: H160,
    orderbook_path: String,
    market_overrides: HashMap<String, MarketOverride>,
}

#[derive(Clone, Default, Debug)]
struct MarketOverride {
    threshold_pct: Option<f64>,
    min_edge_pct: Option<f64>,
    sum_threshold: Option<f64>,
    max_size: Option<f64>,
    max_risk_pct: Option<f64>,
    max_position: Option<f64>,
    trade_start_hour: Option<u32>,
    trade_end_hour: Option<u32>,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let host = env_or("CLOB_HOST", "https://clob.polymarket.com");
        let gamma_api = env_or("GAMMA_API", "https://gamma-api.polymarket.com");
        let polygon_rpc = env_or("POLYGON_RPC", "https://polygon-rpc.com");
        let safe_address = H160::from_str(&env_or("SAFE_ADDRESS", "0xYourSafeAddressHere"))
            .map_err(|e| format!("invalid SAFE_ADDRESS: {e}"))?;
        let bot_address = H160::from_str(&env_or("BOT_ADDRESS", "0xYourBotAddressHere"))
        .map_err(|e| format!("invalid BOT_ADDRESS: {e}"))?;
        let chain_id = env_or_u64("CHAIN_ID", 137);
        let private_key = env_or("PRIVATE_KEY", "0xYourPolygonPrivKeyHere");
        let api_key = env_or("POLYMARKET_API_KEY", "your_api_key");
        let api_secret = env_or("POLYMARKET_API_SECRET", "your_secret");
        let passphrase = env_or("POLYMARKET_API_PASSPHRASE", "your_passphrase");
        let chainlink_username = env_or("CHAINLINK_USERNAME", "your_chainlink_username");
        let chainlink_password = env_or("CHAINLINK_PASSWORD", "your_chainlink_password");
        let telegram_token = env_or("TELEGRAM_TOKEN", "your_telegram_bot_token");
        let telegram_chat_id = env_or_i64("TELEGRAM_CHAT_ID", 0);
        let threshold_pct = env_or_f64("THRESHOLD_PCT", 0.0003);
        let min_edge_pct = env_or_f64("MIN_EDGE_PCT", 0.05);
        let sum_threshold = env_or_f64("SUM_THRESHOLD", 0.995);
        let max_size = env_or_f64("MAX_SIZE", 300.0);
        let risk_pct = env_or_f64("RISK_PCT", 1.0);
        let dry_run = env_or_bool("DRY_RUN", true);
        let cooldown_sec = env_or_u64("COOLDOWN_SEC", 3);
        let delay_to_add_1h = env_or_u64("DELAY_ADD_1H_SEC", 600);
        let max_daily_loss = env_or_f64("MAX_DAILY_LOSS", 50.0);
        let max_inventory = env_or_f64("MAX_INVENTORY", 1000.0);
        let min_liquidity = env_or_f64("MIN_LIQUIDITY", 50.0);
        let token_cache_ttl_sec = env_or_u64("TOKEN_CACHE_TTL_SEC", 900);
        let kill_switch = env_or_bool("KILL_SWITCH", false);
        let enabled_assets = env_or("ENABLED_ASSETS", "btc")
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        let enabled_timeframes = env_or("ENABLED_TIMEFRAMES", "15m,1h")
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();
        let starting_capital = env_or_f64("STARTING_CAPITAL", 300.0);
        let order_ttl_sec = env_or_u64("ORDER_TTL_SEC", 30);
        let order_price_drift_pct = env_or_f64("ORDER_PRICE_DRIFT_PCT", 0.01);
        let feed_stale_sec = env_or_u64("FEED_STALE_SEC", 10);
        let orderbook_stale_sec = env_or_u64("ORDERBOOK_STALE_SEC", 15);
        let order_id_sync_window_sec = env_or_u64("ORDER_ID_SYNC_WINDOW_SEC", 45);
        let order_status_poll_sec = env_or_u64("ORDER_STATUS_POLL_SEC", 20);
        let safe_tx_confirm_timeout_sec = env_or_u64("SAFE_TX_CONFIRM_TIMEOUT_SEC", 90);
        let safe_tx_confirm_poll_sec = env_or_u64("SAFE_TX_CONFIRM_POLL_SEC", 5);
        let orderbook_max_fraction = env_or_f64("ORDERBOOK_MAX_FRACTION", 0.1);
        let order_refresh_window_sec = env_or_u64("ORDER_REFRESH_WINDOW_SEC", 30);
        let order_status_path = env_or("ORDER_STATUS_PATH", "orders");
        let direct_order_submit_enabled = env_or_bool("DIRECT_ORDER_SUBMIT_ENABLED", false);
        let direct_order_submit_mode = env_or("DIRECT_ORDER_SUBMIT_MODE", "single");
        let direct_order_submit_path = env_or("DIRECT_ORDER_SUBMIT_PATH", "orders");
        let direct_order_submit_batch_path = env_or("DIRECT_ORDER_SUBMIT_BATCH_PATH", "orders/batch");
        let direct_order_submit_fallback_safe =
            env_or_bool("DIRECT_ORDER_SUBMIT_FALLBACK_SAFE", true);
        let direct_order_submit_market_field =
            env_or("DIRECT_ORDER_SUBMIT_MARKET_FIELD", "market");
        let direct_order_submit_expiration_sec =
            env_or_u64("DIRECT_ORDER_SUBMIT_EXPIRATION_SEC", 0);
        let market_maker_enabled = env_or_bool("MARKET_MAKER_ENABLED", false);
        let market_maker_spread_pct = env_or_f64("MARKET_MAKER_SPREAD_PCT", 0.01);
        let market_maker_size_pct = env_or_f64("MARKET_MAKER_SIZE_PCT", 0.1);
        let usdc_contract = H160::from_str(&env_or(
            "USDC_CONTRACT",
            "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        ))
        .map_err(|e| format!("invalid USDC_CONTRACT: {e}"))?;
        let usdc_decimals = env_or_u64("USDC_DECIMALS", 6) as u32;
        let clob_contract = H160::from_str(&env_or(
            "CLOB_CONTRACT",
            "0x4b8a35240c4f3ef89a0f49e39e4dded31370f6b7",
        ))
        .map_err(|e| format!("invalid CLOB_CONTRACT: {e}"))?;
        let multi_send_contract = H160::from_str(&env_or(
            "MULTISEND_CONTRACT",
            "0x40A2aCCbd92BCA938b02010E17A5b8929b49130d",
        ))
        .map_err(|e| format!("invalid MULTISEND_CONTRACT: {e}"))?;
        let orderbook_path = env_or("ORDERBOOK_PATH", "orderbook");
        let market_overrides = parse_market_overrides(&env_or("MARKET_OVERRIDES", ""));

        Ok(Self {
            host,
            gamma_api,
            polygon_rpc,
            safe_address,
            bot_address,
            chain_id,
            private_key,
            api_key,
            api_secret,
            passphrase,
            chainlink_username,
            chainlink_password,
            telegram_token,
            telegram_chat_id,
            threshold_pct,
            min_edge_pct,
            sum_threshold,
            max_size,
            risk_pct,
            dry_run,
            cooldown_sec,
            delay_to_add_1h,
            max_daily_loss,
            max_inventory,
            min_liquidity,
            token_cache_ttl_sec,
            kill_switch,
            enabled_assets,
            enabled_timeframes,
            starting_capital,
            order_ttl_sec,
            order_price_drift_pct,
            feed_stale_sec,
            orderbook_stale_sec,
            order_id_sync_window_sec,
            order_status_poll_sec,
            safe_tx_confirm_timeout_sec,
            safe_tx_confirm_poll_sec,
            orderbook_max_fraction,
            order_refresh_window_sec,
            order_status_path,
            direct_order_submit_enabled,
            direct_order_submit_mode,
            direct_order_submit_path,
            direct_order_submit_batch_path,
            direct_order_submit_fallback_safe,
            direct_order_submit_market_field,
            direct_order_submit_expiration_sec,
            market_maker_enabled,
            market_maker_spread_pct,
            market_maker_size_pct,
            usdc_contract,
            usdc_decimals,
            clob_contract,
            multi_send_contract,
            orderbook_path,
            market_overrides,
        })
    }

    fn validate_for_live(&self) -> Result<(), String> {
        if self.dry_run {
            return Ok(());
        }
        let mut missing = Vec::new();
        if self.private_key.starts_with("0xYour") {
            missing.push("PRIVATE_KEY");
        }
        if to_checksum(&self.safe_address, None) == "0xYourSafeAddressHere" {
            missing.push("SAFE_ADDRESS");
        }
        if self.api_key == "your_api_key" {
            missing.push("POLYMARKET_API_KEY");
        }
        if self.api_secret == "your_secret" {
            missing.push("POLYMARKET_API_SECRET");
        }
        if self.passphrase == "your_passphrase" {
            missing.push("POLYMARKET_API_PASSPHRASE");
        }
        if self.telegram_token == "your_telegram_bot_token" {
            missing.push("TELEGRAM_TOKEN");
        }
        if self.telegram_chat_id == 0 {
            missing.push("TELEGRAM_CHAT_ID");
        }
        if !missing.is_empty() {
            return Err(format!(
                "missing live configuration values: {}",
                missing.join(", ")
            ));
        }
        Ok(())
    }

    fn market_params(&self, asset: &str) -> MarketParams {
        let override_cfg = self.market_overrides.get(asset);
        MarketParams {
            threshold_pct: override_cfg
                .and_then(|m| m.threshold_pct)
                .unwrap_or(self.threshold_pct),
            min_edge_pct: override_cfg
                .and_then(|m| m.min_edge_pct)
                .unwrap_or(self.min_edge_pct),
            sum_threshold: override_cfg
                .and_then(|m| m.sum_threshold)
                .unwrap_or(self.sum_threshold),
            max_size: override_cfg
                .and_then(|m| m.max_size)
                .unwrap_or(self.max_size),
            max_risk_pct: override_cfg.and_then(|m| m.max_risk_pct),
            max_position: override_cfg.and_then(|m| m.max_position),
            trade_start_hour: override_cfg.and_then(|m| m.trade_start_hour),
            trade_end_hour: override_cfg.and_then(|m| m.trade_end_hour),
        }
    }
}

#[derive(Clone)]
struct MarketParams {
    threshold_pct: f64,
    min_edge_pct: f64,
    sum_threshold: f64,
    max_size: f64,
    max_risk_pct: Option<f64>,
    max_position: Option<f64>,
    trade_start_hour: Option<u32>,
    trade_end_hour: Option<u32>,
}

#[derive(Clone)]
struct TokenInfo {
    yes_token_id: String,
    no_token_id: String,
    last_refresh: u64,
}

#[derive(Clone, Serialize)]
struct TelemetrySnapshot {
    uptime_sec: u64,
    wallet_balance: f64,
    safe_tx_count: u64,
    positions_placed: u64,
    wins: u64,
    losses: u64,
    drawdown: f64,
    var: f64,
    var_10: f64,
    var_20: f64,
    last_trade: Option<TradeSummary>,
    kill_switch: bool,
    exposure: f64,
    connection_health: Vec<ConnectionHealth>,
    latency: Vec<LatencyRow>,
    pipeline_latency: Vec<LatencyRow>,
    orderbook_depth: Vec<OrderbookDepth>,
    recent_fills: Vec<FillRecord>,
    incidents: Vec<Incident>,
    positions: Vec<PositionSummary>,
    wallets: Vec<WalletEntry>,
    per_market_pnl: Vec<MarketPnl>,
    pnl_history: Vec<f64>,
    env_template: String,
}

#[derive(Clone, Serialize)]
struct TradeSummary {
    market: String,
    side: String,
    price: f64,
    size: f64,
    timestamp: String,
}

#[derive(Clone, Serialize)]
struct ConnectionHealth {
    market: String,
    status: String,
    last_heartbeat_sec: u64,
    reconnects: u64,
}

#[derive(Clone, Serialize)]
struct LatencyRow {
    label: String,
    avg_ms: f64,
    last_ms: f64,
    count: u64,
}

#[derive(Clone, Serialize)]
struct OrderbookDepth {
    market: String,
    bids: f64,
    asks: f64,
    total: f64,
}

#[derive(Clone)]
struct OrderbookSnapshot {
    best_bid: f64,
    best_ask: f64,
    bid_depth: f64,
    ask_depth: f64,
}

#[derive(Clone)]
struct OrderIntent {
    market: String,
    token_id: String,
    label: String,
    direction: String,
    price: f64,
    size: f64,
    order_type: String,
}

#[derive(Clone)]
struct ApiOrderResult {
    label: String,
    direction: String,
    token_id: String,
    price: f64,
    size: f64,
    status: String,
    remote_id: Option<String>,
}

impl OrderIntent {
    fn new(
        market: String,
        token_id: String,
        label: &str,
        direction: &str,
        price: f64,
        size: f64,
        order_type: &str,
    ) -> Self {
        Self {
            market,
            token_id,
            label: label.to_string(),
            direction: direction.to_string(),
            price,
            size,
            order_type: order_type.to_string(),
        }
    }
}

#[derive(Clone, Serialize)]
struct FillRecord {
    market: String,
    action: String,
    pnl: f64,
    size: f64,
    price: f64,
    timestamp: String,
    order_id: Option<String>,
    token_id: Option<String>,
}

#[derive(Clone)]
struct OpenOrder {
    id: String,
    market: String,
    side: String,
    direction: String,
    token_id: String,
    price: f64,
    size: f64,
    created_at: Instant,
    status: String,
    last_update: Instant,
    remote_id: Option<String>,
}

#[derive(Clone)]
struct RemoteOrder {
    id: String,
    market: Option<String>,
    token_id: Option<String>,
    side: String,
    price: f64,
    size: f64,
    status: String,
}

#[derive(Clone, Serialize)]
struct Incident {
    level: String,
    message: String,
    timestamp: String,
}

#[derive(Clone, Serialize)]
struct PositionSummary {
    market: String,
    yes: f64,
    no: f64,
    cash: f64,
    pnl: f64,
}

#[derive(Clone)]
struct PositionState {
    yes_size: f64,
    no_size: f64,
    yes_avg_cost: f64,
    no_avg_cost: f64,
    realized_pnl: f64,
}

#[derive(Clone, Serialize)]
struct WalletEntry {
    label: String,
    address: String,
}

#[derive(Clone, Serialize)]
struct MarketPnl {
    market: String,
    pnl: f64,
}

#[derive(Clone)]
struct Telemetry {
    start: Instant,
    wallet_balance: f64,
    safe_tx_count: u64,
    positions_placed: u64,
    wins: u64,
    losses: u64,
    drawdown: f64,
    var: f64,
    last_trade: Option<TradeSummary>,
    kill_switch: bool,
    exposure: f64,
    connection_health: HashMap<String, ConnectionHealthState>,
    latency: HashMap<String, LatencyStats>,
    pipeline_latency: HashMap<String, LatencyStats>,
    orderbook_depth: HashMap<String, OrderbookDepth>,
    orderbook_last_update: HashMap<String, Instant>,
    recent_fills: VecDeque<FillRecord>,
    incidents: VecDeque<Incident>,
    positions: HashMap<String, PositionSummary>,
    positions_state: HashMap<String, PositionState>,
    open_orders: HashMap<String, OpenOrder>,
    refresh_requests: HashMap<String, Instant>,
    pnl_history: VecDeque<f64>,
    per_market_pnl: HashMap<String, f64>,
    last_stale_alert: HashMap<String, Instant>,
    last_recovery_alert: Option<Instant>,
    token_decimals: HashMap<H160, u32>,
    wallets: Vec<WalletEntry>,
    env_template: String,
    last_fill_ts: Option<DateTime<Utc>>,
    feed_stale_sec: u64,
    orderbook_stale_sec: u64,
}

#[derive(Clone, Default)]
struct LatencyStats {
    avg_ms: f64,
    last_ms: f64,
    count: u64,
}

#[derive(Clone)]
struct ConnectionHealthState {
    status: String,
    reconnects: u64,
    last_heartbeat: Instant,
}

#[derive(Default, Clone)]
struct VolatilityTracker {
    last_price: Option<f64>,
    ema_abs_return: f64,
}

#[derive(Clone)]
struct AppState {
    telemetry: Arc<RwLock<Telemetry>>,
    broadcaster: broadcast::Sender<TelemetrySnapshot>,
    redis_client: Option<redis::Client>,
}

#[derive(Serialize)]
struct StatusResponse {
    api_ok: bool,
    wallet_ok: bool,
    redis_ok: bool,
    kill_switch: bool,
}

#[derive(Deserialize)]
struct KillSwitchRequest {
    enabled: bool,
}

#[tokio::main]
async fn main() {
    let config = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("Config error: {err}");
            return;
        }
    };
    if config.telegram_chat_id == 0 {
        eprintln!("TELEGRAM_CHAT_ID is 0; alerts disabled.");
    }
    println!("Bot address: {}", to_checksum(&config.bot_address, None));
    if let Err(err) = config.validate_for_live() {
        eprintln!("Live config validation error: {err}");
        return;
    }

    let wallet = match LocalWallet::from_str(&config.private_key) {
        Ok(w) => w.with_chain_id(config.chain_id),
        Err(err) => {
            eprintln!("Invalid PRIVATE_KEY: {err}");
            return;
        }
    };

    let provider = match Provider::<Http>::try_from(config.polygon_rpc.clone()) {
        Ok(provider) => provider,
        Err(err) => {
            eprintln!("Invalid POLYGON_RPC: {err}");
            return;
        }
    };

    let http_client = build_http_client(&config.api_key, &config.api_secret, &config.passphrase);
    let telegram_api = Api::new(config.telegram_token.clone());

    let redis_client = redis::Client::open("redis://127.0.0.1/").ok();
    let mut redis_conn = match &redis_client {
        Some(client) => match client.get_async_connection().await {
            Ok(conn) => Some(conn),
            Err(err) => {
                eprintln!("Failed to open Redis connection: {err}");
                None
            }
        },
        None => {
            eprintln!("Failed to create Redis client");
            None
        }
    };

    let (broadcaster, _) = broadcast::channel(64);
    let telemetry = Arc::new(RwLock::new(Telemetry {
        start: Instant::now(),
        wallet_balance: 0.0,
        safe_tx_count: 0,
        positions_placed: 0,
        wins: 0,
        losses: 0,
        drawdown: 0.0,
        var: 0.0,
        var_10: 0.0,
        var_20: 0.0,
        last_trade: None,
        kill_switch: config.kill_switch,
        exposure: 0.0,
        connection_health: HashMap::new(),
        latency: HashMap::new(),
        pipeline_latency: HashMap::new(),
        orderbook_depth: HashMap::new(),
        orderbook_last_update: HashMap::new(),
        recent_fills: VecDeque::with_capacity(20),
        incidents: VecDeque::with_capacity(50),
        positions: HashMap::new(),
        positions_state: HashMap::new(),
        open_orders: HashMap::new(),
        refresh_requests: HashMap::new(),
        pnl_history: VecDeque::with_capacity(200),
        per_market_pnl: HashMap::new(),
        last_stale_alert: HashMap::new(),
        last_recovery_alert: None,
        token_decimals: HashMap::new(),
        wallets: vec![WalletEntry {
            label: "Bot".to_string(),
            address: to_checksum(&config.bot_address, None),
        }],
        env_template: build_env_template(&config),
        last_fill_ts: None,
        feed_stale_sec: config.feed_stale_sec,
        orderbook_stale_sec: config.orderbook_stale_sec,
    }));

    let state = AppState {
        telemetry: telemetry.clone(),
        broadcaster: broadcaster.clone(),
        redis_client: redis_client.clone(),
    };

    let token_cache: Arc<tokio::sync::RwLock<HashMap<String, TokenInfo>>> =
        Arc::new(tokio::sync::RwLock::new(HashMap::new()));
    let vol_trackers: Arc<RwLock<HashMap<String, VolatilityTracker>>> =
        Arc::new(RwLock::new(HashMap::new()));

    let (tx, mut rx) = mpsc::channel(64);

    for asset in &config.enabled_assets {
        for timeframe in &config.enabled_timeframes {
            let http_client_clone = http_client.clone();
            let tx_clone = tx.clone();
            let asset_clone = asset.clone();
            let timeframe_clone = timeframe.clone();
            let state_clone = state.clone();
            tokio::spawn(monitor_feed(
                state_clone,
                http_client_clone,
                asset_clone,
                timeframe_clone,
                tx_clone,
            ));
        }
    }

    if config.enabled_timeframes.contains(&"1h".to_string()) && config.delay_to_add_1h > 0 {
        sleep(Duration::from_secs(config.delay_to_add_1h)).await;
    }

    let api_state = state.clone();
    tokio::spawn(async move {
        run_api(api_state).await;
    });

    let balance_state = state.clone();
    let balance_client = http_client.clone();
    let balance_provider = provider.clone();
    let balance_config = config.clone();
    let balance_token_cache = token_cache.clone();
    tokio::spawn(async move {
        poll_wallet_balance(
            balance_state,
            balance_client,
            balance_provider,
            balance_config,
            balance_token_cache,
        )
        .await;
    });

    let fill_state = state.clone();
    let fill_client = http_client.clone();
    let fill_config = config.clone();
    tokio::spawn(async move {
        poll_fills(fill_state, fill_client, fill_config).await;
    });

    let order_state = state.clone();
    let order_client = http_client.clone();
    let order_config = config.clone();
    tokio::spawn(async move {
        manage_open_orders(order_state, order_client, order_config).await;
    });

    let status_state = state.clone();
    let status_client = http_client.clone();
    let status_config = config.clone();
    tokio::spawn(async move {
        poll_open_order_status(status_state, status_client, status_config).await;
    });

    while let Some((asset, timeframe, price1, price2, latency_ms)) = rx.recv().await {
        if let Err(err) = process_update(
            &http_client,
            &wallet,
            &provider,
            config.clone(),
            &telegram_api,
            redis_conn.as_mut(),
            &token_cache,
            &vol_trackers,
            &state,
            &asset,
            &timeframe,
            price1,
            price2,
            latency_ms,
        )
        .await
        {
            eprintln!("process_update error: {err}");
            push_incident(&state, "error", &format!("process_update: {err}"));
        }
    }
}

async fn run_api(state: AppState) {
    let app = Router::new()
        .route("/", get(index))
        .route("/api/telemetry", get(telemetry_handler))
        .route("/api/status", get(status_handler))
        .route("/api/kill-switch", post(kill_switch_handler))
        .route("/api/ws", get(ws_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("bind api");
    axum::serve(listener, app).await.expect("serve api");
}

async fn index() -> impl IntoResponse {
    Html(include_str!("../dashboard/index.html"))
}

async fn telemetry_handler(State(state): State<AppState>) -> impl IntoResponse {
    let snapshot = build_snapshot(&state);
    Json(snapshot)
}

async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    let redis_ok = state
        .redis_client
        .as_ref()
        .map(|_| true)
        .unwrap_or(false);
    let telemetry = state.telemetry.read();
    Json(StatusResponse {
        api_ok: true,
        wallet_ok: telemetry.wallet_balance > 0.0,
        redis_ok,
        kill_switch: telemetry.kill_switch,
    })
}

async fn kill_switch_handler(
    State(state): State<AppState>,
    Json(payload): Json<KillSwitchRequest>,
) -> impl IntoResponse {
    {
        let mut telemetry = state.telemetry.write();
        telemetry.kill_switch = payload.enabled;
    }
    push_incident(
        &state,
        "warning",
        &format!("Kill switch set to {}", payload.enabled),
    );
    broadcast_snapshot(&state);
    StatusCode::OK
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| async move {
        let mut rx = state.broadcaster.subscribe();
        let snapshot = build_snapshot(&state);
        let _ = socket
            .send(WsMessage::Text(serde_json::to_string(&snapshot).unwrap()))
            .await;
        websocket_loop(socket, &mut rx).await;
    })
}

async fn websocket_loop(mut socket: WebSocket, rx: &mut broadcast::Receiver<TelemetrySnapshot>) {
    loop {
        tokio::select! {
            msg = rx.recv() => {
                if let Ok(snapshot) = msg {
                    let _ = socket.send(WsMessage::Text(serde_json::to_string(&snapshot).unwrap())).await;
                }
            }
            incoming = socket.recv() => {
                if incoming.is_none() {
                    return;
                }
            }
        }
    }
}

fn build_snapshot(state: &AppState) -> TelemetrySnapshot {
    let telemetry = state.telemetry.read();
    TelemetrySnapshot {
        uptime_sec: telemetry.start.elapsed().as_secs(),
        wallet_balance: telemetry.wallet_balance,
        safe_tx_count: telemetry.safe_tx_count,
        positions_placed: telemetry.positions_placed,
        wins: telemetry.wins,
        losses: telemetry.losses,
        drawdown: telemetry.drawdown,
        var: telemetry.var,
        var_10: telemetry.var_10,
        var_20: telemetry.var_20,
        last_trade: telemetry.last_trade.clone(),
        kill_switch: telemetry.kill_switch,
        exposure: telemetry.exposure,
        connection_health: telemetry
            .connection_health
            .iter()
            .map(|(market, row)| {
                let age = row.last_heartbeat.elapsed().as_secs();
                let status = if age > telemetry.feed_stale_sec {
                    "stale".to_string()
                } else {
                    row.status.clone()
                };
                ConnectionHealth {
                    market: market.clone(),
                    status,
                    last_heartbeat_sec: age,
                    reconnects: row.reconnects,
                }
            })
            .collect(),
        latency: telemetry
            .latency
            .iter()
            .map(|(label, stats)| LatencyRow {
                label: label.clone(),
                avg_ms: stats.avg_ms,
                last_ms: stats.last_ms,
                count: stats.count,
            })
            .collect(),
        pipeline_latency: telemetry
            .pipeline_latency
            .iter()
            .map(|(label, stats)| LatencyRow {
                label: label.clone(),
                avg_ms: stats.avg_ms,
                last_ms: stats.last_ms,
                count: stats.count,
            })
            .collect(),
        orderbook_depth: telemetry.orderbook_depth.values().cloned().collect(),
        recent_fills: telemetry.recent_fills.iter().cloned().collect(),
        incidents: telemetry.incidents.iter().cloned().collect(),
        positions: telemetry.positions.values().cloned().collect(),
        wallets: telemetry.wallets.clone(),
        per_market_pnl: telemetry
            .per_market_pnl
            .iter()
            .map(|(market, pnl)| MarketPnl {
                market: market.clone(),
                pnl: *pnl,
            })
            .collect(),
        pnl_history: telemetry.pnl_history.iter().copied().collect(),
        env_template: telemetry.env_template.clone(),
    }
}

fn broadcast_snapshot(state: &AppState) {
    let snapshot = build_snapshot(state);
    let _ = state.broadcaster.send(snapshot);
}

async fn monitor_feed(
    state: AppState,
    _http_client: Client,
    asset: String,
    timeframe: String,
    tx: mpsc::Sender<(String, String, Option<f64>, Option<f64>, f64)>,
) {
    let mut reconnects = 0u64;
    loop {
        let (primary_uri, secondary_uri, sub_primary, sub_secondary) = if timeframe == "15m" {
            let pyth_id = match asset.as_str() {
                "btc" => "e62df6c8b4a85fe1a67db44dc12de5db33b7a0a88a148bbf55b4b1d5e5f2c7c1",
                "eth" => "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
                "sol" => "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
                "xrp" => "23d7315113f5b1d3ba7a83604c44b86d4f86af1d4fd6f24189746169c916de9b",
                _ => return,
            };
            (
                "wss://hermes.pyth.network/ws".to_string(),
                "wss://stream.bybit.com/v5/public/spot".to_string(),
                json!({"type": "subscribe", "ids": [pyth_id]}).to_string(),
                json!({
                    "op": "subscribe",
                    "args": [format!("publicTrade.{}/USDT", asset.to_uppercase())]
                })
                .to_string(),
            )
        } else {
            let symbol = format!("{}/USDT", asset.to_uppercase());
            (
                format!(
                    "wss://stream.binance.com:9443/ws/{}@trade",
                    symbol.to_lowercase().replace('/', "")
                ),
                "wss://stream.bybit.com/v5/public/spot".to_string(),
                "".to_string(),
                json!({"op": "subscribe", "args": [format!("publicTrade.{}", symbol)]}).to_string(),
            )
        };

        let connection_key = format!("{}:{}", asset, timeframe);
        update_connection_health(&state, &connection_key, "connecting", reconnects, 0);

        let ws_primary = connect(Url::parse(&primary_uri).unwrap());
        let ws_secondary = connect(Url::parse(&secondary_uri).unwrap());

        let (mut ws_primary, mut ws_secondary) = match (ws_primary, ws_secondary) {
            (Ok((p, _)), Ok((s, _))) => (p, s),
            _ => {
                reconnects += 1;
                update_connection_health(&state, &connection_key, "down", reconnects, 0);
                push_incident(
                    &state,
                    "warning",
                    &format!("{connection_key} websocket connect failed"),
                );
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        if !sub_primary.is_empty() {
            let _ = ws_primary.send(Message::Text(sub_primary));
        }
        let _ = ws_secondary.send(Message::Text(sub_secondary));
        update_connection_health(&state, &connection_key, "up", reconnects, 0);

        let mut last_tick = Instant::now();
        loop {
            let primary_msg = ws_primary.recv();
            let secondary_msg = ws_secondary.recv();
            let now = Instant::now();
            let latency_ms = now.duration_since(last_tick).as_millis() as f64;
            last_tick = now;

            let price_primary = match primary_msg {
                Ok(Message::Text(msg)) => parse_price(&msg, &timeframe),
                Ok(_) => None,
                Err(_) => {
                    update_connection_health(&state, &connection_key, "down", reconnects, 0);
                    push_incident(
                        &state,
                        "warning",
                        &format!("{connection_key} websocket dropped"),
                    );
                    reconnects += 1;
                    break;
                }
            };
            let price_secondary = match secondary_msg {
                Ok(Message::Text(msg)) => parse_price(&msg, &timeframe),
                Ok(_) => None,
                Err(_) => {
                    update_connection_health(&state, &connection_key, "down", reconnects, 0);
                    push_incident(
                        &state,
                        "warning",
                        &format!("{connection_key} websocket dropped"),
                    );
                    reconnects += 1;
                    break;
                }
            };
            update_connection_health(&state, &connection_key, "up", reconnects, 0);
            update_latency(&state, &connection_key, latency_ms);

            if tx
                .send((
                    asset.clone(),
                    timeframe.clone(),
                    price_primary,
                    price_secondary,
                    latency_ms,
                ))
                .await
                .is_err()
            {
                return;
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
}

fn parse_price(msg: &str, timeframe: &str) -> Option<f64> {
    let data: Value = from_str(msg).unwrap_or(Value::Null);
    if data.is_null() {
        return None;
    }

    if timeframe == "15m" {
        if let Some(price_feed) = data.get("price_feed") {
            if let Some(price_obj) = price_feed.get("price") {
                if let (Some(price_str), Some(expo)) = (
                    price_obj.get("price").and_then(Value::as_str),
                    price_obj.get("expo").and_then(Value::as_i64),
                ) {
                    if let Ok(price) = f64::from_str(price_str) {
                        return Some(price / 10.0_f64.powi(expo as i32));
                    }
                }
            }
        }
    } else {
        if let Some(p) = data.get("p").and_then(Value::as_str) {
            if let Ok(price) = f64::from_str(p) {
                return Some(price);
            }
        }
        if let Some(data_array) = data.get("data").and_then(Value::as_array) {
            if let Some(first) = data_array.first() {
                if let Some(p) = first.get("p").and_then(Value::as_str) {
                    if let Ok(price) = f64::from_str(p) {
                        return Some(price);
                    }
                }
            }
        }
    }
    None
}

async fn process_update(
    http_client: &Client,
    wallet: &LocalWallet,
    provider: &Provider<Http>,
    config: Config,
    telegram_api: &Api,
    redis_conn: Option<&mut redis::aio::Connection>,
    token_cache: &Arc<tokio::sync::RwLock<HashMap<String, TokenInfo>>>,
    vol_trackers: &Arc<RwLock<HashMap<String, VolatilityTracker>>>,
    state: &AppState,
    asset: &str,
    timeframe: &str,
    price1: Option<f64>,
    price2: Option<f64>,
    latency_ms: f64,
) -> Result<(), String> {
    update_pipeline_latency(state, know_stage_label(asset, timeframe, "decision"), latency_ms);

    if state.telemetry.read().kill_switch {
        maybe_recover_kill_switch(state, telegram_api, config.telegram_chat_id);
        if state.telemetry.read().kill_switch {
            return Ok(());
        }
    }
    let market_key = format!("{}:{}", asset, timeframe);
    if is_feed_stale(state, &market_key) {
        auto_kill_on_stale(state, telegram_api, config.telegram_chat_id, &market_key);
        push_incident(state, "warning", &format!("feed {} is stale", market_key));
        return Ok(());
    }

    let key = format!("last_trade_{}_{}", asset, timeframe);
    if let Some(conn) = redis_conn {
        let last_trade_str: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| format!("Redis get error: {e}"))?;
        let last_trade = last_trade_str.and_then(|s| s.parse::<u64>().ok());
        if let Some(lt) = last_trade {
            if SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - lt
                < config.cooldown_sec
            {
                return Ok(());
            }
        }
    }

    let prices = [price1, price2]
        .iter()
        .filter_map(|&p| p)
        .collect::<Vec<_>>();
    if prices.is_empty() {
        return Ok(());
    }
    let avg_price = prices.iter().sum::<f64>() / prices.len() as f64;
    let mut signal_price = avg_price;
    if timeframe == "15m" {
        signal_price = get_chainlink_price(
            http_client,
            &config,
            "0x0002f8da67ea235d4401e394a2bed9965536b1b109da82e429c0a0a9ef29bc85",
        )
        .await
        .unwrap_or(avg_price);
    }

    let last_spot_key = format!("last_spot_{}_{}", asset, timeframe);
    let last_spot = if let Some(conn) = redis_conn {
        let last_spot_str: Option<String> = conn
            .get(&last_spot_key)
            .await
            .map_err(|e| format!("Redis get error: {e}"))?;
        last_spot_str
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(signal_price)
    } else {
        signal_price
    };

    let vol_key = format!("{}:{}", asset, timeframe);
    let volatility = update_volatility(vol_trackers, &vol_key, signal_price);

    let params = config.market_params(asset);
    let pct_change = (signal_price - last_spot).abs() / last_spot;
    let threshold = params.threshold_pct / (1.0 + volatility * 10.0);
    let refresh_pending = refresh_request_active(state, &config, &market_key);
    if pct_change < threshold && !refresh_pending {
        return Ok(());
    }

    let direction = if signal_price > last_spot { "UP" } else { "DOWN" };
    println!(
        "{} {} spot moved {:.2}% -> {}",
        asset.to_uppercase(),
        timeframe,
        pct_change * 100.0,
        direction
    );

    let token_info = fetch_token_info(http_client, &config, token_cache, asset, timeframe).await?;

    let yes_price = fetch_midpoint(http_client, &config.host, &token_info.yes_token_id).await?;
    let no_price = fetch_midpoint(http_client, &config.host, &token_info.no_token_id).await?;
    let yes_book = fetch_orderbook_depth(
        http_client,
        &config.host,
        &config.orderbook_path,
        &token_info.yes_token_id,
    )
    .await
    .ok();
    let no_book = fetch_orderbook_depth(
        http_client,
        &config.host,
        &config.orderbook_path,
        &token_info.no_token_id,
    )
    .await
    .ok();
    let (bids, asks, total_depth) = match (&yes_book, &no_book) {
        (Some(yes), Some(no)) => (
            yes.best_bid,
            yes.best_ask,
            yes.bid_depth + yes.ask_depth + no.bid_depth + no.ask_depth,
        ),
        (Some(yes), None) => (yes.best_bid, yes.best_ask, yes.bid_depth + yes.ask_depth),
        (None, Some(no)) => (no.best_bid, no.best_ask, no.bid_depth + no.ask_depth),
        (None, None) => (yes_price, yes_price, yes_price + no_price),
    };
    update_orderbook_depth(
        state,
        OrderbookDepth {
            market: format!("{}:{}", asset, timeframe),
            bids,
            asks,
            total: total_depth,
        },
    );
    if yes_book.is_some() || no_book.is_some() {
        update_orderbook_timestamp(state, &market_key);
    }

    let balance = fetch_balance(http_client, &config.host).await?;
    let effective_balance = balance.min(config.starting_capital);
    update_wallet_balance(state, effective_balance);

    if is_orderbook_stale(state, &market_key) {
        push_incident(state, "warning", &format!("orderbook {} is stale", market_key));
        return Ok(());
    }

    let risk_status = risk_check(
        state,
        &market_key,
        &params,
        config.max_daily_loss,
        config.max_inventory,
        config.min_liquidity,
    );
    if !risk_status.allowed {
        push_incident(state, "warning", &risk_status.reason);
        return Ok(());
    }

    let mut call_data = vec![];
    let mut action_label = None;
    let mut order_intents: Vec<OrderIntent> = Vec::new();

    if yes_price + no_price < params.sum_threshold {
        let risk_amount = effective_balance * params.max_risk_pct.unwrap_or(config.risk_pct);
        let mut size_yes = params
            .max_size
            .min(risk_amount / 2.0 / yes_price)
            .min(params.max_size / 2.0);
        let mut size_no = params
            .max_size
            .min(risk_amount / 2.0 / no_price)
            .min(params.max_size / 2.0);
        size_yes = cap_size_by_depth(size_yes, yes_book.as_ref(), "buy", config.orderbook_max_fraction);
        size_no = cap_size_by_depth(size_no, no_book.as_ref(), "buy", config.orderbook_max_fraction);
        let clob_addr = config.clob_contract;
        let buy_yes_data = encode_place_order(
            token_info.yes_token_id.clone(),
            yes_price + 0.005,
            size_yes,
            1,
            "gtc".to_string(),
        );
        call_data.push((clob_addr, U256::zero(), buy_yes_data));
        order_intents.push(OrderIntent::new(
            market_key.clone(),
            token_info.yes_token_id.clone(),
            "buy_yes",
            "buy",
            yes_price + 0.005,
            size_yes,
            "gtc",
        ));
        let buy_no_data = encode_place_order(
            token_info.no_token_id.clone(),
            no_price + 0.005,
            size_no,
            1,
            "gtc".to_string(),
        );
        call_data.push((clob_addr, U256::zero(), buy_no_data));
        order_intents.push(OrderIntent::new(
            market_key.clone(),
            token_info.no_token_id.clone(),
            "buy_no",
            "buy",
            no_price + 0.005,
            size_no,
            "gtc",
        ));
        let sell_yes_size = cap_size_by_depth(
            size_yes / 2.0,
            yes_book.as_ref(),
            "sell",
            config.orderbook_max_fraction,
        );
        let sell_yes_data = encode_place_order(
            token_info.yes_token_id.clone(),
            1.0 - (yes_price - 0.005),
            sell_yes_size,
            0,
            "gtc".to_string(),
        );
        call_data.push((clob_addr, U256::zero(), sell_yes_data));
        order_intents.push(OrderIntent::new(
            market_key.clone(),
            token_info.yes_token_id.clone(),
            "sell_yes",
            "sell",
            1.0 - (yes_price - 0.005),
            sell_yes_size,
            "gtc",
        ));
        let sell_no_size = cap_size_by_depth(
            size_no / 2.0,
            no_book.as_ref(),
            "sell",
            config.orderbook_max_fraction,
        );
        let sell_no_data = encode_place_order(
            token_info.no_token_id.clone(),
            1.0 - (no_price - 0.005),
            sell_no_size,
            0,
            "gtc".to_string(),
        );
        call_data.push((clob_addr, U256::zero(), sell_no_data));
        order_intents.push(OrderIntent::new(
            market_key.clone(),
            token_info.no_token_id.clone(),
            "sell_no",
            "sell",
            1.0 - (no_price - 0.005),
            sell_no_size,
            "gtc",
        ));
        action_label = Some("sum_to_one".to_string());
        send_alert(telegram_api, config.telegram_chat_id, "Sum-to-1 batch executed!").await;
    } else if direction == "UP" && yes_price < 0.90 {
        let edge = 1.0 - yes_price - 0.02;
        if edge > params.min_edge_pct {
            let risk_amount = effective_balance * params.max_risk_pct.unwrap_or(config.risk_pct);
            let mut size = params.max_size.min(risk_amount / (yes_price + 0.005));
            size = cap_size_by_depth(size, yes_book.as_ref(), "buy", config.orderbook_max_fraction);
            if size < 1.0 {
                return Ok(());
            }
            let clob_addr = config.clob_contract;
            let buy_data = encode_place_order(
                token_info.yes_token_id.clone(),
                yes_price + 0.005,
                size,
                1,
                "gtc".to_string(),
            );
            call_data.push((clob_addr, U256::zero(), buy_data));
            order_intents.push(OrderIntent::new(
                market_key.clone(),
                token_info.yes_token_id.clone(),
                "buy_yes",
                "buy",
                yes_price + 0.005,
                size,
                "gtc",
            ));
            let sell_size = cap_size_by_depth(
                size / 2.0,
                yes_book.as_ref(),
                "sell",
                config.orderbook_max_fraction,
            );
            let sell_data = encode_place_order(
                token_info.yes_token_id.clone(),
                1.0 - (yes_price - 0.005),
                sell_size,
                0,
                "gtc".to_string(),
            );
            call_data.push((clob_addr, U256::zero(), sell_data));
            order_intents.push(OrderIntent::new(
                market_key.clone(),
                token_info.yes_token_id.clone(),
                "sell_yes",
                "sell",
                1.0 - (yes_price - 0.005),
                sell_size,
                "gtc",
            ));
            action_label = Some("buy_yes".to_string());
            send_alert(telegram_api, config.telegram_chat_id, "Batched YES buy + LP!").await;
        }
    } else if direction == "DOWN" && no_price < 0.90 {
        let edge = 1.0 - no_price - 0.02;
        if edge > params.min_edge_pct {
            let risk_amount = effective_balance * params.max_risk_pct.unwrap_or(config.risk_pct);
            let mut size = params.max_size.min(risk_amount / (no_price + 0.005));
            size = cap_size_by_depth(size, no_book.as_ref(), "buy", config.orderbook_max_fraction);
            if size < 1.0 {
                return Ok(());
            }
            let clob_addr = config.clob_contract;
            let buy_data = encode_place_order(
                token_info.no_token_id.clone(),
                no_price + 0.005,
                size,
                1,
                "gtc".to_string(),
            );
            call_data.push((clob_addr, U256::zero(), buy_data));
            order_intents.push(OrderIntent::new(
                market_key.clone(),
                token_info.no_token_id.clone(),
                "buy_no",
                "buy",
                no_price + 0.005,
                size,
                "gtc",
            ));
            let sell_size = cap_size_by_depth(
                size / 2.0,
                no_book.as_ref(),
                "sell",
                config.orderbook_max_fraction,
            );
            let sell_data = encode_place_order(
                token_info.no_token_id.clone(),
                1.0 - (no_price - 0.005),
                sell_size,
                0,
                "gtc".to_string(),
            );
            call_data.push((clob_addr, U256::zero(), sell_data));
            order_intents.push(OrderIntent::new(
                market_key.clone(),
                token_info.no_token_id.clone(),
                "sell_no",
                "sell",
                1.0 - (no_price - 0.005),
                sell_size,
                "gtc",
            ));
            action_label = Some("buy_no".to_string());
            send_alert(telegram_api, config.telegram_chat_id, "Batched NO buy + LP!").await;
        }
    }

    if call_data.is_empty() && config.market_maker_enabled {
        let spread = config.market_maker_spread_pct.max(0.0);
        let base_size = (params.max_size * config.market_maker_size_pct)
            .min(effective_balance * params.max_risk_pct.unwrap_or(config.risk_pct));
        let clob_addr = config.clob_contract;

        let yes_buy_price = clamp_price(yes_price * (1.0 - spread));
        let yes_sell_price = clamp_price(yes_price * (1.0 + spread));
        let yes_buy_size =
            cap_size_by_depth(base_size, yes_book.as_ref(), "buy", config.orderbook_max_fraction);
        let yes_sell_size = cap_size_by_depth(
            base_size,
            yes_book.as_ref(),
            "sell",
            config.orderbook_max_fraction,
        );

        if yes_buy_size > 0.0 {
            let buy_data = encode_place_order(
                token_info.yes_token_id.clone(),
                yes_buy_price,
                yes_buy_size,
                1,
                "gtc".to_string(),
            );
            call_data.push((clob_addr, U256::zero(), buy_data));
            order_intents.push(OrderIntent::new(
                market_key.clone(),
                token_info.yes_token_id.clone(),
                "mm_buy_yes",
                "buy",
                yes_buy_price,
                yes_buy_size,
                "gtc",
            ));
        }
        if yes_sell_size > 0.0 {
            let sell_data = encode_place_order(
                token_info.yes_token_id.clone(),
                yes_sell_price,
                yes_sell_size,
                0,
                "gtc".to_string(),
            );
            call_data.push((clob_addr, U256::zero(), sell_data));
            order_intents.push(OrderIntent::new(
                market_key.clone(),
                token_info.yes_token_id.clone(),
                "mm_sell_yes",
                "sell",
                yes_sell_price,
                yes_sell_size,
                "gtc",
            ));
        }

        let no_buy_price = clamp_price(no_price * (1.0 - spread));
        let no_sell_price = clamp_price(no_price * (1.0 + spread));
        let no_buy_size =
            cap_size_by_depth(base_size, no_book.as_ref(), "buy", config.orderbook_max_fraction);
        let no_sell_size =
            cap_size_by_depth(base_size, no_book.as_ref(), "sell", config.orderbook_max_fraction);

        if no_buy_size > 0.0 {
            let buy_data = encode_place_order(
                token_info.no_token_id.clone(),
                no_buy_price,
                no_buy_size,
                1,
                "gtc".to_string(),
            );
            call_data.push((clob_addr, U256::zero(), buy_data));
            order_intents.push(OrderIntent::new(
                market_key.clone(),
                token_info.no_token_id.clone(),
                "mm_buy_no",
                "buy",
                no_buy_price,
                no_buy_size,
                "gtc",
            ));
        }
        if no_sell_size > 0.0 {
            let sell_data = encode_place_order(
                token_info.no_token_id.clone(),
                no_sell_price,
                no_sell_size,
                0,
                "gtc".to_string(),
            );
            call_data.push((clob_addr, U256::zero(), sell_data));
            order_intents.push(OrderIntent::new(
                market_key.clone(),
                token_info.no_token_id.clone(),
                "mm_sell_no",
                "sell",
                no_sell_price,
                no_sell_size,
                "gtc",
            ));
        }

        if !order_intents.is_empty() {
            action_label = Some("market_maker".to_string());
        }
    }

    if !call_data.is_empty() {
        let created_at = Instant::now();
        let mut pending_orders = Vec::new();
        let multi_send_addr = config.multi_send_contract;
        let multi_data = encode_multi_send(call_data);
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        let exec_data = encode_exec_transaction(
            multi_send_addr,
            U256::zero(),
            multi_data,
            1,
            U256::from(0),
            U256::from(0),
            U256::from(0),
            H160::zero(),
            H160::zero(),
        );

        let mut placed_via_api = false;
        if config.direct_order_submit_enabled {
            match submit_orders_via_api(http_client, &config, &order_intents).await {
                Ok(api_orders) => {
                    placed_via_api = true;
                    for api_order in api_orders {
                        pending_orders.push(OpenOrder {
                            id: format!("{}:{}:{}", market_key, api_order.label, current_time),
                            market: market_key.clone(),
                            side: api_order.label,
                            direction: api_order.direction,
                            token_id: api_order.token_id,
                            price: api_order.price,
                            size: api_order.size,
                            created_at,
                            status: api_order.status,
                            last_update: Instant::now(),
                            remote_id: api_order.remote_id,
                        });
                    }
                }
                Err(err) => {
                    push_incident(state, "warning", &format!("order submit failed: {err}"));
                    if !config.direct_order_submit_fallback_safe {
                        return Ok(());
                    }
                }
            }
        }

        if !placed_via_api {
            let tx = TransactionRequest::new()
                .to(config.safe_address)
                .data(exec_data)
                .chain_id(config.chain_id);
            let typed_tx: TypedTransaction = tx.into();
            let signature = wallet.sign_transaction(&typed_tx).await.unwrap();
            let rlp = typed_tx.rlp_signed(&signature);
            if !config.dry_run {
                let pending = provider.send_raw_transaction(rlp).await.unwrap();
                let tx_hash = pending.tx_hash();
                let state_clone = state.clone();
                let provider_clone = provider.clone();
                let config_clone = config.clone();
                tokio::spawn(async move {
                    await_safe_tx_receipt(state_clone, provider_clone, tx_hash, config_clone).await;
                });
            } else {
                println!("[DRY-RUN] Batched via Gnosis");
            }
        }

        if let Some(conn) = redis_conn {
            conn.set(key, current_time.to_string()).await.unwrap();
        }

        let base_id = format!("{}:{}", asset, timeframe);
        if !placed_via_api {
            for intent in order_intents.iter() {
                pending_orders.push(OpenOrder {
                    id: format!("{}:{}:{}", base_id, intent.label, current_time),
                    market: base_id.clone(),
                    side: intent.label.clone(),
                    direction: intent.direction.clone(),
                    token_id: intent.token_id.clone(),
                    price: intent.price,
                    size: intent.size,
                    created_at,
                    status: "open".to_string(),
                    last_update: Instant::now(),
                    remote_id: None,
                });
            }
        }
        track_open_orders(state, pending_orders);
        if !placed_via_api {
            sync_open_order_ids(state, http_client, &config).await;
        }

    let trade_summary = TradeSummary {
        market: format!("{}:{}", asset, timeframe),
        side: action_label.clone().unwrap_or_else(|| "trade".to_string()),
        price: signal_price,
        size: config.max_size,
        timestamp: now_string(),
    };
        update_trade(state, trade_summary.clone());
        update_exposure(state, config.max_size);
        update_position_summary(
            state,
            PositionSummary {
                market: format!("{}:{}", asset, timeframe),
                yes: if action_label.as_deref() == Some("buy_yes") {
                    config.max_size
                } else {
                    0.0
                },
                no: if action_label.as_deref() == Some("buy_no") {
                    config.max_size
                } else {
                    0.0
                },
                cash: effective_balance,
                pnl: 0.0,
            },
        );
        push_fill(
            state,
            FillRecord {
                market: trade_summary.market.clone(),
                action: trade_summary.side.clone(),
                pnl: 0.0,
                size: config.max_size,
            price: signal_price,
            timestamp: trade_summary.timestamp.clone(),
            order_id: None,
            token_id: None,
        },
    );

        if let Some(conn) = redis_conn {
            let _ = conn
                .lpush(
                    "trade_log",
                    serde_json::to_string(&trade_summary).unwrap(),
                )
                .await;
        }
    }

    if let Some(conn) = redis_conn {
        conn.set(last_spot_key, signal_price.to_string()).await.unwrap();
    }

    Ok(())
}

fn update_wallet_balance(state: &AppState, balance: f64) {
    {
        let mut telemetry = state.telemetry.write();
        telemetry.wallet_balance = balance;
    }
    broadcast_snapshot(state);
}

fn update_trade(state: &AppState, trade: TradeSummary) {
    {
        let mut telemetry = state.telemetry.write();
        telemetry.positions_placed += 1;
        telemetry.last_trade = Some(trade);
    }
    broadcast_snapshot(state);
}

fn update_exposure(state: &AppState, delta: f64) {
    {
        let mut telemetry = state.telemetry.write();
        telemetry.exposure = (telemetry.exposure + delta).max(0.0);
    }
    broadcast_snapshot(state);
}

fn update_position_summary(state: &AppState, summary: PositionSummary) {
    let mut telemetry = state.telemetry.write();
    telemetry.positions.insert(summary.market.clone(), summary);
}

fn update_orderbook_depth(state: &AppState, depth: OrderbookDepth) {
    let mut telemetry = state.telemetry.write();
    telemetry.orderbook_depth.insert(depth.market.clone(), depth);
}

fn update_orderbook_timestamp(state: &AppState, market: &str) {
    let mut telemetry = state.telemetry.write();
    telemetry
        .orderbook_last_update
        .insert(market.to_string(), Instant::now());
}

fn update_latency(state: &AppState, label: &str, latency_ms: f64) {
    let mut telemetry = state.telemetry.write();
    let entry = telemetry.latency.entry(label.to_string()).or_default();
    entry.count += 1;
    entry.last_ms = latency_ms;
    entry.avg_ms = (entry.avg_ms * (entry.count.saturating_sub(1)) as f64 + latency_ms)
        / entry.count.max(1) as f64;
}

fn update_pipeline_latency(state: &AppState, label: String, latency_ms: f64) {
    let mut telemetry = state.telemetry.write();
    let entry = telemetry
        .pipeline_latency
        .entry(label)
        .or_insert_with(LatencyStats::default);
    entry.count += 1;
    entry.last_ms = latency_ms;
    entry.avg_ms = (entry.avg_ms * (entry.count.saturating_sub(1)) as f64 + latency_ms)
        / entry.count.max(1) as f64;
}

fn update_connection_health(
    state: &AppState,
    label: &str,
    status: &str,
    reconnects: u64,
    last_heartbeat_sec: u64,
) {
    let mut telemetry = state.telemetry.write();
    let heartbeat = if last_heartbeat_sec > 0 {
        Instant::now() - Duration::from_secs(last_heartbeat_sec)
    } else {
        Instant::now()
    };
    telemetry.connection_health.insert(
        label.to_string(),
        ConnectionHealthState {
            status: status.to_string(),
            reconnects,
            last_heartbeat: heartbeat,
        },
    );
}

fn is_feed_stale(state: &AppState, market: &str) -> bool {
    let telemetry = state.telemetry.read();
    let Some(connection) = telemetry.connection_health.get(market) else {
        return true;
    };
    connection.last_heartbeat.elapsed().as_secs() > telemetry.feed_stale_sec
}

fn is_orderbook_stale(state: &AppState, market: &str) -> bool {
    let telemetry = state.telemetry.read();
    let Some(last_update) = telemetry.orderbook_last_update.get(market) else {
        return true;
    };
    last_update.elapsed().as_secs() > telemetry.orderbook_stale_sec
}

async fn poll_wallet_balance(
    state: AppState,
    http_client: Client,
    provider: Provider<Http>,
    config: Config,
    token_cache: Arc<tokio::sync::RwLock<HashMap<String, TokenInfo>>>,
) {
    loop {
        match fetch_usdc_balance(
            &provider,
            config.safe_address,
            config.usdc_contract,
            config.usdc_decimals,
        )
        .await
        {
            Ok(balance) => {
                update_wallet_balance(&state, balance);
            }
            Err(err) => {
                push_incident(
                    &state,
                    "warning",
                    &format!("USDC balance fetch failed: {err}"),
                );
                if let Ok(balance) = fetch_balance(&http_client, &config.host).await {
                    update_wallet_balance(&state, balance);
                }
            }
        }

        if let Ok(count) = provider.get_transaction_count(config.safe_address, None).await {
            let mut telemetry = state.telemetry.write();
            telemetry.safe_tx_count = count.as_u64();
        }

        refresh_token_balances(&state, &provider, &config, &token_cache).await;

        sleep(Duration::from_secs(15)).await;
    }
}

async fn poll_fills(state: AppState, http_client: Client, config: Config) {
    loop {
        match fetch_fills(&http_client, &config.host).await {
            Ok(fills) => {
                apply_fills(&state, fills);
            }
            Err(err) => {
                push_incident(&state, "warning", &format!("fills fetch failed: {err}"));
            }
        }
        sleep(Duration::from_secs(config.order_status_poll_sec.max(5))).await;
    }
}

async fn fetch_fills(http_client: &Client, host: &str) -> Result<Vec<FillRecord>, String> {
    let url = format!("{}/fills?limit=50", host);
    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("fills error: {e}"))?;
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("fills json error: {e}"))?;

    let array = payload
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| payload.as_array())
        .ok_or_else(|| "fills payload not an array".to_string())?;

    let mut fills = Vec::new();
    for item in array {
        let market = item
            .get("market")
            .or_else(|| item.get("market_slug"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let action = item
            .get("side")
            .or_else(|| item.get("action"))
            .and_then(Value::as_str)
            .unwrap_or("fill")
            .to_string();
        let pnl = item.get("pnl").and_then(Value::as_f64).unwrap_or(0.0);
        let price = item
            .get("price")
            .or_else(|| item.get("avg_price"))
            .or_else(|| item.get("fill_price"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let size = item
            .get("size")
            .or_else(|| item.get("qty"))
            .or_else(|| item.get("quantity"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let timestamp = item
            .get("timestamp")
            .or_else(|| item.get("time"))
            .and_then(Value::as_str)
            .map(|s| s.to_string())
            .unwrap_or_else(now_string);
        let order_id = item
            .get("order_id")
            .or_else(|| item.get("id"))
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        let token_id = item
            .get("token_id")
            .or_else(|| item.get("asset_id"))
            .or_else(|| item.get("token"))
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        fills.push(FillRecord {
            market,
            action,
            pnl,
            size,
            price,
            timestamp,
            order_id,
            token_id,
        });
    }

    Ok(fills)
}

fn apply_fills(state: &AppState, fills: Vec<FillRecord>) {
    let mut telemetry = state.telemetry.write();
    let last_seen = telemetry.last_fill_ts;
    let mut new_fills = Vec::new();
    for fill in fills {
        if let Ok(ts) = DateTime::parse_from_rfc3339(&fill.timestamp) {
            let ts = ts.with_timezone(&Utc);
            if last_seen.map(|last| ts > last).unwrap_or(true) {
                telemetry.last_fill_ts = Some(ts);
                new_fills.push(fill);
            }
        } else if last_seen.is_none() {
            new_fills.push(fill);
        }
    }

    for fill in new_fills {
        if fill.pnl >= 0.0 {
            telemetry.wins += 1;
        } else {
            telemetry.losses += 1;
        }
        update_position_from_fill(&mut telemetry, &fill);
        reconcile_open_orders_with_fill(&mut telemetry, &fill);
        update_risk_stats(&mut telemetry, &fill);
        telemetry.recent_fills.push_front(fill.clone());
        if telemetry.recent_fills.len() > 20 {
            telemetry.recent_fills.pop_back();
        }
    }
    drop(telemetry);
    broadcast_snapshot(state);
}

fn update_position_from_fill(telemetry: &mut Telemetry, fill: &FillRecord) {
    let state = telemetry
        .positions_state
        .entry(fill.market.clone())
        .or_insert(PositionState {
            yes_size: 0.0,
            no_size: 0.0,
            yes_avg_cost: 0.0,
            no_avg_cost: 0.0,
            realized_pnl: 0.0,
        });

    let action = fill.action.to_lowercase();
    if action.contains("yes") {
        apply_fill_to_position(
            &mut state.yes_size,
            &mut state.yes_avg_cost,
            &mut state.realized_pnl,
            fill,
            action.contains("sell"),
        );
    } else if action.contains("no") {
        apply_fill_to_position(
            &mut state.no_size,
            &mut state.no_avg_cost,
            &mut state.realized_pnl,
            fill,
            action.contains("sell"),
        );
    }

    telemetry.positions.insert(
        fill.market.clone(),
        PositionSummary {
            market: fill.market.clone(),
            yes: state.yes_size,
            no: state.no_size,
            cash: telemetry.wallet_balance,
            pnl: state.realized_pnl + unrealized_pnl(telemetry, fill.market.as_str()),
        },
    );

    telemetry.exposure = exposure_from_positions(telemetry);
}

fn apply_fill_to_position(
    size: &mut f64,
    avg_cost: &mut f64,
    realized_pnl: &mut f64,
    fill: &FillRecord,
    is_sell: bool,
) {
    if fill.price <= 0.0 || fill.size <= 0.0 {
        return;
    }
    if is_sell {
        let closed = fill.size.min(*size);
        *realized_pnl += (fill.price - *avg_cost) * closed;
        *size = (*size - closed).max(0.0);
        if *size == 0.0 {
            *avg_cost = 0.0;
        }
    } else {
        let total_cost = *avg_cost * *size + fill.price * fill.size;
        *size += fill.size;
        *avg_cost = total_cost / *size.max(1e-6);
    }
}

fn unrealized_pnl(telemetry: &Telemetry, market: &str) -> f64 {
    let Some(state) = telemetry.positions_state.get(market) else {
        return 0.0;
    };
    let Some(depth) = telemetry.orderbook_depth.get(market) else {
        return 0.0;
    };
    let midpoint = (depth.bids + depth.asks) / 2.0;
    let yes_unrealized = (midpoint - state.yes_avg_cost) * state.yes_size;
    let no_midpoint = (1.0 - midpoint).max(0.0);
    let no_unrealized = (no_midpoint - state.no_avg_cost) * state.no_size;
    yes_unrealized + no_unrealized
}

fn exposure_from_positions(telemetry: &Telemetry) -> f64 {
    telemetry
        .positions_state
        .iter()
        .map(|(market, state)| {
            let midpoint = telemetry
                .orderbook_depth
                .get(market)
                .map(|depth| (depth.bids + depth.asks) / 2.0)
                .unwrap_or(0.0);
            let no_midpoint = (1.0 - midpoint).max(0.0);
            state.yes_size * midpoint + state.no_size * no_midpoint
        })
        .sum()
}

fn update_risk_stats(telemetry: &mut Telemetry, fill: &FillRecord) {
    let entry = telemetry
        .per_market_pnl
        .entry(fill.market.clone())
        .or_insert(0.0);
    *entry += fill.pnl;

    telemetry.pnl_history.push_back(fill.pnl);
    if telemetry.pnl_history.len() > 200 {
        telemetry.pnl_history.pop_front();
    }
    telemetry.drawdown = telemetry
        .pnl_history
        .iter()
        .filter(|p| **p < 0.0)
        .map(|p| p.abs())
        .sum::<f64>();
    telemetry.var = calculate_var(&telemetry.pnl_history, 0.05);
    telemetry.var_10 = calculate_var(&telemetry.pnl_history, 0.10);
    telemetry.var_20 = calculate_var(&telemetry.pnl_history, 0.20);
}

fn calculate_var(pnls: &VecDeque<f64>, percentile: f64) -> f64 {
    if pnls.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f64> = pnls.iter().copied().collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((sorted.len() as f64) * percentile).floor() as usize;
    sorted.get(idx).copied().unwrap_or(0.0).abs()
}

fn reconcile_open_orders_with_fill(telemetry: &mut Telemetry, fill: &FillRecord) {
    if let Some(order_id) = fill.order_id.as_deref() {
        if let Some((local_id, _)) = telemetry
            .open_orders
            .iter()
            .find(|(_, order)| {
                order
                    .remote_id
                    .as_deref()
                    .map(|id| id == order_id)
                    .unwrap_or(false)
                    || order.id == order_id
            })
        {
            telemetry.open_orders.remove(local_id);
            return;
        }
    }

    let action = fill.action.to_lowercase();
    let mut candidates: Vec<_> = telemetry
        .open_orders
        .iter()
        .filter(|(_, order)| order.market == fill.market)
        .filter(|(_, order)| {
            if let Some(token_id) = fill.token_id.as_deref() {
                token_id.is_empty() || token_id == order.token_id
            } else {
                true
            }
        })
        .filter(|(_, order)| {
            if action.contains("yes") {
                order.side.contains("yes")
            } else if action.contains("no") {
                order.side.contains("no")
            } else {
                true
            }
        })
        .map(|(id, order)| (id.clone(), order.created_at))
        .collect();
    candidates.sort_by_key(|(_, created)| *created);
    if let Some((id, _)) = candidates.first() {
        telemetry.open_orders.remove(id);
    }
}

fn refresh_request_active(state: &AppState, config: &Config, market: &str) -> bool {
    let mut telemetry = state.telemetry.write();
    if let Some(ts) = telemetry.refresh_requests.get(market) {
        if ts.elapsed().as_secs() < config.order_refresh_window_sec {
            return true;
        }
        telemetry.refresh_requests.remove(market);
    }
    false
}

fn auto_kill_on_stale(state: &AppState, telegram_api: &Api, chat_id: i64, market: &str) {
    let mut telemetry = state.telemetry.write();
    let should_alert = telemetry
        .last_stale_alert
        .get(market)
        .map(|ts| ts.elapsed().as_secs() > 60)
        .unwrap_or(true);
    if should_alert {
        telemetry.last_stale_alert.insert(market.to_string(), Instant::now());
    }
    if !telemetry.kill_switch {
        telemetry.kill_switch = true;
        if should_alert && chat_id != 0 {
            let api = telegram_api.clone();
            let message = format!("Kill switch enabled: feed {market} is stale");
            drop(telemetry);
            tokio::spawn(async move {
                send_alert(&api, chat_id, &message).await;
            });
        }
    }
    drop(telemetry);
    broadcast_snapshot(state);
}

fn maybe_recover_kill_switch(state: &AppState, telegram_api: &Api, chat_id: i64) {
    let mut telemetry = state.telemetry.write();
    if !telemetry.kill_switch {
        return;
    }
    let all_healthy = telemetry.connection_health.values().all(|connection| {
        connection.last_heartbeat.elapsed().as_secs() <= telemetry.feed_stale_sec
    });
    if !all_healthy || telemetry.connection_health.is_empty() {
        return;
    }
    let should_alert = telemetry
        .last_recovery_alert
        .map(|ts| ts.elapsed().as_secs() > 60)
        .unwrap_or(true);
    telemetry.kill_switch = false;
    if should_alert {
        telemetry.last_recovery_alert = Some(Instant::now());
        if chat_id != 0 {
            let api = telegram_api.clone();
            tokio::spawn(async move {
                send_alert(&api, chat_id, "Kill switch cleared: feeds recovered.").await;
            });
        }
    }
    drop(telemetry);
    broadcast_snapshot(state);
}

async fn refresh_token_balances(
    state: &AppState,
    provider: &Provider<Http>,
    config: &Config,
    token_cache: &Arc<tokio::sync::RwLock<HashMap<String, TokenInfo>>>,
) {
    let cache = token_cache.read().await;
    for (market, tokens) in cache.iter() {
        let yes_balance = if looks_like_address(&tokens.yes_token_id) {
            let token = H160::from_str(&tokens.yes_token_id).ok();
            let decimals = get_token_decimals(state, provider, token).await;
            fetch_erc20_balance(provider, config.safe_address, token, decimals)
                .await
                .unwrap_or(0.0)
        } else {
            0.0
        };
        let no_balance = if looks_like_address(&tokens.no_token_id) {
            let token = H160::from_str(&tokens.no_token_id).ok();
            let decimals = get_token_decimals(state, provider, token).await;
            fetch_erc20_balance(provider, config.safe_address, token, decimals)
                .await
                .unwrap_or(0.0)
        } else {
            0.0
        };
        if yes_balance > 0.0 || no_balance > 0.0 {
            update_positions_from_chain(state, market, yes_balance, no_balance);
        }
    }
}

fn update_positions_from_chain(state: &AppState, market: &str, yes_balance: f64, no_balance: f64) {
    let mut telemetry = state.telemetry.write();
    let position = telemetry.positions_state.entry(market.to_string()).or_insert(PositionState {
        yes_size: 0.0,
        no_size: 0.0,
        yes_avg_cost: 0.0,
        no_avg_cost: 0.0,
        realized_pnl: 0.0,
    });
    position.yes_size = yes_balance.max(0.0);
    position.no_size = no_balance.max(0.0);
    telemetry.positions.insert(
        market.to_string(),
        PositionSummary {
            market: market.to_string(),
            yes: position.yes_size,
            no: position.no_size,
            cash: telemetry.wallet_balance,
            pnl: position.realized_pnl + unrealized_pnl(&telemetry, market),
        },
    );
    telemetry.exposure = exposure_from_positions(&telemetry);
}

async fn fetch_erc20_balance(
    provider: &Provider<Http>,
    owner: H160,
    token: Option<H160>,
    decimals: u32,
) -> Result<f64, String> {
    let Some(token) = token else {
        return Ok(0.0);
    };
    let mut data = Vec::with_capacity(4 + 32);
    data.extend_from_slice(&keccak256("balanceOf(address)".as_bytes())[0..4]);
    let mut padded = [0u8; 32];
    padded[12..].copy_from_slice(owner.as_bytes());
    data.extend_from_slice(&padded);
    let call = ethers::types::TransactionRequest::new().to(token).data(data.into());
    let raw = provider
        .call(&call, None)
        .await
        .map_err(|e| format!("token balance error: {e}"))?;
    let balance = U256::from_big_endian(raw.as_ref());
    let divisor = 10u64.pow(decimals.min(18)) as f64;
    Ok(balance.as_u128() as f64 / divisor)
}

fn looks_like_address(value: &str) -> bool {
    value.starts_with("0x") && value.len() == 42
}

async fn get_token_decimals(
    state: &AppState,
    provider: &Provider<Http>,
    token: Option<H160>,
) -> u32 {
    let Some(token) = token else {
        return 18;
    };
    if let Some(decimals) = state.telemetry.read().token_decimals.get(&token) {
        return *decimals;
    }
    let decimals = fetch_erc20_decimals(provider, token).await.unwrap_or(18);
    state.telemetry.write().token_decimals.insert(token, decimals);
    decimals
}

async fn fetch_erc20_decimals(provider: &Provider<Http>, token: H160) -> Result<u32, String> {
    let mut data = Vec::with_capacity(4);
    data.extend_from_slice(&keccak256("decimals()".as_bytes())[0..4]);
    let call = ethers::types::TransactionRequest::new().to(token).data(data.into());
    let raw = provider
        .call(&call, None)
        .await
        .map_err(|e| format!("token decimals error: {e}"))?;
    let decimals = U256::from_big_endian(raw.as_ref());
    Ok(decimals.as_u32())
}

fn track_open_orders(state: &AppState, orders: Vec<OpenOrder>) {
    if orders.is_empty() {
        return;
    }
    let mut telemetry = state.telemetry.write();
    for order in orders {
        telemetry.open_orders.insert(order.id.clone(), order);
    }
}

async fn manage_open_orders(state: AppState, http_client: Client, config: Config) {
    loop {
        let (stale_orders, drift_orders) = collect_cancel_candidates(&state, &config);
        for order in stale_orders.into_iter().chain(drift_orders.into_iter()) {
            let Some(remote_id) = order.remote_id.as_deref() else {
                push_incident(
                    &state,
                    "warning",
                    &format!("skipping cancel for local order {} (no remote id)", order.id),
                );
                remove_open_order(&state, &order.id);
                continue;
            };
            if let Err(err) = cancel_order(&http_client, &config.host, remote_id).await {
                push_incident(
                    &state,
                    "warning",
                    &format!("failed to cancel order {}: {}", remote_id, err),
                );
            }
            if order.created_at.elapsed().as_secs() <= config.order_ttl_sec {
                let mut telemetry = state.telemetry.write();
                telemetry
                    .refresh_requests
                    .insert(order.market.clone(), Instant::now());
            }
            remove_open_order(&state, &order.id);
        }
        sleep(Duration::from_secs(5)).await;
    }
}

async fn poll_open_order_status(state: AppState, http_client: Client, config: Config) {
    loop {
        match fetch_open_orders(&http_client, &config.host).await {
            Ok(remote_orders) => {
                let mut telemetry = state.telemetry.write();
                let mut to_remove = Vec::new();
                let mut needs_status = Vec::new();
                for (local_id, order) in telemetry.open_orders.iter_mut() {
                    let mut matched: Option<&RemoteOrder> = None;
                    if let Some(remote_id) = &order.remote_id {
                        matched = remote_orders.iter().find(|remote| remote.id == *remote_id);
                    } else {
                        let tolerance = config.order_price_drift_pct.max(0.02);
                        matched = remote_orders
                            .iter()
                            .find(|remote| order_matches_remote(order, remote, tolerance));
                        if let Some(remote) = matched {
                            order.remote_id = Some(remote.id.clone());
                        }
                    }
                    if let Some(remote) = matched {
                        order.status = remote.status.clone();
                        order.last_update = Instant::now();
                        if remote.status == "filled"
                            || remote.status == "canceled"
                            || remote.status == "expired"
                        {
                            to_remove.push(local_id.clone());
                        }
                    } else if order
                        .remote_id
                        .is_some()
                        && order.last_update.elapsed().as_secs() >= config.order_status_poll_sec
                    {
                        needs_status.push(local_id.clone());
                    }
                }
                for id in to_remove {
                    telemetry.open_orders.remove(&id);
                }
                drop(telemetry);
                for local_id in needs_status.into_iter().take(5) {
                    sync_order_status_by_id(&state, &http_client, &config, &local_id).await;
                }
            }
            Err(err) => {
                push_incident(&state, "warning", &format!("order status poll failed: {err}"));
            }
        }
        sleep(Duration::from_secs(10)).await;
    }
}

async fn fetch_open_orders(
    http_client: &Client,
    host: &str,
) -> Result<Vec<RemoteOrder>, String> {
    let url = format!("{}/orders?status=open", host);
    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("orders status error: {e}"))?;
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("orders status json error: {e}"))?;
    let array = payload
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| payload.as_array())
        .ok_or_else(|| "orders payload not an array".to_string())?;
    let mut orders = Vec::new();
    for item in array {
        let id = item
            .get("id")
            .or_else(|| item.get("order_id"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let status = item
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("open")
            .to_string();
        let side = item
            .get("side")
            .or_else(|| item.get("action"))
            .and_then(Value::as_str)
            .unwrap_or("buy")
            .to_string();
        let price = item
            .get("price")
            .or_else(|| item.get("limit_price"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let size = item
            .get("size")
            .or_else(|| item.get("quantity"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let market = item
            .get("market")
            .or_else(|| item.get("market_slug"))
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        let token_id = item
            .get("token_id")
            .or_else(|| item.get("asset_id"))
            .and_then(Value::as_str)
            .map(|s| s.to_string());
        orders.push(RemoteOrder {
            id,
            market,
            token_id,
            side,
            price,
            size,
            status,
        });
    }
    Ok(orders)
}

async fn fetch_order_status(
    http_client: &Client,
    host: &str,
    path: &str,
    order_id: &str,
) -> Result<String, String> {
    let url = format!("{}/{}/{}", host, path.trim_matches('/'), order_id);
    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("order status error: {e}"))?;
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("order status json error: {e}"))?;
    let data = payload.get("data").unwrap_or(&payload);
    let status = data
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    Ok(status)
}

async fn submit_orders_via_api(
    http_client: &Client,
    config: &Config,
    intents: &[OrderIntent],
) -> Result<Vec<ApiOrderResult>, String> {
    let mode = config.direct_order_submit_mode.to_lowercase();
    if mode == "batch" {
        submit_orders_batch(http_client, config, intents).await
    } else {
        submit_orders_single(http_client, config, intents).await
    }
}

async fn submit_orders_single(
    http_client: &Client,
    config: &Config,
    intents: &[OrderIntent],
) -> Result<Vec<ApiOrderResult>, String> {
    let mut results = Vec::new();
    for intent in intents {
        let payload = build_order_payload(config, intent);
        let url = format!("{}/{}", config.host, config.direct_order_submit_path.trim_matches('/'));
        let response = http_client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("order submit error: {e}"))?;
        let status = response.status();
        let payload: Value = response
            .json()
            .await
            .map_err(|e| format!("order submit json error: {e}"))?;
        if !status.is_success() {
            return Err(parse_order_error(&payload, status.as_u16()));
        }
        let result = parse_order_result(intent, &payload)?;
        results.push(result);
    }
    Ok(results)
}

async fn submit_orders_batch(
    http_client: &Client,
    config: &Config,
    intents: &[OrderIntent],
) -> Result<Vec<ApiOrderResult>, String> {
    let orders: Vec<Value> = intents
        .iter()
        .map(|intent| build_order_payload(config, intent))
        .collect();
    let payload = json!({ "orders": orders });
    let url = format!(
        "{}/{}",
        config.host,
        config.direct_order_submit_batch_path.trim_matches('/')
    );
    let response = http_client
        .post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("order submit error: {e}"))?;
    let status = response.status();
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("order submit json error: {e}"))?;
    if !status.is_success() {
        return Err(parse_order_error(&payload, status.as_u16()));
    }
    let data = payload
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| payload.as_array())
        .ok_or_else(|| "batch order response not array".to_string())?;
    if data.len() != intents.len() {
        return Err("batch order response length mismatch".to_string());
    }
    let mut results = Vec::new();
    for (intent, item) in intents.iter().zip(data.iter()) {
        let result = parse_order_result(intent, item)?;
        results.push(result);
    }
    Ok(results)
}

fn build_order_payload(config: &Config, intent: &OrderIntent) -> Value {
    let mut payload = json!({
        "token_id": intent.token_id,
        "price": intent.price,
        "size": intent.size,
        "side": intent.direction,
        "order_type": intent.order_type,
        "client_order_id": format!("{}:{}", intent.label, now_string()),
    });
    if config.direct_order_submit_expiration_sec > 0 {
        payload["expiration_sec"] = Value::from(config.direct_order_submit_expiration_sec);
    }
    if !config.direct_order_submit_market_field.is_empty() {
        payload[config.direct_order_submit_market_field.as_str()] = Value::from(intent.market.clone());
    }
    payload
}

fn parse_order_result(intent: &OrderIntent, payload: &Value) -> Result<ApiOrderResult, String> {
    let data = payload.get("data").unwrap_or(payload);
    if let Some(error) = data.get("error") {
        return Err(format!("order submit error: {error}"));
    }
    let status = data
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("open")
        .to_string();
    let remote_id = data
        .get("id")
        .or_else(|| data.get("order_id"))
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    Ok(ApiOrderResult {
        label: intent.label.clone(),
        direction: intent.direction.clone(),
        token_id: intent.token_id.clone(),
        price: intent.price,
        size: intent.size,
        status,
        remote_id,
    })
}

fn parse_order_error(payload: &Value, status: u16) -> String {
    let data = payload.get("data").unwrap_or(payload);
    if let Some(error) = data.get("error") {
        return format!("order submit failed ({status}): {error}");
    }
    if let Some(errors) = data.get("errors") {
        return format!("order submit failed ({status}): {errors}");
    }
    format!("order submit failed ({status})")
}

async fn sync_order_status_by_id(
    state: &AppState,
    http_client: &Client,
    config: &Config,
    local_id: &str,
) {
    let remote_id = {
        let telemetry = state.telemetry.read();
        telemetry
            .open_orders
            .get(local_id)
            .and_then(|order| order.remote_id.clone())
    };
    let Some(remote_id) = remote_id else {
        return;
    };

    match fetch_order_status(http_client, &config.host, &config.order_status_path, &remote_id).await {
        Ok(status) => {
            let mut telemetry = state.telemetry.write();
            if let Some(order) = telemetry.open_orders.get_mut(local_id) {
                order.status = status.clone();
                order.last_update = Instant::now();
            }
            if status == "filled" || status == "canceled" || status == "expired" {
                telemetry.open_orders.remove(local_id);
            }
        }
        Err(err) => {
            push_incident(
                state,
                "warning",
                &format!("order status fetch failed for {}: {}", remote_id, err),
            );
        }
    }
}

async fn sync_open_order_ids(state: &AppState, http_client: &Client, config: &Config) {
    let remote_orders = match fetch_open_orders(http_client, &config.host).await {
        Ok(orders) => orders,
        Err(err) => {
            push_incident(state, "warning", &format!("order id sync failed: {err}"));
            return;
        }
    };

    let mut telemetry = state.telemetry.write();
    let cutoff = Duration::from_secs(config.order_id_sync_window_sec);
    for order in telemetry.open_orders.values_mut() {
        if order.remote_id.is_some() || order.created_at.elapsed() > cutoff {
            continue;
        }
        let tolerance = config.order_price_drift_pct.max(0.02);
        if let Some(remote) = remote_orders
            .iter()
            .find(|remote| order_matches_remote(order, remote, tolerance))
        {
            order.remote_id = Some(remote.id.clone());
            order.status = remote.status.clone();
            order.last_update = Instant::now();
        }
    }
}

async fn await_safe_tx_receipt(
    state: AppState,
    provider: Provider<Http>,
    tx_hash: ethers::types::H256,
    config: Config,
) {
    let timeout = Duration::from_secs(config.safe_tx_confirm_timeout_sec);
    let poll = Duration::from_secs(config.safe_tx_confirm_poll_sec.max(1));
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            push_incident(
                &state,
                "warning",
                &format!("safe tx {tx_hash:?} confirmation timed out"),
            );
            break;
        }
        match provider.get_transaction_receipt(tx_hash).await {
            Ok(Some(receipt)) => {
                if receipt.status.unwrap_or_default().as_u64() == 1 {
                    push_incident(
                        &state,
                        "info",
                        &format!("safe tx confirmed: {tx_hash:?}"),
                    );
                } else {
                    push_incident(
                        &state,
                        "warning",
                        &format!("safe tx failed: {tx_hash:?}"),
                    );
                }
                break;
            }
            Ok(None) => {}
            Err(err) => {
                push_incident(
                    &state,
                    "warning",
                    &format!("safe tx receipt error: {err}"),
                );
                break;
            }
        }
        sleep(poll).await;
    }
}

fn order_matches_remote(order: &OpenOrder, remote: &RemoteOrder, tolerance: f64) -> bool {
    let local_side = normalize_side(&order.direction);
    let remote_side = normalize_side(&remote.side);
    if local_side != remote_side {
        return false;
    }
    if let Some(token_id) = &remote.token_id {
        if !token_id.is_empty() && token_id != &order.token_id {
            return false;
        }
    }
    if let Some(market) = &remote.market {
        if !market.is_empty() && market != &order.market {
            return false;
        }
    }
    let price_match = if remote.price > 0.0 && order.price > 0.0 {
        (remote.price - order.price).abs() / order.price.max(1e-6) <= tolerance
    } else {
        true
    };
    let size_match = if remote.size > 0.0 && order.size > 0.0 {
        (remote.size - order.size).abs() / order.size.max(1e-6) <= tolerance
    } else {
        true
    };
    price_match && size_match
}

fn normalize_side(side: &str) -> String {
    let side = side.to_lowercase();
    if side.contains("sell") {
        "sell".to_string()
    } else if side.contains("buy") {
        "buy".to_string()
    } else {
        side
    }
}

fn collect_cancel_candidates(state: &AppState, config: &Config) -> (Vec<OpenOrder>, Vec<OpenOrder>) {
    let telemetry = state.telemetry.read();
    let mut stale = Vec::new();
    let mut drift = Vec::new();
    for order in telemetry.open_orders.values() {
        if order.created_at.elapsed().as_secs() > config.order_ttl_sec {
            stale.push(order.clone());
            continue;
        }
        if let Some(depth) = telemetry.orderbook_depth.get(&order.market) {
            let midpoint = (depth.bids + depth.asks) / 2.0;
            if midpoint > 0.0 {
                let drift_pct = (midpoint - order.price).abs() / order.price.max(1e-6);
                if drift_pct > config.order_price_drift_pct {
                    drift.push(order.clone());
                }
            }
        }
    }
    (stale, drift)
}

fn remove_open_order(state: &AppState, order_id: &str) {
    let mut telemetry = state.telemetry.write();
    telemetry.open_orders.remove(order_id);
}

async fn cancel_order(http_client: &Client, host: &str, order_id: &str) -> Result<(), String> {
    let url = format!("{}/orders/cancel", host);
    let payload = json!({ "order_id": order_id });
    http_client
        .post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| format!("cancel error: {e}"))?;
    Ok(())
}

fn push_incident(state: &AppState, level: &str, message: &str) {
    let mut telemetry = state.telemetry.write();
    if telemetry.incidents.len() >= 50 {
        telemetry.incidents.pop_back();
    }
    telemetry.incidents.push_front(Incident {
        level: level.to_string(),
        message: message.to_string(),
        timestamp: now_string(),
    });
}

fn push_fill(state: &AppState, fill: FillRecord) {
    let mut telemetry = state.telemetry.write();
    if telemetry.recent_fills.len() >= 20 {
        telemetry.recent_fills.pop_back();
    }
    telemetry.recent_fills.push_front(fill);
}

fn risk_check(
    state: &AppState,
    market: &str,
    params: &MarketParams,
    max_daily_loss: f64,
    max_inventory: f64,
    min_liquidity: f64,
) -> RiskStatus {
    let telemetry = state.telemetry.read();
    if telemetry.wallet_balance < min_liquidity {
        return RiskStatus::blocked(format!(
            "liquidity {:.2} below minimum {:.2}",
            telemetry.wallet_balance, min_liquidity
        ));
    }
    if telemetry.drawdown >= max_daily_loss {
        return RiskStatus::blocked(format!(
            "daily loss {:.2} exceeds {:.2}",
            telemetry.drawdown, max_daily_loss
        ));
    }
    if telemetry.exposure >= max_inventory {
        return RiskStatus::blocked(format!(
            "inventory {:.2} exceeds {:.2}",
            telemetry.exposure, max_inventory
        ));
    }
    if let Some(max_position) = params.max_position {
        let market_exposure = telemetry
            .positions_state
            .get(market)
            .map(|state| {
                let midpoint = telemetry
                    .orderbook_depth
                    .get(market)
                    .map(|depth| (depth.bids + depth.asks) / 2.0)
                    .unwrap_or(0.0);
                let no_midpoint = (1.0 - midpoint).max(0.0);
                state.yes_size * midpoint + state.no_size * no_midpoint
            })
            .unwrap_or(0.0);
        if market_exposure >= max_position {
            return RiskStatus::blocked(format!(
                "market exposure {:.2} exceeds {:.2}",
                market_exposure, max_position
            ));
        }
    }
    if let (Some(start), Some(end)) = (params.trade_start_hour, params.trade_end_hour) {
        let hour = Utc::now().hour();
        if hour < start || hour >= end {
            return RiskStatus::blocked("outside allowed trading hours".to_string());
        }
    }
    RiskStatus::allowed()
}

struct RiskStatus {
    allowed: bool,
    reason: String,
}

impl RiskStatus {
    fn allowed() -> Self {
        Self {
            allowed: true,
            reason: String::new(),
        }
    }

    fn blocked(reason: String) -> Self {
        Self {
            allowed: false,
            reason,
        }
    }
}

fn update_volatility(
    trackers: &Arc<RwLock<HashMap<String, VolatilityTracker>>>,
    key: &str,
    price: f64,
) -> f64 {
    let mut map = trackers.write();
    let tracker = map.entry(key.to_string()).or_default();
    let abs_return = if let Some(last) = tracker.last_price {
        ((price - last) / last).abs()
    } else {
        0.0
    };
    tracker.last_price = Some(price);
    tracker.ema_abs_return = if tracker.ema_abs_return == 0.0 {
        abs_return
    } else {
        tracker.ema_abs_return * 0.9 + abs_return * 0.1
    };
    tracker.ema_abs_return
}

fn know_stage_label(asset: &str, timeframe: &str, stage: &str) -> String {
    format!("{}:{}:{}", asset, timeframe, stage)
}

async fn send_alert(api: &Api, chat_id: i64, message: &str) {
    if chat_id == 0 {
        return;
    }
    let request = SendMessage::new(ChatId::new(chat_id), message.to_string());
    let _ = api.send(request).await;
}

async fn fetch_token_info(
    http_client: &Client,
    config: &Config,
    token_cache: &Arc<tokio::sync::RwLock<HashMap<String, TokenInfo>>>,
    asset: &str,
    timeframe: &str,
) -> Result<TokenInfo, String> {
    let cache_key = format!("{}:{}", asset, timeframe);
    {
        let cache = token_cache.read().await;
        if let Some(info) = cache.get(&cache_key) {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            if now - info.last_refresh < config.token_cache_ttl_sec {
                return Ok(info.clone());
            }
        }
    }

    let url = format!("{}/markets?asset={}&timeframe={}", config.gamma_api, asset, timeframe);
    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("Gamma API error: {e}"))?;
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("Gamma response error: {e}"))?;

    let yes_token_id = payload
        .get("yesTokenId")
        .and_then(Value::as_str)
        .unwrap_or("yes_placeholder")
        .to_string();
    let no_token_id = payload
        .get("noTokenId")
        .and_then(Value::as_str)
        .unwrap_or("no_placeholder")
        .to_string();

    let info = TokenInfo {
        yes_token_id,
        no_token_id,
        last_refresh: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
    };

    let mut cache = token_cache.write().await;
    cache.insert(cache_key, info.clone());

    Ok(info)
}

async fn fetch_midpoint(http_client: &Client, host: &str, token_id: &str) -> Result<f64, String> {
    let url = format!("{}/midpoint/{}", host, token_id);
    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("midpoint error: {e}"))?;
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("midpoint json error: {e}"))?;
    let price = payload
        .get("price")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    Ok(price)
}

async fn fetch_orderbook_depth(
    http_client: &Client,
    host: &str,
    path: &str,
    token_id: &str,
) -> Result<OrderbookSnapshot, String> {
    let url = format!("{}/{}/{}", host, path.trim_matches('/'), token_id);
    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("orderbook error: {e}"))?;
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("orderbook json error: {e}"))?;
    let book = payload.get("data").unwrap_or(&payload);
    let bids = book
        .get("bids")
        .and_then(Value::as_array)
        .ok_or_else(|| "orderbook missing bids".to_string())?;
    let asks = book
        .get("asks")
        .and_then(Value::as_array)
        .ok_or_else(|| "orderbook missing asks".to_string())?;

    let (best_bid, bid_depth) = summarize_orderbook_side(bids);
    let (best_ask, ask_depth) = summarize_orderbook_side(asks);
    Ok(OrderbookSnapshot {
        best_bid,
        best_ask,
        bid_depth,
        ask_depth,
    })
}

fn summarize_orderbook_side(levels: &[Value]) -> (f64, f64) {
    let mut best_price = 0.0;
    let mut depth = 0.0;
    for (idx, level) in levels.iter().enumerate() {
        if let Some((price, size)) = parse_orderbook_level(level) {
            if idx == 0 {
                best_price = price;
            }
            depth += size;
        }
    }
    (best_price, depth)
}

fn parse_orderbook_level(level: &Value) -> Option<(f64, f64)> {
    if let Some(array) = level.as_array() {
        if array.len() >= 2 {
            let price = parse_f64(&array[0])?;
            let size = parse_f64(&array[1])?;
            return Some((price, size));
        }
    }
    let price = level
        .get("price")
        .or_else(|| level.get("p"))
        .and_then(parse_f64);
    let size = level
        .get("size")
        .or_else(|| level.get("qty"))
        .or_else(|| level.get("amount"))
        .and_then(parse_f64);
    match (price, size) {
        (Some(price), Some(size)) => Some((price, size)),
        _ => None,
    }
}

fn parse_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str()?.parse().ok())
}

fn clamp_price(price: f64) -> f64 {
    price.clamp(0.01, 0.99)
}

fn cap_size_by_depth(size: f64, book: Option<&OrderbookSnapshot>, side: &str, fraction: f64) -> f64 {
    let Some(book) = book else {
        return size;
    };
    if fraction <= 0.0 {
        return 0.0;
    }
    let depth = if side.eq_ignore_ascii_case("sell") {
        book.bid_depth
    } else {
        book.ask_depth
    };
    if depth <= 0.0 {
        return size;
    }
    let cap = depth * fraction;
    size.min(cap)
}

async fn get_chainlink_price(http_client: &Client, config: &Config, feed_id: &str) -> Option<f64> {
    let url = "https://api.dataengine.chain.link/api/v1/reports/latest";
    let query = format!("?feedID={}", feed_id);
    let full_path = format!("{}{}", url, query);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let message = format!(
        "GET|{}||{}|{}",
        full_path, config.chainlink_username, timestamp
    );
    let signature = hmac_sha256(config.chainlink_password.as_bytes(), message.as_bytes());
    let sig_hex = hex::encode(signature);

    let mut headers = HeaderMap::new();
    headers.insert("Authorization", config.chainlink_username.parse().ok()?);
    headers.insert(
        "X-Authorization-Timestamp",
        timestamp.to_string().parse().ok()?,
    );
    headers.insert(
        "X-Authorization-Signature-SHA256",
        sig_hex.parse().ok()?,
    );

    let resp = http_client
        .get(full_path)
        .headers(headers)
        .send()
        .await
        .ok()?;
    let data: Value = resp.json().await.ok()?;
    data.get("report")?
        .get("benchmarkPrice")?
        .as_f64()
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

async fn fetch_balance(http_client: &Client, host: &str) -> Result<f64, String> {
    let url = format!("{}/balance", host);
    let response = http_client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("balance error: {e}"))?;
    let payload: Value = response
        .json()
        .await
        .map_err(|e| format!("balance json error: {e}"))?;
    let balance = payload
        .get("balance")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    Ok(balance)
}

async fn fetch_usdc_balance(
    provider: &Provider<Http>,
    owner: H160,
    token: H160,
    decimals: u32,
) -> Result<f64, String> {
    let mut data = Vec::with_capacity(4 + 32);
    data.extend_from_slice(&keccak256("balanceOf(address)".as_bytes())[0..4]);
    let mut padded = [0u8; 32];
    padded[12..].copy_from_slice(owner.as_bytes());
    data.extend_from_slice(&padded);

    let call = ethers::types::TransactionRequest::new().to(token).data(data.into());
    let raw = provider
        .call(&call, None)
        .await
        .map_err(|e| format!("USDC call error: {e}"))?;
    let balance = U256::from_big_endian(raw.as_ref());
    let divisor = 10u64.pow(decimals.min(18)) as f64;
    Ok(balance.as_u128() as f64 / divisor)
}

fn encode_place_order(token_id: String, price: f64, size: f64, side: u8, order_type: String) -> Vec<u8> {
    let sig_hash = keccak256("placeOrder(string,uint256,uint256,uint8,string)".as_bytes());
    let mut encoded = sig_hash[0..4].to_vec();
    encoded.extend_from_slice(&U256::from(160).to_be_bytes());
    let price_u = U256::from((price * 1e18) as u128);
    encoded.extend_from_slice(&price_u.to_be_bytes());
    let size_u = U256::from((size * 1e18) as u128);
    encoded.extend_from_slice(&size_u.to_be_bytes());
    encoded.push(side);
    encoded.extend_from_slice(&U256::from(192).to_be_bytes());
    let token_len = U256::from(token_id.len());
    encoded.extend_from_slice(&token_len.to_be_bytes());
    encoded.extend_from_slice(token_id.as_bytes());
    let type_len = U256::from(order_type.len());
    encoded.extend_from_slice(&type_len.to_be_bytes());
    encoded.extend_from_slice(order_type.as_bytes());
    encoded
}

fn encode_multi_send(calls: Vec<(H160, U256, Vec<u8>)>) -> Vec<u8> {
    let mut data = vec![];
    for (to, value, call_data) in calls {
        data.push(0x8d);
        data.push(0x80);
        data.push(0xdd);
        data.push(0x0b);
        data.extend_from_slice(&to.0);
        data.extend_from_slice(&value.to_be_bytes());
        let data_len = U256::from(call_data.len());
        data.extend_from_slice(&data_len.to_be_bytes());
        data.extend_from_slice(&call_data);
    }
    let sig_hash = keccak256("multiSend(bytes)".as_bytes());
    let mut encoded = sig_hash[0..4].to_vec();
    encoded.extend_from_slice(&U256::from(32).to_be_bytes());
    encoded.extend_from_slice(&U256::from(data.len()).to_be_bytes());
    encoded.extend_from_slice(&data);
    encoded
}

fn encode_exec_transaction(
    to: H160,
    value: U256,
    data: Vec<u8>,
    operation: u8,
    safe_tx_gas: U256,
    base_gas: U256,
    gas_price: U256,
    gas_token: H160,
    refund_receiver: H160,
) -> Vec<u8> {
    let sig_hash = keccak256(
        "execTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address payable,bytes)"
            .as_bytes(),
    );
    let mut encoded = sig_hash[0..4].to_vec();
    encoded.extend_from_slice(&to.0);
    encoded.extend_from_slice(&value.to_be_bytes());
    encoded.extend_from_slice(&U256::from(320).to_be_bytes());
    encoded.push(operation);
    encoded.extend_from_slice(&safe_tx_gas.to_be_bytes());
    encoded.extend_from_slice(&base_gas.to_be_bytes());
    encoded.extend_from_slice(&gas_price.to_be_bytes());
    encoded.extend_from_slice(&gas_token.0);
    encoded.extend_from_slice(&refund_receiver.0);
    encoded.extend_from_slice(&U256::from(data.len() + 320).to_be_bytes());
    encoded.extend_from_slice(&U256::from(data.len()).to_be_bytes());
    encoded.extend_from_slice(&data);
    let safe_tx_hash = keccak256(&encoded);
    let signature = format!("0x{}", hex::encode(safe_tx_hash));
    let sig_bytes = hex::decode(signature.trim_start_matches("0x")).unwrap_or_default();
    encoded.extend_from_slice(&U256::from(sig_bytes.len()).to_be_bytes());
    encoded.extend_from_slice(&sig_bytes);
    encoded
}

fn build_http_client(api_key: &str, api_secret: &str, passphrase: &str) -> Client {
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json".parse().unwrap());
    headers.insert("api_key", api_key.parse().unwrap());
    headers.insert("api_secret", api_secret.parse().unwrap());
    headers.insert("api_passphrase", passphrase.parse().unwrap());
    Client::builder().default_headers(headers).build().unwrap()
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_or_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_or_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(default)
}

fn env_or_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_or_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(default)
}

fn now_string() -> String {
    let now: DateTime<Utc> = Utc::now();
    now.to_rfc3339()
}

fn parse_market_overrides(input: &str) -> HashMap<String, MarketOverride> {
    let mut map = HashMap::new();
    for segment in input.split(';') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        let mut parts = segment.splitn(2, ':');
        let asset = parts.next().unwrap_or("").trim().to_lowercase();
        let params = parts.next().unwrap_or("");
        let mut override_cfg = MarketOverride::default();
        for kv in params.split(',') {
            let mut kv_parts = kv.splitn(2, '=');
            let key = kv_parts.next().unwrap_or("").trim();
            let value = kv_parts.next().unwrap_or("").trim();
            if let Ok(num) = value.parse::<f64>() {
                match key {
                    "threshold" => override_cfg.threshold_pct = Some(num),
                    "min_edge" => override_cfg.min_edge_pct = Some(num),
                    "sum_threshold" => override_cfg.sum_threshold = Some(num),
                    "max_size" => override_cfg.max_size = Some(num),
                    "max_risk" => override_cfg.max_risk_pct = Some(num),
                    "max_position" => override_cfg.max_position = Some(num),
                    "start_hour" => override_cfg.trade_start_hour = Some(num as u32),
                    "end_hour" => override_cfg.trade_end_hour = Some(num as u32),
                    _ => {}
                }
            }
        }
        if !asset.is_empty() {
            map.insert(asset, override_cfg);
        }
    }
    map
}

fn build_env_template(config: &Config) -> String {
    format!(
        "CLOB_HOST={}\nGAMMA_API={}\nPOLYGON_RPC={}\nSAFE_ADDRESS={}\nBOT_ADDRESS={}\nCHAIN_ID={}\nPRIVATE_KEY={}\nPOLYMARKET_API_KEY={}\nPOLYMARKET_API_SECRET={}\nPOLYMARKET_API_PASSPHRASE={}\nCHAINLINK_USERNAME={}\nCHAINLINK_PASSWORD={}\nTELEGRAM_TOKEN={}\nTELEGRAM_CHAT_ID={}\nTHRESHOLD_PCT={}\nMIN_EDGE_PCT={}\nSUM_THRESHOLD={}\nMAX_SIZE={}\nRISK_PCT={}\nDRY_RUN={}\nCOOLDOWN_SEC={}\nDELAY_ADD_1H_SEC={}\nMAX_DAILY_LOSS={}\nMAX_INVENTORY={}\nMIN_LIQUIDITY={}\nTOKEN_CACHE_TTL_SEC={}\nKILL_SWITCH={}\nENABLED_ASSETS={}\nENABLED_TIMEFRAMES={}\nSTARTING_CAPITAL={}\nORDER_TTL_SEC={}\nORDER_PRICE_DRIFT_PCT={}\nFEED_STALE_SEC={}\nORDERBOOK_STALE_SEC={}\nORDER_ID_SYNC_WINDOW_SEC={}\nORDER_STATUS_POLL_SEC={}\nORDER_REFRESH_WINDOW_SEC={}\nORDER_STATUS_PATH={}\nSAFE_TX_CONFIRM_TIMEOUT_SEC={}\nSAFE_TX_CONFIRM_POLL_SEC={}\nORDERBOOK_MAX_FRACTION={}\nDIRECT_ORDER_SUBMIT_ENABLED={}\nDIRECT_ORDER_SUBMIT_MODE={}\nDIRECT_ORDER_SUBMIT_PATH={}\nDIRECT_ORDER_SUBMIT_BATCH_PATH={}\nDIRECT_ORDER_SUBMIT_FALLBACK_SAFE={}\nDIRECT_ORDER_SUBMIT_MARKET_FIELD={}\nDIRECT_ORDER_SUBMIT_EXPIRATION_SEC={}\nMARKET_MAKER_ENABLED={}\nMARKET_MAKER_SPREAD_PCT={}\nMARKET_MAKER_SIZE_PCT={}\nUSDC_CONTRACT={}\nUSDC_DECIMALS={}\nCLOB_CONTRACT={}\nMULTISEND_CONTRACT={}\nORDERBOOK_PATH={}\nMARKET_OVERRIDES={}\n",
        config.host,
        config.gamma_api,
        config.polygon_rpc,
        to_checksum(&config.safe_address, None),
        to_checksum(&config.bot_address, None),
        config.chain_id,
        config.private_key,
        config.api_key,
        config.api_secret,
        config.passphrase,
        config.chainlink_username,
        config.chainlink_password,
        config.telegram_token,
        config.telegram_chat_id,
        config.threshold_pct,
        config.min_edge_pct,
        config.sum_threshold,
        config.max_size,
        config.risk_pct,
        config.dry_run,
        config.cooldown_sec,
        config.delay_to_add_1h,
        config.max_daily_loss,
        config.max_inventory,
        config.min_liquidity,
        config.token_cache_ttl_sec,
        config.kill_switch,
        config.enabled_assets.join(","),
        config.enabled_timeframes.join(","),
        config.starting_capital,
        config.order_ttl_sec,
        config.order_price_drift_pct,
        config.feed_stale_sec,
        config.orderbook_stale_sec,
        config.order_id_sync_window_sec,
        config.order_status_poll_sec,
        config.order_refresh_window_sec,
        config.order_status_path,
        config.safe_tx_confirm_timeout_sec,
        config.safe_tx_confirm_poll_sec,
        config.orderbook_max_fraction,
        config.direct_order_submit_enabled,
        config.direct_order_submit_mode,
        config.direct_order_submit_path,
        config.direct_order_submit_batch_path,
        config.direct_order_submit_fallback_safe,
        config.direct_order_submit_market_field,
        config.direct_order_submit_expiration_sec,
        config.market_maker_enabled,
        config.market_maker_spread_pct,
        config.market_maker_size_pct,
        to_checksum(&config.usdc_contract, None),
        config.usdc_decimals,
        to_checksum(&config.clob_contract, None),
        to_checksum(&config.multi_send_contract, None),
        config.orderbook_path,
        ""
    )
}
