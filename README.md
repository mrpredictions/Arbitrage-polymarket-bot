# Arbitrage-polymarket-bot

## Direct CLOB order submission setup (step-by-step)

Use these steps when you need the bot to submit orders directly to a CLOB endpoint with a
provider-specific payload schema and batch response format.

### 1) Collect a real example from your CLOB provider

Ask your CLOB provider for (or capture from their SDK/docs):

- **Single order request payload** (exact JSON field names).
- **Batch order response** (including the top-level key that holds the array).

You will map those directly into the bot's environment variables in the next steps.

### 2) Map the request field names

Set these env vars to match your provider's exact field names:

- `DIRECT_ORDER_SUBMIT_MARKET_FIELD` → field that identifies the market/condition (e.g. `market`, `market_id`, `condition_id`).
- `DIRECT_ORDER_SUBMIT_EXPIRATION_FIELD` → field name used for expiration/TTL seconds (e.g. `expiration_sec`, `expiry`, `expireAt`).
- `DIRECT_ORDER_SUBMIT_NONCE_FIELD` → field name used for a nonce/idempotency token (e.g. `nonce`, `client_nonce`).

**Recommendation:** if your provider does not support a field, leave it blank (empty string) and the
bot will omit it from the payload. If your provider supports expiration, set
`DIRECT_ORDER_SUBMIT_EXPIRATION_SEC` to a short TTL (e.g. `30`–`60`).

### 3) Configure the nonce (if required)

- If the provider requires a nonce, set `DIRECT_ORDER_SUBMIT_NONCE` to a unique or acceptable
  value (some providers accept a static value, others want a UUID).
- If the provider does not require a nonce, leave both `DIRECT_ORDER_SUBMIT_NONCE_FIELD` and
  `DIRECT_ORDER_SUBMIT_NONCE` empty.

**Recommendation:** prefer a unique value per order if the provider supports it; static values can
lead to rejections on idempotent endpoints.

### 4) Configure batch response parsing

Set `DIRECT_ORDER_SUBMIT_BATCH_RESPONSE_KEY` to the array key used in the batch response.

Examples:

- If the response is `{ "data": [ ... ] }`, set it to `data`.
- If the response is `{ "results": [ ... ] }`, set it to `results`.
- If the response is `[ ... ]` at the root, you can leave it as the default and the bot will
  fall back to the root array.

### 5) Verify with a dry run

1. Set `DRY_RUN=true`.
2. Set `DIRECT_ORDER_SUBMIT_ENABLED=true`.
3. Choose `DIRECT_ORDER_SUBMIT_MODE=single` first, then test `batch`.
4. Submit a small order and confirm the CLOB provider accepts it.

### Example mapping

If the provider expects this **request**:

```json
{
  "token_id": "123",
  "price": 0.42,
  "size": 10,
  "side": "buy",
  "order_type": "limit",
  "client_order_id": "bot:2024-01-01T00:00:00Z",
  "market_id": "BTC-USD",
  "expireAt": 60,
  "nonce": "d3b07384-..."
}
```

Then set:

```
DIRECT_ORDER_SUBMIT_MARKET_FIELD=market_id
DIRECT_ORDER_SUBMIT_EXPIRATION_FIELD=expireAt
DIRECT_ORDER_SUBMIT_EXPIRATION_SEC=60
DIRECT_ORDER_SUBMIT_NONCE_FIELD=nonce
DIRECT_ORDER_SUBMIT_NONCE=d3b07384-...
```

If the **batch response** looks like this:

```json
{ "results": [ { "status": "open" }, { "status": "open" } ] }
```

Then set:

```
DIRECT_ORDER_SUBMIT_BATCH_RESPONSE_KEY=results
```

---

If you share one example request and response from your CLOB provider, you can lock the values
above into your `.env` immediately.
