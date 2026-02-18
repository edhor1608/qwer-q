# QWER-Q Dashboard UX Architecture

> Designed: 2026-02-05

## Design Philosophy

**"The dashboard should feel like looking through a window, not reading a spreadsheet."**

Three principles guide every decision:

1. **Glanceable** -- You should know if something is wrong within 1 second of opening the dashboard. No hunting.
2. **Progressive disclosure** -- Overview first, details on demand. Every click goes deeper, never sideways.
3. **Alive** -- Data is live by default. The dashboard breathes. Numbers tick. Charts flow. Nothing is stale.

Anti-principles (things we will NOT do):
- No configuration wizards -- the broker is configured via CLI/env, not the UI
- No data grids with 20 columns -- if you need a spreadsheet, export to one
- No modals for navigation -- modals are for confirmations only
- No "dashboards of dashboards" -- one screen hierarchy, one URL scheme

---

## Screen Inventory

### 1. Overview (/)

**Purpose:** Answer "Is everything OK?" in under 1 second.

**Primary action:** None. This is a read-only pulse check.

**Information hierarchy:**

```
+------------------------------------------------------------------+
| QWER-Q                                    [search] [?] [uptime]  |
+------------------------------------------------------------------+
|                                                                    |
|  [4 metric cards in a row]                                        |
|  +-----------+ +-----------+ +-----------+ +-----------+          |
|  | Queues    | | Messages  | | Consumers | | Memory    |          |
|  | 12        | | 48.2K/s   | | 34        | | 312/400MB |          |
|  | 2 warning | | +12% 1min | | 3 idle    | | [=====  ] |          |
|  +-----------+ +-----------+ +-----------+ +-----------+          |
|                                                                    |
|  [Throughput sparkline -- last 5 min, publish + consume overlay]  |
|  ~~~/\~~~~/\~~~~~/\/\/\~~~~~/\~~~~/\~~~~                          |
|                                                                    |
|  [Queue health table -- only queues needing attention first]      |
|  Queue            Depth    Rate     Consumers   Status            |
|  orders           12.4K    850/s    3           [!] backing up    |
|  notifications    0        120/s    2           OK                |
|  payments.dlq     47       0/s      0           [!] unprocessed   |
|  ...                                                              |
|                                                                    |
+------------------------------------------------------------------+
```

**Key behaviors:**
- Metric cards use color coding: gray=normal, amber=warning, red=critical
- Warning thresholds: queue depth increasing for >30s, DLQ non-empty, consumers=0 on active queue, memory >80%
- Queue table is sorted by severity (problems first), then alphabetically
- DLQ queues are shown inline with their parent, visually linked
- Throughput sparkline is a 5-minute rolling window, updating every second
- Clicking any metric card navigates to the relevant detail view
- Clicking a queue name goes to Queue Detail

**Empty state:** When no queues exist, show a centered panel with the CLI command to register a schema and publish a first message. Not a tutorial -- just the two commands.

---

### 2. Queues List (/queues)

**Purpose:** Compare all queues side by side. Find the one that needs attention.

**Primary action:** Click a queue name to drill into details.

**Information hierarchy:**

```
+------------------------------------------------------------------+
| Queues (12)                              [filter] [sort] [search] |
+------------------------------------------------------------------+
|                                                                    |
|  [Compact cards, 1 per queue]                                     |
|                                                                    |
|  +--------------------------------------------------------------+ |
|  | orders                                             [3 cons]  | |
|  | Depth: 12,402  |  In-flight: 89  |  Pub: 850/s  Con: 720/s  | |
|  | [depth sparkline ~~~~~~~~]  Oldest: 14s ago   Schema: v3     | |
|  +--------------------------------------------------------------+ |
|  | orders.dlq                                        [0 cons]   | |
|  | Depth: 47      |  In-flight: 0   |  Pub: 2/s    Con: 0/s    | |
|  | [depth sparkline ___/\___]  Oldest: 2h ago    Schema: --     | |
|  +--------------------------------------------------------------+ |
|                                                                    |
|  +--------------------------------------------------------------+ |
|  | notifications                                      [2 cons]  | |
|  | Depth: 0       |  In-flight: 3   |  Pub: 120/s  Con: 120/s  | |
|  | [depth sparkline ______]   Oldest: --         Schema: v1     | |
|  +--------------------------------------------------------------+ |
|                                                                    |
+------------------------------------------------------------------+
```

**Key behaviors:**
- DLQ queues are visually nested under their parent (indented, muted style)
- Each card has an inline depth sparkline (last 5 min, tiny, no axis labels)
- "Oldest message" shows human-readable relative time ("14s ago", "2h ago")
- Sort options: Name, Depth (high first), Publish rate, Consume rate
- Filter: All / Active (depth > 0) / Idle / DLQ only / Problems
- The depth sparkline slope tells the story: flat=healthy, rising=backing up, falling=draining
- Keyboard: `j`/`k` to move between queues, `Enter` to open detail, `/` to search

---

### 3. Queue Detail (/queues/:name)

**Purpose:** Everything about one queue. The control room for a single data flow.

**Primary action:** Purge queue (destructive, requires confirmation).

**Information hierarchy:**

```
+------------------------------------------------------------------+
| <- Queues    orders                          [Purge] [Settings]   |
+------------------------------------------------------------------+
|                                                                    |
|  [Stat row]                                                       |
|  Depth: 12,402  |  In-flight: 89  |  Max: 100,000                |
|  Pub rate: 850/s  |  Con rate: 720/s  |  Ack rate: 718/s         |
|  DLQ: 47 messages  |  Retries: 5 max  |  Policy: dlq             |
|                                                                    |
|  [Tabs: Metrics | Messages | Consumers | Schema | DLQ]           |
|                                                                    |
|  --- Metrics tab (default) ---                                    |
|  [Throughput chart - pub/con/ack lines, 15min window]             |
|  ~~~~/\~~~~~~~/\/\~~~~~                                           |
|                                                                    |
|  [Depth chart - area fill, 15min window]                          |
|  ___/```\___/`````\_____                                          |
|                                                                    |
|  [Latency chart - p50/p99 publish latency, 15min window]         |
|                                                                    |
|  --- Messages tab ---                                             |
|  [Message list - peek at queue contents, read-only]               |
|  See "Message Inspector" below                                    |
|                                                                    |
|  --- Consumers tab ---                                            |
|  [Connected consumers with per-consumer stats]                    |
|  See "Consumers" below                                            |
|                                                                    |
|  --- Schema tab ---                                               |
|  [Current schema definition, version history]                     |
|                                                                    |
|  --- DLQ tab ---                                                  |
|  [DLQ contents with retry/delete actions]                         |
|  Shows badge with count: "DLQ (47)"                               |
|                                                                    |
+------------------------------------------------------------------+
```

**Key behaviors:**
- Stat row updates live. Numbers animate (count up/down, not jump).
- Tabs preserve state when switching. URL updates: `/queues/orders?tab=consumers`
- Charts default to 15-minute window. Time range selector: 5m / 15m / 1h / 6h / 24h
- Charts are synchronized -- hovering one shows a crosshair on all at the same timestamp
- DLQ tab shows a badge with the message count. Badge is red if count > 0.
- Purge button is red, requires typing the queue name to confirm (like GitHub repo deletion)
- The gap between Pub rate and Con rate tells the story: growing gap = problem developing
- Keyboard: `1`-`5` to switch tabs, `t` to cycle time ranges

**DLQ tab specifics:**
- List of DLQ messages with payload preview, original queue, attempt count, failure time
- Bulk actions: Retry All (re-publish to original queue), Delete All
- Per-message: Retry, Delete, Inspect

---

### 4. Message Inspector (/queues/:name/messages/:id)

**Purpose:** Look at one message in detail. Debug why something failed or inspect data.

**Primary action:** Copy payload to clipboard.

**Information hierarchy:**

```
+------------------------------------------------------------------+
| <- orders    Message 01HQXYZ...                      [Copy] [Raw] |
+------------------------------------------------------------------+
|                                                                    |
|  [Metadata panel]                                                 |
|  ID:           01HQXYZ8R3A1B2C3D4E5F6G7H8                       |
|  Published:    2026-02-05 14:32:01.123 (3s ago)                   |
|  Attempt:      2 of 5                                             |
|  Visible at:   2026-02-05 14:32:31.123 (in 27s)                  |
|  State:        In-flight                                          |
|                                                                    |
|  [Headers - collapsible, expanded by default if non-empty]        |
|  correlation_id:  01HQABC...                                      |
|  reply_to:        _reply.10.0.0.1:43210                           |
|  trace_id:        abc123                                          |
|                                                                    |
|  [Payload - decoded via schema]                                   |
|  {                                                                |
|    "order_id": "ORD-12345",                                      |
|    "customer": {                                                  |
|      "id": "USR-789",                                            |
|      "email": "user@example.com"                                 |
|    },                                                             |
|    "items": [                                                     |
|      { "sku": "WIDGET-A", "qty": 3, "price": 29.99 }            |
|    ],                                                             |
|    "total": 89.97                                                 |
|  }                                                                |
|                                                                    |
|  [Raw toggle shows hex dump of the protobuf bytes]               |
|                                                                    |
+------------------------------------------------------------------+
```

**Key behaviors:**
- Payload is decoded using the queue's registered schema (protobuf -> JSON representation)
- If schema decoding fails, show raw bytes with a warning banner
- Raw/Decoded toggle in top-right corner
- Copy button copies the decoded JSON payload (or raw bytes in raw mode)
- Relative timestamps shown alongside absolute timestamps ("3s ago")
- For DLQ messages: show additional "Original queue" and "Failure reason" fields
- No editing. Messages are immutable. This is read-only inspection.
- Keyboard: `c` to copy payload, `r` to toggle raw mode

---

### 5. Consumers (/consumers)

**Purpose:** See who is connected and how they're performing.

**Primary action:** None (read-only). Possible future: disconnect a consumer.

**Information hierarchy:**

```
+------------------------------------------------------------------+
| Consumers (34)                                          [search]  |
+------------------------------------------------------------------+
|                                                                    |
|  [Grouped by queue]                                               |
|                                                                    |
|  orders (3 consumers)                                             |
|  +--------------------------------------------------------------+ |
|  | 10.0.0.5:43210                                               | |
|  | Connected: 2h ago  |  Processed: 12,450  |  Ack rate: 98.2%  | |
|  | Visibility timeout: 30s  |  Prefetch: 1  |  Last ack: 1s ago | |
|  +--------------------------------------------------------------+ |
|  | 10.0.0.6:43211                                               | |
|  | Connected: 2h ago  |  Processed: 12,380  |  Ack rate: 97.9%  | |
|  | Visibility timeout: 30s  |  Prefetch: 1  |  Last ack: 2s ago | |
|  +--------------------------------------------------------------+ |
|  | 10.0.0.7:43212                                               | |
|  | Connected: 5m ago  |  Processed: 310     |  Ack rate: 99.1%  | |
|  | Visibility timeout: 30s  |  Prefetch: 1  |  Last ack: 0s ago | |
|  +--------------------------------------------------------------+ |
|                                                                    |
|  notifications (2 consumers)                                      |
|  ...                                                              |
|                                                                    |
+------------------------------------------------------------------+
```

**Key behaviors:**
- Grouped by queue. Consumers without a queue subscription shown in "Unsubscribed" section.
- "Ack rate" = acks / (acks + nacks) as a percentage. This is the consumer's reliability score.
- "Last ack" gives instant feedback on whether the consumer is alive and processing.
- Stale consumers (last ack > 60s on an active queue) highlighted in amber.
- Round-robin position indicator: subtle marker showing which consumer gets the next message.
- Click a consumer to see its connection detail (future: full activity timeline).
- Keyboard: `j`/`k` navigation, `g` to jump to a queue group.

**Note on data availability:** The current codebase tracks consumers as channels per queue (`[]*Consumer` in `queue.go`). The dashboard REST API will need to enrich this with connection metadata (IP, connect time, message counts) from the `connState` in `server.go`. This requires adding per-connection stats tracking -- see Task #20.

---

### 6. Schemas (/schemas)

**Purpose:** Browse the schema registry. Understand what types of messages flow through the system.

**Primary action:** None (read-only in v1). Future: register schema via upload.

**Information hierarchy:**

```
+------------------------------------------------------------------+
| Schemas (8)                                             [search]  |
+------------------------------------------------------------------+
|                                                                    |
|  [Schema cards]                                                   |
|                                                                    |
|  +--------------------------------------------------------------+ |
|  | orders                                              v3       | |
|  | Message type: myapp.OrderEvent                               | |
|  | Registered: 2026-01-15  |  Updated: 2026-02-01               | |
|  | Fields: order_id, customer, items, total, created_at         | |
|  +--------------------------------------------------------------+ |
|                                                                    |
|  +--------------------------------------------------------------+ |
|  | notifications                                       v1       | |
|  | Message type: myapp.Notification                             | |
|  | Registered: 2026-01-20  |  Updated: --                       | |
|  | Fields: user_id, channel, template, params                   | |
|  +--------------------------------------------------------------+ |
|                                                                    |
+------------------------------------------------------------------+
```

**Schema detail view (/schemas/:queue):**

```
+------------------------------------------------------------------+
| <- Schemas    orders                                              |
+------------------------------------------------------------------+
|                                                                    |
|  Message type: myapp.OrderEvent                                   |
|  Version: 3  |  Registered: 2026-01-15  |  Updated: 2026-02-01   |
|                                                                    |
|  [Proto definition - syntax highlighted]                          |
|  message OrderEvent {                                             |
|    string order_id = 1;                                           |
|    Customer customer = 2;                                         |
|    repeated Item items = 3;                                       |
|    double total = 4;                                              |
|    google.protobuf.Timestamp created_at = 5;                     |
|  }                                                                |
|                                                                    |
|  [Version history]                                                |
|  v3  2026-02-01  Added created_at field                           |
|  v2  2026-01-20  Added total field                                |
|  v1  2026-01-15  Initial schema                                   |
|                                                                    |
+------------------------------------------------------------------+
```

**Key behaviors:**
- Schema cards show a condensed list of top-level field names (up to 6, then "...and 4 more")
- Proto definition is reconstructed from the stored `FileDescriptorSet` and syntax-highlighted
- Version history is a compact timeline. Click a version to see its definition.
- Compatibility status shown per version transition (backward compatible / breaking)
- Keyboard: `j`/`k` navigation, `Enter` to expand

---

### 7. Settings (/settings)

**Purpose:** View broker configuration. Not edit it (config comes from CLI/env).

**Primary action:** None. This is informational.

**Information hierarchy:**

```
+------------------------------------------------------------------+
| Settings                                                          |
+------------------------------------------------------------------+
|                                                                    |
|  Broker Configuration (read-only)                                 |
|  These values are set via CLI flags or environment variables.     |
|                                                                    |
|  Listen address:      :9876                                       |
|  Storage backend:     BadgerDB                                    |
|  Data directory:      /data                                       |
|  Sync interval:       100ms                                       |
|  Memory limit:        400 MB                                      |
|  Default max retries: 5                                           |
|  Default failure policy: dlq                                      |
|  Idempotency TTL:     5m                                          |
|  Max queue size:      10,000                                      |
|                                                                    |
|  Runtime                                                          |
|  Go version:          1.22.1                                      |
|  Uptime:              3d 14h 22m                                  |
|  Goroutines:          148                                         |
|  OS/Arch:             linux/arm64                                  |
|                                                                    |
|  Storage                                                          |
|  DB size on disk:     1.2 GB                                      |
|  Total messages stored: 342,109                                   |
|                                                                    |
+------------------------------------------------------------------+
```

**Key behaviors:**
- Explicitly labeled "read-only" to set expectations
- Each config value shows the corresponding CLI flag/env var on hover
- Runtime stats update live (uptime ticks, goroutine count refreshes)
- No auth configuration in v1 (future: auth tokens, mTLS cert management)

---

## URL Scheme

```
/                           Overview dashboard
/queues                     All queues
/queues/:name               Queue detail (default: metrics tab)
/queues/:name?tab=messages  Queue detail, messages tab
/queues/:name?tab=consumers Queue detail, consumers tab
/queues/:name?tab=schema    Queue detail, schema tab
/queues/:name?tab=dlq       Queue detail, DLQ tab
/queues/:name/messages/:id  Message inspector
/consumers                  All consumers
/schemas                    All schemas
/schemas/:queue             Schema detail
/settings                   Broker config
```

Clean, guessable, bookmarkable. No UUIDs in URLs -- queue names are the identifiers.

---

## Navigation

### Sidebar (persistent, collapsible)

```
[Q]  QWER-Q            [<<]

     Overview            /
     Queues (12)         /queues
     Consumers (34)      /consumers
     Schemas (8)         /schemas
     Settings            /settings
```

- Counts update live
- Active page highlighted
- Sidebar collapses to icons on small screens or via toggle
- No nested navigation. Five items. That's it.

### Breadcrumbs (contextual, top of content area)

Only shown when depth > 1:
- Queue detail: `Queues > orders`
- Message inspector: `Queues > orders > 01HQXYZ...`
- Schema detail: `Schemas > orders`

Breadcrumbs are clickable and serve as the "back" mechanism. No back button needed.

---

## Keyboard Shortcuts

### Global
| Key | Action |
|-----|--------|
| `1` | Go to Overview |
| `2` | Go to Queues |
| `3` | Go to Consumers |
| `4` | Go to Schemas |
| `5` | Go to Settings |
| `/` | Focus search |
| `?` | Toggle shortcut help overlay |
| `Esc` | Close overlay / deselect / back |

### List views (Queues, Consumers, Schemas)
| Key | Action |
|-----|--------|
| `j` / `k` | Move selection down / up |
| `Enter` | Open selected item |
| `f` | Toggle filter panel |
| `s` | Cycle sort order |
| `g` + `g` | Jump to top |
| `G` | Jump to bottom |

### Queue Detail
| Key | Action |
|-----|--------|
| `1`-`5` | Switch tabs (Metrics / Messages / Consumers / Schema / DLQ) |
| `t` | Cycle time range (5m / 15m / 1h / 6h / 24h) |
| `r` | Refresh data |
| `Backspace` | Back to Queues list |

### Message Inspector
| Key | Action |
|-----|--------|
| `c` | Copy payload |
| `r` | Toggle raw / decoded view |
| `j` / `k` | Next / previous message |
| `Backspace` | Back to queue |

Shortcut help (`?`) shows a floating panel -- not a page. Dismisses on `Esc` or `?` again.

---

## Real-Time Update Strategy

### Architecture

```
Browser <-- WebSocket --> Go broker (new /ws endpoint)
                |
                +-- Server-Sent Events fallback (if WS unavailable)
```

### Update channels

| Data | Method | Frequency |
|------|--------|-----------|
| Queue depths & rates | WebSocket push | Every 1s |
| Consumer list changes | WebSocket push | On connect/disconnect |
| Metric card values | WebSocket push | Every 1s |
| Chart data points | WebSocket push | Every 1s (append to client-side buffer) |
| Message list (peek) | HTTP poll | On tab open + manual refresh |
| Schema data | HTTP fetch | On page load (rarely changes) |
| Settings/config | HTTP fetch | On page load (static) |

### WebSocket protocol

Simple JSON messages:

```json
{
  "type": "queue_stats",
  "data": {
    "orders": {
      "depth": 12402,
      "in_flight": 89,
      "pub_rate": 850.2,
      "con_rate": 720.1,
      "ack_rate": 718.4,
      "nack_rate": 1.7
    }
  }
}
```

```json
{
  "type": "system_stats",
  "data": {
    "memory_used": 327155712,
    "memory_limit": 419430400,
    "goroutines": 148,
    "uptime_seconds": 312132,
    "total_connections": 34
  }
}
```

```json
{
  "type": "consumer_change",
  "data": {
    "queue": "orders",
    "event": "connected",
    "consumer_addr": "10.0.0.5:43210"
  }
}
```

### Client-side behavior

- Charts maintain a rolling buffer (configurable window: 5m to 24h)
- On reconnect: full state fetch via HTTP, then resume WebSocket stream
- Connection status indicator in header: green dot = live, amber = reconnecting, red = disconnected
- Stale data detection: if no WebSocket message in 5s, show "Reconnecting..." banner
- No optimistic updates. Dashboard is read-only. Display what the server tells you.

### Rate calculation

Rates (pub/s, con/s) are computed server-side over a 10-second sliding window. The server sends the computed rate, not raw counts. This keeps the client simple and consistent across multiple browser tabs.

---

## User Flows

### Flow 1: "Is my system healthy?" (Daily check)

1. Open dashboard (`/`)
2. Glance at metric cards -- all gray = done. Takes 1 second.
3. If amber/red: scan queue table, sorted by severity
4. Click the problem queue -> Queue Detail
5. Check the Metrics tab charts for trends
6. If DLQ badge is red, click DLQ tab to inspect failures

**Design goal:** Steps 1-2 should be sufficient 90% of the time.

### Flow 2: "Why are messages piling up?" (Incident response)

1. See amber/red Depth card on Overview
2. Click the queue with rising depth
3. Queue Detail -> Metrics tab: compare pub rate vs con rate
4. If con rate is low: check Consumers tab -- are consumers connected? Are they slow?
5. If consumers look healthy: check Messages tab -- are messages failing? Check attempt counts.
6. If high attempt counts: click a message to inspect payload, check for schema issues

**Design goal:** Root cause identified within 4 clicks.

### Flow 3: "What's in this DLQ?" (Debugging failed messages)

1. Overview shows DLQ queues with non-zero depth
2. Click the DLQ queue (or parent queue -> DLQ tab)
3. Scan message list: attempt count, failure time, payload preview
4. Click a message to inspect full payload and headers
5. Identify the issue. Options: Retry (re-publish), Delete, or Retry All

**Design goal:** See failed message contents within 3 clicks.

### Flow 4: "What schema does this queue use?" (Development)

1. Navigate to Schemas
2. Find queue by name (search if many)
3. Click to see proto definition, version, field list
4. Copy what you need

**Design goal:** Schema info within 2 clicks.

---

## Interaction Patterns

### Numbers animate, never jump
When a value changes (depth goes from 12,000 to 12,100), it counts up smoothly over 300ms. This creates a sense of liveness without being distracting.

### Color communicates severity, not decoration
- **Gray/default:** Normal. Nothing to see.
- **Amber/yellow:** Warning. Trending in a bad direction but not critical.
- **Red:** Critical. Action needed now.
- **Green:** Only used for the connection status dot and rate-of-change indicators (positive change).

No other colors. No blue badges, purple tags, or teal accents for informational purposes. The visual palette is intentionally sparse so that amber and red scream.

### Sparklines over numbers
Wherever a number changes over time (depth, rate), prefer a tiny sparkline next to the number. A number tells you where you are. A sparkline tells you where you're going. A depth of 5,000 might be fine (steady) or terrible (rising exponentially). The sparkline disambiguates instantly.

### Empty states are helpful, not cute
No illustrations, mascots, or "Nothing here yet!" messages. Empty states show:
- What would be here
- The CLI command to make it happen

Example for empty queues list:
```
No queues yet.

Register a schema and publish your first message:
  qwer-q schema register --queue orders --proto ./order.proto --type myapp.Order
  qwer-q publish orders '{"order_id": "1"}'
```

### Confirmations for destructive actions only
The only destructive action in v1 is "Purge queue." This gets the full treatment:
- Red button
- Confirmation dialog
- Type queue name to confirm
- 3-second cooldown after confirmation before execution

Non-destructive actions (retry DLQ message, copy payload) execute immediately. No "Are you sure?" for read operations.

### Timestamps are dual-format
Always show both relative and absolute:
- `3s ago` (14:32:01.123)
- `2h ago` (12:35:17.891)

Relative is primary (larger font), absolute is secondary (smaller, muted). Relative timestamps update live (every second for recent, every minute for older).

---

## Search

Global search (`/` shortcut) searches across:
- Queue names
- Schema message types
- Consumer addresses
- Message IDs (exact match)

Results are grouped by type. Selecting a result navigates to its detail view.

Search is client-side for v1 (the dataset is small -- dozens of queues, not thousands). Server-side search can be added when needed.

---

## Responsive Behavior

The dashboard is primarily for desktop use (monitoring on a wall screen or developer workstation). However:

- **< 768px:** Sidebar collapses to bottom tab bar (5 icons). Metric cards stack 2x2. Charts are full-width.
- **768-1024px:** Sidebar collapses to icon-only rail. Content takes remaining width.
- **> 1024px:** Full sidebar with labels. Content in remaining space.

Charts resize fluidly. Tables get horizontal scroll. No breakpoint where information is removed -- it just reflows.

---

## Data Requirements for REST API

The dashboard needs the following endpoints (input for Task #20):

```
GET  /api/v1/overview              System-level stats (queue count, total consumers, memory)
GET  /api/v1/queues                List all queues with stats
GET  /api/v1/queues/:name          Single queue detail
GET  /api/v1/queues/:name/messages Peek at messages (paginated, non-destructive)
GET  /api/v1/queues/:name/messages/:id  Single message detail
POST /api/v1/queues/:name/purge    Purge queue (destructive)
POST /api/v1/queues/:name/dlq/retry     Retry DLQ messages
DELETE /api/v1/queues/:name/dlq/:id      Delete DLQ message
GET  /api/v1/consumers             List all consumers with stats
GET  /api/v1/schemas               List all schemas
GET  /api/v1/schemas/:queue        Schema detail with version history
GET  /api/v1/settings              Broker configuration
WS   /api/v1/ws                    WebSocket for real-time updates
```

### Data not currently tracked (requires broker changes):
- Per-consumer message counts (processed, acked, nacked)
- Per-consumer connection time
- Per-consumer last activity timestamp
- Rate calculations (pub/s, con/s per queue) -- needs sliding window counter
- Message peek without consuming (read from storage without dequeue)
- Schema version history (currently only latest version stored)

These gaps are noted here so the API task (#20) can account for them.
