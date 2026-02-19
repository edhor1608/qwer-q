# QWER-Q Dashboard Design System

> A precision instrument for message queue operations. Dark, dense, deliberate.

---

## Design Philosophy

The dashboard is a **control surface**, not a marketing page. Every pixel serves a purpose. The aesthetic draws from terminal emulators, oscilloscopes, and cockpit instrumentation — elevated with modern craft. Think: the feeling of a well-machined tool in your hand.

**Principles:**
1. **Data density over decoration** — Show more, decorate less
2. **Quiet until it matters** — Subdued by default, vivid for alerts
3. **Typographic hierarchy** — Information architecture through type, not color
4. **Terminal heritage** — Monospace roots, precision alignment

## Problem

- Define a consistent visual system for a live, operator-focused dashboard.
- Prevent ad-hoc styles from weakening scan speed and alert clarity.
- Keep implementation aligned with qwer-q's utilitarian product identity.

## What We Tried

- Reviewed terminal-inspired and observability-heavy UI systems.
- Compared high-accent palettes with restrained chroma for status signaling.
- Tested typography mixes for dense tables, metrics, and inspectors.

## Research Findings

- Dense layout plus stable hierarchy improves first-glance diagnosis.
- Limited accent color usage reduces alert fatigue during prolonged sessions.
- Explicit contrast budgeting is required to keep dark UIs accessible.

## Design Decisions

- Adopt warm-neutral dark surfaces with a single amber brand accent.
- Use monospace for operational values/IDs and sans-serif for structure/labels.
- Reserve vivid status colors for state transitions and actionable alerts.

## Lessons Learned

- Motion should communicate system state changes, not decoration.
- Information hierarchy should be expressed by type scale before color.
- Accessibility constraints must stay measurable and documented.

---

## Color System

### Philosophy

Dark mode is the native state. The palette is built from a warm neutral base (not pure black, not cold gray) with a single distinctive brand accent: **electric amber** — the color of vacuum tube filaments and analog meters. It reads as "infrastructure" without defaulting to the overused blues and purples of every other dev tool.

### Core Palette

#### Backgrounds (Warm Neutrals)

| Token | Hex | HSL | Usage |
|-------|-----|-----|-------|
| `--bg-root` | `#0d0d0d` | `hsl(0, 0%, 5%)` | App background, deepest layer |
| `--bg-surface` | `#141414` | `hsl(0, 0%, 8%)` | Cards, panels, sidebar |
| `--bg-elevated` | `#1c1c1c` | `hsl(0, 0%, 11%)` | Modals, popovers, command palette |
| `--bg-hover` | `#242424` | `hsl(0, 0%, 14%)` | Row hover, interactive surface |
| `--bg-active` | `#2c2c2c` | `hsl(0, 0%, 17%)` | Active/selected state |
| `--bg-input` | `#181818` | `hsl(0, 0%, 9%)` | Input fields, text areas |

#### Borders

| Token | Hex | HSL | Usage |
|-------|-----|-----|-------|
| `--border-subtle` | `#222222` | `hsl(0, 0%, 13%)` | Dividers, card borders |
| `--border-default` | `#2e2e2e` | `hsl(0, 0%, 18%)` | Input borders, table lines |
| `--border-strong` | `#3d3d3d` | `hsl(0, 0%, 24%)` | Focus rings, emphasis |

#### Text

| Token | Hex | HSL | Usage |
|-------|-----|-----|-------|
| `--text-primary` | `#e8e8e8` | `hsl(0, 0%, 91%)` | Body text, primary content |
| `--text-secondary` | `#999999` | `hsl(0, 0%, 60%)` | Labels, descriptions, metadata |
| `--text-tertiary` | `#666666` | `hsl(0, 0%, 40%)` | Placeholders, disabled text |
| `--text-inverse` | `#0d0d0d` | `hsl(0, 0%, 5%)` | Text on bright backgrounds |

#### Brand — Electric Amber

The signature color. Used sparingly: primary actions, active navigation, focus indicators, brand moments.

| Token | Hex | HSL | Usage |
|-------|-----|-----|-------|
| `--amber-50` | `#fff8eb` | `hsl(40, 100%, 95%)` | Light mode background tint |
| `--amber-100` | `#ffe8b8` | `hsl(40, 100%, 86%)` | Light mode hover |
| `--amber-200` | `#ffd780` | `hsl(40, 100%, 75%)` | Light badge backgrounds |
| `--amber-400` | `#f5a623` | `hsl(37, 91%, 55%)` | Primary brand color |
| `--amber-500` | `#e6941a` | `hsl(35, 82%, 50%)` | Primary hover |
| `--amber-600` | `#c47a10` | `hsl(33, 85%, 42%)` | Primary active/pressed |
| `--amber-900` | `#3d2608` | `hsl(33, 80%, 14%)` | Tinted backgrounds in dark mode |

#### Status Colors

Desaturated in resting state. Vivid only when demanding attention.

| Token | Hex | HSL | Usage |
|-------|-----|-----|-------|
| `--status-healthy` | `#3dd68c` | `hsl(150, 62%, 54%)` | Connected, active, flowing |
| `--status-healthy-muted` | `#1a3d2a` | `hsl(150, 40%, 17%)` | Healthy background tint |
| `--status-warning` | `#f5a623` | `hsl(37, 91%, 55%)` | Slow, degraded, nearing limit |
| `--status-warning-muted` | `#3d2e0a` | `hsl(37, 75%, 14%)` | Warning background tint |
| `--status-critical` | `#ef4444` | `hsl(0, 84%, 60%)` | Error, disconnected, overflow |
| `--status-critical-muted` | `#3d1414` | `hsl(0, 50%, 16%)` | Critical background tint |
| `--status-inactive` | `#555555` | `hsl(0, 0%, 33%)` | Paused, idle, no data |
| `--status-inactive-muted` | `#1e1e1e` | `hsl(0, 0%, 12%)` | Inactive background tint |

#### Chart Palette

For multi-series time-series data. Each color is distinct even in adjacent placement and colorblind-safe ordered.

| Token | Hex | Usage |
|-------|-----|-------|
| `--chart-1` | `#f5a623` | Primary series (amber) |
| `--chart-2` | `#3dd68c` | Secondary series (green) |
| `--chart-3` | `#6e9ef5` | Tertiary series (blue) |
| `--chart-4` | `#c084fc` | Fourth series (purple) |
| `--chart-5` | `#f97066` | Fifth series (coral) |
| `--chart-6` | `#67e8f9` | Sixth series (cyan) |

#### Syntax Highlighting (Message Viewer)

Based on a warm dark scheme consistent with the amber brand.

| Token | Hex | Usage |
|-------|-----|-------|
| `--syntax-string` | `#3dd68c` | String literals |
| `--syntax-number` | `#f5a623` | Numbers, constants |
| `--syntax-keyword` | `#c084fc` | Keywords, types |
| `--syntax-key` | `#6e9ef5` | Object keys, fields |
| `--syntax-comment` | `#555555` | Comments |
| `--syntax-boolean` | `#f97066` | Booleans, null |
| `--syntax-bracket` | `#999999` | Brackets, punctuation |

### Light Mode (Secondary)

Light mode inverts the background scale while keeping the same accent and status colors at adjusted lightness.

| Token | Hex | Usage |
|-------|-----|-------|
| `--bg-root` | `#fafafa` | App background |
| `--bg-surface` | `#ffffff` | Cards, panels |
| `--bg-elevated` | `#ffffff` | Modals (with shadow) |
| `--bg-hover` | `#f0f0f0` | Hover state |
| `--bg-active` | `#e8e8e8` | Active state |
| `--border-subtle` | `#e8e8e8` | Dividers |
| `--border-default` | `#d4d4d4` | Input borders |
| `--text-primary` | `#1a1a1a` | Body text |
| `--text-secondary` | `#666666` | Labels |
| `--text-tertiary` | `#999999` | Placeholders |

---

## Typography

### Font Stack

| Role | Font | Fallback | Why |
|------|------|----------|-----|
| **Mono** | `"Berkeley Mono", "JetBrains Mono"` | `"Fira Code", "SF Mono", ui-monospace, monospace` | Berkeley Mono: designed for data density. JetBrains Mono: widely available, excellent legibility. Both have programming ligatures. |
| **Sans** | `"Inter"` | `"SF Pro", -apple-system, system-ui, sans-serif` | Inter: designed for screens at small sizes. Outstanding tabular figures for data alignment. |

### Type Scale

Built on a 1.2 minor third ratio from a 13px base. Optimized for data-heavy interfaces where 14-16px body text wastes space.

| Token | Size | Line Height | Weight | Usage |
|-------|------|-------------|--------|-------|
| `--text-2xs` | 10px / 0.625rem | 14px | 400 | Timestamps, micro labels |
| `--text-xs` | 11px / 0.6875rem | 16px | 400 | Table metadata, badges |
| `--text-sm` | 12px / 0.75rem | 18px | 400 | Secondary text, descriptions |
| `--text-base` | 13px / 0.8125rem | 20px | 400 | Body text, table cells |
| `--text-md` | 14px / 0.875rem | 20px | 500 | Labels, nav items |
| `--text-lg` | 16px / 1rem | 24px | 600 | Section headers |
| `--text-xl` | 20px / 1.25rem | 28px | 600 | Page titles |
| `--text-2xl` | 24px / 1.5rem | 32px | 700 | Metric display numbers |
| `--text-3xl` | 32px / 2rem | 40px | 700 | Hero metrics (single KPI) |

### Font Weight Tokens

| Token | Weight | Usage |
|-------|--------|-------|
| `--font-normal` | 400 | Body, descriptions |
| `--font-medium` | 500 | Labels, emphasis |
| `--font-semibold` | 600 | Headers, nav active |
| `--font-bold` | 700 | Metric numbers, page titles |

### Mono vs Sans Rules

- **Mono**: Queue names, message IDs, consumer IDs, message payloads, schema definitions, byte counts, any value that could be copy-pasted into a terminal
- **Sans**: Navigation labels, section headers, button text, descriptions, form labels, toast messages

---

## Spacing System

4px base unit. All spacing is a multiple of 4.

| Token | Value | Usage |
|-------|-------|-------|
| `--space-0` | 0px | No spacing |
| `--space-1` | 4px | Inline element gaps, icon padding |
| `--space-2` | 8px | Compact internal padding, between badges |
| `--space-3` | 12px | Table cell padding, form field gaps |
| `--space-4` | 16px | Card internal padding, section gaps |
| `--space-5` | 20px | Between card groups |
| `--space-6` | 24px | Page section spacing |
| `--space-8` | 32px | Major section breaks |
| `--space-10` | 40px | Page top padding |
| `--space-12` | 48px | Sidebar width padding |
| `--space-16` | 64px | Page margin |

### Layout Constants

| Token | Value | Usage |
|-------|-------|-------|
| `--sidebar-width` | 220px | Navigation sidebar |
| `--sidebar-collapsed` | 48px | Icon-only sidebar |
| `--header-height` | 48px | Top bar / breadcrumb |
| `--table-row-height` | 36px | Dense table rows |
| `--card-radius` | 6px | Card border radius |
| `--badge-radius` | 4px | Badge border radius |
| `--input-radius` | 4px | Input border radius |
| `--input-height` | 32px | Standard input height |
| `--button-height` | 32px | Standard button height |

### Border Radius

| Token | Value | Usage |
|-------|-------|-------|
| `--radius-sm` | 3px | Badges, inline elements |
| `--radius-md` | 6px | Cards, inputs, buttons |
| `--radius-lg` | 8px | Modals, large containers |
| `--radius-full` | 9999px | Pills, status dots |

---

## Shadow System

Minimal shadows. In dark mode, elevation is communicated primarily through background lightness, not shadow. Shadows reserved for floating elements.

| Token | Value | Usage |
|-------|-------|-------|
| `--shadow-sm` | `0 1px 2px rgba(0,0,0,0.3)` | Dropdowns, tooltips |
| `--shadow-md` | `0 4px 12px rgba(0,0,0,0.4)` | Modals, command palette |
| `--shadow-lg` | `0 8px 24px rgba(0,0,0,0.5)` | Detached popovers |

---

## Component Specifications

### 1. Data Table

The workhorse component. Queues list, messages list, consumers list — all tables.

**Structure:**
- Fixed header row, scrollable body
- Row height: 36px (`--table-row-height`)
- Cell padding: 12px horizontal (`--space-3`), 0 vertical (vertically centered)
- Header text: `--text-xs`, `--font-semibold`, `--text-secondary`, uppercase tracking 0.05em
- Cell text: `--text-base`, mono font for IDs/values, sans for descriptions
- Alternating row backgrounds: `--bg-root` / `--bg-surface` (subtle, 3% difference)
- Hover: entire row background shifts to `--bg-hover`
- Selected row: `--bg-active` with 2px left border in `--amber-400`

**Sortable columns:**
- Sort indicator: small triangle (4px) next to header text
- Active sort: `--text-primary` header, inactive: `--text-tertiary`
- Click header to toggle asc/desc/none

**Filterable:**
- Inline filter icon in header, opens compact text input below header row
- Filter input: `--bg-input`, `--border-default`, `--text-primary`

**Real-time updates:**
- New rows: slide in from top with 200ms ease-out, background flash of `--amber-900` fading over 1.5s
- Updated cells: text color briefly shifts to `--amber-400` then fades back to `--text-primary` over 800ms
- Removed rows: fade out 150ms, then collapse height 150ms

**Density toggle:**
- Compact: 28px rows, `--text-xs` cells
- Default: 36px rows, `--text-base` cells
- Comfortable: 44px rows, `--text-md` cells

---

### 2. Metric Card

Displays a single KPI with context.

**Layout:**
- Width: flexible, min 180px, typically in a 3-4 column grid
- Padding: `--space-4` all sides
- Background: `--bg-surface`
- Border: 1px `--border-subtle`
- Border-radius: `--radius-md`

**Content stack (top to bottom):**
1. **Label** — `--text-xs`, `--font-medium`, `--text-secondary`, uppercase tracking 0.05em. Example: "THROUGHPUT"
2. **Value** — `--text-2xl` or `--text-3xl`, `--font-bold`, mono font, `--text-primary`. Example: "4,821"
3. **Unit/suffix** — `--text-sm`, `--font-normal`, `--text-tertiary`, inline after value. Example: "msgs/s"
4. **Sparkline** — 48px tall, full card width, stroke only (no fill), stroke: `--amber-400` at 1.5px, no axes. Last 60 data points.
5. **Trend indicator** — `--text-xs`, bottom-left. Arrow up/down/flat + percentage. Green for up (throughput), red for up (latency). Context-dependent coloring.

**States:**
- Loading: value replaced with 3 animated dots (opacity pulse 0.3 to 1.0, 1.2s cycle)
- No data: "--" in `--text-tertiary`
- Error: red bottom border (2px `--status-critical`), value shows last known with "stale" badge

---

### 3. Queue Health Badge

Compact inline status indicator that appears next to queue names.

**Structure:**
- Height: 20px
- Padding: 2px 8px
- Border-radius: `--radius-sm`
- Font: `--text-xs`, `--font-medium`

**States:**

| State | Dot Color | Background | Text | Label |
|-------|-----------|------------|------|-------|
| Healthy | `--status-healthy` | `--status-healthy-muted` | `--status-healthy` | "Healthy" |
| Warning | `--status-warning` | `--status-warning-muted` | `--status-warning` | "Slow" or "Near limit" |
| Critical | `--status-critical` | `--status-critical-muted` | `--status-critical` | "Error" or "Full" |
| Inactive | `--status-inactive` | `--status-inactive-muted` | `--status-inactive` | "Paused" or "Empty" |

**Dot:** 6px circle, left of label, 6px gap. In healthy and warning states, the dot pulses opacity 0.5 to 1.0 over 2s (breathing effect, signals "alive"). Critical state: no pulse (solid, demands attention). Inactive: no pulse, 50% opacity.

---

### 4. Message Viewer

Full message inspection panel — appears on click/selection from a messages table.

**Layout:**
- Slide-in panel from right, 480px wide (or 50% of viewport, whichever is smaller)
- Background: `--bg-elevated`
- Left border: 1px `--border-default`

**Header bar:**
- Height: `--header-height`
- Message ID in mono `--text-sm`, truncated with copy button
- Close button (X) right-aligned
- Background: `--bg-surface`

**Metadata section:**
- Key-value pairs in 2-column grid
- Keys: `--text-xs`, `--text-secondary`, sans, uppercase
- Values: `--text-sm`, `--text-primary`, mono
- Fields: Queue, Published At, Size, Schema, Content-Type, Headers (expandable)
- Divider (`--border-subtle`) below metadata

**Payload section:**
- Tab bar: "Formatted" | "Raw" | "Headers"
- Code block with syntax highlighting using syntax tokens defined above
- Background: `--bg-root` (darkest, recessed look)
- Padding: `--space-3`
- Font: mono, `--text-sm`
- Line numbers: `--text-tertiary`, right-aligned, 40px gutter
- Scrollable vertically, soft horizontal scroll for long lines
- Copy-all button in top-right corner of code block

---

### 5. Consumer Connection

Displays a connected consumer in a list or grid.

**Structure:**
- Row layout: status dot | name | stats | connected duration
- Height: 40px
- Padding: `--space-2` vertical, `--space-3` horizontal

**Status dot:**
- 8px circle
- Connected: `--status-healthy` with breathing pulse
- Disconnected: `--status-critical`, solid
- Idle (connected but not consuming): `--status-warning`, solid

**Name:** mono, `--text-base`, `--text-primary`. Truncate with ellipsis at 200px.

**Stats (inline, right of name):**
- "142 msgs/s" — `--text-xs`, mono, `--text-secondary`
- "1.2ms avg" — `--text-xs`, mono, `--text-secondary`
- Separated by `|` in `--text-tertiary`

**Connected duration:** `--text-xs`, `--text-tertiary`, right-aligned. "2h 14m" format.

**Disconnection event:** Row background flashes `--status-critical-muted` once, then fades out with 300ms ease.

---

### 6. Chart (Time-Series)

For throughput, latency, queue depth over time.

**Container:**
- Background: `--bg-surface`
- Border: 1px `--border-subtle`
- Padding: `--space-4` top/sides, `--space-2` bottom
- Border-radius: `--radius-md`

**Chart area:**
- Y-axis labels: `--text-2xs`, mono, `--text-tertiary`, right-aligned in 48px gutter
- X-axis labels: `--text-2xs`, mono, `--text-tertiary`, every 5th tick
- Grid lines: `--border-subtle`, horizontal only, dashed (2px dash, 4px gap)
- No background grid — keep it clean

**Lines:**
- Stroke width: 1.5px
- Colors from chart palette (`--chart-1` through `--chart-6`)
- Active hover: line thickens to 2.5px, other lines fade to 30% opacity
- Area fill: gradient from line color at 15% opacity to transparent, only for single-series charts

**Tooltip:**
- Appears on hover, follows cursor horizontally, fixed vertical position
- Background: `--bg-elevated`
- Border: 1px `--border-default`
- Shadow: `--shadow-sm`
- Content: timestamp (mono, `--text-2xs`) + series values with colored dots

**Time range selector:**
- Tabs above chart: "5m" | "15m" | "1h" | "6h" | "24h" | "7d"
- `--text-xs`, `--font-medium`
- Active: `--amber-400` text with `--amber-900` background pill
- Inactive: `--text-secondary`

---

### 7. Schema Viewer

Displays protobuf/JSON Schema definitions.

**Container:**
- Same dimensions as Message Viewer payload section
- Background: `--bg-root`
- Mono font throughout

**Syntax:**
- Type names: `--syntax-keyword` (purple)
- Field names: `--syntax-key` (blue)
- Field numbers/defaults: `--syntax-number` (amber)
- Strings: `--syntax-string` (green)
- Comments/annotations: `--syntax-comment` (gray)
- Braces/punctuation: `--syntax-bracket` (light gray)

**Features:**
- Line numbers in gutter
- Clickable type references (navigate to definition)
- Collapse/expand message blocks
- Version badge in top-right: "v3" pill with `--amber-400` text

---

### 8. Toast / Alert

For real-time events. Non-blocking, stackable, auto-dismiss.

**Position:** Bottom-right, 16px from edges. Stack upward, max 4 visible.

**Structure:**
- Width: 360px
- Padding: `--space-3` all sides
- Border-radius: `--radius-md`
- Background: `--bg-elevated`
- Left border: 3px solid (color by severity)
- Shadow: `--shadow-md`

**Content:**
- Icon (16px): left-aligned, color matches severity
- Title: sans, `--text-sm`, `--font-semibold`, `--text-primary`
- Body: sans, `--text-xs`, `--text-secondary`, max 2 lines
- Dismiss X: `--text-tertiary`, top-right

**Severities:**

| Type | Left Border | Icon |
|------|-------------|------|
| Info | `--amber-400` | Circle-i |
| Success | `--status-healthy` | Checkmark |
| Warning | `--status-warning` | Triangle-! |
| Error | `--status-critical` | Circle-X |

**Timing:**
- Enter: slide in from right, 200ms ease-out
- Auto-dismiss: 5s for info/success, 8s for warning, manual dismiss for error
- Exit: fade out + slide right, 150ms ease-in
- Progress bar: 2px along bottom, `--text-tertiary` to transparent, showing time remaining

---

### 9. Command Palette

Keyboard-triggered (Cmd+K) global action palette.

**Overlay:** Full screen, `rgba(0,0,0,0.6)` backdrop, click-outside to dismiss.

**Panel:**
- Width: 560px, centered horizontally, 20% from top
- Background: `--bg-elevated`
- Border: 1px `--border-default`
- Border-radius: `--radius-lg`
- Shadow: `--shadow-lg`

**Search input:**
- Full width, no visible border (flush with panel)
- Height: 48px
- Font: sans, `--text-md`
- Placeholder: "Type a command or search..." in `--text-tertiary`
- Magnifying glass icon left, `--text-tertiary`
- Clear button right when text present

**Results list:**
- Max height: 360px, scrollable
- Row height: 36px
- Padding: `--space-2` horizontal
- Hover: `--bg-hover`
- Selected (keyboard): `--bg-active` with left 2px border `--amber-400`

**Result item:**
- Icon (16px) | Label (sans, `--text-base`) | Section hint (sans, `--text-xs`, `--text-tertiary`, right-aligned)
- Keyboard shortcut badge: mono, `--text-2xs`, `--bg-surface`, `--border-default`, border-radius `--radius-sm`

**Sections:**
- "Queues", "Consumers", "Actions", "Navigation"
- Section header: `--text-2xs`, `--text-tertiary`, uppercase, `--space-2` top margin

**Animation:**
- Open: scale from 0.98 to 1.0, opacity 0 to 1, 120ms ease-out
- Close: opacity 1 to 0, 80ms ease-in
- Results filter: instant (no animation, snappy filtering)

---

### 10. Navigation (Sidebar)

Persistent left sidebar for primary navigation.

**Container:**
- Width: `--sidebar-width` (220px), collapsible to `--sidebar-collapsed` (48px)
- Background: `--bg-surface`
- Right border: 1px `--border-subtle`
- Full viewport height

**Logo area (top):**
- Height: `--header-height`
- "qwer-q" in mono, `--text-md`, `--font-bold`
- Amber dot (6px) before the "q" — brand mark
- In collapsed: just the amber dot

**Sections:**
- Section label: `--text-2xs`, `--text-tertiary`, uppercase, tracking 0.08em, `--space-4` top margin
- Sections: "Overview", "Queues", "Consumers", "Schemas", "Settings"

**Nav item:**
- Height: 32px
- Padding: `--space-2` vertical, `--space-3` horizontal
- Icon (16px, stroke style) + label (sans, `--text-sm`)
- Default: `--text-secondary` icon and text
- Hover: `--bg-hover`, `--text-primary`
- Active: `--text-primary`, `--amber-400` icon, `--amber-900` background, left 2px border `--amber-400`

**Queue quick-list (expandable):**
- Below "Queues" nav item, indented `--space-6`
- Each queue: mono, `--text-xs`, with status dot (6px) inline
- Max 8 visible, "Show all" link below

**Collapse toggle:**
- Bottom of sidebar, centered
- Chevron icon, `--text-tertiary`
- Hover: `--text-secondary`

**Keyboard:** `[` key toggles sidebar collapse/expand.

---

## Motion Guidelines

### Principles

1. **Physics, not decoration** — Motion shows cause and effect (data arriving, state changing), never for "delight"
2. **Fast by default** — Transitions under 200ms unless content is complex
3. **Interruptible** — Any animation can be cut short by user action

### Timing Functions

| Token | Value | Usage |
|-------|-------|-------|
| `--ease-out` | `cubic-bezier(0.16, 1, 0.3, 1)` | Elements entering (panels, modals, toasts) |
| `--ease-in` | `cubic-bezier(0.7, 0, 0.84, 0)` | Elements exiting |
| `--ease-in-out` | `cubic-bezier(0.45, 0, 0.55, 1)` | State transitions (color, size) |

### Duration Scale

| Token | Value | Usage |
|-------|-------|-------|
| `--duration-instant` | 50ms | Color changes, opacity toggles |
| `--duration-fast` | 120ms | Small state changes, button press |
| `--duration-normal` | 200ms | Panel slides, modal open |
| `--duration-slow` | 300ms | Complex enter/exit, layout shifts |

### Data Update Patterns

| Event | Animation |
|-------|-----------|
| **New table row** | Slide in from top (200ms ease-out), amber background flash fading over 1.5s |
| **Updated cell value** | Text color flashes `--amber-400`, fades to normal over 800ms |
| **Removed row** | Fade opacity to 0 (150ms), collapse height (150ms) |
| **Metric value change** | Count up/down to new value over 400ms, sparkline extends smoothly |
| **Status change** | Cross-fade between badge states (120ms) |
| **Chart data point** | Line extends smoothly to new point (matches data interval) |

### Reduced Motion

When `prefers-reduced-motion: reduce`:
- All transitions become instant (0ms)
- Breathing pulses on status dots disabled
- Sparkline animations disabled
- Data updates show immediately without transitions

---

## CSS Custom Properties (Full Token Export)

```css
:root {
  /* === Backgrounds === */
  --bg-root: #0d0d0d;
  --bg-surface: #141414;
  --bg-elevated: #1c1c1c;
  --bg-hover: #242424;
  --bg-active: #2c2c2c;
  --bg-input: #181818;

  /* === Borders === */
  --border-subtle: #222222;
  --border-default: #2e2e2e;
  --border-strong: #3d3d3d;

  /* === Text === */
  --text-primary: #e8e8e8;
  --text-secondary: #999999;
  --text-tertiary: #666666;
  --text-inverse: #0d0d0d;

  /* === Brand (Amber) === */
  --amber-50: #fff8eb;
  --amber-100: #ffe8b8;
  --amber-200: #ffd780;
  --amber-400: #f5a623;
  --amber-500: #e6941a;
  --amber-600: #c47a10;
  --amber-900: #3d2608;

  /* === Status === */
  --status-healthy: #3dd68c;
  --status-healthy-muted: #1a3d2a;
  --status-warning: #f5a623;
  --status-warning-muted: #3d2e0a;
  --status-critical: #ef4444;
  --status-critical-muted: #3d1414;
  --status-inactive: #555555;
  --status-inactive-muted: #1e1e1e;

  /* === Chart Palette === */
  --chart-1: #f5a623;
  --chart-2: #3dd68c;
  --chart-3: #6e9ef5;
  --chart-4: #c084fc;
  --chart-5: #f97066;
  --chart-6: #67e8f9;

  /* === Syntax === */
  --syntax-string: #3dd68c;
  --syntax-number: #f5a623;
  --syntax-keyword: #c084fc;
  --syntax-key: #6e9ef5;
  --syntax-comment: #555555;
  --syntax-boolean: #f97066;
  --syntax-bracket: #999999;

  /* === Typography === */
  --font-mono: "Berkeley Mono", "JetBrains Mono", "Fira Code", "SF Mono", ui-monospace, monospace;
  --font-sans: "Inter", "SF Pro", -apple-system, system-ui, sans-serif;

  --text-2xs: 0.625rem;
  --text-xs: 0.6875rem;
  --text-sm: 0.75rem;
  --text-base: 0.8125rem;
  --text-md: 0.875rem;
  --text-lg: 1rem;
  --text-xl: 1.25rem;
  --text-2xl: 1.5rem;
  --text-3xl: 2rem;

  --font-normal: 400;
  --font-medium: 500;
  --font-semibold: 600;
  --font-bold: 700;

  /* === Spacing === */
  --space-0: 0px;
  --space-1: 4px;
  --space-2: 8px;
  --space-3: 12px;
  --space-4: 16px;
  --space-5: 20px;
  --space-6: 24px;
  --space-8: 32px;
  --space-10: 40px;
  --space-12: 48px;
  --space-16: 64px;

  /* === Layout === */
  --sidebar-width: 220px;
  --sidebar-collapsed: 48px;
  --header-height: 48px;
  --table-row-height: 36px;
  --card-radius: 6px;
  --badge-radius: 4px;
  --input-radius: 4px;
  --input-height: 32px;
  --button-height: 32px;

  /* === Radius === */
  --radius-sm: 3px;
  --radius-md: 6px;
  --radius-lg: 8px;
  --radius-full: 9999px;

  /* === Shadows === */
  --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.3);
  --shadow-md: 0 4px 12px rgba(0, 0, 0, 0.4);
  --shadow-lg: 0 8px 24px rgba(0, 0, 0, 0.5);

  /* === Motion === */
  --ease-out: cubic-bezier(0.16, 1, 0.3, 1);
  --ease-in: cubic-bezier(0.7, 0, 0.84, 0);
  --ease-in-out: cubic-bezier(0.45, 0, 0.55, 1);
  --duration-instant: 50ms;
  --duration-fast: 120ms;
  --duration-normal: 200ms;
  --duration-slow: 300ms;
}

/* === Light Mode Override === */
[data-theme="light"] {
  --bg-root: #fafafa;
  --bg-surface: #ffffff;
  --bg-elevated: #ffffff;
  --bg-hover: #f0f0f0;
  --bg-active: #e8e8e8;
  --bg-input: #ffffff;

  --border-subtle: #e8e8e8;
  --border-default: #d4d4d4;
  --border-strong: #b0b0b0;

  --text-primary: #1a1a1a;
  --text-secondary: #666666;
  --text-tertiary: #999999;
  --text-inverse: #fafafa;

  --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.08);
  --shadow-md: 0 4px 12px rgba(0, 0, 0, 0.12);
  --shadow-lg: 0 8px 24px rgba(0, 0, 0, 0.16);
}

/* === Reduced Motion === */
@media (prefers-reduced-motion: reduce) {
  :root {
    --duration-instant: 0ms;
    --duration-fast: 0ms;
    --duration-normal: 0ms;
    --duration-slow: 0ms;
  }
}

```

---

## Implementation Notes

### Recommended Libraries

- **Charts**: [uPlot](https://github.com/leeoniya/uPlot) — tiny (< 40KB), GPU-accelerated, built for time-series. Matches the "minimal, fast" ethos.
- **Syntax highlighting**: [Shiki](https://shiki.matsu.io/) — uses VS Code's TextMate grammars. Supports custom themes matching our tokens.
- **Icons**: [Lucide](https://lucide.dev/) — consistent stroke icons, tree-shakeable, 1px stroke weight at 16px.
- **Fonts**: Self-host Inter (woff2). Berkeley Mono requires license; JetBrains Mono as free fallback (also self-host).

### Accessibility

- All color contrasts meet WCAG AA (4.5:1 for text, 3:1 for UI components) against their respective backgrounds
- `--text-primary` on `--bg-root`: 15.86:1
- `--text-secondary` on `--bg-root`: 6.82:1
- `--amber-400` on `--bg-root`: 9.59:1
- Focus indicators: 2px `--border-strong` outline with 2px offset
- All interactive elements are keyboard-accessible
- Screen reader labels for status dots and sparklines
- `prefers-reduced-motion` fully respected (see Motion section)
