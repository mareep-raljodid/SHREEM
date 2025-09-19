# SHREEM

> **Scope:** All fields below are **predictions for the next regular session** and are designed for **bar-close decisioning** (e.g., 1–5-minute bars), not tick-chasing.  
> **Convention:** Unless the name says `tomo_raw__*`, prices are **rebased so that `tomo__open = 1.0`**.  
> **Sanity:** Convert between rebased and raw with `tomo__x = tomo_raw__x / tomo_raw__open`.

---

## Quick Summary Table

| Feature | Intuition | What “high” means | Bias/Action (bar-close only) |
|---|---|---|---|
| `tomo__intraday_sharpe` | Quality of trend (mean/std of bar returns) | Clean trend day (noise low) | Trend tactics; trail winners; fewer fades |
| `tomo__winrate_ratio` | % of bars up vs down | One-way tape, buyers > sellers | Buy pullbacks above VWAP; sell strength if < 1 |
| `tomo__hurst_whole_day` | Persistence (trend vs mean-reversion) | >0.55: persistence | Momentum if >0.55; fade if <0.45 |
| `tomo__slope_open_to_close_pct` | Drift from O→C beyond OC return | Persistent grind | With-trend entries on higher-lows / lower-highs |
| `tomo__area_over_under_ratio` | VWAP dominance (time/area above vs below) | Buyers own the day | Buy dips if >1; sell rips if <1 |
| `tomo__max_drawdown` | Worst intraday flush vs peak | Deeper air-pockets likely | Wider stops/smaller size; wait for pullback |
| `tomo__max_run_up` | Largest excursion above open | Stretch potential | Trail, don’t cap gains early |
| `tomo__avg_vwap_velocity` | VWAP drift direction | VWAP rising | Buy pullbacks > VWAP; opposite if negative |
| `tomo__avg_vwap_acceleration` | Trend strengthening vs stalling | Accel aligns with velocity | Add on confirmation; cut if divergence |
| `tomo__oc_pct` | Next day O→C return | Directional bias | Choose momentum vs fade toolkit |
| `tomo__o_over_c_today_pct` | Gap vs today’s close | Up-gap | Gap-and-go vs gap-fade logic with Hurst |
| `tomo__volume` | Volume z-score | Liquidity tailwind | Size up if >0; size down if <0 |

> **Rule of thumb:**  
> **Bias** = `oc_pct` + `intraday_sharpe` + `hurst` + `area_over_under_ratio`.  
> **Execution** = VWAP + `velocity/accel`.  
> **Risk** = `max_drawdown` + `volume`.  
> **Targets** = `tomo__high/low/close/vwap` (rebased) or `tomo_raw__*` (absolute).

---

## Feature-by-Feature: Definitions, Context, and Playbooks

### `tomo__intraday_sharpe`
- **What:** Sharpe of intraday bar returns (`mean/std`).  
- **Why:** Distinguishes clean trend vs messy chop.  
- **Playbook:**  
  - If > +0.5: trend tactics, bar-close pullbacks above VWAP.  
  - If < −0.5: mean-reversion, quicker exits.  
- **Alpha combo:** Sharpe > 0 & winrate > 1.05 → grind-up regime.

### `tomo__winrate_ratio`
- **What:** Up-bars / down-bars.  
- **Why:** Captures tape persistence.  
- **Playbook:**  
  - >1.1: buy dips, don’t fade.  
  - <0.9: sell rips, avoid knives.  
- **Alpha combo:** >1.1 + VWAP velocity >0 → buy VWAP retest holds.

### `tomo__max_drawdown`
- **What:** Worst peak-to-trough move intraday.  
- **Why:** Sets stop budget.  
- **Playbook:**  
  - Large → wait for pullback, use wider stops.  
  - Small → size normal, exits quicker.

### `tomo__slope_open_to_close_pct`
- **What:** Regression slope O→C path (%).  
- **Why:** Identifies grind vs stall.  
- **Playbook:**  
  - Positive + winrate > 1 → higher-low bar entries.  
  - Negative + AOU < 1 → VWAP rejection shorts.

### `tomo__hurst_whole_day`
- **What:** Hurst exponent (0–1).  
- **Why:** Momentum vs reversion gating.  
- **Playbook:**  
  - >0.55: momentum; trail winners.  
  - <0.45: fade sigma moves to VWAP.

### `tomo__area_over_under_ratio`
- **What:** VWAP dominance measure.  
- **Why:** Who owns the day.  
- **Playbook:**  
  - ≫1: buyers → longs above VWAP.  
  - ≪1: sellers → shorts below VWAP.

### `tomo__max_run_up`
- **What:** Largest excursion above open.  
- **Why:** Sets ambition for targets.  
- **Playbook:**  
  - Big: use trailing stops.  
  - Small: take profits early.

### `tomo__avg_vwap_velocity` & `tomo__avg_vwap_acceleration`
- **What:** VWAP drift (velocity) and trend change (acceleration).  
- **Playbook:**  
  - Vel >0 & Accel >0: trend strengthening → breakout continuation.  
  - Vel >0 & Accel <0: stalling → partial exits.

### `tomo__oc_pct`
- **What:** Tomorrow O→C %.  
- **Why:** Core directional bias.  
- **Playbook:**  
  - Positive: bias long setups.  
  - Negative: bias short setups.

### Gaps (`tomo__o_over_c_today_pct`, `tomo__oo_plus1_pct`, `tomo__co_plus1_pct`)
- **Gap vs today’s close (O/C today):** drives open tactics.  
- **Open→Open:** swing/overnight tilt.  
- **Close→today’s open:** roll-forward PnL measure.  
- **Playbook:**  
  - Gap-and-go: gap up + Hurst >0.55 → buy above VWAP.  
  - Gap-fade: big gap up + Sharpe <0 → short into VWAP.

### Rebased Levels (`tomo__high/low/close/vwap`)
- **What:** Relative to tomorrow’s open = 1.0.  
- **Playbook:**  
  - Use highs/lows as range bands.  
  - VWAP vs 1.0 to set bias.

### Liquidity: `tomo__volume`
- **What:** Z-score of volume.  
- **Why:** Sizing, credibility of moves.  
- **Playbook:**  
  - >0: size up, looser stops.  
  - <0: size down, fade more.

### Initial Impulse: `tomo__consec_run_len` & `tomo__consec_run_total_pct`
- **What:** First monotone run from open (sign = direction).  
- **Playbook:**  
  - Negative large: sell VWAP retest.  
  - Positive long: buy shallow pullback.

### Raw Levels: `tomo_raw__*`
- **What:** Absolute prices/volume.  
- **Why:** Routing, sizing, converting back.  
- **Playbook:**  
  - `abs_target = rebased_target * raw_open`.

---

## Intraday Setups (Bar-Close Only)

### 1. Dominance Trend Long
- **Gates:** `oc_pct > 0`, Sharpe >0, winrate >1.05, AOU >1.15, VWAP velocity >0.  
- **Entry:** 2-bar pullback close above VWAP.  
- **Exit:** Trail swing lows or into `tomo__close`.

### 2. Seller Control Short
- **Gates:** `oc_pct < 0`, slope <0, AOU <0.85, VWAP velocity <0.  
- **Entry:** Lower-high close below VWAP.  
- **Exit:** Cover at `tomo__low` or VWAP tag.

### 3. Hurst-Fade Lunch
- **Gates:** Hurst <0.45, winrate ~1, vol ≤0.  
- **Entry:** Fade 3–4σ deviation from VWAP on bar-close.  
- **Exit:** VWAP or OPEN.

### 4. Gap-Triaged Play
- **If up-gap >0.5%:**  
  - With Hurst >0.55 + VWAP velocity >0 → gap-and-go.  
  - With Sharpe <0 or AOU <1 → gap-fade.

---

## Risk Template
- **Stop width:** max(MDD, 0.20%).  
- **Shares:** `risk$ / (stop_width * price)`.  
- **Participation:** POV ≤ 10–20 bps of forecast volume.  
- **Scaling:** only if VWAP alignment holds.

---

## Practical Alpha Notes
- Use **percentile thresholds** (rolling) not fixed values.  
- First hour realizes `consec_run` edge; last 90 mins realizes Sharpe/slope edge.  
- Cross-section tilt: on high-vol days, rank longs by `winrate*hurst`.

---

## Glossary Snap
- Rebased = relative to tomorrow open=1.  
- Raw = absolute.  
- VWAP dominance (`AOU`) = who owns the tape.  
- Hurst = persistence vs reversion.  
- Consec run = opening drive.

---
