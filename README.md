# Low‑Touch Intraday Alpha — Daily Plan from `tomo__*`
**Production‑builder compatible. One bracket per name. Two decisions per day. No tick‑chasing.**

> You’ve already built the right plumbing (minute→daily intraday metrics, tomorrow targets, TA, Catch22 z‑blocks, astro/Shadbala, robust UPSERT).  
> This README turns those signals into a **simple, elegant, profitable** playbook with **minimal intraday rebalancing**.

---

## What the pipeline gives you (and how we’ll use it)

| Block | Examples | We use it to… |
|---|---|---|
| **Tomorrow targets (rebased)** | `tomo__open=1`, `tomo__high/low/close/vwap`, `tomo__oc_pct`, `tomo__volume` | Pick side, stops/targets, and size **before** the session. |
| **Intraday shape** | `tomo__intraday_sharpe`, `tomo__winrate_ratio`, `tomo__hurst_whole_day`, `tomo__area_over_under_ratio`, `tomo__slope_open_to_close_pct`, `tomo__max_drawdown`, `tomo__max_run_up`, `tomo__avg_vwap_velocity/acceleration` | Decide **regime** (trend vs reversion), bracket widths, and whether we hold to close. |
| **Gap/carry chain** | `tomo__o_over_c_today_pct`, `tomo__oo_plus1_pct`, `tomo__co_plus1_pct`, `tomo__consec_run_*` | Triage open tactics and **overnight** tilt with one rule. |
| **TA (no TA‑Lib)** | RSI/MACD/ADX/BBW/OBV/MFI/CMF | Light confirmation only. Don’t overfit; keep weights small. |
| **Catch22 z50** | `z50__<base>__CET/WL/...` (22 features × bases) | Pattern‑regime confirmation (trendiness vs choppiness) using last ~50d. |
| **Astro/Shadbala** | `shadbala__*_total`, `astro_recursive_bala__*`, aspects/conjs, kendra flags, tithi/yoga/karana | **Micro‑tilt** the book (±3–5% notional). Treat as additive prior, never as a hard gate. |
| **Raw levels** | `tomo_raw__open/high/low/close/vwap/volume` | Route orders, set participation caps, and translate rebased targets ↔ absolute. |

---

## Operating principles (so we don’t churn)
- **One bracket per position.** Optional **one add**; that’s it.  
- **Decision times:** (A) **09:45–10:15 ET** entry window, (B) **~15:35 ET** exit/hold decision.  
- **Bar‑close logic only** (5‑minute bars work fine).  
- **Portfolio first, ticks last:** pick names **before the open**; don’t switch lanes midday.

---

## 1) Pre‑Open: Regime & Side (scores from your `tomo__*`)

Define light‑weight z‑scores **cross‑sectionally for today’s trade list** (exclude names with NaNs):

- \( z(x) = \) cross‑sectional z of the *predicted* `tomo__` feature for that name.

### 1.1 Trend vs Reversion classifier (no ML needed)

**TrendScore**
```
TrendScore = 
  + 0.40 * z(tomo__intraday_sharpe)
  + 0.25 * z(tomo__hurst_whole_day)           # persistence
  + 0.20 * z(tomo__winrate_ratio - 1)
  + 0.15 * z(tomo__area_over_under_ratio - 1)
  + 0.10 * z(tomo__avg_vwap_velocity)
  + 0.10 * z(tomo__volume)                    # follow-through needs liquidity
```

**RevertScore**
```
RevertScore = 
  + 0.45 * z(0.5 - tomo__hurst_whole_day)     # anti-persistence
  + 0.30 * z(-abs(tomo__intraday_sharpe))     # noisy/choppy days
  + 0.25 * z(-abs(tomo__slope_open_to_close_pct))
  + 0.10 * z(-tomo__volume)                   # lower vol favors mean reversion
```

Pick **lane**:
- If `TrendScore - RevertScore > 0.25` → **Trend lane**  
- If `< -0.25` → **Reversion lane**  
- Else → **Skip** (you’re not paid to guess)

> **Why this works:** We’re stacking **persistence**, **directional cleanliness**, and **VWAP dominance** into one scalar. No black magic.

### 1.2 Side & conviction (long vs short)
- **Directional bias** = sign of `tomo__oc_pct` (bigger |z| ⇒ higher conviction).  
- Confirm with `tomo__vwap` vs 1.0 and the sign of `tomo__slope_open_to_close_pct`.

**Conviction**
```
Conviction = 
  0.5 * sign(tomo__oc_pct) * z(abs(tomo__oc_pct))
+ 0.3 * sign(tomo__slope_open_to_close_pct) * z(abs(tomo__slope_open_to_close_pct))
+ 0.2 * sign(tomo__vwap - 1.0) * z(abs(tomo__vwap - 1.0))
```

- **Go long** if Conviction > +0.15;  
- **Go short** if < −0.15;  
- Otherwise only trade in **Reversion lane** (fade extremes), or skip.

---

## 2) Brackets you can pre‑stage (low touch)

Let \(H = \) `tomo__high - 1`, \(L = 1 - tomo__low\). These are **expected up/down excursions from open**.

**Stop width** (respect forecast drawdowns, don’t blow up on a flush):
```
StopPct = max( 0.35 * L (for longs) or 0.35 * H (for shorts),
               0.20%, 
               0.75 * |tomo__max_drawdown| )
```

**Targets** (two‑stage so you can automate partials):
```
TP1 = 0.40 * (H for longs, L for shorts)
TP2 = 0.85 * (H for longs, L for shorts)   # remainder trails
```

**Sizing** (vol‑aware but simple):
```
UnitRisk$   = AccountEquity * 0.20%           # adjust to taste (0.10–0.30%)
Shares      = UnitRisk$ / (StopPct * tomo_raw__open)
SizeAdj     = 1 + 0.35 * z(tomo__volume)      # +/− ~35% based on forecast liquidity
Shares     *= clamp(SizeAdj, 0.6, 1.6)
POV cap     = min( 0.20% * tomo_raw__volume, exchange/venue caps )
```

---

## 3) Entries that don’t need babysitting

### A) Trend lane (dominance continuation)
**Gates:** `TrendScore high`, `Conviction same sign`, `tomo__area_over_under_ratio > 1` for longs (or <1 for shorts).

- **Window:** 09:45–10:15 ET.  
- **Trigger (bar‑close only):** First **5‑min** close that **confirms** the side:
  - Longs: close **above** VWAP or prior bar high **and** bar closes **above** its midpoint.  
  - Shorts: mirror below VWAP.

**Bracket:** Place stop at `StopPct`, TP1/TP2 as above.  
**One add (optional):** If price tags **TP1** and closes above prior bar high (longs), add **half** and **recompute** StopPct with the new `L/H` **but never tighten beyond 0.20%**.

### B) Reversion lane (sigma‑fade to VWAP)
**Gates:** `RevertScore high`, `TrendScore low`, volume z ≤ 0.

- **Window:** 10:15–11:30 ET (avoid open thrash).  
- **Trigger:** Enter **only** on 5‑min close at **≥ 3σ** from VWAP (use your minute‑bar σ), **against** the extreme.  
- **Exit:** Single target at **VWAP**, or **OPEN** if closer; **flat by 14:00** unless TrendScore flips (rare).

> **Two lanes, one bracket.** That’s the point: **no midday lane‑switching.**

---

## 4) Gap triage (decide at 09:35, don’t overthink it)

Let `Gap = tomo__o_over_c_today_pct`.

- **Gap‑and‑go:** Gap > +0.5%, `TrendScore > 0`, `tomo__avg_vwap_velocity > 0`  
  → Run the **Trend lane** long setup (skip if `StopPct > 0.5%`).

- **Gap‑fade:** Gap > +0.7%, `tomo__intraday_sharpe < 0`, `tomo__area_over_under_ratio < 1`  
  → One **Reversion** short into VWAP; no add.

- **Gap‑down analogues** symmetric.

---

## 5) Hold‑into‑close and overnight (one switch at 15:35)

**CloseHoldScore** (should I hold the remainder into the close?)
```
CloseHoldScore =
  0.45 * z(tomo__intraday_sharpe)
+ 0.30 * z(tomo__winrate_ratio - 1)
+ 0.25 * z(tomo__hurst_whole_day)
+ 0.15 * z(sign * tomo__avg_vwap_velocity)    # sign = +1 for longs, -1 for shorts
```
If `CloseHoldScore > 0.4` (and not near Stop), **hold to MOC** (market‑on‑close) or trail last swing.

**OvernightKeep** (carry a slice?)
```
OvernightKeepProb ≈ σ( 2.0 * z(tomo__oo_plus1_pct) + 1.2 * z(tomo__co_plus1_pct) + 0.8 * z(tomo__volume) )
```
- If > 0.65 and **Trend lane**, keep **≤ 30%** of the position overnight; else **flat**.

> **Astro micro‑tilt:** If `astro_recursive_bala__Venus + astro_recursive_bala__Jupiter` is in its **top 20%** historical range **and** `astro__kendra_to_moon_count ≥ 5`, allow OvernightKeepProb threshold to drop by **0.05**. Cap total astro effect to **±5% notional**. (Belief‑agnostic and risk‑controlled.)

---

## 6) Portfolio construction (cross‑section first)

- Build **two lists** pre‑open:
  - **Trend long list:** top **N** names by `TrendScore * max(Conviction,0)`  
  - **Trend short list:** top **N** by `TrendScore * max(-Conviction,0)`  
  - **Reversion list:** top **M** by `RevertScore` (only if Conviction is weak)
- **Net exposure rules:**  
  - If index proxy (SPY) has `TrendScore_SPY > 0.3` → allow **net** in that direction up to **+40% gross**.  
  - Else hold **near‑neutral** by balancing long/short counts or using an index hedge **once** at 09:40.
- **Diversity:** one name per sector bucket unless its score beats the next candidate by **> 0.5σ**.

---

## 7) Risk template (don’t improvise mid‑day)
- **Per‑name stop** from `StopPct` only; no ad‑hoc nudges.  
- **Portfolio VAR approximation:** sum of per‑name UnitRisk$ × correlation proxy (sector buckets). Keep daily risk ≤ **0.8–1.2% equity**.  
- **Hard skip:** If `|tomo__max_drawdown| > 1.2%` **and** `tomo__volume < 0`, we pass—range is large but paper‑thin.

---

## 8) Practical alpha edges (beyond the obvious)
- **Catch22 z‑blocks**: build a 1‑component PCA on `z50__close__*` and `z50__vwap__*`.  
  - If PC1 (trendiness) is **high** and `tomo__hurst_whole_day > 0.55`, **raise TP2 allocation** (+10% of remaining).  
  - If PC1 low, prefer **Reversion lane** even if Conviction is small; chop loves VWAP.
- **Time‑of‑day asymmetry:**  
  - If `tomo__consec_run_len` predicts **down‑open drive** (negative, |len|≥2), give shorts **first priority**; longs wait for 10:10.
- **TA as brakes, not gas:**  
  - Longs: if `ta_adx_last_14` < 12 **and** `tomo__volume < 0`, cap size at 60% of Shares.  
  - Shorts: if `ta_mfi_last_14` < 20 + `tomo__o_over_c_today_pct > 0`, avoid catching the falling knife—use Reversion lane only.

---

## Tiny summary table (pin this to your desk)

| Feature | Why it matters | What to do |
|---|---|---|
| `tomo__intraday_sharpe` | Cleanliness of trend | High → Trend lane; low → Reversion lane |
| `tomo__hurst_whole_day` | Persistence vs reversion | >0.55 momentum, <0.45 fade |
| `tomo__winrate_ratio` | One‑way tape | >1.05 adds patience to longs; <0.95 patience to shorts |
| `tomo__area_over_under_ratio` | VWAP dominance | >1 buyers, <1 sellers; set side + stop reference |
| `tomo__slope_open_to_close_pct` | Drift | Align Conviction with side |
| `tomo__max_drawdown` | Pullback depth | Sets StopPct floor; larger → smaller size or skip |
| `tomo__avg_vwap_velocity/accel` | Drift & stalling | Same sign = press; opposite = partials |
| `tomo__high/low` | Expected range | Sets TP1/TP2 and ambition |
| `tomo__volume` | Liquidity | SizeAdj 0.6–1.6× |
| `tomo__o_over_c_today_pct` | Gap regime | Gap‑and‑go vs gap‑fade |
| `tomo__oo_plus1_pct`, `co_plus1_pct` | Carry/overnight | Small keep if high and TrendScore strong |
| Catch22 (z50) | Pattern regime | PCA trendiness boosts Trend lane |
| Astro micro‑tilt | Prior | ±5% book tilt max, never a gate |

---

## Minimal config (YAML‑ish you can keep in the repo)
```yaml
bars: "5min"
entry_window_et: ["09:45","10:15"]
final_decision_et: "15:35"
lane_thresholds:
  trend_minus_revert_min: 0.25
  conviction_min: 0.15
risk:
  stop_floor_pct: 0.20
  stop_dd_mult: 0.75
  tp1_frac_of_range: 0.40
  tp2_frac_of_range: 0.85
  unit_risk_pct: 0.20
  pov_cap_bps_of_volume: 20
size:
  volume_z_adj_slope: 0.35
  min_mult: 0.6
  max_mult: 1.6
overnight:
  keep_prob_cut: 0.65
astro_microtilt:
  enable: true
  max_notional_pct: 5
  bonus_prob_cut: 0.05
skip_rules:
  max_drawdown_abs_pct: 1.20
  require_volume_z_nonneg_for_trend: true
```

---

## Appendix: field meanings (ultra‑short)
- **Rebased levels**: `tomo__open=1`; `tomo__high/low/close/vwap` are multiplicative vs open.  
- **`tomo__oc_pct`**: (Close/Open − 1) for **tomorrow** → primary bias.  
- **`tomo__max_drawdown`**: worst intraday peak→trough % → stop budget.  
- **`tomo__area_over_under_ratio`**: area above vs below VWAP → who owns the tape.  
- **`tomo__avg_vwap_velocity/acceleration`**: VWAP drift & change in drift.  
- **`tomo__winrate_ratio`**: up‑bar count / down‑bar count.  
- **Catch22 z50**: 22 canonical shape features, z‑scored over rolling 50d, used via PCA.  
- **Astro/Shadbala**: use **only** as bounded micro‑tilt (risk stays first).

---

### Final word
Keep it boring: **pick the lane, place the bracket, touch the book twice**.  
If you need a third click, the setup wasn’t yours.
