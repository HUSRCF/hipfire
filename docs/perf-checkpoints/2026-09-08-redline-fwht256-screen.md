# Redline attention-tile screen and PM4 Q8 follow-up on gfx1201

- **Date:** 2026-09-08
- **Lifecycle:** historical negative result
- **Issue:** [#649](https://github.com/warpfront/hipfire/issues/649)
- **Commit:** `f4e1412e` (`origin/master` at test time)
- **Host:** Radeon AI PRO R9700, `gfx1201`
- **Model:** `qwen3.6-35b-a3b.mq4r`, SHA-256
  `4685c140c46b1a6f31a0fd9053bf09d5faf1d2529d715b84794249b66cde0428`
- **Workload:** ordinary AR, Q8 KV, DFlash/MTP/CASK off, automatic clocks,
  retained PM4 route, `max_seq=32768`

## Question

Issue #649 asks for a screen of 256-token FWHT attention tiles on the current
long-context path before attempting compatible K/V writer fusion. The current
master exposes two separate tile controls. `HIPFIRE_ATTN_TILE_SIZE` controls
the batched asym/FWHT launcher, while the Qwen3.6 Q8 decode path used by this
product benchmark reads `HIPFIRE_Q8_FLASH_TILE`. The first screen used the
former and therefore did not change the Q8 kernel; the corrected PM4 follow-up
below uses the latter.

## Method

Each arm used a fresh daemon process and the same product benchmark:

```text
HIP_VISIBLE_DEVICES=0
HIPFIRE_ATTN_TILE_SIZE={64,128,256,512}
context={8192,16384}
iterations=80, warmups=3, warmup_iterations=32, runs=5
transport=pm4, max_seq=32768, kv_mode=q8
```

The benchmark used the custom `benchmarks/prompts/bare_factual.txt` coherence
smoke with thinking disabled and `max_tokens=256`. Both HIP and retained arms
passed coherence. PM4 preflight passed with 603 dispatches for every arm; the
retained route proof passed with five observed rows and the expected positions.

The first 8K tile-128 diagnostic run was initially blocked in the coherence client because
the host environment had `http_proxy`, `https_proxy`, and `all_proxy` set to a
local proxy. Subsequent runs explicitly cleared those variables and set
`NO_PROXY/no_proxy=127.0.0.1,localhost,::1`. The 8K tile-128 performance run
used `--skip-coherence`, so its report is marked `valid=false`; its five
fresh-process performance rows and retained route proof are still usable as
the paired baseline. The complete coherence gate was independently confirmed
by the same custom prompt in the 16K tile-128 run.

## Results

The rows below are the original `HIPFIRE_ATTN_TILE_SIZE` screen. They are
diagnostic for the batched asym/FWHT family, but they are not Q8 tile A/B rows:
the Q8 kernel kept its default tile128 geometry in all of them.

| context | tile | HIP median | retained PM4 median | PM4 delta vs tile128 |
|---:|---:|---:|---:|---:|
| 8K | 128 | 160.247 tok/s | 183.716 tok/s | baseline |
| 8K | 256 | 167.907 tok/s | 183.537 tok/s | -0.098% |
| 16K | 128 | 157.350 tok/s | 165.164 tok/s | baseline |
| 16K | 256 | 156.980 tok/s | 164.946 tok/s | -0.132% |
| 16K | 64 | 155.602 tok/s* | 165.064 tok/s | -0.061% |
| 16K | 512 | 156.934 tok/s | 165.058 tok/s | -0.064% |

The original four performance arms passed the stationarity gate. The HIP-only
8K increase did not survive retained replay, and the 16K HIP result was
slightly lower. The PM4 differences are well inside the observed noise floor
and do not establish a win. The additional 16K tile-64 run was marginally
outside the harness spread threshold on the HIP arm (`0.527%` versus a
`0.5%` limit), although its route and coherence checks passed; it is therefore
marked with `*` and treated as a negative screening signal rather than a valid
acceptance result. Tile 512 passed all gates and was also neutral.

## Disposition

The original batched asym/FWHT screen selected no winning geometry. The
correct Q8 PM4 follow-up also rejects tile256. Do not proceed to K/V writer
fusion on either result: the issue orders that experiment after selecting a
winning tile, and this screen found none. Multi-token retained replay remains
deferred as specified by #649.

The original no-op screen did not retain a source change. The corrected
follow-up retains the partial-row stride separation as an in-progress
optimization; it is not yet a performance win over tile128.

## Correct Q8 PM4 follow-up

The Qwen3.6 product path is scalar Q8 attention, not the batched asym/FWHT
launcher. A kernel-level profile showed that the earlier 128/256 rows had the
same Q8 grid (`[16,256,1]`), so `HIPFIRE_ATTN_TILE_SIZE=256` was a no-op for
those rows. The corrected 16K run used `HIPFIRE_Q8_FLASH_TILE=256`:

| Q8 tile | HIP median | retained PM4 median | PM4 delta vs Q8 tile128 |
|---:|---:|---:|---:|
| 128 | 157.350 tok/s | 165.164 tok/s | baseline |
| 256 | 156.759 tok/s | 160.647 tok/s | -2.734% |

The corrected run was valid: both coherence arms passed, PM4 preflight captured
603 dispatches, retained route proof passed for all five timed rows, and both
measurement stability gates passed.

The PM4 dispatch profile explains the regression. At 16K, tile256 reduces the
Q8 tile-kernel total from `503.64 us` to `33.46 us` per replay, but the
gfx1201 gated reduce/MQ-rotate total rises from `508.54 us` to `1102.30 us`.
The attention pair therefore grows from `1012.18 us` to `1135.76 us`, while
the complete 603-dispatch tape grows from `5792.58 us` to `5946.86 us`.
The large-tile candidate is rejected for PM4. The next optimization must fix
the reducer's tile-geometry behavior or fuse the tile/reduce work; changing
the retained tape alone cannot recover the lost time.

## In-progress layout correction

The retained source change separates the recorded tile grid capacity from the
partials row stride. For Q8 tile sizes above 128, the tile kernel now writes
using the stride reserved by the Qwen scratch allocator (`min(tile_size,128)`)
while the grid still uses the larger tile geometry. The reducer receives that
same partial stride and computes its active tile count from `pos_buf` and the
runtime tile size.

On the corrected 16K PM4 profile, this changed the full tape from `5946.86 us`
to `5850.06 us` and the product PM4 median from `160.647` to `163.401 tok/s`.
The result remains below the tile128 baseline, so the reducer optimization is
still required before this can be proposed as a performance change.

## Reducer scan

A retained dispatch profile with the corrected layout shows a non-monotonic
gfx1201 reducer response:

| Q8 tile | reducer total per tape | full tape |
|---:|---:|---:|
| 64 | 308.64 us | 5898.94 us |
| 128 | 508.54 us | 5792.58 us |
| 256 | 1096.74 us | 5850.06 us |

The 128-thread launch, gfx1201 launch-bounds specialization, and a tile256
compile-time/unroll specialization were all bit-exact but did not reduce the
tile256 reducer time. Those probes were discarded. The remaining hypothesis
is a gfx1201 code-generation or memory-schedule issue in the reducer's
tile-size-dependent loop, rather than PM4 packet overhead.
