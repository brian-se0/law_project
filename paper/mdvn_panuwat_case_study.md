# MDVN -> INCY After *Panuwat*: A Reproducible Case Study of Related-Securities Watchlists

## Question

This paper studies whether a reproducible law-and-economics workflow can document abnormal pre-disclosure options activity in a related security around a target-firm M&A announcement. The focal case is Medivation (`MDVN`) and Incyte (`INCY`) around Pfizer's August 22, 2016 announcement that it would acquire Medivation. The legal motivation is *SEC v. Panuwat*. The empirical objective is narrower than proving liability: it is to measure suspicious pre-disclosure footprints in the complaint-named `INCY` contracts and translate the evidence into a related-securities watchlist rule.

## Design

The event table freezes the canonical MDVN announcement using the first public disclosure timestamp and then maps that timestamp into trading time under the repo's event-alignment rules. The options sample is limited to single-name equity options in the case-study window, with the primary legally focal series fixed ex ante in config:

- `INCY 2016-09-16 C 80.0`
- `INCY 2016-09-16 C 82.5`
- `INCY 2016-09-16 C 85.0`

The linkage context is also fixed ex ante. Horizontal peers come from lagged TNIC and vertical context comes from lagged VTNIC. The watchlist logic therefore does not cherry-pick peers ex post to fit the facts of the case. It asks a simpler compliance question: if a firm froze trading in MDVN before public disclosure, which economically linked single-name shares and listed options should have been on the same restricted list?

## Empirical framing

The draft now treats the complaint-named exact `INCY` contracts as the headline evidence layer. This choice is important. In a single-case paper, pooled exact-series evidence is more legally relevant than a generic bucket average because the SEC complaint identifies specific contracts. The scripted table at `outputs/tables/mdvn_exact_contract_window_summary.md` is therefore the first empirical table to cite in the text.

The short-dated OTM call bucket remains in the paper, but only as a benchmark and sensitivity check. That benchmark is motivated by takeover-options evidence in Augustin, Brenner, and Subrahmanyam (2019). It is useful because it links this case to the broader literature, but it should not displace the exact-series layer when the two point in different directions.

## Live case result

The live 2016 rerun now produces a substantive single-case result. In the pooled exact-series pre-event window `[-5,-1]`, the three configured `INCY` September 16, 2016 call contracts show:

- mean abnormal volume of `3.9680` standard deviations
- mean abnormal premium of `2.1255` standard deviations
- mean abnormal lead open-interest change of `9.4876` standard deviations
- pooled raw volume of `1,197` contracts
- pooled premium of `$274,631.81`
- `34.3%` of all INCY call volume
- `68.1%` of same-expiry INCY call volume

The exact-series signal remains strong in the terminal `[-2,-1]` window, where pooled abnormal volume is `4.1335`, pooled abnormal premium is `1.9409`, pooled raw volume is `754`, and the three contracts account for `69.9%` of same-expiry INCY call volume. The exact-series layer is therefore economically and legally more informative than the generic bucket layer for this case.

## Bucket benchmark and linkage context

The broader bucket benchmark remains mixed. In the `INCY` short-dated OTM call bucket, the pre-event `[-5,-1]` mean abnormal volume is `-0.3173`, while the terminal `[-2,-1]` mean abnormal volume turns positive at `0.5638`. That divergence is precisely why the paper should lead with exact-series evidence and treat the bucket layer as a benchmark and sensitivity check rather than the central result.

The ex ante linkage context also now reads coherently. In the retained MDVN case-study watchlist, `INCY` is force-retained as the legally focal related security, with lagged horizontal TNIC rank `32` and percentile `0.7615` within the full relevant horizontal link set. The retained watchlist then adds `10` other lagged TNIC peers plus `81` lagged VTNIC relations as unsigned context. This is the right doctrinal framing: linkage is an ex ante watchlist tool, not proof that every retained firm was materially affected by the deal.

## Main statement

The main empirical statement should remain modest:

Abnormal pre-disclosure activity appears in the complaint-named `INCY` contracts in the days before the MDVN announcement, within an ex ante relatedness setting that would have placed `INCY` on a related-securities watchlist.

The paper should separate three layers clearly:

1. Exact-series evidence: pooled and per-series abnormal volume, premium, lead open-interest change, and IV in the complaint-named contracts.
2. Bucket benchmark: terminal-case and pre-event short-dated OTM call metrics for `INCY`.
3. Linkage context: lagged TNIC and VTNIC rankings that justify why `INCY` belongs on a watchlist ex ante.

## Compliance translation

The policy contribution is a watchlist rule, not a liability rule. A compliance program informed by this case should:

- start with the source issuer and its listed options
- add the explicitly identified related security and its listed options
- add lagged horizontal peers retained from the ex ante linkage table
- add lagged vertical relations as unsigned context
- use matched non-linked controls only for calibration, not for surveillance scope

The repo's watchlist figure and memo should therefore be read as a compliance design output. They do not imply that every linked name is materially affected by the deal, and they do not imply that abnormal activity proves insider trading.

## Limits

This is a single-case reconstruction using daily options data. It does not identify traders, recover intraday order flow, or establish causation. Open interest is start-of-day OCC open interest, so any opening-demand proxy is necessarily approximate. Linkages are ex ante materiality proxies for watchlist design, not proof of doctrinal materiality by themselves.

## Working references

The current bibliography lives in `references.bib`. The literature review in `docs/literature_review.md` explains why the paper relies on takeover-options evidence, peer-security information migration, TNIC/VTNIC relatedness, and options-price informativeness.
