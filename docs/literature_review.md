# Literature Review

## Core framing

This project is a single-case law-and-economics paper on abnormal pre-disclosure options activity in a related security. The literature we need is therefore narrower than a broad insider-trading survey. The key question is whether the MDVN -> INCY design is anchored in prior evidence on takeover options trading, information migration into economically linked firms, and ex ante relatedness measures that can support a compliance watchlist rather than an ex post accusation.

## Benchmark evidence on takeover options trading

- Augustin, Brenner, and Subrahmanyam (2019), "Informed Options Trading Prior to Takeover Announcements: Insider Trading?," *Management Science* 65(12):5697-5720. This is the benchmark for the paper's own-target comparison. Its central empirical pattern is that abnormal pre-announcement activity is concentrated in short-dated, out-of-the-money calls. We should use this as a benchmark layer for MDVN and as context for why the INCY exact-series evidence matters.
- The MDVN case should therefore keep the bucket benchmark, but the legally focal evidence remains the complaint-named INCY contracts rather than a generic own-target bucket average.

## Why options can carry informative signals

- Cao, Goyal, Ke, and Zhan (2024), "Options Trading and Stock Price Informativeness," *Journal of Financial and Quantitative Analysis* 59(4):1516-1540. This paper supports the premise that option trading can improve information incorporation into prices and is a sensible place to look for informed activity.
- In this repo, that literature supports keeping option volume, premium, lead open-interest change, and IV/spread shifts as interpretable component measures rather than collapsing everything into a black-box score.

## Related-securities and peer-security information migration

- Deuskar, Khatri, and Sunder (2024 working paper, accepted at *Management Science*), "Insider Trading Restrictions and Informed Trading in Peer Stocks." This is the closest doctrinally to the shadow-trading problem. The paper's contribution is the idea that information can be fungible across peer securities when direct trading constraints tighten.
- Du and Hilliard (2025), "Informed Option Trading of Target Firms' Rivals Prior to M&A Announcements," *Journal of Futures Markets* 45(10). This provides a direct options-market analogue for related-firm activity around M&A announcements.
- For this project, those papers justify treating INCY as the primary related security and describing the result as a suspicious footprint or shadow-trading risk signal, not as proof of liability.

## Ex ante linkage measurement

- Hoberg and Phillips (2010), "Product Market Synergies and Competition in Mergers and Acquisitions: A Text-Based Analysis," *Review of Financial Studies* 23(10):3773-3811. This supports using text-based product-market similarity in an M&A setting rather than hand-picked ex post peers.
- Hoberg and Phillips (2016), "Text-Based Network Industries and Endogenous Product Differentiation," *Journal of Political Economy* 124(5):1423-1465. This is the direct TNIC measurement anchor and should be the main citation when we explain the horizontal linkage table.
- Fresard, Hoberg, Phillips, and Cornelli (2020), "Innovation Activities and Integration through Vertical Acquisitions," *Review of Financial Studies* 33(7):2937-2976. This is the main vertical-relatedness anchor and supports keeping VTNIC as unsigned context in the watchlist rather than forcing a directional claim we cannot defend in this repo.

## What the paper should claim

- Exact-series evidence is the headline empirical layer because the SEC complaint identifies those contracts specifically.
- Short-dated OTM bucket evidence is still useful, but here it is a literature benchmark and sensitivity check.
- Ex ante linkages are watchlist tools and materiality proxies. They are not proof that a related security was material to the deal, and they are not proof of unlawful trading.
- The paper should separate empirical claims from normative claims. The empirical claim is that the repo can document abnormal pre-disclosure activity in complaint-named INCY contracts within an ex ante linkage setting. The normative claim, if any, is limited to compliance design: related-securities watchlists should cover economically linked single-name shares and listed options before public disclosure.

## Open citation notes

- The Deuskar-Khatri-Sunder paper is still best treated as a working-paper citation in the draft, even though SSRN reports it as accepted at *Management Science*.
- The Du-Hilliard paper is recent enough that we should keep the journal citation but avoid overloading the draft with strong doctrinal claims based on a single new article.
- `references.bib` contains the working citation set for the current draft.
