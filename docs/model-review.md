# Semantic model review

`vehicles_model` was reviewed with [semantic-model-analyzer](https://github.com/DataCraftersLab/craftys-fabric-skills) (Crafty's Fabric Skills, `sma` 1.0.1, Apache-2.0). It reads the TMDL in `fabric/vehicles_model.SemanticModel/` as text, offline, and reports findings with file and line. It never sees data or the report.

Loop: export with `fab bulk-export` → run `sma report` → fix in the TMDL → `fab import` into the Dev workspace → check the numbers with a DAX query → export again → run `sma` again on Fabric's own version.

## Before → after

| | Before | After |
|---|---|---|
| Findings worth acting on | 3 | 0 |
| Findings worth knowing | 7 | 6 |
| AI readiness checks passed (of 16) | 4 | 9 |
| Measures described | 0 of 7 | 7 of 7 |
| Measures formatted | 5 of 7 | 6 of 7 |

## What it found, and what changed

| Finding | Where | Fix |
|---|---|---|
| `implicit-measure`: `price_bam`, `mileage_km`, `engine_power_kw` summarise as Sum | `fact_listing.tmdl` | `summarizeBy: none`, and `discourageImplicitMeasures` on the model. Numbers go through the measures, so nobody drags in a "Sum of price" that competes with `Median Price (BAM)`. |
| `unformatted-measure`: `Avg Price (BAM)` | `fact_listing.tmdl` | `formatString: #,0` |
| Key columns visible: `fact_listing[listing_id]`, `[manufacturer]`, `[fuel_type]` | readiness check | hidden; filter by the dimension columns instead. `dim_fuel[fuel_type]` (the Bosnian code) hidden too, since `fuel_label` is the one to show. |
| No descriptions on tables, columns or measures | readiness check | 25 descriptions, one per table, column and measure, written as `///` lines in the TMDL. They show as tooltips in the field list and are what Copilot reads. |

The live model gave the same numbers after the import (Toyota 414 listings, 15.1% share, 232 diesel).

## What's left, on purpose

- **6 × `unreferenced-measure`**: no other measure reads them. The tool can't see the report, and its docs say this is a list to check, not to delete. Five are on the report; `Avg Price (BAM)` is kept for comparing mean and median in DAX queries.
- **No date table**: the model has no dates, only `production_year` as a label.
- **`Listings Label` has no format string**: it's a text measure (`FORMAT` returns text), so there's nothing to format.
- **5 readiness checks unknown**: linguistic schema, AI instructions, synonyms, verified answers. A service export doesn't carry them.
