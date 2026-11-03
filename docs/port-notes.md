# Port notes: vehicle-data-lab → Fabric

Base: `02_data_preprocessing.ipynb` from [vehicle-data-lab](https://github.com/vrosi21/vehicle-data-lab) (pandas, course project).
Target: `notebooks/vehicle_medallion.ipynb` (PySpark in a Fabric lakehouse, Delta tables).
The port was drafted with Claude Code; every change below was checked against the raw data before it went in.

## Kept from the original
- Rename map (raw column names → one canonical name per field)
- Source 7 semicolon separator; source 10 `all_attributes` unpacking (colour)
- hp → kW factor 0.7355
- Thresholds: mileage > 900,000 km → missing; power 30-600 kW; year 1950-2025; price < 50 KM = no price
- `Mercedes` → `Mercedes-Benz`; colour `Nepoznato` → `Unknown`; model `Drugi` → missing
- Duplicate merge rule: first non-missing value per column
- The 12 manual fixes for duplicates that disagreed (2 listings removed)
- `general_model` mapping; `Unknown` for missing transmission / fuel / colour

## Bugs fixed (found while porting)
| # | Original behaviour | Evidence | Fix |
|---|---|---|---|
| 1 | Mileage regex `[^\d\.]` keeps the dots, so `350.000km` → 350.0 | 739 values in sources 1, 7, 12 were 1000x too small | One number parser for every field (dot + 3 digits = thousands), unit-tested in the notebook |
| 2 | Source 10's `engine_power_kW` column read as kW | Equals `Konjskih snaga` (hp) in 45/45 rows; "Ferrari F8 ... 720PS" → 720; 8 real cars then dropped by the > 600 kW rule | Read source 10's power as hp; convert by column meaning, not by file index |
| 3 | Duplicates matched on URL text | `www.olx.ba/...` vs `olx.ba/...` vs trailing `/`: 40 duplicates found, 49 real | Match on the listing ID number |

## Deliberate changes (different goal: a report, not an ML model)
- **No imputation.** The original filled missing price / mileage / power / year with mean or median to feed a model. A report must not show invented values: missing stays missing, and a listing without a price is rejected.
- **Bad power no longer drops the listing.** 26 listings had power like 0, 0.7 or 5575 kW. Their price is still valid, so power becomes missing and the listing stays. Cost: a few real Renault R4s (~25 kW) lose their power value.
- **Nothing is silently dropped.** Every rejected row lands in `silver_rejects` with a reason, and the gate checks raw = silver + rejected + merged duplicates.

## Numbers (local run, same as Fabric)
- Raw 3171 → silver 2737 + rejected 385 (383 no price, 2 manual removals) + 49 merged duplicates
- 32 manufacturers

## Semantic model + report (Fabric, built through the browser)
- Direct Lake model `vehicles_model` on `fact_listing`, `dim_manufacturer`, `dim_fuel`; 2 many-to-one relationships; measures Listings, Median Price (BAM), Avg Price (BAM), Median Mileage (km).
- Checked with a DAX query: every listing maps to a fuel (737 + 1051 + 86 + 21 + 17 + 825 = 2737). The "(Blank)" slicer tile is an empty member, not lost rows.
- Model fix: `production_year` defaulted to "Summarize by: Sum" (Power BI adds up any number column), so it couldn't go on a chart axis. Set to None. Same idea as treating IDs and years as labels, not quantities.
- Report `vehicles_report`: card, fuel slicer, median price by brand, median price by year, brand table, caveat note. Screenshot: `docs/report.png`.
- Second pass after review:
  - Brand chart only shows brands with 10+ listings, year chart only years with 5+. A median over 1-2 cars is noise (Corvette and Maybach topped the chart with one listing each).
  - Card used "Auto" display units (3K) with no override, so a text measure `Listings Label = FORMAT([Listings], "#,0")` shows 2,737. Trade-off: text measures can't be summed or sorted as numbers.
  - Slicer hides the empty (Blank) member; prices shown with 0 decimals; one grid layout.

## Data caveats
- 38% of listings (1051) have unknown fuel: sources 3, 7, 8 and 12 had no fuel column.
- Each source scraped a different slice: one brand per group, source 9 only listings around 95k BAM and up. "Median price by brand" shows how the data was collected, not the market.
- Source 9 has 90 listings under 1,000 km. 86 of them are 2024-2025 cars from dealers ("na stanju" = in stock), so they're real new cars, not errors.
- Source 1's power field is assumed kW (OLX asks for kW), but some sellers typed hp (e.g. a RAV4 2.2 D-CAT at "177"). Not fixable without parsing titles; left as a known limitation.
