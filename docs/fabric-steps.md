# Fabric setup, step by step

A walk-through for rebuilding this project in your own Fabric workspace, written for someone meeting Fabric for the first time. Fabric's UI changes often, so button names may differ slightly. About 2 hours end to end.

## 1. Access
- You need a Fabric capacity: a 60-day Fabric trial (work or school account), or a paid F SKU.
- Start a trial from your profile picture (top right) → **Start trial** → pick a region → **Activate**. If the button is missing, your tenant admin has disabled trials.
- The trial does not include Copilot or AI functions. Nothing here needs them.

## 2. Workspace
1. **Workspaces** → **+ New workspace**, e.g. `vehicle-data-dev`.
2. **Advanced** → license mode **Trial** (or your F capacity) → **Apply**.

A workspace is the unit you share, secure and deploy. Every item in it runs on the workspace's capacity.

## 3. Lakehouse and bronze files
1. In the workspace: **+ New item** → **Lakehouse**, e.g. `vehicles_lh` (letters, numbers, underscores).
2. Explorer → hover **Files** → `...` → **New subfolder** → `bronze`.
3. `bronze` → `...` → **Upload** → **Upload files** → the 12 CSVs from `data/raw/`.

**Files** holds raw files of any kind. **Tables** holds Delta tables, which Spark, the SQL endpoint and Power BI can all read. Both live in OneLake, and the SQL analytics endpoint is created automatically.

## 4. Notebook
1. Workspace → **Import** → **Notebook** → **From this computer** → `notebooks/vehicle_medallion.ipynb`.
2. Open it → Explorer → **Add data items** / **Add lakehouse** → **Existing** → `vehicles_lh`. It becomes the default lakehouse, so `Files/bronze` resolves.
3. **Run all**. The first cell waits for a Spark session.
4. Expected output: `to_number: 11/11 cases pass`, seven `PASS` lines, `silver_listings: 2737 rows`.
5. Lakehouse → **Tables** → `...` → **Refresh**: six tables under `dbo`.

Optional: switch to the SQL analytics endpoint and run
```sql
SELECT reject_reason, COUNT(*) FROM silver_rejects GROUP BY reject_reason;
```
It's the same Delta table, read with T-SQL instead of Spark.

## 5. Semantic model
1. Lakehouse ribbon → **New semantic model** → tick `fact_listing`, `dim_manufacturer`, `dim_fuel`. It is created in **Direct Lake** mode: no import, no refresh schedule.
2. Relationships (many-to-one, single direction):
   - `fact_listing[manufacturer]` → `dim_manufacturer[manufacturer]`
   - `fact_listing[fuel_type]` → `dim_fuel[fuel_type]`
3. Measures on `fact_listing`:
   ```DAX
   Listings = COUNTROWS(fact_listing)
   Median Price (BAM) = MEDIAN(fact_listing[price_bam])
   Median Mileage (km) = MEDIAN(fact_listing[mileage_km])
   Listings Label = FORMAT([Listings], "#,0")
   Diesel Listings = CALCULATE([Listings], dim_fuel[fuel_label] = "Diesel")
   Share of Listings = DIVIDE([Listings], CALCULATE([Listings], ALL(dim_manufacturer)))
   ```
   `Diesel Listings` replaces the slicer's fuel filter, so it doesn't move when you pick another fuel. `Share of Listings` removes only the brand filter for the denominator, so it's each brand's share within the selected fuel. Format it as a percentage.
4. Set `production_year` → **Summarize by: None**. Otherwise Power BI adds up years and they can't go on an axis.

Facts hold the numbers, dimensions hold what you filter by, measures hold the agreed calculations.

## 6. Report
- Card: `Listings Label`. Slicer: `dim_fuel[fuel_label]` (tile style, filter out blank).
- Bar chart: manufacturer × `Median Price (BAM)`, visual filter `Listings >= 10`.
- Column chart: `production_year` × `Median Price (BAM)`, visual filter `Listings >= 5`.
- Table: manufacturer, `Listings`, `Share of Listings`, medians, `Diesel Listings`; column auto-size **Grow to fit**.
- A text panel stating the data caveats (see README).

## 7. Deployment pipeline
1. Workspace → **Create deployment pipeline** → stages **Development** and **Test**.
2. Assign the workspace to Development → select the Test stage → tick all items → **Deploy**. A Test workspace is created.
3. What you'll see: every item arrives, the lakehouse arrives **empty**, and the Test semantic model binds to the Test lakehouse. So the Test report has no data until Test gets its own load (upload files, run the notebook). Definitions move between stages, data doesn't.

## 8. Item definitions in Git
- If your tenant allows it: **Workspace settings** → **Git integration** → GitHub or Azure DevOps → branch and folder → **Connect and sync**.
- If Git sync is disabled by policy, export definitions with the Fabric CLI and commit them yourself:
  ```bash
  pip install ms-fabric-cli
  fab auth login
  fab bulk-export "vehicle-data-dev.Workspace" -o ./fabric --recursive -f
  ```
  That's how the `fabric/` folder in this repo was produced: the notebook as Python, the semantic model as TMDL, the report as JSON.
- The way back works too: edit a definition and import it. The report's `definition.pbir` points to the model by folder (`byPath`), which suits Git, but the import API wants `byConnection` (`semanticmodelid=<model id>`). Import from a copy with that swapped:
  ```bash
  fab get "vehicle-data-dev.Workspace/vehicles_model.SemanticModel" -q id
  fab import "vehicle-data-dev.Workspace/vehicles_report.Report" -i <copy>/vehicles_report.Report -f
  ```
