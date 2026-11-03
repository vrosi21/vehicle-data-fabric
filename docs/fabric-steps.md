# Fabric build, click by click

Time box: ~2.5 h for steps 1-6, step 7 only if time is left.
After each step, tell Claude what you see (or paste the error). Fabric's UI moves things around, so names may differ slightly.

## 1. Access (15 min)
1. Open `app.fabric.microsoft.com`, sign in with the ETF account.
2. Profile photo (top right) → **Start trial** → pick region (West/North Europe) → **Activate**.
3. No **Start trial** button = tenant blocked. Stop and tell Claude (fallbacks: own Azure tenant, F2 capacity, local).

Note: the trial does not include Copilot. That's fine, we don't need it.

## 2. Workspace (5 min)
1. Left bar → **Workspaces** → **+ New workspace**.
2. Name: `vehicle-data-dev`.
3. **Advanced** → License mode: **Fabric Trial** (or "Trial") → **Apply**.

Why: a workspace is like a Power Platform environment. Items live in it, and it runs on a capacity (here the trial).

## 3. Lakehouse + upload bronze (10 min)
1. In the workspace: **+ New item** → **Lakehouse** → name `vehicles_lh` (letters, numbers, underscore only).
2. In the lakehouse explorer, left side: **Files** → `...` → **New subfolder** → `bronze`.
3. `bronze` → `...` → **Upload** → **Upload files** → select all 12 files from `C:\Projects\vehicle-data-fabric\data\raw\`.

Why: **Files** = any raw files (the lake). **Tables** = Delta tables, which Spark, SQL and Power BI can all read. Both sit in OneLake.

## 4. Notebook (20-30 min)
1. Back in the workspace: **Import** → **Notebook** → **From this computer** → `notebooks\vehicle_medallion.ipynb`.
2. Open it. Left pane (Explorer) → **Add data items** / **Lakehouses** → **Existing lakehouse** → `vehicles_lh`. It becomes the default lakehouse, which is how `Files/bronze` resolves.
3. Read the markdown cells top to bottom (5 min). You should be able to explain every cell. What changed vs your original notebook: `docs/port-notes.md`.
4. **Run all**. The first run waits for a Spark session (can take a minute or more).
5. Expected: `to_number: 11/11 cases pass`, then seven `PASS` lines and `silver_listings: 2737 rows`.
6. Lakehouse → **Tables** → `...` → **Refresh**. You should see: `bronze_listings`, `silver_listings`, `silver_rejects`, `fact_listing`, `dim_manufacturer`, `dim_fuel`.
7. Optional, 2 min: click `silver_rejects` to preview it, or open the **SQL analytics endpoint** (top right switcher) and run:
   ```sql
   SELECT reject_reason, COUNT(*) FROM silver_rejects GROUP BY reject_reason;
   ```
   Same Delta table, queried with SQL instead of Spark. That's the OneLake point.

## 5. Semantic model (30 min, the new part for you)
1. In the lakehouse ribbon: **New semantic model** → name `vehicles_model` → tick `fact_listing`, `dim_manufacturer`, `dim_fuel` → **Confirm**.
   It is created in **Direct Lake** mode: Power BI reads the Delta files directly. No import, no scheduled refresh.
2. Open the model (**Open data model** / **Edit tables**, in the web editor).
3. Relationships, drag column onto column:
   - `fact_listing[manufacturer]` → `dim_manufacturer[manufacturer]`
   - `fact_listing[fuel_type]` → `dim_fuel[fuel_type]`
   Both should show many-to-one (`*` → `1`), single direction.
4. Select `fact_listing` → **New measure**, one at a time:
   ```DAX
   Listings = COUNTROWS(fact_listing)
   ```
   ```DAX
   Median Price (BAM) = MEDIAN(fact_listing[price_bam])
   ```
   ```DAX
   Avg Price (BAM) = AVERAGE(fact_listing[price_bam])
   ```
   ```DAX
   Avg Mileage (km) = AVERAGE(fact_listing[mileage_km])
   ```
5. Optional polish: hide `fact_listing[fuel_type]` (users should slice by `dim_fuel[fuel_label]`).

Why: the model is the business layer. Relationships say how tables join; measures are the agreed definitions ("what is 'median price'"), so every report uses the same numbers.

## 6. Report (20-30 min)
1. From the model: **New report** / **Create report** → start blank.
2. Visuals:
   - **Card**: `Listings`
   - **Bar chart**: Y = `dim_manufacturer[manufacturer]`, X = `Median Price (BAM)`
   - **Column chart**: X = `fact_listing[year]`, Y = `Median Price (BAM)`
   - **Slicer**: `dim_fuel[fuel_label]`
   - **Table**: manufacturer, `Listings`, `Median Price (BAM)`, `Avg Mileage (km)`
3. Save as `vehicles_report`.
4. Click a fuel in the slicer and watch all visuals filter. That's the relationship working.
5. Screenshot the report for the write-up.

Watch out (good interview point): each raw file was scraped differently, some by brand and file 009 only premium listings, so "median price by brand" reflects how the data was collected, not the market. Say that, don't hide it.

## 7. Git + deployment pipeline (optional, 45-60 min)
Needs a GitHub repo first (tell Claude, it can set this folder up as one).
1. Workspace → **Workspace settings** → **Git integration** → **GitHub** → **Add account**: a fine-grained personal access token *you* create on GitHub (Contents: read and write, this repo only) + repo URL.
2. Connect: branch `main`, folder `fabric` → **Connect and sync**.
3. **Source control** (workspace toolbar) → commit all items. On GitHub: each item is a folder of text files (`.Notebook`, `.SemanticModel`, `.Report`, `.Lakehouse`).
4. Change a measure → Source control shows 1 change → commit. That's the diff-able ALM story.
5. Workspace → **Create deployment pipeline** → stages Development / Test → assign `vehicle-data-dev` to Development → **Deploy** to Test.
6. Open the Test workspace and note what did NOT come over: the lakehouse is there but **empty** (definitions move, data doesn't), and the notebook still points at a lakehouse. Fixing that is what **deployment rules** and per-stage runs are for.
   Dataverse parallel: solutions carry schema and components, never environment data; environment variables and connection references are the per-stage bindings.
