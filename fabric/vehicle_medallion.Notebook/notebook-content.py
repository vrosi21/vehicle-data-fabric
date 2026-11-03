# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "24f6cfe5-12d0-4b66-b9af-f5a2fc61e11b",
# META       "default_lakehouse_name": "vehicles_lh",
# META       "default_lakehouse_workspace_id": "192d94da-0f3e-4809-a2ff-4402fc17463d",
# META       "known_lakehouses": [
# META         {
# META           "id": "24f6cfe5-12d0-4b66-b9af-f5a2fc61e11b"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Vehicle listings: bronze → silver → gold
# 
# Port of the cleaning in [vehicle-data-lab](https://github.com/vrosi21/vehicle-data-lab) (`02_data_preprocessing.ipynb`, pandas) to a Fabric lakehouse (PySpark + Delta).
# Same 12 OLX.ba source files, same rename map, thresholds and manual fixes. Three bugs in the original were found while porting and are fixed here (section 3).
# 
# | Layer | What | Where |
# |---|---|---|
# | Bronze | raw CSVs as uploaded + one table with all 12 stacked, everything still text | `Files/bronze/`, `bronze_listings` |
# | Silver | typed, cleaned, one row per listing; rejected rows kept with a reason | `silver_listings`, `silver_rejects` |
# | Gold | star schema for Power BI | `fact_listing`, `dim_manufacturer`, `dim_fuel` |
# 
# **One deliberate change from the original:** the original imputed missing price, mileage, power and year with mean/median, because it fed an ML model. A report should not show invented values, so here missing stays missing (and a listing without a price is rejected).


# MARKDOWN ********************

# ## 0. Parameters
# In Fabric: `...` on this cell → **Toggle parameter cell**. `LOCAL_TEST = True` only on a laptop (tables become temp views instead of Delta tables).

# CELL ********************

BRONZE_PATH = "Files/bronze"   # lakehouse folder holding the 12 raw CSVs
LOCAL_TEST = False             # True = laptop test run, no Delta writes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Load the 12 sources (bronze)
# Each source was scraped by a different group, so column names differ. `RENAME_MAP` is the original notebook's map (lowercased), with each raw name pointing to one canonical name.
# Source-specific quirks, all from the original notebook:
# - **Source 7** uses `;` as separator.
# - **Source 10** keeps colour inside an `all_attributes` text dict; `Boja` (colour) is pulled out of it.
# - **Source 10** labels its power column `engine_power_kW`, but the values are horsepower (they equal `Konjskih snaga` in all 45 rows, e.g. "Ferrari F8 ... 720PS" → 720). It is read as hp. *Fix #2.*

# CELL ********************

from functools import reduce
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SOURCES = [(f"vehicle_listings_{i:03d}.csv", ";" if i == 7 else ",") for i in range(1, 13)]

# raw column name (lowercased) -> canonical name. From the original RENAME_MAP.
RENAME_MAP = {
    "listing_title": "listing_title", "title": "listing_title",
    "listing_url": "listing_url", "url": "listing_url",
    "price_bam": "price_bam", "price": "price_bam",
    "manufacturer": "manufacturer", "make": "manufacturer",
    "model": "model",
    "production_year": "production_year", "year": "production_year", "year_of_making": "production_year",
    "milage_km": "mileage_km", "mileage_km": "mileage_km",
    "engine_power_kw": "engine_power_kw", "engine_kw": "engine_power_kw", "engine_power": "engine_power_kw",
    "engine_power_hp": "engine_power_hp",
    "fuel_type": "fuel_type", "fuel type": "fuel_type",
    "transmission": "transmission",
    "color": "color",
}
CANONICAL = ["listing_title", "listing_url", "price_bam", "manufacturer", "model", "production_year",
             "engine_power_kw", "engine_power_hp", "mileage_km", "fuel_type", "transmission", "color"]
POWER_IS_HP = {"vehicle_listings_010.csv"}   # fix #2: this file's 'engine_power_kW' holds hp


def read_source(file_name, sep):
    df = (spark.read
          .option("header", True)
          .option("sep", sep)
          .option("quote", '"')
          .option("escape", '"')       # quotes inside values are doubled
          .option("multiLine", True)   # source 12 has a title with a line break inside it
          .csv(f"{BRONZE_PATH}/{file_name}"))
    renames = dict(RENAME_MAP)
    if file_name in POWER_IS_HP:
        renames["engine_power_kw"] = "engine_power_hp"

    cols = {}
    for raw in df.columns:
        target = renames.get(raw.strip().lower())
        if target and target not in cols:
            cols[target] = F.col(f"`{raw}`").cast("string")
    if "all_attributes" in [c.lower() for c in df.columns] and "color" not in cols:
        cols["color"] = F.regexp_extract("all_attributes", r"'Boja': '([^']*)'", 1)

    return df.select(*[cols.get(c, F.lit(None).cast("string")).alias(c) for c in CANONICAL],
                     F.lit(file_name).alias("source_file"))


def save_table(df, name):
    if LOCAL_TEST:
        df.createOrReplaceTempView(name)
    else:
        df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(name)


bronze = reduce(DataFrame.unionByName, [read_source(f, sep) for f, sep in SOURCES])
save_table(bronze, "bronze_listings")
print(f"bronze_listings: {spark.table('bronze_listings').count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Number parsing, with tests
# OLX writes numbers Bosnian-style: `11.999 KM`, `350.000km` (dot = thousands separator). Some scrapers wrote plain floats: `7450.0`.
# Rule (same as the original's price cleaner): a dot followed by exactly three digits, repeated, is a thousands separator.
# The original applied this rule to **price only**. Mileage went through a regex that kept the dots, so `350.000km` became `350.0` km: 739 values in sources 1, 7 and 12 were 1000x too small. *Fix #1:* one parser for every number, tested below before anything uses it.

# CELL ********************

def to_number(col):
    s = F.regexp_replace(F.lower(F.trim(col)), r"\s*km$", "")                 # '350.000km' -> '350.000'
    s = F.when(s.rlike(r"^\d{1,3}(\.\d{3})+$"), F.regexp_replace(s, r"\.", "")).otherwise(s)
    return F.when(s.rlike(r"^\d+(\.\d+)?$"), s.cast("double"))               # anything else -> null


CASES = [("11.999 KM", 11999.0), ("350.000km", 350000.0), ("181.241km", 181241.0), ("2.900.000km", 2900000.0),
         ("7450.0", 7450.0), ("900 KM", 900.0), ("160.92262303875552", 160.92262303875552),
         ("Na upit", None), ("Po dogovoru", None), ("-1.0", None), ("None", None)]
got = spark.createDataFrame(CASES, "raw string, expected double").withColumn("parsed", to_number(F.col("raw"))).collect()
bad = [(r.raw, r.expected, r.parsed) for r in got if r.expected != r.parsed]
assert not bad, f"to_number broken: {bad}"
print(f"to_number: {len(CASES)}/{len(CASES)} cases pass")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Silver: clean, merge duplicates, apply manual fixes
# Order follows the original notebook:
# 1. **Parse + convert units.** hp → kW (×0.7355) wherever a column holds hp, decided by column, not by file position (the original converted "sources 4 and 12" by list index).
# 2. **Standardise categories** (original rules): trim, `Mercedes` → `Mercedes-Benz`, colour `Nepoznato` → `Unknown`, model `Drugi` ("other") → missing.
# 3. **Merge duplicates** (original rule: keep the first non-missing value per column). *Fix #3:* match on the listing ID number, not the URL text. `www.olx.ba/artikal/1` and `olx.ba/artikal/1/` are the same listing; the text match missed 9 of 49 duplicates.
# 4. **Manual fixes** from the original notebook: 12 listings whose duplicates disagreed, resolved by hand (2 removed).
# 5. **Outliers** (original thresholds): mileage > 900,000 km → missing; price below 50 KM → treated as no price; power outside 30-600 kW → missing. The original dropped the whole listing for bad power, which throws away a valid price over one bad field (values like 0, 0.7 or 5575 kW). Known cost: a few real Renault R4s (~25 kW) lose their power value.
# 6. `Unknown` for missing transmission / fuel / colour (original), done after the merge so `Unknown` never wins over a real value.


# CELL ********************

HP_TO_KW = 0.7355
NO_PRICE = ["na upit", "po dogovoru", "-1.0", "n/a", "none"]   # expected "no price" spellings

parsed = (spark.table("bronze_listings")
    .withColumn("listing_id", F.regexp_extract("listing_url", r"artikal/(\d+)", 1))
    .withColumn("listing_id", F.when(F.col("listing_id") != "", F.col("listing_id")))
    .withColumn("price_raw", F.col("price_bam"))
    .withColumn("price_bam", to_number(F.col("price_bam")))
    .withColumn("production_year", to_number(F.col("production_year")).cast("int"))
    .withColumn("mileage_km", to_number(F.col("mileage_km")).cast("int"))
    .withColumn("engine_power_kw", F.round(F.coalesce(to_number(F.col("engine_power_kw")),
                                                      to_number(F.col("engine_power_hp")) * HP_TO_KW), 1))
    .withColumn("unparsed_price", F.col("price_raw").isNotNull() & F.col("price_bam").isNull()
                                  & ~F.lower(F.trim("price_raw")).isin(NO_PRICE))
    .drop("engine_power_hp"))

for c in ["listing_title", "manufacturer", "model", "fuel_type", "transmission", "color"]:
    parsed = parsed.withColumn(c, F.when(F.trim(c) != "", F.trim(c)))
parsed = (parsed
    .withColumn("manufacturer", F.when(F.col("manufacturer") == "Mercedes", "Mercedes-Benz").otherwise(F.col("manufacturer")))
    .withColumn("color", F.when(F.lower("color") == "nepoznato", None).otherwise(F.col("color")))
    .withColumn("model", F.when(F.lower("model").isin("drugi", "nan"), None).otherwise(F.col("model"))))

# --- merge duplicates: first non-missing value per column, sources in file order ---
VALUE_COLS = ["listing_title", "price_bam", "manufacturer", "model", "production_year",
              "engine_power_kw", "mileage_km", "fuel_type", "transmission", "color", "source_file", "price_raw"]
by_id = Window.partitionBy("listing_id").orderBy("source_file")
whole = by_id.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
with_id = parsed.filter(F.col("listing_id").isNotNull())
merged = with_id
for c in VALUE_COLS:
    merged = merged.withColumn(c, F.first(c, ignorenulls=True).over(whole))
merged = (merged
    .withColumn("unparsed_price", F.max(F.col("unparsed_price").cast("int")).over(whole) == 1)
    .withColumn("n_sources", F.count("*").over(whole))
    .withColumn("_n", F.row_number().over(by_id))
    .filter("_n = 1").drop("_n"))
n_merged_away = with_id.count() - merged.count()

# --- manual fixes from the original notebook (listing id -> values) ---
MANUAL_FIXES = [  # listing_id, engine_power_kw, mileage_km, price_bam, remove
    ("66956069", 350.0, 127000, None, False), ("68347869", 350.0, 163000, None, False),
    ("68982587", None, 60000, None, False),   ("70103813", None, None, 97000.0, False),
    ("70242959", 335.0, 309807, None, False), ("71021446", None, 107000, None, False),
    ("71088002", None, None, None, True),     ("71488901", None, 20200, None, False),
    ("71987321", None, 93090, None, False),   ("72119148", None, 194000, None, False),
    ("72126673", 430.0, 27400, None, False),  ("72224585", None, None, None, True),
]
fixes = spark.createDataFrame(MANUAL_FIXES, "listing_id string, fix_kw double, fix_km int, fix_price double, remove boolean")
fixed = (merged.join(F.broadcast(fixes), "listing_id", "left")
    .withColumn("engine_power_kw", F.coalesce("fix_kw", "engine_power_kw"))
    .withColumn("mileage_km", F.coalesce("fix_km", "mileage_km"))
    .withColumn("price_bam", F.coalesce("fix_price", "price_bam"))
    .withColumn("removed", F.coalesce("remove", F.lit(False)))
    .drop("fix_kw", "fix_km", "fix_price", "remove"))

# --- outliers (original thresholds) ---
fixed = (fixed
    .withColumn("mileage_km", F.when(F.col("mileage_km") <= 900_000, F.col("mileage_km")))
    .withColumn("production_year", F.when(F.col("production_year").between(1950, 2025), F.col("production_year")))
    .withColumn("engine_power_kw", F.when(F.col("engine_power_kw").between(30, 600), F.col("engine_power_kw")))
    .withColumn("price_bam", F.when(F.col("price_bam") >= 50, F.col("price_bam"))))

reason = (F.when(F.col("removed"), "removed in manual review")
           .when(F.col("unparsed_price"), "unparsed price")
           .when(F.col("price_bam").isNull(), "no price"))
fixed = fixed.withColumn("reject_reason", reason)

for c in ["fuel_type", "transmission", "color"]:
    fixed = fixed.withColumn(c, F.coalesce(c, F.lit("Unknown")))
fixed = fixed.withColumn("listing_url", F.concat(F.lit("https://olx.ba/artikal/"), "listing_id"))

no_id = (parsed.filter(F.col("listing_id").isNull())
         .select(F.lit("no listing id").alias("reject_reason"), "listing_title", "source_file", "price_raw"))

SILVER_COLS = ["listing_id", "listing_title", "manufacturer", "model", "production_year", "price_bam",
               "mileage_km", "engine_power_kw", "fuel_type", "transmission", "color", "listing_url",
               "source_file", "n_sources"]
save_table(fixed.filter(F.col("reject_reason").isNull()).select(*SILVER_COLS), "silver_listings")
save_table(fixed.filter(F.col("reject_reason").isNotNull())
                .select("reject_reason", "listing_title", "source_file", "price_raw", "listing_id", "engine_power_kw")
                .unionByName(no_id, allowMissingColumns=True),
           "silver_rejects")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Quality gate
# The port was drafted with an AI assistant (Claude Code). These checks decide whether its output is trusted: if one fails, the notebook stops before gold is built.

# CELL ********************

silver = spark.table("silver_listings")
rejects = spark.table("silver_rejects")
n_raw, n_silver, n_rej = spark.table("bronze_listings").count(), silver.count(), rejects.count()

display(rejects.groupBy("reject_reason").count().orderBy(F.desc("count")))
print(f"raw {n_raw} = silver {n_silver} + rejected {n_rej} + merged duplicates {n_merged_away}")

checks = {
    "every raw row accounted for":         n_silver + n_rej + n_merged_away == n_raw,
    "listing_id unique":                   silver.select("listing_id").distinct().count() == n_silver,
    "no price missing in silver":          silver.filter(F.col("price_bam").isNull()).count() == 0,
    "parser handled every price":          rejects.filter(F.col("reject_reason") == "unparsed price").count() == 0,
    "fix 1: no dotted mileage under 1000": silver.join(spark.table("bronze_listings")
                                                .filter(F.col("mileage_km").rlike(r"\d\.\d{3}"))
                                                .select(F.regexp_extract("listing_url", r"artikal/(\d+)", 1).alias("listing_id")),
                                                "listing_id").filter(F.col("mileage_km") < 1000).count() == 0,
    "fix 2: source 10 power under 600 kW": silver.filter((F.col("source_file") == "vehicle_listings_010.csv")
                                                       & (F.col("engine_power_kw") > 600)).count() == 0,
    "reject rate under 20%":               n_rej / n_raw < 0.20,
}
for name, ok in checks.items():
    print(("PASS " if ok else "FAIL ") + name)
assert all(checks.values()), "Quality gate failed, gold not built"
print(f"silver_listings: {n_silver} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Gold: star schema
# One fact table (one row per listing) and two small dimensions. The semantic model joins them; Power BI slices the fact by the dimensions.
# `general_model` groups variant spellings into one model (the original's mapping). `dim_fuel` translates the Bosnian OLX labels for an English audience.

# CELL ********************

GENERAL_MODEL = [  # manufacturer, model (lowercased) -> general model. From the original notebook.
    ("Kia", "sportage 1.6 crdi ex fresh", "Sportage"), ("Kia", "sportage 2021 - dct7 - black edition", "Sportage"),
    ("Kia", "sportage 2.0 crdi 4x4 automat uvoz ch", "Sportage"),
    ("Kia", "sportage 2.0 crdi style 4wd uvoz švicarska -automatik", "Sportage"),
    ("Kia", "cee'd / ceed 2023", "Ceed"), ("Kia", "cee'd / ceed 2016", "Ceed"),
    ("Kia", "ceed 1.6 benzin/plin 2009 model / moze zamjena", "Ceed"),
    ("Kia", "cee'd / ceed registrovan, plin top stanje", "Ceed"), ("Kia", "cee'd sportswagon", "Ceed"),
    ("Kia", "pro cee'd / proceed", "Ceed"),
    ("BMW", "x5 m50", "X5 M"), ("BMW", "640 gran coupe", "640"), ("BMW", "328 gran turismo", "328"), ("BMW", "m340i", "M3"),
    ("Ford", "ka / ka+", "Ka"), ("Renault", "megane grandcoupe", "Megane"), ("Renault", "megane grandtour", "Megane"),
]
gm = spark.createDataFrame(GENERAL_MODEL, "manufacturer string, model_lc string, gm string")

fuel_labels = spark.createDataFrame([
    ("Benzin", "Petrol"), ("Dizel", "Diesel"), ("Hibrid", "Hybrid"),
    ("Plin", "LPG"), ("Elektro", "Electric"), ("Unknown", "Unknown"),
], ["fuel_type", "fuel_label"])

fact = (silver.withColumn("model_lc", F.lower("model"))
    .join(F.broadcast(gm), ["manufacturer", "model_lc"], "left")
    .withColumn("general_model", F.coalesce("gm", "model"))
    .select("listing_id", "manufacturer", "model", "general_model", "production_year", "fuel_type",
            "transmission", "color", "price_bam", "mileage_km", "engine_power_kw", "source_file"))

unknown_fuels = fact.select("fuel_type").distinct().join(fuel_labels, "fuel_type", "left_anti").collect()
assert not unknown_fuels, f"fuel value without a label in dim_fuel: {unknown_fuels}"

save_table(fact, "fact_listing")
save_table(fact.select("manufacturer").distinct().orderBy("manufacturer"), "dim_manufacturer")
save_table(fuel_labels, "dim_fuel")

for t in ["fact_listing", "dim_manufacturer", "dim_fuel"]:
    print(t, spark.table(t).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Quick look
# The question the report answers: median asking price by manufacturer.
# Caveat: each source scraped a different slice (some one brand, source 9 only premium listings), so this shows how the data was collected, not the whole market.

# CELL ********************

display(spark.table("fact_listing").groupBy("manufacturer")
        .agg(F.count("*").alias("listings"),
             F.expr("percentile(price_bam, 0.5)").alias("median_price_bam"),
             F.expr("percentile(mileage_km, 0.5)").alias("median_mileage_km"))
        .orderBy(F.desc("listings")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
