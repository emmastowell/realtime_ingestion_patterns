# Databricks notebook source
# MAGIC %md
# MAGIC # WebTRIS Traffic Data Fetcher
# MAGIC
# MAGIC Fetches the latest 15-minute traffic readings from the
# MAGIC WebTRIS REST API for the focus motorways (M25, M1, M6) and writes them
# MAGIC as JSON files to the UC Volume for Auto Loader ingestion.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Reference data loaded (run `run_reference_loader` first)
# MAGIC - UC Volume `raw_traffic` created in the target catalog/schema
# MAGIC - Packages: `httpx`, `pydantic`

# COMMAND ----------

# MAGIC %pip install httpx pydantic
# COMMAND ----------

import sys
sys.path.insert(0, "../src")

from nh_dataflows.ingestion.webtris_fetcher import run

run(spark)
