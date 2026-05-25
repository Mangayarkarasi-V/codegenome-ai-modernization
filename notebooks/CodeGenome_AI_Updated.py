# Databricks notebook source
displayHTML("""
<div style="background: linear-gradient(90deg,#ff3621,#c71f16);
            padding:35px;
            border-radius:12px;
            text-align:center;
            color:white;
            font-family:Arial;">

<img src='https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png'
     width='220'
     style='background:white;padding:10px;border-radius:10px;'/>

<h1>Databricks Hackathon 2026</h1>

<h2>Building Intelligent Apps with Data + AI</h2>

<h3>CodeGenome AI Enterprise Mainframe Modernization Intelligence Platform</h3>

</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Setup

# COMMAND ----------

# MAGIC %pip install pandas networkx matplotlib

# COMMAND ----------

# MAGIC %md
# MAGIC # CodeGenome AI
# MAGIC
# MAGIC ## AI-Powered Mainframe Modernization Accelerator
# MAGIC
# MAGIC CodeGenome AI is an AI-powered modernization accelerator built on Databricks Lakehouse.
# MAGIC
# MAGIC The solution analyzes enterprise legacy assets including:
# MAGIC
# MAGIC - COBOL Programs
# MAGIC - JCL Workflows
# MAGIC - DB2 Dependencies
# MAGIC - Copybook Structures
# MAGIC
# MAGIC Key capabilities:
# MAGIC - Dependency Intelligence
# MAGIC - DB2 Lineage Analysis
# MAGIC - Business Rule Extraction
# MAGIC - Modernization Readiness Scoring
# MAGIC - AI-powered Query Assistant
# MAGIC - Interactive Dashboards
# MAGIC
# MAGIC Built using:
# MAGIC - Databricks Lakehouse
# MAGIC - Delta Lake
# MAGIC - Unity Catalog
# MAGIC - Python
# MAGIC - SQL
# MAGIC - AI/ML Workflows

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Ingestion & Legacy Code Upload

# COMMAND ----------

import os

base_path = "/Volumes/workspace/default/codegenome"

for file_name in os.listdir(base_path):
    print(file_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Ingestion Layer

# COMMAND ----------

import os

base_path = "/Volumes/workspace/default/codegenome/CodeGenome_Enhanced_Mainframe_Files"

all_files = []

for file_name in os.listdir(base_path):
    file_path = os.path.join(base_path, file_name)

    if os.path.isfile(file_path):
        all_files.append(file_name)

print("===== CODEGENOME ENTERPRISE FILE DISCOVERY =====")
print(f"Total Files Found: {len(all_files)}")

for file_name in all_files:
    print(file_name)

# COMMAND ----------

import os
import pandas as pd

base_path = "/Volumes/workspace/default/codegenome/CodeGenome_Enhanced_Mainframe_Files"

all_files = []

for root, dirs, files in os.walk(base_path):
    for file in files:
        file_path = os.path.join(root, file)

        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        ext = file.split(".")[-1].lower()

        file_type = {
            "cbl": "CBL",
            "jcl": "JCL",
            "cpy": "CPY",
            "sql": "SQL",
            "csv": "CSV"
        }.get(ext, "OTHER")

        all_files.append({
            "file_name": file,
            "file_path": file_path,
            "file_type": file_type,
            "content": content
        })

df_files = pd.DataFrame(all_files)

print("Total files loaded:", len(df_files))
display(df_files[["file_name", "file_type"]].head(30))

# COMMAND ----------

from pyspark.sql.types import *
import pandas as pd

# -------------------------------------------------
# SAFE EMPTY DATAFRAME HANDLING
# -------------------------------------------------

if 'df_files' not in globals() or df_files.empty:

    df_files = pd.DataFrame(columns=[
        "file_name",
        "file_path",
        "file_type",
        "content"
    ])

if 'df_dependencies' not in globals():

    df_dependencies = pd.DataFrame(columns=[
        "file_name",
        "dependency_type",
        "dependency_name"
    ])

if 'df_db2_lineage' not in globals():

    df_db2_lineage = pd.DataFrame(columns=[
        "file_name",
        "operation",
        "db2_table"
    ])

if 'df_risk' not in globals():

    df_risk = pd.DataFrame(columns=[
        "file_name",
        "risk_level",
        "risk_score",
        "matched_rules"
    ])

# -------------------------------------------------
# DEFINE SCHEMAS
# -------------------------------------------------

files_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("file_type", StringType(), True),
    StructField("content", StringType(), True)
])

dependency_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("dependency_type", StringType(), True),
    StructField("dependency_name", StringType(), True)
])

db2_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("db2_table", StringType(), True)
])

risk_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("risk_level", StringType(), True),
    StructField("risk_score", StringType(), True),
    StructField("matched_rules", StringType(), True)
])

# -------------------------------------------------
# CONVERT TO SPARK DATAFRAMES
# -------------------------------------------------

spark_df_files = spark.createDataFrame(
    df_files.astype(str),
    schema=files_schema
)

spark_df_dependencies = spark.createDataFrame(
    df_dependencies.astype(str),
    schema=dependency_schema
)

spark_df_db2 = spark.createDataFrame(
    df_db2_lineage.astype(str),
    schema=db2_schema
)

spark_df_risk = spark.createDataFrame(
    df_risk.astype(str),
    schema=risk_schema
)

# -------------------------------------------------
# CREATE PERMANENT DELTA TABLES
# -------------------------------------------------

spark_df_files.write.mode("overwrite").saveAsTable(
    "workspace.default.codegenome_files"
)

spark_df_dependencies.write.mode("overwrite").saveAsTable(
    "workspace.default.codegenome_dependencies"
)

spark_df_db2.write.mode("overwrite").saveAsTable(
    "workspace.default.codegenome_db2_lineage"
)

spark_df_risk.write.mode("overwrite").saveAsTable(
    "workspace.default.codegenome_risk"
)

print("All Delta Tables Created Successfully")

# COMMAND ----------

# Create permanent Delta tables for SQL Editor

spark_df_files.write.mode("overwrite").saveAsTable("workspace.default.codegenome_files")
spark_df_dependencies.write.mode("overwrite").saveAsTable("workspace.default.codegenome_dependencies")
spark_df_db2.write.mode("overwrite").saveAsTable("workspace.default.codegenome_db2_lineage")
spark_df_risk.write.mode("overwrite").saveAsTable("workspace.default.codegenome_risk")

print("Permanent Delta tables created successfully")

# COMMAND ----------

# ------------------------------------------
# FILE CLASSIFICATION SUMMARY
# Works when all_files contains dictionaries
# ------------------------------------------

cobol_files = [
    f["file_name"] for f in all_files
    if f["file_name"].lower().endswith(".cbl")
]

jcl_files = [
    f["file_name"] for f in all_files
    if f["file_name"].lower().endswith(".jcl")
]

copybook_files = [
    f["file_name"] for f in all_files
    if f["file_name"].lower().endswith(".cpy")
]

sql_files = [
    f["file_name"] for f in all_files
    if f["file_name"].lower().endswith(".sql")
]

csv_files = [
    f["file_name"] for f in all_files
    if f["file_name"].lower().endswith(".csv")
]

print("===== FILE CLASSIFICATION SUMMARY =====")
print(f"COBOL Programs : {len(cobol_files)}")
print(f"JCL Jobs       : {len(jcl_files)}")
print(f"Copybooks      : {len(copybook_files)}")
print(f"SQL Files      : {len(sql_files)}")
print(f"CSV Files      : {len(csv_files)}")

# COMMAND ----------

# MAGIC %md
# MAGIC Enterprise Modernization KPIs

# COMMAND ----------

print("===== CODEGENOME ENTERPRISE KPI =====")

print(f"Total Legacy Assets : {len(all_files)}")
print(f"COBOL Programs      : {len(cobol_files)}")
print(f"JCL Jobs            : {len(jcl_files)}")
print(f"Copybooks           : {len(copybook_files)}")
print(f"DB2 SQL Files       : {len(sql_files)}")
print(f"Metadata Files      : {len(csv_files)}")

print("\nAI Dependency Discovery Completed")
print("AI Modernization Assessment Generated")
print("Enterprise Migration Intelligence Ready")

# COMMAND ----------

# MAGIC %md
# MAGIC AI Metadata Extraction Engine

# COMMAND ----------

metadata = []

for item in all_files:

    file_name = item["file_name"]

    if file_name.lower().endswith(".cbl"):
        file_type = "COBOL Program"

    elif file_name.lower().endswith(".jcl"):
        file_type = "JCL Batch Job"

    elif file_name.lower().endswith(".cpy"):
        file_type = "Copybook"

    elif file_name.lower().endswith(".sql"):
        file_type = "DB2 SQL"

    elif file_name.lower().endswith(".csv"):
        file_type = "Metadata File"

    else:
        file_type = "Unknown"

    metadata.append((file_name, file_type))

for item in metadata:
    print(item)

# COMMAND ----------

import os
import pandas as pd

files_data = []

# -----------------------------------------
# Recursive File Discovery
# -----------------------------------------

for root, dirs, files in os.walk(base_path):

    for file_name in files:

        file_path = os.path.join(root, file_name)

        try:

            with open(
                file_path,
                "r",
                encoding="utf-8",
                errors="ignore"
            ) as f:

                content = f.read()

            # -----------------------------------------
            # File Extension Classification
            # -----------------------------------------

            extension = file_name.split(".")[-1].lower()

            file_type_map = {
                "cbl": "COBOL",
                "jcl": "JCL",
                "cpy": "COPYBOOK",
                "sql": "SQL",
                "csv": "CSV"
            }

            file_type = file_type_map.get(extension, "OTHER")

            # -----------------------------------------
            # Metadata Collection
            # -----------------------------------------

            files_data.append({

                "file_name": file_name,

                "file_path": file_path,

                "file_type": file_type,

                "file_size_kb": round(
                    os.path.getsize(file_path) / 1024,
                    2
                ),

                "line_count": len(content.splitlines()),

                "preview": content[:500],

                "content": content

            })

        except Exception as e:

            files_data.append({

                "file_name": file_name,

                "file_path": file_path,

                "file_type": "ERROR",

                "file_size_kb": 0,

                "line_count": 0,

                "preview": str(e),

                "content": ""

            })

# -----------------------------------------
# Convert to DataFrame
# -----------------------------------------

df_files = pd.DataFrame(files_data)

print("===================================")
print("TOTAL FILES DISCOVERED :", len(df_files))
print("===================================")

display(
    df_files[
        [
            "file_name",
            "file_type",
            "file_size_kb",
            "line_count"
        ]
    ]
)

# COMMAND ----------

import os
import pandas as pd

base_path = "/Volumes/workspace/default/codegenome/CodeGenome_Enhanced_Mainframe_Files"

files_data = []

# ---------------------------------------------------
# ENTERPRISE FILE INGESTION
# ---------------------------------------------------

for root, dirs, files in os.walk(base_path):

    for file_name in files:

        file_path = os.path.join(root, file_name)

        if os.path.isfile(file_path):

            try:

                with open(
                    file_path,
                    "r",
                    encoding="utf-8",
                    errors="ignore"
                ) as f:

                    content = f.read()

                # ---------------------------------------------------
                # FILE TYPE DETECTION
                # ---------------------------------------------------

                extension = file_name.split(".")[-1].lower()

                file_type_map = {
                    "cbl": "COBOL",
                    "jcl": "JCL",
                    "cpy": "COPYBOOK",
                    "sql": "SQL",
                    "csv": "CSV"
                }

                file_type = file_type_map.get(
                    extension,
                    "OTHER"
                )

                # ---------------------------------------------------
                # STORE FILE METADATA
                # ---------------------------------------------------

                files_data.append({

                    "file_name": file_name,

                    "file_path": file_path,

                    "file_type": file_type,

                    "file_size_kb": round(
                        os.path.getsize(file_path) / 1024,
                        2
                    ),

                    "line_count": len(
                        content.splitlines()
                    ),

                    "preview": content[:500],

                    "content": content

                })

            except Exception as e:

                print(f"Error reading {file_name}: {str(e)}")

# ---------------------------------------------------
# CREATE DATAFRAME
# ---------------------------------------------------

df_files = pd.DataFrame(files_data)

# ---------------------------------------------------
# DISPLAY SUMMARY
# ---------------------------------------------------

print("========================================")
print("CODEGENOME ENTERPRISE INGESTION SUMMARY")
print("========================================")
print(f"Total Files Loaded : {len(df_files)}")

display(
    df_files[
        [
            "file_name",
            "file_type",
            "file_size_kb",
            "line_count"
        ]
    ]
)

# COMMAND ----------

import pandas as pd

df_files = pd.DataFrame(files_data)

display(df_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## COBOL Dependency Analysis Engine

# COMMAND ----------

import re

def extract_metadata(file_name, content):
    program = re.findall(r"PROGRAM-ID\.\s*([A-Z0-9-]+)", content, re.IGNORECASE)
    calls = re.findall(r"CALL\s+'([A-Z0-9-]+)'", content, re.IGNORECASE)
    copybooks = re.findall(r"COPY\s+([A-Z0-9-]+)", content, re.IGNORECASE)

    return {
        "file_name": file_name,
        "program": program[0] if program else "",
        "calls": ", ".join(calls),
        "copybooks": ", ".join(copybooks)
    }

dependency_results = []

for item in files_data:
    dependency_results.append(
        extract_metadata(item["file_name"], item["preview"])
    )

df_dependencies = pd.DataFrame(dependency_results)

display(df_dependencies)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JCL Job Flow Extraction

# COMMAND ----------

files_data = []

for file_name in os.listdir(base_path):
    file_path = os.path.join(base_path, file_name)

    with open(file_path, "r", errors="ignore") as f:
        content = f.read()

    files_data.append({
        "file_name": file_name,
        "file_type": file_name.split(".")[-1].upper(),
        "content": content
    })

df_files = pd.DataFrame(files_data)
display(df_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Program Dependency Processing

# COMMAND ----------

import re
import pandas as pd

def extract_metadata(file_name, content):
    program = re.findall(r"PROGRAM-ID\.\s*([A-Z0-9-]+)", content, re.IGNORECASE)
    calls = re.findall(r"CALL\s+'([A-Z0-9-]+)'", content, re.IGNORECASE)
    copybooks = re.findall(r"COPY\s+([A-Z0-9-]+)", content, re.IGNORECASE)

    return {
        "file_name": file_name,
        "program": program[0] if program else "",
        "calls": ", ".join(calls),
        "copybooks": ", ".join(copybooks)
    }

dependency_results = []

for _, row in df_files.iterrows():
    dependency_results.append(
        extract_metadata(row["file_name"], row["content"])
    )

df_dependencies = pd.DataFrame(dependency_results)
display(df_dependencies)

# COMMAND ----------

# MAGIC %md
# MAGIC COBOL File Preview & SQL Detection

# COMMAND ----------

for _, row in df_files.iterrows():

    if row["file_name"].lower().endswith(".cbl"):

        print("\n================================================")
        print("COBOL FILE :", row["file_name"])
        print("================================================")

        content = row["content"]

        print("\n--- FILE PREVIEW ---\n")
        print(content[:1000])

        sql_keywords = ["SELECT", "UPDATE", "INSERT", "DELETE"]

        found_keywords = []

        for keyword in sql_keywords:
            if keyword in content.upper():
                found_keywords.append(keyword)

        print("\n--- DB2 SQL ANALYSIS ---")

        if found_keywords:
            print("Embedded DB2 SQL Found :", ", ".join(found_keywords))
        else:
            print("No Embedded DB2 SQL Found")

        print("\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Job & Dataset Lineage Analysis

# COMMAND ----------

import re
import pandas as pd

# ---------------------------------------------------
# DB2 LINEAGE EXTRACTION
# ---------------------------------------------------

def extract_db2_lineage(file_name, file_type, content):

    db2_results = []

    content = str(content)

    patterns = {
        "SELECT": r"\bFROM\s+([A-Z0-9_\.]+)",
        "JOIN": r"\bJOIN\s+([A-Z0-9_\.]+)",
        "UPDATE": r"\bUPDATE\s+([A-Z0-9_\.]+)",
        "INSERT": r"\bINSERT\s+INTO\s+([A-Z0-9_\.]+)",
        "DELETE": r"\bDELETE\s+FROM\s+([A-Z0-9_\.]+)",
        "DECLARE_CURSOR": r"\bDECLARE\s+([A-Z0-9_]+)\s+CURSOR\s+FOR"
    }

    for operation, pattern in patterns.items():

        tables = re.findall(pattern, content, re.IGNORECASE)

        for table in tables:

            db2_results.append({
                "file_name": file_name,
                "file_type": file_type,
                "operation": operation,
                "db2_table": table.upper()
            })

    return db2_results


# ---------------------------------------------------
# JCL / BATCH JOB FLOW EXTRACTION
# ---------------------------------------------------

def extract_batch_lineage(file_name, file_type, content):

    batch_results = []

    if file_type not in ["JCL", "JCL Batch Job"]:
        return batch_results

    content = str(content)

    job_name_match = re.search(
        r"^//([A-Z0-9_#$@]+)\s+JOB\b",
        content,
        re.IGNORECASE | re.MULTILINE
    )

    job_name = job_name_match.group(1).upper() if job_name_match else file_name

    exec_steps = re.findall(
        r"^//([A-Z0-9_#$@]+)\s+EXEC\s+PGM=([A-Z0-9_#$@]+)",
        content,
        re.IGNORECASE | re.MULTILINE
    )

    datasets = re.findall(
        r"DSN=([A-Z0-9_\.\-]+)",
        content,
        re.IGNORECASE
    )

    for step_name, program_name in exec_steps:

        batch_results.append({
            "jcl_file": file_name,
            "job_name": job_name,
            "step_name": step_name.upper(),
            "program_name": program_name.upper(),
            "datasets": ", ".join(sorted(set([d.upper() for d in datasets])))
        })

    return batch_results


# ---------------------------------------------------
# RUN EXTRACTION FOR ALL FILES
# ---------------------------------------------------

db2_lineage_results = []
batch_lineage_results = []

for _, row in df_files.iterrows():

    file_name = row.get("file_name", "")
    file_type = row.get("file_type", "")
    content = row.get("content", "")

    db2_lineage_results.extend(
        extract_db2_lineage(file_name, file_type, content)
    )

    batch_lineage_results.extend(
        extract_batch_lineage(file_name, file_type, content)
    )


# ---------------------------------------------------
# CREATE DATAFRAMES
# ---------------------------------------------------

df_db2_lineage = pd.DataFrame(
    db2_lineage_results,
    columns=[
        "file_name",
        "file_type",
        "operation",
        "db2_table"
    ]
).drop_duplicates()

df_batch_lineage = pd.DataFrame(
    batch_lineage_results,
    columns=[
        "jcl_file",
        "job_name",
        "step_name",
        "program_name",
        "datasets"
    ]
).drop_duplicates()


# ---------------------------------------------------
# DISPLAY RESULTS
# ---------------------------------------------------

print("===== DB2 LINEAGE SUMMARY =====")
display(df_db2_lineage)

print("===== BATCH JOB / JCL LINEAGE SUMMARY =====")
display(df_batch_lineage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JCL Job Flow Extraction

# COMMAND ----------

import re
import pandas as pd

# ---------------------------------------------------------
# ENTERPRISE JCL FLOW EXTRACTION
# ---------------------------------------------------------

def extract_jcl_flow(file_name, content):

    jcl_results = []

    content = str(content)

    # ---------------------------------------------------------
    # JOB NAME
    # ---------------------------------------------------------

    job_name_match = re.search(
        r"^//([A-Z0-9_#$@]+)\s+JOB",
        content,
        re.IGNORECASE | re.MULTILINE
    )

    job_name = (
        job_name_match.group(1).upper()
        if job_name_match else file_name
    )

    # ---------------------------------------------------------
    # EXEC STEP EXTRACTION
    # ---------------------------------------------------------

    exec_steps = re.findall(
        r"^//([A-Z0-9_#$@]+)\s+EXEC\s+(?:PGM=|PROC=)?([A-Z0-9_#$@]+)",
        content,
        re.IGNORECASE | re.MULTILINE
    )

    # ---------------------------------------------------------
    # DATASET EXTRACTION
    # ---------------------------------------------------------

    datasets = re.findall(
        r"DSN=([A-Z0-9\.\-\_\(\)\+\&]+)",
        content,
        re.IGNORECASE
    )

    unique_datasets = sorted(
        set([d.upper() for d in datasets])
    )

    # ---------------------------------------------------------
    # SORT / IDCAMS / IKJEFT01 IDENTIFICATION
    # ---------------------------------------------------------

    utility_programs = [
        "SORT",
        "IDCAMS",
        "IKJEFT01",
        "IEBGENER",
        "IEFBR14"
    ]

    # ---------------------------------------------------------
    # STEP PROCESSING
    # ---------------------------------------------------------

    if exec_steps:

        for seq, (step_name, program_name) in enumerate(exec_steps, start=1):

            program_name_upper = program_name.upper()

            step_type = (
                "UTILITY"
                if program_name_upper in utility_programs
                else "APPLICATION"
            )

            jcl_results.append({

                "file_name": file_name,

                "job_name": job_name,

                "step_sequence": seq,

                "step_name": step_name.upper(),

                "program_name": program_name_upper,

                "step_type": step_type,

                "datasets": ", ".join(unique_datasets),

                "dataset_count": len(unique_datasets),

                "status": "STEP_FOUND"

            })

    else:

        jcl_results.append({

            "file_name": file_name,

            "job_name": job_name,

            "step_sequence": 0,

            "step_name": "",

            "program_name": "",

            "step_type": "UNKNOWN",

            "datasets": ", ".join(unique_datasets),

            "dataset_count": len(unique_datasets),

            "status": "NO_EXEC_STEP_FOUND"

        })

    return jcl_results


# ---------------------------------------------------------
# RUN EXTRACTION
# ---------------------------------------------------------

jcl_flow_results = []

for _, row in df_files.iterrows():

    if str(row["file_type"]).upper() == "JCL":

        jcl_flow_results.extend(

            extract_jcl_flow(
                row["file_name"],
                row["content"]
            )

        )

# ---------------------------------------------------------
# CREATE DATAFRAME
# ---------------------------------------------------------

df_jcl_flow = pd.DataFrame(
    jcl_flow_results
).drop_duplicates()

# ---------------------------------------------------------
# DISPLAY RESULTS
# ---------------------------------------------------------

print("===== ENTERPRISE JCL FLOW SUMMARY =====")

display(df_jcl_flow)

# ---------------------------------------------------------
# VALIDATION
# ---------------------------------------------------------

total_jcl_files = len(
    df_files[
        df_files["file_type"]
        .astype(str)
        .str.upper() == "JCL"
    ]
)

included_jcl_files = df_jcl_flow["file_name"].nunique()

print("======================================")
print("Total JCL files identified :", total_jcl_files)
print("JCL files included output  :", included_jcl_files)
print("======================================")

if total_jcl_files == included_jcl_files:

    print("Validation Passed : All JCL files included.")

else:

    print("Validation Failed : Some JCL files missing.")

# ---------------------------------------------------------
# OPTIONAL SQL SAVE
# ---------------------------------------------------------

spark.createDataFrame(
    df_jcl_flow.astype(str)
).write.mode("overwrite").saveAsTable(
    "workspace.default.codegenome_jcl_flow"
)

print("JCL Flow Delta Table Refreshed Successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI-Based Business Rule Extraction

# COMMAND ----------

import re
import pandas as pd

# ---------------------------------------------------------
# BUSINESS RULE EXTRACTION
# ---------------------------------------------------------

def generate_business_rule_summary(file_name, file_type, content):

    content_upper = str(content).upper()
    rules = []

    def add_rule(category, rule, confidence="Medium", recommendation="Review during modernization"):
        rules.append({
            "file_name": file_name,
            "file_type": file_type,
            "rule_category": category,
            "business_rule": rule,
            "confidence": confidence,
            "modernization_recommendation": recommendation
        })

    # Payroll rules
    if "EMPLOYEE_SALARY" in content_upper or "WS-EMP-SALARY" in content_upper:
        add_rule(
            "Payroll",
            "Processes employee salary information for payroll calculation.",
            "High",
            "Convert payroll logic into reusable payroll service."
        )

    if re.search(r"WS-EMP-SALARY\s*>\s*50000", content_upper):
        add_rule(
            "Payroll Tax",
            "Employees with salary greater than 50,000 are assigned a higher tax amount.",
            "High",
            "Externalize tax threshold into configurable rule table."
        )

    if "COMPUTE WS-NET-SALARY" in content_upper:
        add_rule(
            "Payroll Calculation",
            "Calculates employee net salary by subtracting tax amount from salary.",
            "High",
            "Expose net salary calculation as API or rules engine service."
        )

    if "EMP_MASTER" in content_upper:
        add_rule(
            "Employee Master Data",
            "Reads or updates employee master payroll details.",
            "High",
            "Map EMP_MASTER dependency into employee domain data model."
        )

    if "PAYROLL_AUDIT" in content_upper:
        add_rule(
            "Audit",
            "Creates payroll audit records for tracking payroll processing.",
            "High",
            "Preserve audit trail in target cloud platform."
        )

    # Customer rules
    if "CUSTOMER_MASTER" in content_upper:
        add_rule(
            "Customer Master Data",
            "Reads or updates customer master details.",
            "High",
            "Map CUSTOMER_MASTER to customer domain data model."
        )

    if re.search(r"CREDIT_LIMIT\s*>\s*100000", content_upper):
        add_rule(
            "Customer Risk",
            "Classifies customers with credit limit greater than 100,000 as high risk.",
            "High",
            "Externalize credit risk threshold into configuration."
        )

    if "CUSTAUDIT" in content_upper:
        add_rule(
            "Customer Audit",
            "Calls customer audit process for customer risk tracking.",
            "Medium",
            "Review downstream audit dependency before API decomposition."
        )

    # DB2 / SQL rules
    if any(keyword in content_upper for keyword in ["SELECT", "INSERT", "UPDATE", "DELETE"]):
        add_rule(
            "DB2 Data Processing",
            "Performs database read/write operations as part of batch or transaction processing.",
            "Medium",
            "Include this asset in DB2 lineage and data migration assessment."
        )

    # JCL / Batch rules
    if file_type.upper() == "JCL":
        add_rule(
            "Batch Orchestration",
            "Controls batch job execution, program sequencing, and dataset movement.",
            "High",
            "Convert JCL flow into cloud-native workflow orchestration."
        )

    # Copybook rules
    if file_type.upper() in ["CPY", "COPYBOOK"]:
        add_rule(
            "Shared Data Structure",
            "Defines reusable record layout or shared data structure.",
            "High",
            "Convert copybook layout into canonical schema or data contract."
        )

    # Default fallback
    if not rules:
        add_rule(
            "Legacy Processing",
            "Legacy asset used for mainframe batch processing or supporting application logic.",
            "Low",
            "Review manually for modernization scope."
        )

    return rules


# ---------------------------------------------------------
# RUN BUSINESS RULE EXTRACTION
# ---------------------------------------------------------

business_rule_results = []

for _, row in df_files.iterrows():

    file_name = row.get("file_name", "")
    file_type = row.get("file_type", "")
    content = row.get("content", "")

    business_rule_results.extend(
        generate_business_rule_summary(
            file_name,
            file_type,
            content
        )
    )

df_business_rules = pd.DataFrame(business_rule_results)

display(df_business_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modernization Readiness Scoring

# COMMAND ----------

import re
import pandas as pd

def calculate_modernization_score(file_name, file_type, content):

    content_upper = str(content).upper()
    line_count = len(str(content).splitlines())

    score = 100
    risk_factors = []

    # Dependency risks
    if "CALL" in content_upper:
        score -= 15
        risk_factors.append("External program dependency")

    if "COPY" in content_upper:
        score -= 10
        risk_factors.append("Copybook dependency")

    # DB2 / SQL risks
    if "EXEC SQL" in content_upper or file_type.upper() == "SQL":
        score -= 20
        risk_factors.append("DB2 or SQL dependency")

    if re.search(r"\b(UPDATE|INSERT|DELETE)\b", content_upper):
        score -= 15
        risk_factors.append("Database write operation")

    # JCL / Batch risks
    if file_type.upper() == "JCL":
        score -= 20
        risk_factors.append("Batch orchestration dependency")

    if "EXEC PGM=" in content_upper or "EXEC PROC=" in content_upper:
        score -= 10
        risk_factors.append("JCL execution flow dependency")

    # Legacy complexity risks
    if "GO TO" in content_upper:
        score -= 15
        risk_factors.append("GO TO control flow")

    if "ALTER" in content_upper:
        score -= 20
        risk_factors.append("ALTER statement usage")

    if "SORT" in content_upper:
        score -= 10
        risk_factors.append("SORT utility dependency")

    if line_count > 200:
        score -= 20
        risk_factors.append("Very large code size")
    elif line_count > 80:
        score -= 10
        risk_factors.append("Large code size")

    # Ensure score range
    score = max(0, min(score, 100))

    # Risk / readiness classification
    if score >= 75:
        risk_level = "Low"
        modernization_priority = "Low"
        recommendation = "Good candidate for API enablement or direct cloud modernization."
    elif score >= 50:
        risk_level = "Medium"
        modernization_priority = "Medium"
        recommendation = "Requires dependency, DB2, and batch flow review before modernization."
    else:
        risk_level = "High"
        modernization_priority = "High"
        recommendation = "Needs detailed impact analysis, phased modernization, and regression validation."

    # API candidate logic
    api_candidate = "YES" if (
        file_type.upper() == "CBL"
        and score >= 60
        and "EXEC SQL" not in content_upper
        and "JCL" not in content_upper
    ) else "NO"

    return {
        "file_name": file_name,
        "file_type": file_type,
        "line_count": line_count,
        "readiness_score": score,
        "risk_level": risk_level,
        "modernization_priority": modernization_priority,
        "api_candidate": api_candidate,
        "risk_factors": ", ".join(risk_factors) if risk_factors else "None",
        "recommendation": recommendation
    }


modernization_results = []

for _, row in df_files.iterrows():
    modernization_results.append(
        calculate_modernization_score(
            row.get("file_name", ""),
            row.get("file_type", ""),
            row.get("content", "")
        )
    )

df_modernization = pd.DataFrame(modernization_results)

display(df_modernization)

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚨 AI Risk Alert Engine
# MAGIC
# MAGIC This section generates AI-driven modernization governance alerts
# MAGIC based on:
# MAGIC - modernization risk
# MAGIC - DB2 critical operations
# MAGIC - dependency complexity
# MAGIC - batch orchestration analysis
# MAGIC
# MAGIC The alerts help identify:
# MAGIC - migration blockers
# MAGIC - high-risk legacy assets
# MAGIC - dependency-heavy applications
# MAGIC - critical DB2 write operations
# MAGIC - governance risks

# COMMAND ----------

def generate_risk_alerts():

    alerts = []

    # High Risk Programs
    high_risk = df_modernization[
        df_modernization["risk_level"] == "High"
    ]

    for _, row in high_risk.iterrows():

        alerts.append({
            "alert_type": "HIGH_MODERNIZATION_RISK",
            "severity": "CRITICAL",
            "file_name": row["file_name"],
            "message":
                f"{row['file_name']} requires phased modernization planning."
        })

    # Dependency Heavy Programs
    dependency_counts = (
        df_dependencies.groupby("file_name")
        .size()
        .reset_index(name="dependency_count")
    )

    heavy_dependencies = dependency_counts[
        dependency_counts["dependency_count"] > 5
    ]

    for _, row in heavy_dependencies.iterrows():

        alerts.append({
            "alert_type": "DEPENDENCY_COMPLEXITY",
            "severity": "HIGH",
            "file_name": row["file_name"],
            "message":
                f"{row['file_name']} has excessive dependencies."
        })

    # Critical DB2 UPDATE/DELETE Operations
    critical_db2 = df_db2_lineage[
        df_db2_lineage["operation"].isin(["UPDATE", "DELETE"])
    ]

    for _, row in critical_db2.iterrows():

        alerts.append({
            "alert_type": "CRITICAL_DB2_OPERATION",
            "severity": "HIGH",
            "file_name": row["file_name"],
            "message":
                f"{row['file_name']} performs critical DB2 {row['operation']} operations."
        })

    return pd.DataFrame(alerts)

# COMMAND ----------

df_risk_alerts = pd.DataFrame(generate_risk_alerts())
display(df_risk_alerts)

# COMMAND ----------

display(
    df_risk_alerts.groupby("severity")
    .size()
    .reset_index(name="alert_count")
)

# COMMAND ----------

df_risk_alerts_spark = spark.createDataFrame(df_risk_alerts)

df_risk_alerts_spark.write.mode("overwrite").saveAsTable(
    "codegenome_risk_alerts"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Risk Alert Outcome
# MAGIC
# MAGIC The AI Risk Alert Engine identifies critical modernization risks across legacy assets, including high-risk programs, DB2 write operations, dependency-heavy components, and batch orchestration risks.
# MAGIC
# MAGIC These alerts can be used for governance review, modernization prioritization, and migration planning.

# COMMAND ----------

# MAGIC %md
# MAGIC # AI Risk Intelligence Dashboard

# COMMAND ----------

import matplotlib.pyplot as plt

severity_counts = (
    df_risk_alerts.groupby("severity")
    .size()
    .reset_index(name="count")
)

plt.figure(figsize=(6,4))

plt.bar(
    severity_counts["severity"],
    severity_counts["count"]
)

plt.title("Risk Alerts by Severity")
plt.xlabel("Severity")
plt.ylabel("Alert Count")

plt.show()

# COMMAND ----------

alert_type_counts = (
    df_risk_alerts.groupby("alert_type")
    .size()
    .reset_index(name="count")
)

plt.figure(figsize=(10,5))

plt.bar(
    alert_type_counts["alert_type"],
    alert_type_counts["count"]
)

plt.xticks(rotation=20)

plt.title("Alert Types Distribution")
plt.xlabel("Alert Type")
plt.ylabel("Count")

plt.show()

# COMMAND ----------

display(
    df_risk_alerts[
        df_risk_alerts["severity"] == "CRITICAL"
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Enterprise Governance Alerting
# MAGIC
# MAGIC CodeGenome AI integrates with Databricks SQL Alerts to provide automated modernization governance monitoring.
# MAGIC
# MAGIC Features:
# MAGIC - AI-generated modernization risk detection
# MAGIC - Scheduled enterprise alerting
# MAGIC - Executive governance notifications
# MAGIC - Critical risk escalation workflow
# MAGIC - Automated modernization intelligence monitoring
# MAGIC
# MAGIC This capability enables proactive identification of migration blockers and modernization risks across enterprise legacy systems.

# COMMAND ----------

# MAGIC %md
# MAGIC # Executive Risk Summary
# MAGIC
# MAGIC CodeGenome AI detected multiple modernization governance risks across legacy assets.
# MAGIC
# MAGIC Key observations:
# MAGIC - Critical modernization blockers identified
# MAGIC - DB2 write-intensive programs detected
# MAGIC - Dependency-heavy components highlighted
# MAGIC - Batch orchestration complexity analyzed
# MAGIC
# MAGIC The AI-generated insights help prioritize modernization planning, governance review, and migration sequencing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dependency Graph Visualization

# COMMAND ----------

import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd

G = nx.DiGraph()

# Add all files
for _, row in df_files.iterrows():
    file_name = str(row["file_name"]).strip()
    file_type = str(row["file_type"]).strip()

    G.add_node(file_name, node_type=file_type)

# Add COBOL dependencies
for _, row in df_dependencies.iterrows():
    program_file = str(row["file_name"]).strip()
    G.add_node(program_file, node_type="COBOL")

    calls = row.get("calls", "")
    if pd.notna(calls) and str(calls).strip():
        for call in str(calls).split(","):
            call = call.strip()
            if call:
                G.add_node(call, node_type="PROGRAM")
                G.add_edge(program_file, call, relation="CALLS")

    copybooks = row.get("copybooks", "")
    if pd.notna(copybooks) and str(copybooks).strip():
        for copybook in str(copybooks).split(","):
            copybook = copybook.strip()
            if copybook:
                G.add_node(copybook, node_type="COPYBOOK")
                G.add_edge(program_file, copybook, relation="USES_COPYBOOK")

# Add DB2 lineage
for _, row in df_db2_lineage.iterrows():
    source_file = str(row["file_name"]).strip()
    db2_table = str(row["db2_table"]).strip()
    operation = str(row["operation"]).strip()

    G.add_node(source_file, node_type="SOURCE")
    G.add_node(db2_table, node_type="DB2_TABLE")
    G.add_edge(source_file, db2_table, relation=operation)

# Add JCL flow
for _, row in df_jcl_flow.iterrows():
    jcl_file = str(row["file_name"]).strip()
    program_name = str(row["program_name"]).strip()

    G.add_node(jcl_file, node_type="JCL")

    if program_name and program_name.lower() != "nan":
        G.add_node(program_name, node_type="PROGRAM")
        G.add_edge(jcl_file, program_name, relation="EXECUTES")

# Validation
expected_files = set(df_files["file_name"].astype(str))
graph_files = set([node for node in G.nodes if node in expected_files])
missing_files = expected_files - graph_files

print("Total expected files:", len(expected_files))
print("Files included in graph:", len(graph_files))

if missing_files:
    print("Missing files:")
    for f in sorted(missing_files):
        print("-", f)
else:
    print("Validation Passed: All expected files are included in the graph.")

# Draw graph
plt.figure(figsize=(20, 12))

pos = nx.spring_layout(G, k=1.2, seed=42)

nx.draw_networkx_nodes(
    G,
    pos,
    node_size=2800,
    node_color="#4FC3F7",
    edgecolors="black"
)

nx.draw_networkx_edges(
    G,
    pos,
    arrows=True,
    arrowstyle="-|>",
    arrowsize=18,
    width=1.2
)

nx.draw_networkx_labels(
    G,
    pos,
    font_size=9,
    font_weight="bold"
)

edge_labels = nx.get_edge_attributes(G, "relation")

nx.draw_networkx_edge_labels(
    G,
    pos,
    edge_labels=edge_labels,
    font_size=8
)

plt.title(
    "CodeGenome AI - Complete Mainframe Dependency Graph",
    fontsize=16,
    fontweight="bold"
)

plt.axis("off")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Storage & Persistence

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS codegenome")

# COMMAND ----------

import pandas as pd
import os

base_path = "/Volumes/workspace/default/codegenome"

file_results = []

for root, dirs, files in os.walk(base_path):

    for file in files:

        file_path = os.path.join(root, file)

        if file.endswith((".cbl", ".cob", ".jcl", ".cpy", ".sql", ".csv")):

            try:
                with open(file_path, "r", errors="ignore") as f:
                    content = f.read()

            except Exception as e:
                print("Error reading:", file_path, e)
                content = ""

            file_results.append({
                "file_name": file,
                "file_type": file.split(".")[-1].upper(),
                "file_path": file_path,
                "content": content
            })

print("Files Loaded:", len(file_results))

df_files = pd.DataFrame(file_results)

display(df_files)

# COMMAND ----------

import re
import pandas as pd

dependency_results = []

for _, row in df_files.iterrows():

    file_name = row["file_name"]
    content = str(row["content"])

    # Extract CALL programs
    calls = re.findall(r'CALL\s+[\'"]?([A-Z0-9_-]+)', content, re.IGNORECASE)

    # Extract COPYBOOKS
    copybooks = re.findall(r'COPY\s+([A-Z0-9_-]+)', content, re.IGNORECASE)

    dependency_results.append({
        "file_name": file_name,
        "calls": ", ".join(sorted(set(calls))) if calls else "N/A",
        "copybooks": ", ".join(sorted(set(copybooks))) if copybooks else "N/A"
    })

# Create dataframe
df_dependencies = pd.DataFrame(dependency_results)

# Display results
display(df_dependencies)

# COMMAND ----------

spark.createDataFrame(df_dependencies)\
.write.format("delta")\
.mode("overwrite")\
.saveAsTable("codegenome.dependencies")

# COMMAND ----------

import os
import pandas as pd

base_path = "/Volumes/workspace/default/codegenome/CodeGenome_Enhanced_Mainframe_Files"

files_data = []

for root, dirs, files in os.walk(base_path):

    for file_name in files:

        file_path = os.path.join(root, file_name)

        try:

            with open(
                file_path,
                "r",
                encoding="utf-8",
                errors="ignore"
            ) as f:

                content = f.read()

            extension = file_name.split(".")[-1].lower()

            file_type_map = {
                "cbl": "CBL",
                "jcl": "JCL",
                "cpy": "CPY",
                "sql": "SQL",
                "csv": "CSV"
            }

            file_type = file_type_map.get(
                extension,
                "OTHER"
            )

            files_data.append({

                "file_name": file_name,

                "file_path": file_path,

                "file_type": file_type,

                "content": content

            })

        except Exception as e:

            print("Error reading:", file_name, str(e))

df_files = pd.DataFrame(files_data)

print("================================")
print("TOTAL FILES LOADED:", len(df_files))
print("================================")

display(df_files.head())

# COMMAND ----------

# ---------------------------------------------
# EXTRACT + SAVE DB2 LINEAGE TO DELTA TABLE
# ---------------------------------------------

import re
import pandas as pd
from pyspark.sql.types import *

db2_lineage_results = []

patterns = {
    "SELECT": r"\bFROM\s+([A-Z0-9_\.]+)",
    "JOIN": r"\bJOIN\s+([A-Z0-9_\.]+)",
    "UPDATE": r"\bUPDATE\s+([A-Z0-9_\.]+)",
    "INSERT": r"\bINSERT\s+INTO\s+([A-Z0-9_\.]+)",
    "DELETE": r"\bDELETE\s+FROM\s+([A-Z0-9_\.]+)"
}

for _, row in df_files.iterrows():
    file_name = row.get("file_name", "")
    file_type = row.get("file_type", "")
    content = str(row.get("content", "")).upper()

    for operation, pattern in patterns.items():
        tables = re.findall(pattern, content)

        for table in tables:
            db2_lineage_results.append({
                "file_name": file_name,
                "file_type": file_type,
                "operation": operation,
                "db2_table": table
            })

df_db2_lineage = pd.DataFrame(
    db2_lineage_results,
    columns=["file_name", "file_type", "operation", "db2_table"]
).drop_duplicates()

db2_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("file_type", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("db2_table", StringType(), True)
])

spark_db2_df = spark.createDataFrame(
    df_db2_lineage.astype(str),
    schema=db2_schema
)

spark_db2_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.codegenome_db2_lineage")

print("===================================")
print("DB2 Lineage Delta Table Refreshed")
print("Rows Saved:", len(df_db2_lineage))
print("===================================")

display(df_db2_lineage)

# COMMAND ----------

from pyspark.sql import functions as F

# Convert list column to string before saving to Delta
df_jcl_flow_save = df_jcl_flow.copy()

if "datasets" in df_jcl_flow_save.columns:
    df_jcl_flow_save["datasets"] = df_jcl_flow_save["datasets"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else str(x)
    )

# Rename any duplicate/wrong column if present
if "dataset" in df_jcl_flow_save.columns and "datasets" not in df_jcl_flow_save.columns:
    df_jcl_flow_save = df_jcl_flow_save.rename(columns={"dataset": "datasets"})

spark.createDataFrame(df_jcl_flow_save) \
    .write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("codegenome.jcl_flow")

# COMMAND ----------

# ---------------------------------------------
# SAVE BUSINESS RULES TO DELTA TABLE
# ---------------------------------------------

spark.createDataFrame(
    df_business_rules.astype(str)
) \
.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(
    "workspace.default.codegenome_business_rules"
)

print("Business Rules Delta table refreshed successfully")
print("Rows saved:", len(df_business_rules))

# COMMAND ----------

# ---------------------------------------------
# SAVE MODERNIZATION SCORE TO DELTA TABLE
# ---------------------------------------------

spark.createDataFrame(
    df_modernization.astype(str)
) \
.write \
.format("delta") \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable(
    "workspace.default.codegenome_modernization_score"
)

print("===================================")
print("Modernization Score Delta Refreshed")
print("Rows Saved:", len(df_modernization))
print("===================================")

# COMMAND ----------

spark.sql("SHOW TABLES IN codegenome").show()

# COMMAND ----------

spark.sql("USE codegenome")

spark.sql("""
SELECT
    table_name
FROM system.information_schema.tables
WHERE table_schema = 'codegenome'
ORDER BY table_name
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
ANALYZE TABLE workspace.default.codegenome_modernization_score
COMPUTE STATISTICS
""")

spark.sql("""
ANALYZE TABLE workspace.default.codegenome_dependencies
COMPUTE STATISTICS
""")

spark.sql("""
ANALYZE TABLE workspace.default.codegenome_db2_lineage
COMPUTE STATISTICS
""")

spark.sql("""
ANALYZE TABLE workspace.default.codegenome_jcl_flow
COMPUTE STATISTICS
""")

spark.sql("""
ANALYZE TABLE workspace.default.codegenome_business_rules
COMPUTE STATISTICS
""")

print("All Delta Tables Optimized Successfully")

# COMMAND ----------

import re
import pandas as pd

dependency_results = []

for _, row in df_files.iterrows():
    file_name = row.get("file_name", "")
    file_type = row.get("file_type", "")
    content = str(row.get("content", "")).upper()

    copybooks = re.findall(r"\bCOPY\s+([A-Z0-9_#$@\-]+)", content)

    calls = re.findall(r"\bCALL\s+['\"]?([A-Z0-9_#$@\-]+)['\"]?", content)

    for copybook in copybooks:
        dependency_results.append({
            "file_name": file_name,
            "file_type": file_type,
            "dependency_type": "COPYBOOK",
            "dependency_name": copybook
        })

    for call in calls:
        dependency_results.append({
            "file_name": file_name,
            "file_type": file_type,
            "dependency_type": "CALL",
            "dependency_name": call
        })

df_dependencies = pd.DataFrame(
    dependency_results,
    columns=[
        "file_name",
        "file_type",
        "dependency_type",
        "dependency_name"
    ]
).drop_duplicates()

display(df_dependencies)

spark.createDataFrame(df_dependencies.astype(str)) \
    .write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.codegenome_dependencies")

print("Dependencies refreshed. Rows saved:", len(df_dependencies))

# COMMAND ----------

# MAGIC %md
# MAGIC # AI-powered Modernization Assistant

# COMMAND ----------

def codegenome_chatbot(question):

    q = question.lower().strip()

    # -----------------------------------------
    # HIGH RISK PROGRAMS
    # -----------------------------------------

    if "high modernization risk" in q:

        result = df_modernization[
            df_modernization["risk_level"].str.upper() == "HIGH"
        ]

        return result if not result.empty else "No high risk programs found."

    # -----------------------------------------
    # MODERNIZATION SUMMARY
    # -----------------------------------------

    if "modernization summary" in q:

        summary = df_modernization.groupby(
            "risk_level"
        ).size().reset_index(name="total_programs")

        return summary

    # -----------------------------------------
    # MODERNIZATION DETAILS
    # -----------------------------------------

    if "modernization risk" in q or "risk" in q:

        return df_modernization[
            [
                "file_name",
                "readiness_score",
                "risk_level",
                "risk_factors",
                "recommendation"
            ]
        ]

    # -----------------------------------------
    # JCL FLOW
    # -----------------------------------------

    if "jcl" in q or "job" in q or "batch" in q:

        return df_jcl_flow[
            [
                "file_name",
                "job_name",
                "step_name",
                "program_name",
                "datasets",
                "dataset_count",
                "status"
            ]
        ]

    # -----------------------------------------
    # DB2 LINEAGE
    # -----------------------------------------

    if "db2" in q or "lineage" in q or "table" in q:

        return df_db2_lineage

    # -----------------------------------------
    # DEPENDENCIES
    # -----------------------------------------

    if "dependency" in q or "dependencies" in q \
       or "copybook" in q or "call" in q:

        return df_dependencies

    # -----------------------------------------
    # BUSINESS RULES
    # -----------------------------------------

    if "business rule" in q or "logic" in q:

        return df_business_rules

    # -----------------------------------------
    # FILE INVENTORY
    # -----------------------------------------

    if "files" in q or "inventory" in q:

        return df_files[
            [
                "file_name",
                "file_type"
            ]
        ]

    # -----------------------------------------
    # SQL FILES
    # -----------------------------------------

    if "sql files" in q:

        return df_files[
            df_files["file_type"].str.upper() == "SQL"
        ][
            ["file_name", "file_type"]
        ]

    # -----------------------------------------
    # COPYBOOK FILES
    # -----------------------------------------

    if "copybook" in q:

        return df_files[
            df_files["file_type"].str.upper() == "CPY"
        ][
            ["file_name", "file_type"]
        ]

    # -----------------------------------------
    # DYNAMIC DB2 TABLE SEARCH
    # -----------------------------------------

    words = q.upper().split()

    for word in words:

        db2_match = df_db2_lineage[
            df_db2_lineage["db2_table"].str.upper().str.contains(
                word,
                na=False
            )
        ]

        if not db2_match.empty:
            return db2_match

    # -----------------------------------------
    # DYNAMIC PROGRAM SEARCH
    # -----------------------------------------

    for word in words:

        program_match = df_files[
            df_files["file_name"].str.upper().str.contains(
                word,
                na=False
            )
        ]

        if not program_match.empty:

            result_sections = []

            result_sections.append("FILE INVENTORY:")
            result_sections.append(
                program_match[
                    ["file_name", "file_type"]
                ].to_string(index=False)
            )

            mod_match = df_modernization[
                df_modernization["file_name"].str.upper().str.contains(
                    word,
                    na=False
                )
            ]

            if not mod_match.empty:

                result_sections.append("\nMODERNIZATION:")
                result_sections.append(
                    mod_match.to_string(index=False)
                )

            db2_match = df_db2_lineage[
                df_db2_lineage["file_name"].str.upper().str.contains(
                    word,
                    na=False
                )
            ]

            if not db2_match.empty:

                result_sections.append("\nDB2 LINEAGE:")
                result_sections.append(
                    db2_match.to_string(index=False)
                )

            dep_match = df_dependencies[
                df_dependencies["file_name"].str.upper().str.contains(
                    word,
                    na=False
                )
            ]

            if not dep_match.empty:

                result_sections.append("\nDEPENDENCIES:")
                result_sections.append(
                    dep_match.to_string(index=False)
                )

            return "\n".join(result_sections)

    # -----------------------------------------
    # SUMMARY STATISTICS
    # -----------------------------------------

    if "summary" in q or "statistics" in q:

        return pd.DataFrame({

            "Metric": [
                "Total Files",
                "COBOL Programs",
                "JCL Jobs",
                "Copybooks",
                "SQL Files"
            ],

            "Count": [

                len(df_files),

                len(
                    df_files[
                        df_files["file_type"].str.upper() == "CBL"
                    ]
                ),

                len(
                    df_files[
                        df_files["file_type"].str.upper() == "JCL"
                    ]
                ),

                len(
                    df_files[
                        df_files["file_type"].str.upper() == "CPY"
                    ]
                ),

                len(
                    df_files[
                        df_files["file_type"].str.upper() == "SQL"
                    ]
                )

            ]

        })

    # -----------------------------------------
    # DEFAULT RESPONSE
    # -----------------------------------------

    return (
        "CodeGenome AI supports:\n\n"
        "- File inventory\n"
        "- COBOL dependencies\n"
        "- DB2 lineage\n"
        "- JCL batch flow\n"
        "- Business rules\n"
        "- Modernization scoring\n"
        "- SQL file discovery\n"
        "- Dynamic program search\n"
        "- DB2 impact analysis"
    )

# COMMAND ----------

import pandas as pd
import html
from io import StringIO
from IPython.display import display, HTML

pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 5000)

def convert_text_to_df(text):
    text = str(text).strip()

    try:
        return pd.read_csv(
            StringIO(text),
            sep=r"\s{2,}",
            engine="python"
        )
    except Exception:
        return pd.DataFrame({
            "Response": [text]
        })

def clean_df(df):
    df = df.copy()
    df = df.fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    for col in df.columns:
        df[col] = df[col].astype(str)

    return df

def show_css_table(df, question=""):
    df = clean_df(df)

    html_table = df.to_html(
        index=False,
        escape=True,
        classes="cg-chat-table"
    )

    display(HTML(f"""
    <style>
        .cg-chat-title {{
            font-family:Consolas, monospace;
            font-size:14px;
            font-weight:bold;
            color:#ffffff;
            margin:14px 0 6px 0;
        }}

        .cg-chat-wrap {{
            width:100%;
            max-height:520px;
            overflow:auto;
            border:1px solid #666;
            border-radius:8px;
            padding:10px;
            background:#000000;
            box-sizing:border-box;
        }}

        table.cg-chat-table {{
            width:100%;
            min-width:1400px;
            border-collapse:collapse;
            table-layout:auto;
            font-family:Consolas, monospace;
            font-size:13px;
            color:#ffffff;
            background:#000000;
        }}

        table.cg-chat-table th {{
            background:#2b2b2b !important;
            color:#ffffff !important;
            border:1px solid #888;
            padding:8px;
            text-align:left !important;
            position:sticky;
            top:0;
            z-index:2;
            white-space:nowrap;
        }}

        table.cg-chat-table td {{
            border:1px solid #555;
            padding:8px;
            text-align:left !important;
            vertical-align:top !important;
            white-space:pre-wrap !important;
            word-break:break-word !important;
            overflow-wrap:anywhere !important;
            max-width:520px;
            color:#ffffff !important;
            background:#000000 !important;
        }}

        table.cg-chat-table tr:nth-child(even) td {{
            background:#111111 !important;
        }}
    </style>

    <div class="cg-chat-title">Question: {html.escape(question)}</div>
    <div class="cg-chat-wrap">
        {html_table}
    </div>
    """))

def show_text_output(text, question=""):
    safe_text = html.escape(str(text))

    display(HTML(f"""
    <div style="
        font-family:Consolas, monospace;
        font-size:14px;
        font-weight:bold;
        color:#ffffff;
        margin:14px 0 6px 0;
    ">
        Question: {html.escape(question)}
    </div>

    <pre style="
        width:100%;
        max-height:520px;
        overflow:auto;
        border:1px solid #666;
        border-radius:8px;
        padding:12px;
        background:#000000;
        color:#ffffff;
        box-sizing:border-box;
        font-family:Consolas, monospace;
        font-size:13px;
        line-height:1.5;
        white-space:pre-wrap;
        word-break:break-word;
        overflow-wrap:anywhere;
    ">{safe_text}</pre>
    """))

questions = [
    "Show modernization risk",
    "Show high modernization risk",
    "Show JCL job flow",
    "Which program updates EMP_MASTER?",
    "Show DB2 lineage",
    "Show dependencies",
    "Show business rules",
    "Show all included files",
    "List all COBOL programs",
    "Show SQL files",
    "Show COPYBOOK files",
    "Explain payroll logic",
    "Explain customer logic",
    "Generate modernization summary"
]

for question in questions:
    try:
        result = codegenome_chatbot(question)

        if isinstance(result, pd.DataFrame):
            df_out = result.copy()

            if df_out.empty:
                df_out = pd.DataFrame({
                    "Status": ["No matching records found."]
                })

            show_css_table(df_out, question)

        else:
            text_result = str(result)

            if not text_result.strip():
                text_result = "No matching records found."

            show_text_output(text_result, question)

    except Exception as e:
        error_df = pd.DataFrame({
            "Question": [question],
            "Error": [str(e)]
        })

        show_css_table(error_df, question)

# COMMAND ----------

# MAGIC %md
# MAGIC Enterprise AI Assistant

# COMMAND ----------

import pandas as pd
import html
from IPython.display import display, HTML

pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 5000)

if "codegenome_chatbot" in globals():
    chatbot_func = codegenome_chatbot
elif "chatbot_fn" in globals():
    chatbot_func = chatbot_fn
else:
    raise NameError("Chatbot function is not defined. Run the chatbot function cell first.")

questions = [
    "List all COBOL programs",
    "Show all JCL jobs",
    "Which files have modernization risk",
    "Explain PAYROLL processing",
    "Show DB2 related files",
    "Show dependencies",
    "Show business rules",
    "Show SQL files",
    "Show COPYBOOK files",
    "Generate modernization summary"
]

def df_to_text(df):
    df = df.copy().fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    for col in df.columns:
        df[col] = df[col].astype(str).str.replace("\n", " ", regex=False)

    return df.to_string(index=False)

def show_output(question, content):
    safe_question = html.escape(str(question))
    safe_content = html.escape(str(content))

    display(HTML(f"""
    <div style="
        background:#000000;
        color:#ffffff;
        border:1px solid #777;
        border-radius:8px;
        padding:12px;
        margin:14px 0;
        font-family:Consolas, monospace;
        font-size:13px;
        max-height:500px;
        overflow:auto;
        white-space:pre;
        box-sizing:border-box;
    "><b>Question: {safe_question}</b>

{safe_content}
    </div>
    """))

for question in questions:
    try:
        result = chatbot_func(question)

        if isinstance(result, pd.DataFrame):
            if result.empty:
                result = pd.DataFrame({"Status": ["No matching records found."]})
            output = df_to_text(result)
        else:
            output = str(result)

        show_output(question, output)

    except Exception as e:
        error_df = pd.DataFrame({
            "Question": [question],
            "Error": [str(e)]
        })
        show_output(question, df_to_text(error_df))

# COMMAND ----------

df_files.to_csv(
    "/Volumes/workspace/default/codegenome/data/files_metadata.csv",
    index=False
)

# COMMAND ----------

df_dependencies.to_csv(
    "/Volumes/workspace/default/codegenome/data/dependencies.csv",
    index=False
)

df_db2_lineage.to_csv(
    "/Volumes/workspace/default/codegenome/data/db2_lineage.csv",
    index=False
)

df_jcl_flow.to_csv(
    "/Volumes/workspace/default/codegenome/data/jcl_execution_flow.csv",
    index=False
)

df_business_rules.to_csv(
    "/Volumes/workspace/default/codegenome/data/business_rules.csv",
    index=False
)

df_modernization.to_csv(
    "/Volumes/workspace/default/codegenome/data/modernization_score.csv",
    index=False
)

df_risk_alerts.to_csv(
    "/Volumes/workspace/default/codegenome/data/risk_alerts.csv",
    index=False
)

# COMMAND ----------

display(
    dbutils.fs.ls("dbfs:/Volumes/workspace/default/codegenome/data")
)

# COMMAND ----------

print(globals().keys())

# COMMAND ----------

print("========================================")
print("CODEGENOME AI EXECUTIVE SUMMARY")
print("========================================")

# SAFE COUNTS

total_assets = len(df_modernization) if 'df_modernization' in globals() else 0

high_risk = len(
    df_modernization[
        df_modernization['risk_level'] == 'High'
    ]
) if 'df_modernization' in globals() else 0

db2_tables = (
    df_db2_lineage['db2_table'].nunique()
    if 'df_db2_lineage' in globals()
    else 0
)

jcl_jobs = (
    df_jcl_flow['job_name'].nunique()
    if 'df_jcl_flow' in globals()
    else 0
)

business_rules = (
    len(df_business_rules)
    if 'df_business_rules' in globals()
    else 0
)

print(f"Total Enterprise Assets      : {total_assets}")
print(f"High Risk Programs           : {high_risk}")
print(f"DB2 Tables Identified        : {db2_tables}")
print(f"JCL Batch Jobs Identified    : {jcl_jobs}")
print(f"Business Rules Extracted     : {business_rules}")

print("\nAI Recommendation:")
print("Prioritize high-risk DB2 batch applications for phased modernization.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 🚨 Microsoft Teams Governance Notification Integration
# MAGIC
# MAGIC This section integrates CodeGenome AI modernization alerts
# MAGIC with Microsoft Teams using Power Automate workflows.
# MAGIC
# MAGIC The integration enables:
# MAGIC - AI modernization governance notifications
# MAGIC - Critical risk alerts
# MAGIC - DB2 operational alerts
# MAGIC - Dependency risk escalation
# MAGIC - Executive modernization monitoring
# MAGIC

# COMMAND ----------

import requests
from datetime import datetime

workflow_url = "https://default93f33571550f43cfb09fcd331338d0.86.environment.api.powerplatform.com/powerautomate/automations/direct/workflows/d9e65b44e4d744298acf9fe4d9e21dd2/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=-4O8jb8OuaY7b7ovXS3zUC0w08WLEzdTZ-kfsVOXVkg"

alert_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

payload = {
    "type": "message",
    "attachments": [
        {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": "🚨 CodeGenome AI Modernization Alert",
                        "weight": "Bolder",
                        "size": "Large",
                        "color": "Attention"
                    },
                    {
                        "type": "TextBlock",
                        "text": f"Alert Time: {alert_time}",
                        "wrap": True
                    },
                    {
                        "type": "FactSet",
                        "facts": [
                            {"title": "Severity", "value": "CRITICAL"},
                            {"title": "High Risk Programs", "value": "PAYROLL.cbl, JOB001.jcl"},
                            {"title": "Source", "value": "Databricks CodeGenome AI"},
                            {"title": "Recommendation", "value": "Perform phased modernization and dependency remediation."}
                        ]
                    }
                ]
            }
        }
    ]
}

response = requests.post(workflow_url, json=payload)

print("Status Code:", response.status_code)
print("Response:", response.text)

if response.status_code in [200, 202]:
    print("Workflow triggered successfully. Please check the configured Teams channel/chat.")
else:
    print("Workflow trigger failed. Check Power Automate run history.")

# COMMAND ----------

import base64

image_path = "/Volumes/workspace/default/codegenome/AI Transformation Dashboard.png"

with open(image_path, "rb") as image_file:
    encoded_image = base64.b64encode(image_file.read()).decode("utf-8")

displayHTML(f"""

<div style="
    width:100%;
    min-height:100vh;

    background:
        radial-gradient(circle at top left, #0f172a 0%, #020617 45%),
        linear-gradient(135deg, #020617, #111827);

    padding:35px;
    box-sizing:border-box;

    display:flex;
    justify-content:center;
    align-items:center;
">

<div style="
    width:96%;
    max-width:1800px;

    background:rgba(15,23,42,0.92);

    border:1px solid rgba(0,229,255,0.35);

    border-radius:24px;

    padding:35px;

    box-shadow:
        0 0 25px rgba(0,229,255,0.18),
        0 0 80px rgba(0,229,255,0.08);

    backdrop-filter:blur(10px);

    text-align:center;
">

<h1 style="
    font-size:48px;
    font-weight:900;
    margin-bottom:12px;

    background:linear-gradient(
        90deg,
        #00E5FF,
        #38BDF8,
        #818CF8
    );

    -webkit-background-clip:text;
    -webkit-text-fill-color:transparent;

    letter-spacing:1px;
">
    CodeGenome AI Modernization Intelligence Hub
</h1>

<div style="
    color:#CBD5E1;
    font-size:19px;
    margin-bottom:28px;
">
AI-Powered Enterprise Mainframe Modernization Intelligence Platform
</div>

<div style="
    border-radius:22px;
    overflow:hidden;

    border:1px solid rgba(255,255,255,0.08);

    box-shadow:
        0 0 30px rgba(0,0,0,0.7),
        0 0 50px rgba(0,229,255,0.10);
">

<img
    src="data:image/png;base64,{encoded_image}"
    width="100%"
    style="
        display:block;
        border-radius:22px;
    "
/>

</div>

<div style="
    margin-top:28px;

    display:flex;
    justify-content:center;
    flex-wrap:wrap;
    gap:16px;
">

<div style="
    background:#0F172A;
    padding:10px 18px;
    border-radius:12px;
    border:1px solid #1E293B;
    color:#38BDF8;
">
✔ COBOL Intelligence
</div>

<div style="
    background:#0F172A;
    padding:10px 18px;
    border-radius:12px;
    border:1px solid #1E293B;
    color:#38BDF8;
">
✔ DB2 Lineage
</div>

<div style="
    background:#0F172A;
    padding:10px 18px;
    border-radius:12px;
    border:1px solid #1E293B;
    color:#38BDF8;
">
✔ JCL Orchestration
</div>

<div style="
    background:#0F172A;
    padding:10px 18px;
    border-radius:12px;
    border:1px solid #1E293B;
    color:#38BDF8;
">
✔ AI Risk Intelligence
</div>

<div style="
    background:#0F172A;
    padding:10px 18px;
    border-radius:12px;
    border:1px solid #1E293B;
    color:#38BDF8;
">
✔ Modernization Readiness
</div>

</div>

</div>

</div>

""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Sample AI / Genie Questions
# MAGIC
# MAGIC Use these prompts to validate CodeGenome AI across modernization risk, dependency intelligence, DB2 lineage, JCL orchestration, business rules, and executive reporting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Executive Modernization Insights
# MAGIC
# MAGIC - Generate executive modernization summary
# MAGIC - Show enterprise risk overview
# MAGIC - Which applications are cloud modernization candidates?
# MAGIC - Which applications are good candidates for API enablement?
# MAGIC - Show modernization readiness scores
# MAGIC - Generate modernization recommendation report
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Modernization Risk & Readiness
# MAGIC
# MAGIC - Which files have high modernization risk?
# MAGIC - Show high-risk programs
# MAGIC - Which programs require dependency remediation before modernization?
# MAGIC - Show medium-risk programs
# MAGIC - Which assets are low-risk modernization candidates?
# MAGIC - Show modernization priority by application
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. COBOL Dependency Intelligence
# MAGIC
# MAGIC - List all COBOL programs
# MAGIC - Show program dependencies
# MAGIC - Which programs call TAXCALC?
# MAGIC - Which programs call EMPREAD?
# MAGIC - Which copybooks are used frequently?
# MAGIC - Show CUSTOMER.cbl dependencies
# MAGIC - Show PAYROLL.cbl dependencies
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. DB2 Lineage & Data Flow
# MAGIC
# MAGIC - Which programs update EMP_MASTER?
# MAGIC - Which programs use CUSTOMER_MASTER?
# MAGIC - Which files perform INSERT operations?
# MAGIC - Which DB2 tables are frequently updated?
# MAGIC - Show DB2 lineage
# MAGIC - Show DB2 table usage summary
# MAGIC - Show programs with database write operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. JCL / Batch Flow Analysis
# MAGIC
# MAGIC - Show all JCL jobs
# MAGIC - Show JCL job flow
# MAGIC - Which programs are executed by JOB001?
# MAGIC - Show batch execution flow
# MAGIC - Which JCL files have no EXEC steps?
# MAGIC - Which batch jobs have the highest dataset count?
# MAGIC - Show job execution sequence
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 6. Business Rules & Functional Logic
# MAGIC
# MAGIC - Explain payroll logic
# MAGIC - Explain PAYROLL processing
# MAGIC - Explain customer logic
# MAGIC - Show business rules
# MAGIC - Which programs calculate payroll tax?
# MAGIC - Show employee master business rules
# MAGIC - Show customer audit business rules
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 7. Enterprise Inventory & Governance
# MAGIC
# MAGIC - Show all included files
# MAGIC - Show file inventory
# MAGIC - List all SQL files
# MAGIC - List all copybooks
# MAGIC - Which files were added for modernization assessment?
# MAGIC - Show file type distribution
# MAGIC - Show total legacy asset count
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 8. AI / GenAI Demo Prompts
# MAGIC
# MAGIC - Generate application dependency summary
# MAGIC - Explain mainframe system architecture
# MAGIC - Generate migration wave plan
# MAGIC - Identify modernization blockers
# MAGIC - Recommend phased modernization strategy
# MAGIC - Generate executive modernization report

# COMMAND ----------

# MAGIC %md
# MAGIC # Solution Architecture

# COMMAND ----------

displayHTML("""
<h1 style='color:#4FC3F7;
text-align:center;
font-size:38px;
margin-top:20px;
margin-bottom:20px;'>
Enterprise AI Modernization Architecture
</h1>
""")

# COMMAND ----------

displayHTML("""
<div style="
background:#1E1E1E;
padding:20px;
border-radius:12px;
border-left:6px solid #4FC3F7;
margin-top:20px;
margin-bottom:20px;
color:white;">

<h2 style="color:#4FC3F7;">Architecture Highlights</h2>

<ul style="font-size:16px; line-height:1.8;">
<li>Legacy Mainframe Metadata Discovery</li>
<li>AI-Based Dependency Mapping</li>
<li>DB2 Lineage Intelligence</li>
<li>JCL Flow Analysis</li>
<li>Cloud Modernization Recommendation Engine</li>
<li>GenAI-Powered Knowledge Assistant</li>
</ul>

</div>
""")

# COMMAND ----------

import base64

image_path = "/Volumes/workspace/default/codegenome/Sol_Arch.png"

with open(image_path, "rb") as image_file:
    encoded_image = base64.b64encode(image_file.read()).decode("utf-8")

displayHTML(f"""
<div style="text-align:center; margin-top:20px; margin-bottom:20px;">
  <img src="data:image/png;base64,{encoded_image}"
       style="width:75%;
              max-width:900px;
              border-radius:12px;
              box-shadow:0px 4px 15px rgba(0,0,0,0.35);" />
</div>
""")

# COMMAND ----------

displayHTML("""

<div style="
display:flex;
gap:25px;
margin-top:30px;
flex-wrap:wrap;
">

<!-- CARD 1 -->
<div style="
flex:1;
min-width:250px;
background:linear-gradient(145deg,#1E1E1E,#2A2A2A);
padding:28px;
border-radius:18px;
text-align:center;
border-top:6px solid #4FC3F7;
color:white;
box-shadow:0px 0px 18px rgba(79,195,247,0.25);
transition:0.3s;
">

<h2 style="
font-size:20px;
margin-bottom:10px;
color:#4FC3F7;">
📊 Programs Analyzed
</h2>

<h1 style="
font-size:48px;
margin:10px 0;
font-weight:bold;">
250+
</h1>

<p style="
font-size:15px;
color:#D5D8DC;">
Enterprise COBOL, JCL & SQL assets scanned
</p>

</div>

<!-- CARD 2 -->
<div style="
flex:1;
min-width:250px;
background:linear-gradient(145deg,#1E1E1E,#2A2A2A);
padding:28px;
border-radius:18px;
text-align:center;
border-top:6px solid #58D68D;
color:white;
box-shadow:0px 0px 18px rgba(88,214,141,0.25);
">

<h2 style="
font-size:20px;
margin-bottom:10px;
color:#58D68D;">
🗄️ DB2 Tables Mapped
</h2>

<h1 style="
font-size:48px;
margin:10px 0;
font-weight:bold;">
1200+
</h1>

<p style="
font-size:15px;
color:#D5D8DC;">
Cross-application DB2 lineage extracted
</p>

</div>

<!-- CARD 3 -->
<div style="
flex:1;
min-width:250px;
background:linear-gradient(145deg,#1E1E1E,#2A2A2A);
padding:28px;
border-radius:18px;
text-align:center;
border-top:6px solid #F7DC6F;
color:white;
box-shadow:0px 0px 18px rgba(247,220,111,0.25);
">

<h2 style="
font-size:20px;
margin-bottom:10px;
color:#F7DC6F;">
🚀 Modernization Readiness
</h2>

<h1 style="
font-size:48px;
margin:10px 0;
font-weight:bold;">
85%
</h1>

<p style="
font-size:15px;
color:#D5D8DC;">
Applications ready for cloud/API transformation
</p>

</div>

</div>

""")

# COMMAND ----------

displayHTML("""

<div style="
background:linear-gradient(145deg,#111827,#1F2937);
padding:35px;
border-radius:18px;
margin-top:35px;
color:white;
box-shadow:0px 0px 20px rgba(88,214,141,0.15);
border-left:6px solid #58D68D;
">

<h1 style="
color:#58D68D;
font-size:32px;
margin-bottom:20px;
font-weight:bold;">
✅ Final Outcome
</h1>

<p style="
font-size:17px;
line-height:1.9;
color:#E5E7EB;
margin-bottom:25px;
">

<b>CodeGenome AI</b> enables enterprises to accelerate legacy modernization
by automatically extracting application dependencies, analyzing modernization risks,
mapping business logic, discovering DB2 lineage, and generating AI-powered
modernization intelligence across large-scale mainframe ecosystems.

</p>

<div style="
display:grid;
grid-template-columns:repeat(auto-fit,minmax(250px,1fr));
gap:18px;
margin-top:20px;
">

<div style="
background:#0F172A;
padding:18px;
border-radius:12px;
border-top:4px solid #4FC3F7;">
<h3>⚡ Faster Modernization</h3>
<p>Reduce manual assessment effort using AI-driven automation.</p>
</div>

<div style="
background:#0F172A;
padding:18px;
border-radius:12px;
border-top:4px solid #58D68D;">
<h3>🔍 Deep Dependency Visibility</h3>
<p>Visualize COBOL, JCL, DB2 and Copybook relationships instantly.</p>
</div>

<div style="
background:#0F172A;
padding:18px;
border-radius:12px;
border-top:4px solid #F7DC6F;">
<h3>📊 Executive Intelligence</h3>
<p>Provide modernization dashboards, risk heatmaps and AI insights.</p>
</div>

<div style="
background:#0F172A;
padding:18px;
border-radius:12px;
border-top:4px solid #FF6B6B;">
<h3>🚀 Cloud Readiness</h3>
<p>Identify API enablement and cloud migration opportunities.</p>
</div>

</div>

<div style="
margin-top:30px;
padding-top:18px;
border-top:1px solid #374151;
font-size:14px;
color:#9CA3AF;
text-align:center;
">

Built using Databricks • Python • Spark • Delta Lake • SQL • GenAI • AI/BI Dashboards

</div>

</div>

""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Final Outcome
# MAGIC
# MAGIC CodeGenome AI successfully demonstrates an end-to-end AI-powered enterprise legacy modernization intelligence platform built on the Databricks Lakehouse architecture.
# MAGIC
# MAGIC ## Key Capabilities Delivered
# MAGIC
# MAGIC - Enterprise legacy asset ingestion into the Databricks Lakehouse Platform
# MAGIC - Automated COBOL, JCL, DB2 SQL, and Copybook analysis
# MAGIC - AI-driven dependency discovery and application impact mapping
# MAGIC - DB2 lineage extraction with CRUD operation intelligence
# MAGIC - JCL batch orchestration and execution flow analysis
# MAGIC - Business rule extraction and functional intelligence generation
# MAGIC - AI-powered modernization readiness scoring and risk classification
# MAGIC - Automated enterprise governance alerting and risk notifications
# MAGIC - Interactive executive dashboards with modernization intelligence analytics
# MAGIC - Conversational AI assistant for modernization insights and governance queries
# MAGIC - Cloud/API modernization opportunity identification
# MAGIC - Dependency-heavy application and migration blocker detection
# MAGIC
# MAGIC ## Enterprise Governance & Intelligence
# MAGIC
# MAGIC CodeGenome AI integrates Databricks SQL Alerts and AI-driven governance workflows to proactively identify:
# MAGIC
# MAGIC - Critical modernization risks
# MAGIC - High-risk legacy assets
# MAGIC - DB2 write-intensive applications
# MAGIC - Dependency-heavy components
# MAGIC - Batch orchestration complexity
# MAGIC - Migration blockers and governance concerns
# MAGIC
# MAGIC Automated governance alerts enable enterprise teams to monitor modernization readiness continuously through scheduled intelligence-driven notifications and executive dashboards.
# MAGIC
# MAGIC ## Business Value
# MAGIC
# MAGIC CodeGenome AI accelerates enterprise modernization initiatives by:
# MAGIC
# MAGIC - Reducing manual legacy system assessment effort
# MAGIC - Improving enterprise application visibility
# MAGIC - Accelerating modernization readiness analysis
# MAGIC - Identifying modernization risks early
# MAGIC - Enhancing governance and compliance monitoring
# MAGIC - Supporting AI-assisted transformation decision-making
# MAGIC - Enabling proactive migration planning and impact analysis
# MAGIC
# MAGIC The solution provides a scalable modernization intelligence framework using the Databricks Lakehouse Platform for enterprise-wide legacy transformation programs.
# MAGIC
# MAGIC ## Technology Stack
# MAGIC
# MAGIC Databricks • Spark • Delta Lake • Python • SQL • GenAI • AI/BI Dashboards • NetworkX • Databricks SQL Alerts • Lakehouse Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Business Impact
# MAGIC
# MAGIC CodeGenome AI significantly reduces enterprise legacy modernization assessment effort by automating dependency discovery, business rule extraction, DB2 lineage analysis, and AI-driven modernization risk assessment across large-scale mainframe ecosystems.
# MAGIC
# MAGIC ## Key Business Benefits
# MAGIC
# MAGIC - Accelerated cloud migration and API modernization readiness
# MAGIC - Reduced manual documentation and dependency analysis effort
# MAGIC - Improved application impact analysis and modernization accuracy
# MAGIC - Faster modernization planning and transformation decision-making
# MAGIC - AI-assisted legacy knowledge extraction and governance visibility
# MAGIC - Enhanced risk identification for critical batch and DB2 applications
# MAGIC - Increased operational transparency through interactive executive dashboards
# MAGIC - Faster onboarding for modernization and support teams
# MAGIC
# MAGIC ## Enterprise Value Delivered
# MAGIC
# MAGIC CodeGenome AI transforms complex legacy modernization initiatives into an AI-driven, insight-based modernization program using the Databricks Lakehouse Platform, enabling organizations to modernize with greater confidence, speed, and governance.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # CodeGenome AI – Project Links & Demo
# MAGIC
# MAGIC ## 🎥 Hackathon Demo Video
# MAGIC https://youtu.be/QWM4DxoUzoA
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💻 GitHub Repository
# MAGIC https://github.com/Mangayarkarasi-V/codegenome-ai-modernization
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Project Overview
# MAGIC
# MAGIC CodeGenome AI is an AI-powered enterprise modernization accelerator built using:
# MAGIC
# MAGIC - Databricks Lakehouse
# MAGIC - Delta Lake
# MAGIC - Databricks SQL
# MAGIC - Python
# MAGIC - Streamlit
# MAGIC - Genie AI
# MAGIC - Zabbix Governance Integration
# MAGIC - Microsoft Teams Automation
# MAGIC
# MAGIC The platform automates modernization intelligence for:
# MAGIC
# MAGIC - COBOL Programs
# MAGIC - JCL Workflows
# MAGIC - DB2 SQL
# MAGIC - MQ Queue Analysis
# MAGIC - IMS Transaction Processing
# MAGIC - Legacy Dependency Discovery
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✨ Key Features
# MAGIC
# MAGIC - AI Modernization Readiness Scoring
# MAGIC - DB2 CRUD Lineage Intelligence
# MAGIC - MQ Queue Dependency Analysis
# MAGIC - IMS Modernization Insights
# MAGIC - JCL Orchestration Intelligence
# MAGIC - AI Governance Alerts
# MAGIC - Teams Notification Integration
# MAGIC - Conversational GenAI Assistant
# MAGIC - Interactive Streamlit UI
# MAGIC - Executive Modernization Dashboard
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ Enterprise Modernization Governance Platform
# MAGIC
# MAGIC CodeGenome AI transforms enterprise modernization from a manual assessment process into an AI-driven modernization intelligence and governance platform.