# Microsoft Fabric notebook source
# This notebook has been adapted from the original Databricks version
# Available at https://github.com/databricks-industry-solutions/smart-claims

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import expr, col
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("✅ Libraries imported successfully")

# COMMAND ----------

print("🗃️ Setting up claims rules table...")

try:
    # Drop existing table if it exists (for clean setup)
    spark.sql("DROP TABLE IF EXISTS claims_rules")
    
    # Create the rules table
    create_table_sql = """
    CREATE TABLE claims_rules (
        rule_id BIGINT GENERATED ALWAYS AS IDENTITY,
        rule STRING, 
        check_name STRING,
        check_code STRING,
        check_severity STRING,
        is_active BOOLEAN,
        created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    ) USING DELTA
    """
    
    spark.sql(create_table_sql)
    print("✅ Claims rules table created successfully")
    
    # Show table structure
    print("\n📋 Rules table schema:")
    spark.sql("DESCRIBE claims_rules").show()
    
except Exception as e:
    logger.error(f"Error creating rules table: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Business Rules

# COMMAND ----------

print("⚙️ Configuring business rules...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rule 1: Policy Date Validation

# COMMAND ----------

print("📅 Adding Policy Date Validation rule...")

invalid_policy_date = '''
CASE WHEN to_date(pol_eff_date, "yyyy-MM-dd") <= to_date(claim_date) 
          AND to_date(pol_expiry_date, "yyyy-MM-dd") >= to_date(claim_date) 
     THEN "VALID" 
     ELSE "NOT VALID"  
END
'''

try:
    insert_sql = f"""
    INSERT INTO claims_rules(rule, check_name, check_code, check_severity, is_active) 
    VALUES('invalid policy date', 'valid_date', '{invalid_policy_date}', 'HIGH', TRUE)
    """
    
    spark.sql(insert_sql)
    print("✅ Policy date validation rule added")
    
except Exception as e:
    logger.error(f"Error adding policy date rule: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rule 2: Policy Amount Validation

# COMMAND ----------

print("💰 Adding Policy Amount Validation rule...")

exceeds_policy_amount = '''
CASE WHEN sum_insured >= claim_amount_total 
     THEN "claim value in the range of premium"
     ELSE "claim value more than premium"
END 
'''

try:
    insert_sql = f"""
    INSERT INTO claims_rules(rule, check_name, check_code, check_severity, is_active) 
    VALUES('exceeds policy amount', 'valid_amount', '{exceeds_policy_amount}', 'HIGH', TRUE)
    """
    
    spark.sql(insert_sql)
    print("✅ Policy amount validation rule added")
    
except Exception as e:
    logger.error(f"Error adding policy amount rule: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rule 3: Severity Matching

# COMMAND ----------

print("🎯 Adding Severity Matching rule...")

severity_mismatch = '''
CASE WHEN incident_severity="Total Loss" AND severity > 0.9 THEN "Severity matches the report"
     WHEN incident_severity="Major Damage" AND severity > 0.8 THEN "Severity matches the report"
     WHEN incident_severity="Minor Damage" AND severity > 0.7 THEN "Severity matches the report"
     WHEN incident_severity="Trivial Damage" AND severity > 0.4 THEN "Severity matches the report"
     ELSE "Severity does not match"
END 
'''

try:
    insert_sql = f"""
    INSERT INTO claims_rules(rule, check_name, check_code, check_severity, is_active) 
    VALUES('severity mismatch', 'reported_severity_check', '{severity_mismatch}', 'HIGH', TRUE)
    """
    
    spark.sql(insert_sql)
    print("✅ Severity matching rule added")
    
except Exception as e:
    logger.error(f"Error adding severity rule: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rule 4: Speed Validation

# COMMAND ----------

print("🚗 Adding Speed Validation rule...")

exceeds_speed = '''
CASE WHEN telematics_speed <= 45 AND telematics_speed > 0 THEN "Normal Speed"
     WHEN telematics_speed > 45 THEN "High Speed"
     ELSE "Invalid speed"
END
'''

try:
    insert_sql = f"""
    INSERT INTO claims_rules(rule, check_name, check_code, check_severity, is_active) 
    VALUES('exceeds speed', 'speed_check', '{exceeds_speed}', 'HIGH', TRUE)
    """
    
    spark.sql(insert_sql)
    print("✅ Speed validation rule added")
    
except Exception as e:
    logger.error(f"Error adding speed rule: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rule 5: Final Decision Rule

# COMMAND ----------

print("⚖️ Adding Final Decision rule...")

release_funds = '''
CASE WHEN reported_severity_check="Severity matches the report" 
          AND valid_amount="claim value in the range of premium" 
          AND valid_date="VALID" 
     THEN "release funds"
     ELSE "claim needs more investigation" 
END
'''

try:
    insert_sql = f"""
    INSERT INTO claims_rules(rule, check_name, check_code, check_severity, is_active) 
    VALUES('release funds', 'release_funds', '{release_funds}', 'HIGH', TRUE)
    """
    
    spark.sql(insert_sql)
    print("✅ Final decision rule added")
    
except Exception as e:
    logger.error(f"Error adding final decision rule: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Configured Rules

# COMMAND ----------

print("📋 Current rules configuration:")

try:
    rules_df = spark.sql("SELECT * FROM claims_rules ORDER BY rule_id")
    
    print(f"\n📊 Total rules configured: {rules_df.count()}")
    print("\n🔍 Rules summary:")
    
    display(rules_df.select("rule_id", "rule", "check_name", "check_severity", "is_active"))
    
    # Count active rules
    active_rules = spark.sql("SELECT COUNT(*) as active_count FROM claims_rules WHERE is_active = TRUE").collect()[0][0]
    print(f"\n✅ Active rules: {active_rules}")
    
except Exception as e:
    logger.error(f"Error viewing rules: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Claims Data

# COMMAND ----------

# Schema configuration
silver = "silver"
gold = "gold"
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold}")
    print(f"✅ Ensured schemas '{silver}' and '{gold}' exist")
except Exception as sc_err:
    print(f"⚠️ Schema creation issue: {sc_err}")

print("📥 Loading claims data for rule application...")

try:
    # Check if the source table exists (schema-qualified)
    source_tables = [
        f"{silver}.silver_claim_policy_accident",
        f"{silver}.silver_claim_policy_location",
        f"{silver}.silver_claim_policy"
    ]
    source_table = None
    for table in source_tables:
        try:
            test_df = spark.table(table)
            source_table = table
            print(f"✅ Found source table: {source_table}")
            break
        except Exception:
            continue
    if not source_table:
        raise Exception(f"None of the expected source tables found: {source_tables}")
    # Load the data
    claims_df = spark.sql(f"SELECT * FROM {source_table}")
    
    record_count = claims_df.count()
    column_count = len(claims_df.columns)
    
    print(f"📊 Source data loaded:")
    print(f"   Table: {source_table}")
    print(f"   Records: {record_count:,}")
    print(f"   Columns: {column_count}")
    
    # Show sample data
    print(f"\n📄 Sample source data:")
    display(claims_df.limit(5))
    
except Exception as e:
    logger.error(f"Error loading claims data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Rules Dynamically

# COMMAND ----------

print("⚡ Applying rules dynamically to claims data...")

try:
    # Get active rules
    rules_query = """
    SELECT rule_id, rule, check_name, check_code, check_severity 
    FROM claims_rules 
    WHERE is_active = TRUE 
    ORDER BY rule_id
    """
    
    active_rules = spark.sql(rules_query).collect()
    
    print(f"🔧 Applying {len(active_rules)} active rules...")
    
    # Start with the original claims data
    enhanced_df = claims_df
    
    # Apply each rule as a new column
    applied_rules = []
    for rule in active_rules:
        try:
            print(f"   Applying: {rule.rule} -> {rule.check_name}")
            
            # Apply the rule as a new column
            enhanced_df = enhanced_df.withColumn(rule.check_name, expr(rule.check_code))
            applied_rules.append(rule.rule)
            
        except Exception as e:
            logger.warning(f"Failed to apply rule '{rule.rule}': {str(e)}")
    
    print(f"✅ Successfully applied {len(applied_rules)} rules")
    
    # Show the enhanced data with applied rules
    print(f"\n📊 Enhanced data with applied rules:")
    rule_columns = [rule.check_name for rule in active_rules if rule.rule in applied_rules]
    
    # Select a subset of columns for display
    display_columns = ["policy_no", "claim_no"] + rule_columns[:5]  # Show first 5 rule results
    display(enhanced_df.select(*display_columns).limit(10))
    
except Exception as e:
    logger.error(f"Error applying rules: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Enhanced Data as Gold Table

# COMMAND ----------

print("💾 Saving enhanced data to gold_insights table...")

try:
    # Add processing metadata
    final_df = enhanced_df.withColumn("rules_processed_timestamp", F.current_timestamp())
    
    # Write to gold insights table (schema-qualified)
    (final_df.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{gold}.gold_insights"))
    
    # Optimize the table
    spark.sql(f"OPTIMIZE {gold}.gold_insights")
    
    # Verify the save
    saved_count = spark.table(f"{gold}.gold_insights").count()
    print(f"✅ Successfully saved {saved_count:,} records to {gold}.gold_insights")
    
    print(f"\n📋 Gold table schema:")
    spark.table(f"{gold}.gold_insights").printSchema()
    
except Exception as e:
    logger.error(f"Error saving gold insights: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rules Analysis and Insights

# COMMAND ----------

print("📊 Generating rules analysis and insights...")

try:
    # Profile the insights generated
    insights_df = spark.table(f"{gold}.gold_insights")
    
    # Analyze rule results
    rule_columns = [rule.check_name for rule in active_rules if rule.rule in applied_rules]
    
    print(f"\n🔍 Rule Application Results:")
    
    for rule_col in rule_columns:
        print(f"\n📈 Analysis for: {rule_col}")
        
        try:
            # Get value counts for this rule
            result_counts = (insights_df
                           .groupBy(rule_col)
                           .count()
                           .orderBy(F.desc("count")))
            
            display(result_counts)
            
        except Exception as e:
            print(f"   ⚠️ Could not analyze {rule_col}: {str(e)}")
    
    # Summary analysis for key decision metrics
    print(f"\n🎯 Key Decision Metrics Summary:")
    
    key_metrics = ["valid_date", "valid_amount", "reported_severity_check", "release_funds"]
    available_metrics = [col for col in key_metrics if col in insights_df.columns]
    
    if available_metrics:
        summary_df = insights_df.select(*available_metrics)
        display(summary_df.limit(20))
        
        # Count final decisions
        if "release_funds" in available_metrics:
            decision_summary = (insights_df
                              .groupBy("release_funds")
                              .count()
                              .orderBy(F.desc("count")))
            
            print(f"\n⚖️ Final Decision Summary:")
            display(decision_summary)
    
except Exception as e:
    logger.error(f"Error generating insights analysis: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Intelligence Summary

# COMMAND ----------

print("="*60)
print("CLAIMS RULES ENGINE SUMMARY")
print("="*60)

try:
    # Get final statistics
    total_claims = spark.table(f"{gold}.gold_insights").count()
    total_rules = spark.sql("SELECT COUNT(*) FROM claims_rules WHERE is_active = TRUE").collect()[0][0]
    
    print(f"📊 Processing Summary:")
    print(f"   Total claims processed: {total_claims:,}")
    print(f"   Active rules applied: {total_rules}")
    print(f"   Output table: {gold}.gold_insights")
    
    # Decision breakdown
    if "release_funds" in [rule.check_name for rule in active_rules]:
        try:
            release_count = spark.sql(f"""
                SELECT COUNT(*) as count 
                FROM {gold}.gold_insights 
                WHERE release_funds = 'release funds'
            """).collect()[0][0]
            
            investigate_count = total_claims - release_count
            
            print(f"\n💰 Fund Release Decisions:")
            print(f"   ✅ Ready for release: {release_count:,} ({(release_count/total_claims)*100:.1f}%)")
            print(f"   🔍 Need investigation: {investigate_count:,} ({(investigate_count/total_claims)*100:.1f}%)")
            
        except Exception as e:
            print(f"   ⚠️ Could not calculate decision breakdown: {str(e)}")
    
    print(f"\n🎯 Business Impact:")
    print(f"   • Automated routine claim processing")
    print(f"   • Reduced manual review workload")
    print(f"   • Consistent rule application")
    print(f"   • Faster claim resolution")
    print(f"   • Improved fraud detection")
    
    print(f"\n📈 Next Steps:")
    print(f"   1. Review flagged claims requiring investigation")
    print(f"   2. Create Power BI dashboards for rule monitoring")
    print(f"   3. Add new rules as business requirements evolve")
    print(f"   4. Set up automated processing for new claims")
    print(f"   5. Monitor rule effectiveness and accuracy")
    
except Exception as e:
    print(f"❌ Error in summary generation: {str(e)}")

print("="*60)
print("✅ Claims rules engine processing completed!")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule Management Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add New Rule (Example Function)

# COMMAND ----------

def add_new_rule(rule_name, check_name, check_code, severity="MEDIUM", is_active=True):
    """
    Function to add a new rule to the claims_rules table
    
    Parameters:
    - rule_name: Descriptive name for the rule
    - check_name: Column name for the rule result
    - check_code: SQL CASE statement for the rule logic
    - severity: HIGH, MEDIUM, or LOW
    - is_active: Boolean to activate/deactivate the rule
    """
    try:
        insert_sql = f"""
        INSERT INTO claims_rules(rule, check_name, check_code, check_severity, is_active) 
        VALUES('{rule_name}', '{check_name}', '{check_code}', '{severity}', {is_active})
        """
        
        spark.sql(insert_sql)
        print(f"✅ Successfully added rule: {rule_name}")
        
    except Exception as e:
        print(f"❌ Error adding rule: {str(e)}")

# Example usage (commented out):
# new_rule_code = '''
# CASE WHEN driver_age < 25 AND incident_severity IN ("Major Damage", "Total Loss") 
#      THEN "High Risk Young Driver"
#      ELSE "Standard Risk"
# END
# '''
# add_new_rule("Young Driver Risk", "young_driver_risk", new_rule_code, "MEDIUM", True)

print("📋 Rule management functions defined")
print("   Use add_new_rule() to add new business rules dynamically")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deactivate Rule (Example Function)

# COMMAND ----------

def toggle_rule_status(rule_name, is_active):
    """
    Function to activate or deactivate a rule
    
    Parameters:
    - rule_name: Name of the rule to toggle
    - is_active: Boolean to set the rule status
    """
    try:
        update_sql = f"""
        UPDATE claims_rules 
        SET is_active = {is_active}, updated_timestamp = CURRENT_TIMESTAMP()
        WHERE rule = '{rule_name}'
        """
        
        spark.sql(update_sql)
        status = "activated" if is_active else "deactivated"
        print(f"✅ Successfully {status} rule: {rule_name}")
        
    except Exception as e:
        print(f"❌ Error updating rule status: {str(e)}")

# Example usage (commented out):
# toggle_rule_status("exceeds speed", False)  # Deactivate speed rule
# toggle_rule_status("exceeds speed", True)   # Reactivate speed rule

print("📋 Rule toggle function defined")
print("   Use toggle_rule_status() to activate/deactivate rules as needed")
