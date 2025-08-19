# Insurance Claims Processing Data Agent Use Case

## Overview

This document outlines the comprehensive design and implementation strategy for creating Microsoft Fabric Data Agents for the insurance claims processing use case. Based on the analysis of the fabric-claimsprocessing repository's data model and business requirements, this guide provides specific recommendations for data agent configurations, use cases, and example queries.

## 1. Suggested Data Agent Use Cases

Based on the rich insurance data model in this repository, here are the most valuable data agent scenarios:

### Primary Use Cases
1. **Claims Operations Agent** - For claims adjusters and processors
2. **Risk Assessment Agent** - For underwriters and risk managers  
3. **Financial Performance Agent** - For executives and actuaries
4. **Fraud Detection Agent** - For investigators and compliance teams
5. **Customer Service Agent** - For customer service representatives

### Secondary Use Cases
6. **Telematics Insights Agent** - For usage-based insurance analysis
7. **Location Analytics Agent** - For geographic risk assessment
8. **Policy Management Agent** - For sales and underwriting teams

## 2. Primary Data Agent Configuration

### Recommended Agent: "Insurance Claims Intelligence Agent"

#### Agent Description
```markdown
You are an Insurance Claims Intelligence Assistant specializing in property & casualty insurance analytics. You help insurance professionals analyze claims data, assess risks, monitor financial performance, and identify patterns in the comprehensive insurance dataset.

Your expertise includes:
- Claims processing and settlement analysis
- Risk assessment using telematics and severity modeling
- Financial metrics like loss ratios, claim frequencies, and exposure analysis
- Geographic and demographic risk profiling
- Fraud and suspicious activity detection
- Policy performance and customer analytics

Always provide data-driven insights with specific metrics and context. When analyzing claims, consider the relationships between policies, vehicles, locations, and risk factors. Include relevant financial figures, percentages, and trends in your responses.

Key Business Terms:
- "Claim Amount" refers to total claim cost including vehicle, injury, and property damage
- "Loss Ratio" = Total Claim Amount / Premium Amount
- "Claim Frequency" = Total Claims / Total Policies
- "High Severity" includes claims categorized as "High" or "Medium-High" severity
- "Processing Flag" indicates claims requiring special attention (REVIEW_REQUIRED, TOTAL_LOSS_CANDIDATE)
- "Speed Risk Indicator" from telematics data shows driving behavior risk levels
- "Calendar Year" = January-December, "Policy Year" = policy issue date anniversary
```

## 3. Data Source Configuration

### Primary Data Sources
1. **gold.fact_claims** - Core claims data with amounts and categorizations
2. **gold.dim_policy** - Policy details including customer and premium information  
3. **gold.dim_vehicle** - Vehicle make, model, and characteristics
4. **gold.dim_location** - Geographic information for risk assessment
5. **gold.dim_date** - Time intelligence for trending analysis
6. **gold.fact_telematics** - Driving behavior and risk indicators
7. **gold.dim_risk** - Risk profile categorizations

### Secondary Data Sources
8. **gold.fact_accident_severity** - ML-driven severity scores
9. **gold.fact_rules** - Rules engine validation results
10. **gold.constraint_audit** - Data quality metrics

### Data Source Instructions
```markdown
Claims Data Analysis Instructions:
- Use fact_claims as the primary table for claim amounts and counts
- Always join to dim_policy for customer demographics and premium data
- Join to dim_vehicle for make/model analysis
- Join to dim_location for geographic insights
- Use dim_date for time-based analysis and trending
- Include telematics data (fact_telematics) when analyzing driving behavior
- Filter by processing_flag for claims requiring special attention
- Use severity_category values: "Low", "Medium-Low", "Medium", "Medium-High", "High"
- Speed risk indicators: "LOW_SPEED_SAFE", "MODERATE_SPEED_RISK", "HIGH_SPEED_SEVERE"
- Location identifiers use location_id (GUID) for joins
- Risk profiles use risk_key for dimensional analysis
```

## 4. Example Questions and SQL Queries

### A. Claims Operations Questions

#### Question 1: "What are the top 10 highest claim amounts this year and what are their details?"
```sql
SELECT TOP 10 
    fc.claim_no,
    fc.claim_amount_total,
    fc.claim_date,
    fc.severity_category,
    fc.processing_flag,
    dp.customer_id,
    dv.make,
    dv.model,
    dl.neighborhood,
    dl.zip_code
FROM gold.fact_claims fc
JOIN gold.dim_policy dp ON fc.policy_no = dp.policy_no
JOIN gold.dim_vehicle dv ON fc.chassis_no = dv.chassis_no  
JOIN gold.dim_location dl ON fc.location_id = dl.location_id
WHERE YEAR(fc.claim_date) = YEAR(GETDATE())
ORDER BY fc.claim_amount_total DESC
```

#### Question 2: "How many claims need manual review and what's the average claim amount for review cases?"
```sql
SELECT 
    COUNT(*) as claims_requiring_review,
    AVG(claim_amount_total) as avg_review_claim_amount,
    SUM(claim_amount_total) as total_review_amount
FROM gold.fact_claims 
WHERE processing_flag = 'REVIEW_REQUIRED'
```

### B. Risk Assessment Questions

#### Question 3: "What is the loss ratio by driver age group?"
```sql
SELECT 
    dp.[Driver Age Bucket],
    SUM(fc.claim_amount_total) as total_claims,
    SUM(dp.premium) as total_premiums,
    SUM(fc.claim_amount_total) / SUM(dp.premium) as loss_ratio,
    COUNT(DISTINCT fc.claim_no) as claim_count
FROM gold.fact_claims fc
JOIN gold.dim_policy dp ON fc.policy_no = dp.policy_no
GROUP BY dp.[Driver Age Bucket]
ORDER BY loss_ratio DESC
```

#### Question 4: "Which locations have the highest concentration of high-severity claims?"
```sql
SELECT 
    dl.neighborhood,
    dl.borough,
    dl.zip_code,
    COUNT(*) as total_claims,
    SUM(CASE WHEN fc.severity_category IN ('High', 'Medium-High') THEN 1 ELSE 0 END) as high_severity_claims,
    (SUM(CASE WHEN fc.severity_category IN ('High', 'Medium-High') THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as high_severity_percentage,
    AVG(fc.claim_amount_total) as avg_claim_amount
FROM gold.fact_claims fc
JOIN gold.dim_location dl ON fc.location_id = dl.location_id
GROUP BY dl.neighborhood, dl.borough, dl.zip_code
HAVING COUNT(*) >= 10
ORDER BY high_severity_percentage DESC, total_claims DESC
```

### C. Financial Performance Questions

#### Question 5: "What is our monthly claims trend and how does it compare to last year?"
```sql
SELECT 
    dd.year,
    dd.month_name,
    COUNT(DISTINCT fc.claim_no) as total_claims,
    SUM(fc.claim_amount_total) as total_claim_amount,
    AVG(fc.claim_amount_total) as avg_claim_amount
FROM gold.fact_claims fc
JOIN gold.dim_date dd ON fc.claim_date = dd.date_key
WHERE dd.year IN (YEAR(GETDATE()), YEAR(GETDATE())-1)
GROUP BY dd.year, dd.month_name, dd.month_of_year
ORDER BY dd.year, dd.month_of_year
```

#### Question 6: "What vehicle makes have the highest claim frequency and amounts?"
```sql
SELECT 
    dv.make,
    COUNT(DISTINCT fc.claim_no) as total_claims,
    COUNT(DISTINCT fc.policy_no) as policies_with_claims,
    SUM(fc.claim_amount_total) as total_claim_amount,
    AVG(fc.claim_amount_total) as avg_claim_amount,
    (COUNT(DISTINCT fc.claim_no) * 100.0 / COUNT(DISTINCT fc.policy_no)) as claim_frequency_pct
FROM gold.fact_claims fc
JOIN gold.dim_vehicle dv ON fc.chassis_no = dv.chassis_no
GROUP BY dv.make
HAVING COUNT(DISTINCT fc.claim_no) >= 20
ORDER BY total_claim_amount DESC
```

### D. Telematics & Risk Behavior Questions

#### Question 7: "How does telematics speed correlate with claim severity and amounts?"
```sql
SELECT 
    ft.speed_risk_indicator,
    COUNT(DISTINCT fc.claim_no) as total_claims,
    AVG(ft.telematics_speed) as avg_speed,
    AVG(fc.severity) as avg_severity_score,
    SUM(fc.claim_amount_total) as total_claim_amount,
    AVG(fc.claim_amount_total) as avg_claim_amount
FROM gold.fact_claims fc
JOIN gold.fact_telematics ft ON fc.claim_no = ft.claim_no
GROUP BY ft.speed_risk_indicator
ORDER BY avg_claim_amount DESC
```

#### Question 8: "What is the relationship between distance from accident location and claim amounts?"
```sql
SELECT 
    CASE 
        WHEN ft.accident_telematics_distance_miles < 1 THEN 'Under 1 mile'
        WHEN ft.accident_telematics_distance_miles < 5 THEN '1-5 miles'
        WHEN ft.accident_telematics_distance_miles < 10 THEN '5-10 miles'
        ELSE 'Over 10 miles'
    END as distance_category,
    COUNT(*) as claim_count,
    AVG(fc.claim_amount_total) as avg_claim_amount,
    AVG(ft.accident_telematics_distance_miles) as avg_distance_miles
FROM gold.fact_claims fc
JOIN gold.fact_telematics ft ON fc.claim_no = ft.claim_no
WHERE ft.accident_telematics_distance_miles IS NOT NULL
GROUP BY 
    CASE 
        WHEN ft.accident_telematics_distance_miles < 1 THEN 'Under 1 mile'
        WHEN ft.accident_telematics_distance_miles < 5 THEN '1-5 miles'
        WHEN ft.accident_telematics_distance_miles < 10 THEN '5-10 miles'
        ELSE 'Over 10 miles'
    END
ORDER BY avg_claim_amount DESC
```

### E. Fraud Detection Questions

#### Question 9: "What patterns exist in claims flagged for suspicious activity?"
```sql
SELECT 
    fc.processing_flag,
    COUNT(*) as total_claims,
    AVG(fc.claim_amount_total) as avg_claim_amount,
    SUM(fc.claim_amount_total) as total_amount,
    AVG(DATEDIFF(day, fc.claim_date, dp.pol_issue_date)) as avg_days_since_policy_start
FROM gold.fact_claims fc
JOIN gold.dim_policy dp ON fc.policy_no = dp.policy_no
WHERE fc.processing_flag IN ('REVIEW_REQUIRED', 'TOTAL_LOSS_CANDIDATE')
GROUP BY fc.processing_flag
```

### F. Data Quality & Operational Questions

#### Question 10: "What is our current data quality status and any issues to address?"
```sql
SELECT 
    entity,
    metric,
    value,
    CASE 
        WHEN metric LIKE '%orphan%' AND CAST(value AS INT) > 0 THEN 'ATTENTION REQUIRED'
        WHEN metric LIKE '%duplicate%' AND CAST(value AS INT) > 0 THEN 'ATTENTION REQUIRED'
        ELSE 'OK'
    END as status
FROM gold.constraint_audit
ORDER BY 
    CASE WHEN CAST(value AS INT) > 0 THEN 0 ELSE 1 END,
    entity, metric
```

## 5. Implementation Best Practices

### Data Agent Configuration Guidelines

1. **Minimize Scope**: Focus on the essential 7-10 tables from the gold layer
2. **Clear Instructions**: Provide specific guidance on table relationships and business logic
3. **Business Context**: Define domain-specific terminology and calculations
4. **Example Queries**: Include representative queries for common use cases

### Security and Governance

1. **Row-Level Security**: Consider implementing RLS on sensitive customer data
2. **Data Lineage**: Leverage the existing constraint_audit table for data quality monitoring
3. **Performance**: Use the star schema design for optimal query performance
4. **Incremental Updates**: Ensure the data agent works with the medallion architecture's incremental processing

### Integration with Existing Architecture

1. **Direct Lake Connectivity**: Connect to the existing Power BI semantic model
2. **Real-time Data**: Consider integration with the telematics streaming capabilities
3. **ML Integration**: Leverage existing severity prediction models
4. **Rules Engine**: Incorporate business rules validation results

## 6. Additional Specialized Agents

### Risk Assessment Agent
- Focus on dim_risk, fact_accident_severity, and telematics data
- Specialized for underwriting and risk management scenarios

### Customer Service Agent  
- Emphasis on policy details, claim status, and customer-facing metrics
- Simplified language for non-technical users

### Executive Dashboard Agent
- High-level KPIs and trend analysis
- Focus on financial metrics and operational efficiency

## 7. Deployment Considerations

### Prerequisites
- Microsoft Fabric workspace with Data Science experience enabled
- Access to the gold layer tables in the lakehouse
- Appropriate permissions for data agent creation
- Power BI semantic model deployment (optional but recommended)

### Performance Optimization
- Leverage existing table partitioning and indexing
- Use the star schema relationships for efficient joins
- Consider materialized views for frequently queried aggregations

### Monitoring and Maintenance
- Regular review of data quality metrics from constraint_audit
- Performance monitoring of complex queries
- User feedback collection for agent improvement

## 8. References and Sources

### Microsoft Documentation
- [Microsoft Fabric Data Agent Concepts](https://learn.microsoft.com/en-us/fabric/data-science/concept-data-agent)
- [How to Create Data Agent](https://learn.microsoft.com/en-us/fabric/data-science/how-to-create-data-agent)
- [Best Practices for Configuring Your Data Agent](https://learn.microsoft.com/en-us/fabric/data-science/data-agent-configuration-best-practices)
- [Data Agent Configurations](https://learn.microsoft.com/en-us/fabric/data-science/data-agent-configurations)
- [Microsoft Fabric for Financial Services](https://learn.microsoft.com/en-us/industry/financial-services/microsoft-fabric-fsi)
- [Privacy, Security, and Responsible Use of Copilot in Notebooks and Fabric Data Agents](https://learn.microsoft.com/en-us/fabric/fundamentals/copilot-data-science-privacy-security)

### Repository Analysis Sources
- **Repository Structure**: Analysis of `/notebooks/`, `/resource/`, and `/pbi-report/` directories
- **Data Model**: Power BI semantic model files in `Claims Analysis.SemanticModel/definition/`
  - `model.tmdl` - Main model structure
  - `tables/*.tmdl` - Individual table definitions (fact_claims, dim_policy, etc.)
  - `relationships.tmdl` - Star schema relationships
  - `measures_table.tmdl` - DAX measures and KPIs
- **Sample Data**: 
  - `resource/data_sources/Policies/policies.csv` - Policy data structure
  - `resource/data_sources/Claims/*.json` - Claims data format
- **Processing Notebooks**: 
  - `notebooks/07_policy_claims_accident_Goldviews.py` - Gold layer construction
  - Other notebooks for understanding the data pipeline
- **Documentation**: `README.md` - Business context and architecture overview

### Data Architecture References
- **Medallion Architecture**: Bronze → Silver → Gold layer pattern from repository implementation
- **Star Schema Design**: Dimensional modeling with facts and dimensions as implemented in the gold layer
- **Fabric Lakehouse Pattern**: OneLake storage with Delta tables as documented in repository guides

### Business Context Sources
- **Insurance Domain Knowledge**: Derived from data model analysis and README.md business requirements
- **Claims Processing Workflow**: Understanding from notebook processing pipeline and data relationships
- **KPI Definitions**: Based on existing DAX measures in the Power BI semantic model

---

*This document provides a comprehensive foundation for implementing Microsoft Fabric Data Agents in the insurance claims processing domain, leveraging the existing data architecture and business requirements. All recommendations are based on analysis of the actual repository structure, data model, and Microsoft's official documentation for data agent best practices.*