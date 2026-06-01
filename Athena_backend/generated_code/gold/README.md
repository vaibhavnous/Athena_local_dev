# Gold Scripts

Generated at: `2026-05-19T19:51:09.048919`
Generated scripts: `2`
Blocked mappings: `8`

| KPI | Source Silver | Target Gold | Status | Fact Script | Dimension Script | Mode |
| --- | --- | --- | --- | --- | --- | --- |
| `Claims Data Ingestion Duration` | `silver.silver_policy_transactions` | `gold.fact_claims_data_ingestion_duration` | `BLOCKED` | - | - | `-` |
| `Claims Data Ingestion Rate` | `silver.silver_policy_transactions` | `gold.fact_claims_data_ingestion_rate` | `BLOCKED` | - | - | `-` |
| `Claims Record Count` | `silver.silver_policy_transactions` | `gold.fact_claims_record_count` | `APPROVED` | [gold_kpi_461a6c90_a1c0_475f_9a22_d43fa88e486a_claims_record_count.py](C:\Users\vaibhavmalik\Documents\Athena-Agentic-\Athena_backend\generated_code\gold\gold_kpi_461a6c90_a1c0_475f_9a22_d43fa88e486a_claims_record_count.py) | [gold_dim_461a6c90_a1c0_475f_9a22_d43fa88e486a_claims_record_count.py](C:\Users\vaibhavmalik\Documents\Athena-Agentic-\Athena_backend\generated_code\gold\gold_dim_461a6c90_a1c0_475f_9a22_d43fa88e486a_claims_record_count.py) | `DETERMINISTIC` |
| `Claims Record Traceability Percentage` | `silver.silver_policy_transactions` | `gold.fact_claims_record_traceability_percentage` | `BLOCKED` | - | - | `-` |
| `Claims to Policy Linkage Ratio` | `silver.silver_policy_transactions` | `gold.fact_claims_to_policy_linkage_ratio` | `BLOCKED` | - | - | `-` |
| `Identifier Consistency Rate` | `silver.silver_policy_transactions` | `gold.fact_identifier_consistency_rate` | `BLOCKED` | - | - | `-` |
| `Policy Data Ingestion Duration` | `silver.silver_policy_transactions` | `gold.fact_policy_data_ingestion_duration` | `BLOCKED` | - | - | `-` |
| `Policy Data Ingestion Rate` | `silver.silver_policy_transactions` | `gold.fact_policy_data_ingestion_rate` | `BLOCKED` | - | - | `-` |
| `Policy Record Count` | `silver.silver_policy_transactions` | `gold.fact_policy_record_count` | `APPROVED` | [gold_kpi_461a6c90_a1c0_475f_9a22_d43fa88e486a_policy_record_count.py](C:\Users\vaibhavmalik\Documents\Athena-Agentic-\Athena_backend\generated_code\gold\gold_kpi_461a6c90_a1c0_475f_9a22_d43fa88e486a_policy_record_count.py) | [gold_dim_461a6c90_a1c0_475f_9a22_d43fa88e486a_policy_record_count.py](C:\Users\vaibhavmalik\Documents\Athena-Agentic-\Athena_backend\generated_code\gold\gold_dim_461a6c90_a1c0_475f_9a22_d43fa88e486a_policy_record_count.py) | `DETERMINISTIC` |
| `Policy Record Traceability Percentage` | `silver.silver_policy_transactions` | `gold.fact_policy_record_traceability_percentage` | `BLOCKED` | - | - | `-` |
