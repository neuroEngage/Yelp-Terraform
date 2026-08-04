# Final Architecture Documentation — Yelp Big Data Platform

This document describes the design, execution flow, and architectural principles of the automated Yelp Big Data pipeline.

---

## High-Level Architecture Diagram

```mermaid
flowchart TD
    Dev([Developer]) -->|Push / PR| GithubRepo[GitHub Repository]
    
    subgraph CI_CD [GitHub Actions Workflows]
        RepoPlan[terraform-plan.yml]
        RepoApply[terraform-apply.yml]
        RepoIngest[ingest.yml]
        RepoDestroy[terraform-destroy.yml]
    end

    GithubRepo --> CI_CD

    subgraph State_Management [Infrastructure Deployment]
        HCP[HCP Terraform / Local Backend]
        TF[Terraform Engine]
    end

    RepoApply --> HCP --> TF
    TF --> AWS_Infra[AWS Infrastructure Provisioned]

    subgraph AWS_Cloud [AWS Cloud Environment]
        subgraph Storage [Amazon S3 Data Lake]
            Bronze[(s3://bucket/bronze/)]
            Silver[(s3://bucket/silver/)]
            Gold[(s3://bucket/gold/)]
            Scripts[(s3://bucket/scripts/)]
            AthenaRes[(s3://bucket/athena-results/)]
        end

        subgraph Ingestion [Data Acquisition]
            Kaggle[Kaggle API]
            PyIngest[ingest.py Script]
        end

        subgraph ETL [AWS Glue Serverless ETL]
            Job1[Glue Job 1: Bronze -> Silver]
            Job2[Glue Job 2: Silver -> Gold]
            Workflow[Glue Workflow Orchestrator]
            Crawler[Glue Crawler]
            Catalog[(Glue Data Catalog DB)]
        end

        subgraph Monitoring [Observability]
            CW[CloudWatch Log Groups]
        end

        subgraph Analytics [Data Serving]
            Athena[Amazon Athena SQL]
            PowerBI[Power BI Dashboard]
        end
    end

    RepoIngest --> PyIngest
    PyIngest -->|Download & Unzip| Kaggle
    PyIngest -->|Filter photos/ & Upload JSON| Bronze
    PyIngest -->|Trigger Workflow| Workflow

    Workflow --> Job1
    Job1 -->|Clean & Partition Parquet| Silver
    Job1 --> Job2
    Job2 -->|Aggregate & Joins| Gold
    Job2 --> Crawler
    Crawler --> Catalog

    Catalog --> Athena
    AthenaRes --> Athena
    Athena --> PowerBI
    Workflow --> CW
```

---

## Architectural Principles & Highlights

1. **Separation of Concerns**:
   - **Terraform** provisions all infrastructure declaratively.
   - **GitHub Actions** automates CI/CD and data ingestion execution.
   - **AWS Glue** handles heavy distributed PySpark transformations.
   - **Athena & Power BI** serve query traffic and business insights.

2. **Data Lake Medallion Architecture**:
   - **Bronze**: Raw JSON dataset files ingested directly from Kaggle.
   - **Silver**: Cleaned, schema-enforced, partitioned Parquet tables.
   - **Gold**: Business-level aggregated KPI metrics optimized for BI queries.

3. **AWS Academy Compatibility**:
   - Designed to work smoothly under AWS Academy Learner Lab constraints (customizable `var.glue_service_role_arn` to inherit `LabRole`).
   - On-demand lifecycle automation with `terraform-destroy.yml` to prevent unwanted cost accumulation.
