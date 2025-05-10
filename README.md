# ATEDA (Astronomical Transient Event Data & Analysis) Platform [incubating]

**Note:** This project is currently under development and is intended for portfolio purposes.

## Overview

The ATEDA (Astronomical Transient Event Data & Analysis) Platform is designed to ingest, process, store, and analyze astronomical transient event data, primarily focusing on alerts from facilities like the Zwicky Transient Facility (ZTF). The platform aims to provide a robust and scalable solution for handling large volumes of astronomical data, enabling efficient querying, cross-matching with astronomical catalogs, and an environment for scientific analysis and discovery.

The architecture leverages modern data engineering tools and practices, including:

- **Data Ingestion:** A Rust-based ingestor for high-performance processing of raw alert data (Avro format) and storage into a data lake (MinIO).
- **Data Lake & Warehouse:** MinIO for S3-compatible object storage serving as the data lake, with Apache Iceberg table formats managed by a Nessie catalog. StarRocks is utilized as the query engine and data warehouse.
- **Data Transformation:** DBT (Data Build Tool) for managing SQL-based transformations from bronze (raw) to silver (cleaned, conformed) and gold (aggregated, analysis-ready) data layers.
- **Orchestration:** Dagster for orchestrating data pipelines and asset materialization.
- **Infrastructure:** Kubernetes (Kind for local development) for container orchestration, with components deployed via Helm and Helmfile.
- **Core Technologies:** Apache Spark for distributed processing, PostgreSQL for metadata and application backends.

## Current Status & Features

- **Data Ingestion Pipeline:** Ingests ZTF alert Avro files, separates alert data and cutout image data, and stores them in MinIO.
- **Bronze Layer:** Raw alert data stored in Apache Iceberg format.
- **Silver Layer:** Cleaned and transformed data models including:
  - `ztf_alert`: Core alert information.
  - `detection`: Candidate detections from alerts.
  - `non_detection`: Upper limits from previous non-detections.
  - `forced_photometry`: Forced photometry measurements.
  - `reference`: Reference image information.
  - Cross-matches with Gaia, PS1, and Solar System small bodies.
- **Gold Layer:** Aggregated and analysis-ready data models.
  - Currently designing and developing the gold layer models.
- **Infrastructure as Code:** Kubernetes manifests, Helm charts, and Helmfile for reproducible deployments.
- **Development Environment:** Kind-based local Kubernetes cluster setup.

## Components in Proof-of-Concept (POC) Stage

The following components are currently in the Proof-of-Concept (POC) stage and under active development for integration:

- **Superset:** For data exploration and visualization.
- **Prometheus:** For metrics collection and monitoring.
- **Grafana:** For dashboards and visualization of metrics.

## Future Work

- Development of Gold data layer models for specific scientific use cases.
- Enhanced analytics capabilities.
- Full integration and production-hardening of POC components.
- Implementation of CI/CD pipelines.
- User interface for data exploration and interaction.

## Getting Started

(Details to be added as the project matures)

## Contributing

(Details to be added as the project matures)
