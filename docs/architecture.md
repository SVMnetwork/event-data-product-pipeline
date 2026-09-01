# GCP T+1 production architecture decisions

This document expands the production reference design in the README. It is not deployed infrastructure. The working local Python component remains a Raw/ODS-to-DWD demonstration.

## Assumptions and layer responsibilities

The design assumes GCP; structured files, APIs, relational databases, and event streams; 50-200 GB of new data per day; T+1 incremental batch; a next-morning SLA; possible late arrivals; retained Raw data; sensitive customer identifiers; isolated environments; and a small-to-medium team that prefers managed services. Event streams may be captured continuously, but real-time analytical publication is not an initial requirement. These are assessment assumptions because the prompt supplies no cloud, volume, or SLA constraints.

| Layer | GCP representation | Contract and consumers |
| --- | --- | --- |
| Ingestion | Source-specific export, scheduled Python job, incremental database extraction, or Pub/Sub event capture | Authenticates, captures source metadata, and lands data without business transformation |
| Raw / ODS (Bronze) | Immutable, encrypted GCS objects plus manifests | Source-faithful replay/audit boundary with restricted engineering access |
| Staging | Run-scoped BigQuery tables | Isolates parsing, reconciliation, and failed runs from published data |
| DWD (Silver) | Partitioned BigQuery tables | Typed, normalized, validated, deduplicated event detail with business keys and traceability |
| DWS | Partitioned BigQuery tables | Reusable subject-oriented aggregates, such as customer daily activity |
| ADS (Gold) | Consumer-specific BigQuery tables/views | Stable datasets for BI, reports, analytics, ML features, and data products |
| Operations | Cloud Composer/Airflow, Cloud Logging, Cloud Monitoring | Dependencies, retries, backfills, SLA monitoring, and ownership |

DWS/ADS models are not implemented locally. They should be based on validated consumer requirements rather than speculative business logic.

## Source-aware ingestion

- **Files:** the source exports new business-day files to an inbound location; ingestion copies them to immutable GCS Raw/ODS paths and records checksum, object generation, source, business date, schema/contract version, and arrival time.
- **APIs:** Composer schedules a Python job to request the logical `process_date` where supported. Use source-specific pagination or cursors only when that API requires them. Responses land in GCS Raw or controlled staging before publication.
- **Relational databases:** extract incrementally using business date, `updated_at`, or CDC where appropriate for the source and T+1 requirement. Airflow's `process_date` defines the pipeline interval, and changes land in GCS Raw and/or BigQuery staging.
- **Event streams:** Pub/Sub continuously and durably captures events, and an ingestion sink writes source-faithful batches to GCS Raw/ODS. The daily Composer DAG processes the bounded business-date interval from GCS; this does not require real-time downstream transformation.

Each source has an explicit completeness rule for the requested `process_date`.

Ingestion latency and analytical freshness are independent. Continuous event capture protects and decouples the source stream, while DWD/DWS/ADS publication remains T+1 under the current assumptions.

## T+1 processing model

The daily DAG is parameterized by logical `process_date`, for example `2026-09-01`. Wall-clock retries keep the same process date.

```text
process_date=2026-09-01 -> ingest relevant source data -> GCS Raw/ODS
                        -> run-scoped BigQuery staging -> DWD -> DWS -> ADS
                        -> quality checks -> publish before the next-morning SLA
```

Daily processing remains incremental:

- ingest only new file objects;
- request `process_date` from APIs and use source-specific pagination/cursors when necessary;
- filter database changes by business date or `updated_at`, or consume CDC when appropriate;
- read only the captured event-stream objects for the bounded business-date interval;
- `MERGE` corrected business keys into DWD or replace the affected business-date partition;
- recompute only affected DWS/ADS partitions where dependency boundaries allow.

BigQuery SQL is the default for joins, window functions, aggregations, and DWS/ADS business modeling. Python remains appropriate for complex parsing, contract validation, normalization, or custom logic that is clearer outside SQL.

## Idempotency, replay, and backfill

```text
Immutable GCS input -> run-scoped staging -> validation and reconciliation
                    -> MERGE or controlled partition replacement
                    -> quality gate -> publish
```

A rerun uses the same `process_date` and deterministic business rules. Use business-key `MERGE` when individual records can be corrected; use partition replacement when a complete interval is available and replacement is simpler to reason about. Never append a second copy of the same interval. Failed staging is not consumer-visible. Publication occurs only after reconciliation and quality checks, and affected downstream DWS/ADS partitions are rebuilt when DWD changes.

A historical backfill specifies source, date range, code/config version, and reason, then invokes the same DAG logic as a normal run. Limit overlapping runs for partitions that could conflict. Record source objects, run ID, process date, row counts, target table/partition, and code/config version in a processing ledger.

## Schema evolution, duplicates, and late data

- **Schema evolution:** version contracts. Add nullable fields compatibly; breaking renames, type changes, or new required fields need a migration and consumer deprecation window. Raw/ODS retains unknown source fields even when DWD does not publish them.
- **Duplicates:** assume at-least-once delivery. For this event domain, merge on `event_id` and select the greatest `ingestion_timestamp`, with a deterministic source sequence as tie-breaker. Monitor duplicate rate.
- **Late arrivals:** derive affected `business_date` from the event, not arrival time. A late record lands in immutable GCS, then the pipeline merges its key or reprocesses the DWD partition, recomputes affected DWS/ADS partitions, reruns quality/reconciliation, and publishes the correction.
- **Deletes/corrections:** represent explicit source operations or versions when available; do not infer deletion from absence.

## Data quality and failure lifecycle

| Signal | Example production control |
| --- | --- |
| Completeness/nullability | Required published fields are 100% complete |
| Type/validity | Contract types, valid timestamps, accepted domains, and amount constraints |
| Uniqueness | No duplicate business keys after DWD merge |
| Referential integrity | Orphans rejected or held when a relationship is required |
| Freshness | Previous business-day ADS is published before 06:00 business time |
| Volume | Compare source/input/output/reject counts with control totals and historical bands |
| Reconciliation | Account for accepted, invalid, and superseded records before publication |
| Operational | Duration, quarantine rate, duplicate count, publication status, and bytes processed |

BigQuery SQL validation queries provide the production quality gates. A recoverable row error goes to a restricted quarantine table or GCS prefix. An incompatible schema, corrupt dataset, failed reconciliation, or unsafe quality threshold fails the DAG and blocks publication.

```text
detect -> quarantine or fail -> alert -> diagnose -> retry/reprocess -> validate recovery
```

Use bounded exponential-backoff retries for transient network, API/service, compute, or I/O failures. Do not blindly retry deterministic schema, contract, corruption, or reconciliation failures. Correct the data, configuration, or code, then replay immutable Raw/ODS input and validate recovery.

## Composer/Airflow and SLA

The DAG dependency chain is:

```text
ingest -> validate -> transform_dwd -> quality_check
       -> transform_dws -> transform_ads -> publish -> freshness_check
```

Composer supplies the logical execution date as `process_date`. Tasks use deterministic paths/table names, bounded retries with exponential backoff, controlled catchup, and `max_active_runs` where publication could conflict. The same logic supports scheduled runs and backfills.

The example SLO is previous business-day ADS/Gold published by 06:00 in the business timezone. Cloud Monitoring alerts the owner on missed source arrival, failed stage, failed reconciliation, or missed publication SLA. Cloud Logging and Airflow metadata support diagnosis.

## Security and governance

Use separate GCP projects or strongly isolated datasets/service accounts for dev, test, and prod. Create distinct least-privilege service accounts for ingestion, transformation, and consumption. Prefer short-lived workload identity federation in CI over stored service-account keys.

Encryption in transit and at rest is the default; use customer-managed encryption keys only when policy requires them. Apply BigQuery dataset/table permissions, row-level security, column-level policy controls or masking, and tokenization where broad consumers do not need identifiers. Raw and quarantine access is especially restricted. Store secrets in Secret Manager and record administrative/data access in Cloud Audit Logs. Retention and deletion policy covers GCS, BigQuery, quarantine, staging, logs, and backups.

## Observability, lineage, and auditability

Track records read/accepted/rejected/deduplicated, source and target counts, latest published business date, freshness, duration, quarantine rate, duplicate count, reconciliation status, publication status, query duration, and BigQuery bytes processed.

Trace each target partition to source object/generation, source business date, process date, run ID, contract/code/config version, owner, and SLO. Airflow metadata plus the processing ledger provides baseline lineage; a standard such as OpenLineage can be added if organizational tooling requires it.

## BigQuery cost and performance

Prefer incremental processing and partition pruning over full refreshes. Partition on the date/timestamp used by common access and reprocessing patterns. Add clustering only after query evidence identifies stable filter or join keys. Select only required columns, constrain partitions, monitor bytes processed and duration, and review repeated expensive queries. Materialize DWS/ADS aggregates when repeated consumption offsets their storage and maintenance cost.

Use GCS lifecycle rules for Raw data under audit/replay retention. Review BigQuery on-demand versus capacity pricing as workloads become large and predictable. BigQuery supports multi-terabyte and larger workloads; volume alone is not a reason to replace it. First optimize partitions, clustering, incremental logic, query design, materialization, and pricing. Use Dataflow or Spark only when complex non-SQL/custom processing is a better fit or measurements justify another engine.

## Warehouse versus lakehouse trade-off

This design is warehouse-centric because its primary consumers are BI/analytics users, most transformations are relational SQL, and a small-to-medium team benefits from BigQuery's managed operational model. GCS remains the durable source/replay boundary.

An open-table lakehouse can be preferable when multiple independent engines need shared object-storage tables, open formats and portability are strategic requirements, or direct distributed lake processing is the dominant access pattern. The choice should follow workload, governance, portability, and team-operating requirements rather than treating either architecture as universally superior.

## Evolution and organizational scale

If hourly freshness becomes necessary, keep batch first: change the logical interval to an hour, extract incrementally, merge keys or replace affected partitions, and strengthen interval/overlapping-run controls. If seconds/minutes and event-driven processing are truly required, evaluate Pub/Sub with Dataflow, Flink, or another stateful streaming engine and the appropriate serving layer.

Organizational scale is a standardization problem as well as a workload problem. Evolve this first pipeline into repository and DAG templates, machine-readable contracts, reusable validation, CI/CD templates, naming/modeling conventions, standard SLOs and monitoring, ownership/lineage metadata, access patterns, backfill conventions, and runbooks. Adding compute alone does not create a scalable data organization.
