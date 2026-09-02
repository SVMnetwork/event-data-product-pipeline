"""Illustrative Cloud Composer/Airflow DAG; not required by the local demo."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task


@dag(
    dag_id="event_data_product_t_plus_1",
    # Illustrative: configure the production timezone/cutoff for its business calendar and SLA.
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["events", "data-product", "gcp", "t-plus-1"],
)
def event_data_product():
    @task
    def ingest(data_interval_start=None):
        """Land source-specific extracts in immutable GCS Raw/ODS for process_date."""
        process_date = data_interval_start.date().isoformat()
        return {
            "process_date": process_date,
            "raw_uri": f"gs://raw/events/process_date={process_date}",
            "run_id": "{{ run_id }}",
        }

    @task
    def validate(manifest: dict):
        """Validate the contract, quarantine row errors, and fail unsafe datasets."""
        return manifest

    @task
    def transform_dwd(manifest: dict):
        """Normalize and deduplicate staging data, then MERGE/replace the DWD partition."""
        return manifest

    @task
    def quality_check(manifest: dict):
        """Run completeness, uniqueness, volume, and reconciliation SQL gates."""
        return manifest

    @task
    def transform_dws(manifest: dict):
        """Recompute affected reusable subject-level aggregate partitions."""
        return manifest

    @task
    def transform_ads(manifest: dict):
        """Recompute affected consumer-specific ADS/Gold partitions."""
        return manifest

    @task
    def publish(manifest: dict):
        """Publish the validated process_date and advance its processing watermark."""
        return manifest

    @task
    def freshness_check(manifest: dict):
        """Emit completion metrics and check the next-morning publication SLO."""

    raw = ingest()
    checked = validate(raw)
    dwd = transform_dwd(checked)
    passed = quality_check(dwd)
    dws = transform_dws(passed)
    ads = transform_ads(dws)
    published = publish(ads)
    freshness_check(published)


event_pipeline_dag = event_data_product()
