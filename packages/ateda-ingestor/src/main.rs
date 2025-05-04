#[macro_use]
extern crate dagster_pipes_rust;
mod cli;
mod error;
mod model;
mod processor;
mod progress;
mod writer;

use apache_avro::Schema;
use clap::Parser;
use futures::future::join_all;
use glob::glob;
use log::{error, info};
use std::{
    env, // For environment variables
    fs::File,
    path::PathBuf,
    sync::Arc,
    sync::atomic::AtomicUsize,
};
use tokio::fs as tokio_fs;
use tokio::time::{Instant, timeout};
use uuid::Uuid;
use tokio::signal;

// Add minio-rsc imports
use minio_rsc::Minio;
use minio_rsc::provider::StaticProvider;
use url::Url;

use crate::cli::Cli;
use crate::error::Error;
use crate::model::CutoutPacket;

use dagster_pipes_rust::open_dagster_pipes;

async fn run_ingestor(cli: Cli) -> std::result::Result<(), Error> {
    env_logger::init();
    let start_time = Instant::now();

    info!("Configuration: {:?}", cli);

    // Validate and create temporary directory
    validate_or_create_dir(&cli.temp_dir).await?;

    // Initialize MinIO client
    let minio_client = build_minio_client(&cli)?;

    // Fail fast: check MinIO connectivity
    match minio_client.list_buckets().await {
        Ok(_) => info!("Successfully connected to MinIO and listed buckets."),
        Err(e) => {
            error!("Failed to connect to MinIO: {}", e);
            return Err(Error::Minio(e.into()));
        }
    }

    // Find Avro files
    let paths = find_avro_files(&cli.input_dir)?;
    if paths.is_empty() {
        error!(
            "No files found matching pattern: {}/**/*.avro",
            cli.input_dir.display()
        );
        return Ok(());
    }
    info!("Found {} Avro files to process.", paths.len());

    let schema = parse_schema(&cli.alert_schema_file)?;

    let (alert_senders, alert_writer_handles) = writer::spawn_alert_writer_tasks(
        cli.num_alert_writers,
        Arc::clone(&schema),
        minio_client.clone(),
        cli.s3_bucket_alerts.clone(),
        cli.s3_prefix_alerts.clone(),
        cli.partition_name.clone(),
        cli.partition_value.clone(),
        cli.file_prefix.clone(),
        cli.temp_dir.clone(),
        cli.alert_writer_buffer_size,
    );

    let (cutout_senders, cutout_writer_handles) = writer::spawn_cutout_writer_tasks(
        cli.num_cutout_writers,
        minio_client.clone(),
        cli.s3_bucket_cutout.clone(),
        cli.s3_prefix_cutout.clone(),
        cli.cutout_writer_buffer_size,
    );

    // Spawn progress tracker
    let (processed_files_count, progress_tx, reporting_task) = progress::spawn_progress_tracker(paths.len());

    // Process files and handle Ctrl+C
    let alert_writer_index = Arc::new(AtomicUsize::new(0));
    let cutout_writer_index = Arc::new(AtomicUsize::new(0));
    let process_future = processor::process_files(
        paths,
        cli.concurrency,
        alert_senders.clone(),
        alert_writer_index,
        cutout_senders.clone(),
        cutout_writer_index,
        processed_files_count,
    );

    tokio::select! {
        _ = process_future => {
            info!("File processing stream finished.");
            drop(alert_senders);
            drop(cutout_senders);
            info!("Signaled writer/uploader tasks to complete. Waiting...");
            let alert_writer_results = join_all(alert_writer_handles).await;
            for (i, result) in alert_writer_results.into_iter().enumerate() {
                match result {
                    Err(join_err) => error!("Alert writer task {} panicked: {}", i, join_err),
                    Ok(Ok(())) => info!("Alert writer task {} completed successfully.", i),
                    Ok(Err(app_err)) => error!("Alert writer task {} failed: {}", i, app_err),
                }
            }
            info!("All alert writer tasks finished.");
            let cutout_writer_results = join_all(cutout_writer_handles).await;
            for (i, result) in cutout_writer_results.into_iter().enumerate() {
                match result {
                    Err(join_err) => error!("Cutout uploader task {} panicked: {}", i, join_err),
                    Ok(Ok(())) => info!("Cutout uploader task {} completed successfully.", i),
                    Ok(Err(app_err)) => error!("Cutout uploader task {} failed: {}", i, app_err),
                }
            }
            info!("All cutout uploader tasks finished.");
        }
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down early.");
            drop(alert_senders);
            drop(cutout_senders);
            for (i, handle) in alert_writer_handles.iter().enumerate() {
                handle.abort();
                info!("Aborted alert writer task {}", i);
            }
            for (i, handle) in cutout_writer_handles.iter().enumerate() {
                handle.abort();
                info!("Aborted cutout uploader task {}", i);
            }
            info!("All writer/uploader tasks aborted.");
        }
    }

    // Signal and wait for the reporting task
    info!("Signaling reporting task to stop.");
    drop(progress_tx);
    if let Err(e) = reporting_task.await {
        error!("Reporting task failed: {}", e);
    }
    info!("Reporting task finished.");

    // Clean up temporary directory
    info!("Cleaning up temporary directory: {:?}", cli.temp_dir);
    if let Err(e) = tokio_fs::remove_dir_all(&cli.temp_dir).await {
        error!(
            "Failed to clean up temporary directory {:?}: {}",
            cli.temp_dir, e
        );
    }

    let duration = start_time.elapsed();
    info!("Processing completed in {:?}", duration);

    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    let cli = Cli::parse();
    run_ingestor(cli).await?;
    Ok(())
}

// Function to build the Minio client
fn build_minio_client(cli: &Cli) -> std::result::Result<Minio, Error> {
    info!("Building MinIO client...");

    // Get credentials from environment variables
    let access_key = cli.aws_access_key_id.clone();
    let secret_key = cli.aws_secret_access_key.clone();
    let provider = StaticProvider::new(&access_key, &secret_key, None);
    info!("Using credentials from environment variables (AWS_ACCESS_KEY_ID)");

    // Parse endpoint URL
    let url = Url::parse(&cli.s3_endpoint_url).map_err(|e| {
        Error::InvalidUrl(format!(
            "Failed to parse endpoint URL '{}': {}",
            cli.s3_endpoint_url, e
        ))
    })?;

    let endpoint = url
        .host_str()
        .ok_or_else(|| Error::InvalidUrl("Endpoint URL has no host".to_string()))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| Error::InvalidUrl("Endpoint URL has no port".to_string()))?;
    let secure = url.scheme() == "https";

    let endpoint_str = format!("{}:{}", endpoint, port);
    info!(
        "Connecting to MinIO at: {} (secure: {})",
        endpoint_str, secure
    );

    Minio::builder()
        .endpoint(&endpoint_str)
        .provider(provider)
        .secure(secure)
        .build()
        .map_err(|e| Error::Minio(e.into()))
}

async fn validate_or_create_dir(dir_path: &PathBuf) -> std::result::Result<(), Error> {
    if !dir_path.exists() {
        info!("Creating directory: {:?}", dir_path);
        tokio_fs::create_dir_all(dir_path).await?;
    } else if !dir_path.is_dir() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            format!("Path {:?} exists but is not a directory.", dir_path),
        )));
    }
    Ok(())
}

fn find_avro_files(input_dir: &PathBuf) -> std::result::Result<Vec<PathBuf>, Error> {
    let avro_pattern = format!("{}/**/*.avro", input_dir.display());
    info!("Searching for Avro files using pattern: {}", avro_pattern);
    let paths = glob(&avro_pattern)?
        .filter_map(|entry| entry.ok())
        .collect();
    Ok(paths)
}

fn parse_schema(alert_schema_file: &PathBuf) -> std::result::Result<Arc<Schema>, Error> {
    let mut file_reader = File::open(alert_schema_file)?;
    let schema = Schema::parse_reader(&mut file_reader)?;
    Ok(Arc::new(schema))
}
