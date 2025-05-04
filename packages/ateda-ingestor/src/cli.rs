use clap::Parser;
use num_cpus;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    // Input/Output Configuration
    #[arg(short, long)]
    pub input_dir: PathBuf,

    #[arg(short, long, help = "Path to alert-only Avro schema file (no cutout fields)")]
    pub alert_schema_file: PathBuf,

    #[arg(long, default_value = "/tmp/ateda_ingestor")]
    pub temp_dir: PathBuf,

    // S3 Alert Storage Configuration
    #[arg(long, env)]
    pub aws_access_key_id: String, // Shared credential

    #[arg(long, env)]
    pub aws_secret_access_key: String, // Shared credential

    #[arg(long, env)]
    pub s3_endpoint_url: String, // Shared endpoint

    #[arg(long)]
    pub s3_bucket_alerts: String,

    #[arg(long, default_value = "alerts/")]
    pub s3_prefix_alerts: String,

    // S3 Cutout Storage Configuration
    #[arg(long, env)]
    pub s3_bucket_cutout: String,

    #[arg(long, default_value = "cutouts/")]
    pub s3_prefix_cutout: String,

    // File/Partition Naming
    #[arg(long, default_value = "data")]
    pub file_prefix: String,

    #[arg(long)]
    pub partition_name: String, // e.g., observation_date

    #[arg(long)]
    pub partition_value: String, // e.g., 2025-01-01

    // Concurrency and Performance
    #[arg(long)]
    pub alert_writer_buffer_size: usize,

    #[arg(short, long, default_value_t = num_cpus::get() * 2)]
    pub concurrency: usize,

    #[arg(long)]
    pub cutout_writer_buffer_size: usize,

    #[arg(long)]
    pub num_alert_writers: usize,

    #[arg(long)]
    pub num_cutout_writers: usize,
}
