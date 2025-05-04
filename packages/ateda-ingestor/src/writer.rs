use crate::{error::Error, model::CutoutPacket};
use apache_avro::{Schema, Writer, types::Value};
use log::{debug, error, info, warn};
use minio_rsc::Minio;
use std::{fs::File, io::BufWriter, path::PathBuf, sync::Arc};
use tokio::{fs as tokio_fs, sync::mpsc, task::JoinHandle};
use uuid::Uuid;

pub fn spawn_alert_writer_tasks(
    num_writers: usize,
    schema: Arc<Schema>,
    minio_client: Minio,
    s3_bucket: String,
    s3_prefix: String,
    partition_name: String,
    partition_value: String,
    file_prefix: String,
    temp_dir: PathBuf,
    buffer_size: usize,
) -> (Vec<mpsc::Sender<Value>>, Vec<JoinHandle<Result<(), Error>>>) {
    let mut writer_handles = Vec::with_capacity(num_writers);
    let mut record_senders = Vec::with_capacity(num_writers);

    for i in 0..num_writers {
        let (tx, mut rx) = mpsc::channel::<Value>(buffer_size);
        record_senders.push(tx);

        let writer_schema = Arc::clone(&schema);
        let client_clone = minio_client.clone();
        let bucket_clone = s3_bucket.clone();
        let prefix_clone = s3_prefix.clone();
        let partition_name_clone = partition_name.clone();
        let partition_value_clone = partition_value.clone();
        let file_prefix_clone = file_prefix.clone();
        let temp_dir_clone = temp_dir.clone();

        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            let key_prefix = if prefix_clone.ends_with('/') {
                prefix_clone
            } else {
                format!("{}/", prefix_clone)
            };
            let s3_key = format!(
                "{}{}={}/{}_part_{}.avro",
                key_prefix, partition_name_clone, partition_value_clone, file_prefix_clone, i
            );
            let s3_path = format!("s3://{}/{}", bucket_clone, s3_key);

            let temp_file_name =
                format!("{}_writer_{}_{}.avro", file_prefix_clone, i, Uuid::new_v4());
            let temp_file_path = temp_dir_clone.join(temp_file_name);
            info!(
                "Writer task {} writing temporary file: {:?}",
                i, temp_file_path
            );

            let write_result = {
                let temp_file = File::create(&temp_file_path)?;
                let buf_writer = BufWriter::new(temp_file);
                let mut writer = Writer::new(&writer_schema, buf_writer);

                while let Some(record) = rx.recv().await {
                    writer.append(record).map_err(|e| {
                        error!(
                            "Failed to append record to temp file {:?}: {}",
                            temp_file_path, e
                        );
                        Error::Avro(e)
                    })?;
                }

                writer.flush().map_err(|e| {
                    error!(
                        "Failed to flush Avro writer for temp file {:?}: {}",
                        temp_file_path, e
                    );
                    Error::Avro(e)
                })?
            };
            info!(
                "Writer task {} finished writing temp file: {:?}",
                i, temp_file_path
            );

            info!("Uploading temp file {:?} to {}", temp_file_path, s3_path);
            client_clone
                .fput_object(&bucket_clone, &s3_key, &temp_file_path)
                .await
                .map_err(|e| {
                    error!(
                        "Failed to upload {} from {:?}: {}",
                        s3_path, temp_file_path, e
                    );
                    Error::Minio(e)
                })?;

            info!("Writer task {} successfully uploaded to: {}", i, s3_path);

            if let Err(e) = tokio_fs::remove_file(&temp_file_path).await {
                warn!(
                    "Failed to delete temporary file {:?}: {}. Manual cleanup might be needed.",
                    temp_file_path, e
                );
            }

            Ok(())
        });
        writer_handles.push(handle);
    }

    (record_senders, writer_handles)
}

pub fn spawn_cutout_writer_tasks(
    num_cutout_writers: usize,
    minio_client: Minio,
    s3_bucket: String,
    s3_prefix: String,
    buffer_size: usize,
) -> (
    Vec<mpsc::Sender<CutoutPacket>>,
    Vec<JoinHandle<Result<(), Error>>>,
) {
    let mut writer_handles = Vec::with_capacity(num_cutout_writers);
    let mut senders = Vec::with_capacity(num_cutout_writers);

    for i in 0..num_cutout_writers {
        let (tx, mut rx) = mpsc::channel::<CutoutPacket>(buffer_size);
        senders.push(tx);

        let client_clone = minio_client.clone();
        let bucket_clone = s3_bucket.clone();
        let prefix_clone = s3_prefix.clone();

        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            let key_prefix = if prefix_clone.ends_with('/') {
                prefix_clone
            } else {
                format!("{}/", prefix_clone)
            };

            while let Some(packet) = rx.recv().await {
                let s3_key = format!(
                    "{}{}/{}_{}.fits.gz",
                    key_prefix,
                    packet.candid,
                    packet.candid,
                    packet.cutout_type.replace("cutout", "").to_lowercase()
                );

                let s3_path = format!("s3://{}/{}", bucket_clone, s3_key);
                debug!("Uploader task {} uploading cutout to: {}", i, s3_path);

                client_clone
                    .put_object(&bucket_clone, &s3_key, packet.stamp_data)
                    .await
                    .map_err(|e| {
                        error!(
                            "Failed to upload cutout {} (candid: {}, type: {}): {}",
                            s3_path, packet.candid, packet.cutout_type, e
                        );
                        Error::Minio(e)
                    })?;
            }
            info!("Cutout uploader task {} finished.", i);
            Ok(())
        });
        writer_handles.push(handle);
    }

    (senders, writer_handles)
}
