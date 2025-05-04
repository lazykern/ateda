use crate::{error::Error, model::CutoutPacket};
use apache_avro::{Reader, types::Value};
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use log::{error, info, warn};
use std::{
    fs::File,
    path::PathBuf,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::{sync::mpsc, task};

pub async fn process_files(
    paths: Vec<PathBuf>,
    concurrency_limit: usize,
    alert_senders: Vec<mpsc::Sender<Value>>,
    alert_writer_index: Arc<AtomicUsize>,
    cutout_senders: Vec<mpsc::Sender<CutoutPacket>>,
    cutout_writer_index: Arc<AtomicUsize>,
    processed_files_count: Arc<AtomicUsize>,
) {
    stream::iter(paths)
        .for_each_concurrent(concurrency_limit, |path| {
            let alert_senders = alert_senders.clone();
            let alert_writer_index = Arc::clone(&alert_writer_index);
            let cutout_senders = cutout_senders.clone();
            let cutout_writer_index = Arc::clone(&cutout_writer_index);
            let counter_clone = Arc::clone(&processed_files_count);
            let path_for_err = path.clone(); // Clone for error reporting
            let path_for_blocking = path.clone();

            async move {
                let blocking_result = task::spawn_blocking(move || -> Result<(Vec<Value>, Vec<CutoutPacket>), Error> {
                    let mut alert_records_to_send = Vec::new();
                    let mut cutout_data_to_send: Vec<CutoutPacket> = Vec::new();
                    let mut file_reader = File::open(&path_for_blocking)?;
                    let reader = Reader::new(&mut file_reader)?;

                    let mut candid: Option<i64> = None;
                    let mut cutout_fields: Vec<(String, Value)> = Vec::new();
                    let mut alert_fields: Vec<(String, Value)> = Vec::new();

                    for record_result in reader {
                        match record_result {
                            Ok(record) => {
                                if let Value::Record(fields) = record {
                                    for (name, value) in fields {
                                        if name.starts_with("cutout") {
                                            cutout_fields.push((name.to_string(), value));
                                        } else {
                                            alert_fields.push((name.to_string(), value.clone()));
                                            if name.to_lowercase() == "candid" {
                                                if let Value::Long(id) = value {
                                                    candid = Some(id);
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    warn!(
                                        "Expected Value::Record, found different type in file {:?}",
                                        path_for_blocking
                                    );
                                }
                            }
                            Err(e) => {
                                error!("Failed to read record from {:?}: {}", path_for_blocking, e);
                            }
                        }
                    }

                    if let Some(candid) = candid {
                        if !alert_fields.is_empty() {
                            alert_records_to_send.push(Value::Record(alert_fields));
                        }

                        if !cutout_fields.is_empty() {
                            for (cutout_type, value) in cutout_fields {
                                if let Value::Union(_, value) = value {
                                    if let Value::Record(fields) = *value {
                                        for (name, value) in fields {
                                            if name.to_lowercase() == "stampdata" {
                                                if let Value::Bytes(stamp_data) = value {
                                                    cutout_data_to_send.push(CutoutPacket {
                                                        candid,
                                                        cutout_type: cutout_type.clone(),
                                                        stamp_data: Bytes::from(stamp_data),
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            if !alert_records_to_send.is_empty() {
                                warn!(
                                    "No cutout data found for candid {} in file {:?}",
                                    candid, path_for_blocking
                                );
                            }
                        }
                    } else {
                        if !alert_fields.is_empty() || !cutout_fields.is_empty() {
                           warn!("Processed fields but no candid found in file {:?}", path_for_blocking);
                        }
                    }
                    Ok((alert_records_to_send, cutout_data_to_send))
                })
                .await;

                match blocking_result {
                    Ok(Ok((alert_records, cutout_data))) => {
                        let num_alert_writers = alert_senders.len();
                        for alert_record in alert_records {
                            let current_index =
                                alert_writer_index.fetch_add(1, Ordering::Relaxed) % num_alert_writers;

                            if let Err(e) = alert_senders[current_index].send(alert_record).await {
                                error!(
                                    "Failed to send alert record from {:?} to writer {}: {}",
                                    path_for_err, // Use cloned path for error
                                    current_index,
                                    e
                                );
                            }
                        }
                        let num_cutout_writers = cutout_senders.len();
                        for cutout_data in cutout_data {
                            let current_index =
                                cutout_writer_index.fetch_add(1, Ordering::Relaxed) % num_cutout_writers;

                            if let Err(e) = cutout_senders[current_index].send(cutout_data).await {
                                error!(
                                    "Failed to send cutout data from {:?} to writer {}: {}",
                                    path_for_err,
                                    current_index,
                                    e
                                );
                            }
                        }
                    }
                    Ok(Err(app_err)) => {
                        error!("Error processing file {:?}: {}", path_for_err, app_err);
                    }
                    Err(join_err) => {
                        error!(
                            "Blocking task for file {:?} panicked: {}",
                            path_for_err, join_err
                        );
                    }
                }

                counter_clone.fetch_add(1, Ordering::Relaxed);
            }
        })
        .await;
}
