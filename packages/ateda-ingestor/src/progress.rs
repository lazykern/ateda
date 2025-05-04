use log::info;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration, Instant};

pub fn spawn_progress_tracker(total_files: usize) -> (Arc<AtomicUsize>, mpsc::Sender<()>, tokio::task::JoinHandle<()>) {
    let processed_files_count = Arc::new(AtomicUsize::new(0));
    let (progress_tx, mut progress_rx) = mpsc::channel::<()>(1);

    let progress_counter_clone = Arc::clone(&processed_files_count);
    let last_report = Arc::new(AtomicUsize::new(0));
    let reporting_task = tokio::spawn(async move {
        let mut last_report_time = Instant::now();
        loop {
            sleep(Duration::from_secs(1)).await;
            match progress_rx.try_recv() {
                Ok(_) | Err(mpsc::error::TryRecvError::Disconnected) => {
                    let final_count = progress_counter_clone.load(Ordering::Relaxed);
                    info!("Reporting task: Final processed files count: {}", final_count);
                    break;
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    let current_count = progress_counter_clone.load(Ordering::Relaxed);
                    let last_report_count = last_report.load(Ordering::Relaxed);
                    if current_count == last_report_count {
                        continue;
                    }
                    let now = Instant::now();
                    let elapsed = now.duration_since(last_report_time).as_secs_f64();
                    let processed = current_count - last_report_count;
                    let rate = if elapsed > 0.0 {
                        processed as f64 / elapsed
                    } else {
                        0.0
                    };
                    let files_left = if current_count < total_files {
                        total_files - current_count
                    } else {
                        0
                    };
                    let eta_secs = if rate > 0.0 {
                        files_left as f64 / rate
                    } else {
                        0.0
                    };
                    let eta = Duration::from_secs(eta_secs.round() as u64);
                    let eta_str = if files_left == 0 {
                        "done".to_string()
                    } else {
                        format!("{:02}:{:02}:{:02}", eta.as_secs() / 3600, (eta.as_secs() % 3600) / 60, eta.as_secs() % 60)
                    };
                    info!("Processed {} of {} files so far... ({:.2} files/sec, ETA: {})", current_count, total_files, rate, eta_str);
                    last_report.store(current_count, Ordering::Relaxed);
                    last_report_time = now;
                }
            }
        }
        info!("Reporting task exiting.");
    });

    (processed_files_count, progress_tx, reporting_task)
} 