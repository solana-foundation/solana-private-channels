#![allow(dead_code)]

//! Services on their own runtimes, so a test can stop one like a process exit and start
//! it again on the same database.

use std::time::Duration;

/// A worker on its own runtime, so stopping it ends every task it spawned, like a process exit.
pub struct Worker {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Worker {
    /// Returns once `start` has finished, so workers start one at a time.
    pub async fn spawn<F, Fut>(start: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (ready, started) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("worker runtime");
            runtime.block_on(async move {
                start().await;
                let _ = ready.send(());
                let _ = stopped.await;
            });
            runtime.shutdown_timeout(Duration::from_secs(5));
        });
        let _ = started.await;
        Self {
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    pub async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .expect("join the worker thread")
                .expect("a worker failed to start");
        }
    }
}
