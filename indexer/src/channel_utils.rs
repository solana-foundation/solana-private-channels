/// Channel send utilities with standardized error handling patterns
use tokio::sync::mpsc;
use tracing::error;

/// Send guaranteed - reserve/permit pattern for zero data loss
///
/// Use this for absolutely critical messages that cannot be lost (e.g., committed checkpoints).
/// Reserves capacity in the channel first, then sends the message.
/// Once capacity is reserved, the send is guaranteed to succeed.
///
/// # Example
/// ```ignore
/// send_guaranteed(&tx, message, "committed checkpoint").await?;
/// ```
pub async fn send_guaranteed<T>(
    tx: &mpsc::Sender<T>,
    msg: T,
    context: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let permit = tx.reserve().await.inspect_err(|&e| {
        error!(
            "Failed to reserve capacity ({}): channel closed - {:?}",
            context, e
        );
    })?;

    permit.send(msg);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    // ============================================================================
    // send_guaranteed Tests
    // ============================================================================

    #[tokio::test]
    async fn test_send_guaranteed_success() {
        let (tx, mut rx) = mpsc::channel(10);
        let test_msg = "test_message";

        let result = send_guaranteed(&tx, test_msg, "test context").await;

        assert!(result.is_ok());
        assert_eq!(rx.recv().await, Some(test_msg));
    }

    #[tokio::test]
    async fn test_send_guaranteed_closed_channel() {
        let (tx, rx) = mpsc::channel::<String>(10);
        drop(rx); // Close receiver

        let result = send_guaranteed(&tx, "test".to_string(), "closed channel test").await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("channel closed"));
    }

    #[tokio::test]
    async fn test_send_guaranteed_multiple_sequential() {
        let (tx, mut rx) = mpsc::channel(10);

        send_guaranteed(&tx, 1, "first").await.unwrap();
        send_guaranteed(&tx, 2, "second").await.unwrap();
        send_guaranteed(&tx, 3, "third").await.unwrap();

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    }

    #[tokio::test]
    async fn test_send_guaranteed_bounded_channel_at_capacity() {
        let (tx, mut rx) = mpsc::channel(2);

        // Fill channel to capacity
        send_guaranteed(&tx, 1, "first").await.unwrap();
        send_guaranteed(&tx, 2, "second").await.unwrap();

        // Spawn a task that will wait for capacity
        let tx_clone = tx.clone();
        let send_task = tokio::spawn(async move {
            send_guaranteed(&tx_clone, 3, "third")
                .await
                .map_err(|e| e.to_string())
        });

        // Give send_task time to attempt the send
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Free up capacity by receiving one message
        assert_eq!(rx.recv().await, Some(1));

        // Now the send should complete
        let result = send_task.await.unwrap();
        assert!(result.is_ok());

        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    }

    #[tokio::test]
    async fn test_send_guaranteed_large_batch() {
        let (tx, mut rx) = mpsc::channel(200);

        // Send many messages
        for i in 0..100 {
            send_guaranteed(&tx, i, "batch test")
                .await
                .expect("send should succeed");
        }

        // Verify all messages were sent
        for i in 0..100 {
            assert_eq!(rx.recv().await, Some(i));
        }
    }
}
