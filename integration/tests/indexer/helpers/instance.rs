#![allow(dead_code)]

pub use test_utils::instance_helper::test_instance;

/// Serve `test_instance()` as a live escrow instance. Create it before any catch-all
/// `getAccountInfo` mock, since mockito matches mocks in creation order.
pub async fn mock_instance_account(rpc: &mut mockito::Server) -> mockito::Mock {
    rpc.mock("POST", "/")
        .match_body(mockito::Matcher::AllOf(vec![
            mockito::Matcher::Regex(r#""method"\s*:\s*"getAccountInfo""#.into()),
            mockito::Matcher::Regex(test_instance().to_string()),
        ]))
        .with_status(200)
        .with_body(
            serde_json::json!({
                "jsonrpc": "2.0",
                "result": {
                    "context": {"slot": 300},
                    "value": test_utils::instance_helper::test_instance_account()
                },
                "id": 1
            })
            .to_string(),
        )
        .create_async()
        .await
}
