#![allow(dead_code)]

pub use test_utils::instance_helper::test_instance;

/// Serve `test_instance()` as a live escrow instance. Create it before any catch-all
/// `getAccountInfo` mock, since mockito matches mocks in creation order.
pub async fn mock_instance_account(rpc: &mut mockito::Server) -> mockito::Mock {
    // The boot key check reads the AllowedMint of each mint the database knows (confirmed,
    // unlike the custody sweep); none is allowed. Kept for the server's life.
    std::mem::forget(no_allowed_mints(rpc).await);
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

/// Answer the boot key check's `AllowedMint` reads with no account, so no escrow account is
/// configured for confidential transfers.
pub async fn no_allowed_mints(rpc: &mut mockito::Server) -> mockito::Mock {
    rpc.mock("POST", "/")
        .match_body(mockito::Matcher::AllOf(vec![
            mockito::Matcher::Regex(r#""method"\s*:\s*"getMultipleAccounts""#.into()),
            mockito::Matcher::Regex(r#""commitment"\s*:\s*"confirmed""#.into()),
        ]))
        .with_body_from_request(|request| {
            let body: serde_json::Value = serde_json::from_slice(request.body().unwrap()).unwrap();
            let keys = body["params"][0].as_array().map_or(0, Vec::len);
            serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": {
                "context": {"slot": 300}, "value": vec![serde_json::Value::Null; keys]
            }})
            .to_string()
            .into()
        })
        .create_async()
        .await
}
