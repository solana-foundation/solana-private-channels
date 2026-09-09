use private_channel_metrics::{counter_vec, histogram_vec};

counter_vec!(
    GATEWAY_REQUESTS_TOTAL,
    "private_channel_gateway_requests_total",
    "Total gateway requests by method, target, and status",
    &["method", "target", "status"]
);

histogram_vec!(
    GATEWAY_REQUEST_DURATION,
    "private_channel_gateway_request_duration_seconds",
    "End-to-end gateway request latency",
    &["method", "target"]
);

counter_vec!(
    GATEWAY_ERRORS_TOTAL,
    "private_channel_gateway_errors_total",
    "Pre-routing and backend errors by type",
    &["error_type"]
);

counter_vec!(
    GATEWAY_REJECTED_TOTAL,
    "private_channel_gateway_rejected_total",
    "Connections or requests rejected by a resource guard, by reason",
    &["reason"]
);

pub fn init() {
    private_channel_metrics::init_metrics!(
        GATEWAY_REQUESTS_TOTAL,
        GATEWAY_REQUEST_DURATION,
        GATEWAY_ERRORS_TOTAL,
        GATEWAY_REJECTED_TOTAL,
    );
}
