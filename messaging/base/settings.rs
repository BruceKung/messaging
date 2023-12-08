#[derive(Clone)]
pub struct Settings {
    pub dir_prefix: String,
    pub channel: String,
    pub stream_id: i32,
    pub number_of_messages: i64,
    pub linger_timeout_ms: u64,
}
