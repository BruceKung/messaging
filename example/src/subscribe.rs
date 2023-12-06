use std::ffi::CString;
use subscriber::{Settings, AeronSubscriber};


pub fn main() {

    let setting = Settings{
        dir_prefix: "/home/api/rzhao/dev/aeron/install/data".to_string(),
        channel: "aeron:udp?endpoint=localhost:40123".to_string(),
        stream_id: 1001,
        number_of_messages: 1000000,
        linger_timeout_ms: 1000,
    };

    let subscriber = AeronSubscriber::new(&setting);

    loop {
        subscriber.subscribe();
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }
}
