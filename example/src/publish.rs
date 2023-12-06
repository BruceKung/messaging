use std::ffi::CString;

use publisher::{Settings, AeronPublisher};

pub fn main() {

    let setting = Settings{
        dir_prefix: "/home/api/rzhao/dev/aeron/install/data".to_string(),
        channel: "aeron:udp?endpoint=localhost:40123".to_string(),
        stream_id: 1001,
        number_of_messages: 1000000,
        linger_timeout_ms: 1000,
    };

    let publisher = AeronPublisher::new(&setting);

    let mut index : i32 = 0;
    loop {
        let str_msg = format!("Basic publisher msg {}", index);
        let c_str_msg = CString::new(str_msg).unwrap();
        publisher.publish(c_str_msg.as_bytes(),  c_str_msg.as_bytes().len() as i32 );
        std::thread::sleep(std::time::Duration::from_millis(1000));
        index += 1;
    }
}
