use std::ffi::CString;

use messaging::core::messaging::Messaging;
use messaging::base::settings::Settings;
use messaging::requester::requester::Requester;


pub fn main() {

    let setting = Settings{
        dir_prefix: "/home/api/rzhao/dev/aeron/install/data".to_string(),
        channel: "aeron:udp?endpoint=localhost:40123".to_string(),
        stream_id: 1001,
        number_of_messages: 1000000,
        linger_timeout_ms: 1000,
    };

    let mut messaging = Messaging::new();
    match messaging.connect(&setting) {
        Ok(_) => {}
        Err(e) => {
            println!("Error: {:?}", e);
            return;
        }
    }

    let stream_id = 1001;

    let result = messaging.new_publisher(stream_id);
    if result.is_err() {
        println!("Error: {:?}", result.err());
        return;
    }

    let publiser = result.unwrap();

    let mut index : i32 = 0;
    loop {
        let str_msg = format!("Basic publisher msg {}", index);
        let c_str_msg = CString::new(str_msg).unwrap();

        match publiser.clone().publish( c_str_msg.as_bytes(),  c_str_msg.as_bytes().len() as i32) {
            Ok(_) => {}
            Err(e) => {
                println!("Error: {:?}", e);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(1000));
        index += 1;
    }
}
