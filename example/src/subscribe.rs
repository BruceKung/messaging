use std::ffi::CString;
use messaging::core::messaging::Messaging;
use messaging::base::settings::Settings;
use messaging::publisher::publisher::Publisher;
use messaging::subscriber::subscriber::Subscriber;
use crossbeam::channel::{Sender, Receiver};

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

    let (sender, receiver ) = crossbeam::channel::unbounded::<Vec<u8>>();

    match messaging.subscribe(stream_id, sender) {
        Ok(_) => {}
        Err(e) => {
            println!("Error: {:?}", e);
            return;
        }
    }

    while let Ok(msg) = receiver.recv() {
        let msg = CString::new(msg).unwrap();
        println!("Received message: {:?}", msg);
    }

}
