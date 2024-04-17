use std::ffi::CString;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use messaging::core::messaging::Messaging;
use messaging::base::settings::Settings;
use messaging::requester::requester::Requester;


pub fn main() {

    let setting = Settings{
        dir_prefix: "/home/api/rzhao/dev/aeron/install/data".to_string(),
        channel: "aeron:ipc".to_string(),
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
    let stop = Arc::new(AtomicBool::new(false));

    let (sender, receiver ) = crossbeam::channel::unbounded::<Vec<u8>>();    

    let topic = "test_topic".to_string();
    let result  = messaging.new_requester( topic.as_str(), sender, stop.clone());
    if result.is_err() {
        println!("Error: {:?}", result.err());
        return;
    }
    let requester = result.unwrap();

    let mut index : i32 = 0;
    loop {
        let str_msg = format!("Basic Request msg {}", index);
        let c_str_msg = CString::new(str_msg).unwrap();

        requester.clone().send( c_str_msg.as_bytes(),  c_str_msg.as_bytes().len() as i32) ;

        if let Ok(msg) = receiver.recv() {
            let msg = CString::new(msg).unwrap();
            println!("Received message: {:?}", msg);
        }

        std::thread::sleep(std::time::Duration::from_millis(1000));
        index += 1;
    }
}
