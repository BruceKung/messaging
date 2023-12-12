use std::ffi::CString;

use messaging::core::messaging::Messaging;
use messaging::base::settings::Settings;
use messaging::publisher::publisher::Publisher;
use messaging::subscriber::subscriber::Subscriber;

use protobuf::Message;

#[path = "person.rs"] mod person;


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

    let topic = "test_topic".to_string();

    let result = messaging.new_publisher(topic.as_str());
    if result.is_err() {
        println!("Error: {:?}", result.err());
        return;
    }

    let publiser = result.unwrap();

    loop {
        if publiser.is_connected() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let mut index : i32 = 0;
    loop {

        let mut p = person::Person::new();
        p.set_name("Test publisher".to_string());
        p.set_age(18 + index);

        let data = p.write_to_bytes().unwrap();

        match publiser.clone().publish( data.as_slice(), data.len() as i32) {
            Ok(_) => {}
            Err(e) => {
                println!("Error: {:?}", e);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(1000));
        index += 1;
    }
}
