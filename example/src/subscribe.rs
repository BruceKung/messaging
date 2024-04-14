use std::{ffi::CString, time::UNIX_EPOCH};
use messaging::core::messaging::Messaging;
use messaging::base::settings::Settings;
use messaging::publisher::publisher::Publisher;
use messaging::subscriber::subscriber::Subscriber;
use crossbeam::channel::{Sender, Receiver};
use clap::Parser;
use protobuf::Message;

#[path = "test.rs"] mod test;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "/dev/shm/aeron")]
    dir_prefix: Option<String>,
    #[arg(short, long, default_value = "1")]
    count: Option<i64>,
}

pub fn main() {
    pretty_env_logger::init();

    let args = Args::parse();
    let dir_prefix = args.dir_prefix.unwrap_or("".to_string());

    let setting = Settings{
        dir_prefix,
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

    let (sender, receiver ) = crossbeam::channel::unbounded::<Vec<u8>>();

    match messaging.subscribe(topic.as_str(), sender) {
        Ok(_) => {}
        Err(e) => {
            println!("Error: {:?}", e);
            return;
        }
    }

    let mut index = 0;
    let mut start = 0;
    while let Ok(msg) = receiver.recv() {     
        // get now timestamp 
        if start == 0 {
            start = std::time::SystemTime::now().duration_since(UNIX_EPOCH).expect("failed to get ts").as_nanos();
        }
        let _p = test::OrderInfo::parse_from_bytes(msg.as_slice()).unwrap();

        index += 1;

        if index == args.count.as_ref().unwrap().to_owned() {
            let now = std::time::SystemTime::now().duration_since(UNIX_EPOCH).expect("failed to get ts").as_nanos();
            let dur = now - start;
            println!("Received last message {:?} , dur {}",  _p, dur);
            index = 0;
        } 
    }

}
