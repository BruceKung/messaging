use std::ffi::CString;
use clap::Parser;
use messaging::core::messaging::Messaging;
use messaging::base::settings::Settings;
use messaging::publisher::publisher::Publisher;
use messaging::subscriber::subscriber::Subscriber;

use protobuf::Message;

#[path = "test.rs"] mod test;

fn create_order() -> test::OrderInfo {
    test::OrderInfo {
        id: 1,
        trade_date: 2,
        strategy_order_id: 3,
        basket_id: 4,
        account_id: "5".to_string(),
        broker_id: 6,
        trade_acc: "7".to_string(),
        stock_code: "8".to_string(),
        strategy_type: 9,
        tag: "10".to_string(),
        status: 11,
        status_msg: "12".to_string(),
        order_time: 13,
        order_price: 14.0,
        order_vol: 15,
        bs_flag: 16,
        localId: "17".to_string(),
        sys_no: "18".to_string(),
        trade_price: 19.0,
        trade_vol: 20,
        cancel_vol: 21,
        remark: "22".to_string(),
        create_tm: 23,
        update_tm: 24,
        exchange_id: 25,
        quote_time: 26,
        cancel_num: 27,
        channel_id: "28".to_string(),
        bus_strategy_order_id: "29".to_string(),
        trade_no: "30".to_string(),
        trade_amt: 31.0,
        trade_tm: 32,
        mark1: 33,
        mark2: "34".to_string(),
        start_push: true,
        department: "36".to_string(),
        ..Default::default()
    }
}


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "/dev/shm/aeron")]
    dir_prefix: Option<String>,
    #[arg(short, long, default_value = "publisher1")]
    instance: Option<String>,
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

    let result = messaging.new_publisher(topic.as_str());
    if result.is_err() {
        println!("Error: {:?}", result.err());
        return;
    }

    let publiser = result.unwrap();

    println!("Publisher connected");

    let mut index : i64 = 0;
    loop {

        let mut o = create_order();
        o.id = index;
        o.tag  = args.instance.as_ref().unwrap().clone();

        let data = o.write_to_bytes().unwrap();

        match publiser.clone().publish( data.as_slice(), data.len() as i32) {
            Ok(_) => {}
            Err(e) => {
                println!("Error: {:?}", e);
            }
        }
        index += 1;

        std::thread::sleep(std::time::Duration::from_millis(1000));
    }
}
