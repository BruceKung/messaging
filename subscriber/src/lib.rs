use std::{ffi::CString, sync::{Arc, Mutex}, slice};

use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::{AlignedBuffer, AtomicBuffer},
        status::status_indicator_reader::channel_status_to_str, logbuffer::header::Header,
    },
    context::Context, subscription, image::Image,
    utils::{errors::AeronError, types::Index},
};

#[derive(Clone)]
pub struct Settings {
    pub dir_prefix: String,
    pub channel: String,
    pub stream_id: i32,
    pub number_of_messages: i64,
    pub linger_timeout_ms: u64,
}

fn str_to_c(val: &str) -> CString {
    CString::new(val).expect("Error converting str to CString")
}

pub struct AeronSubscriber {
    aeron: Aeron,
    subscription: Arc<Mutex<subscription::Subscription>>,
}

impl AeronSubscriber {
    pub fn new(settings : &Settings ) -> Self {
        let mut ctx = Context::new();
        if !settings.dir_prefix.is_empty() {
            ctx.set_aeron_dir(settings.dir_prefix.clone());
        }
        ctx.set_new_subscription_handler(Box::new(Self::on_new_subscription_handler));
        ctx.set_error_handler(Box::new(Self::error_handler));
        ctx.set_available_image_handler(Box::new(Self::available_image_handler));
        ctx.set_unavailable_image_handler(Box::new(Self::unavailable_image_handler));
        ctx.set_pre_touch_mapped_memory(true);

        let aeron = Aeron::new(ctx);
        if aeron.is_err() {
            println!("Aeron instance is closed");
            std::process::exit(1);
        }

        let mut aeron = aeron.unwrap();

        let subscription_id = aeron
            .add_subscription(str_to_c(&settings.channel), settings.stream_id)
            .expect("Could not add subscription");

        let mut subscription = aeron.find_subscription(subscription_id);
        while subscription.is_err() {
            std::thread::sleep(std::time::Duration::from_millis(100));
            subscription = aeron.find_subscription(subscription_id);
        }
        let subscription: std::sync::Arc<std::sync::Mutex<subscription::Subscription>> = subscription.unwrap();

        let channel_status = subscription.lock().unwrap().channel_status();
        println!(
            "Subscription channel status for {} is {}",
            settings.channel,
            channel_status_to_str(channel_status)
        );

        Self {
            aeron,
            subscription,
        }
    }

    pub fn subscribe(&self) {
        let mut subscription = self.subscription.lock().unwrap();
        let _ = subscription.poll(&mut Self::on_new_fragment, 10);
    }


    pub fn on_new_fragment(buffer: &AtomicBuffer, offset: Index, length: Index, header: &Header) {
        unsafe {
            let slice_msg = slice::from_raw_parts_mut(buffer.buffer().offset(offset as isize), length as usize);
            let msg = CString::new(slice_msg).unwrap();
            println!(
                "Message to stream {} from session {} ({}@{}): <<{}>>",
                header.stream_id(),
                header.session_id(),
                length,
                offset,
                msg.to_str().unwrap()
            );
        }
    }

    pub fn available_image_handler(image: &Image) {
        println!(
            "Available image correlation_id={} session_id={} at position={} from {}",
            image.correlation_id(),
            image.session_id(),
            image.position(),
            image.source_identity().to_str().unwrap()
        );
    }

    pub fn unavailable_image_handler(image: &Image) {
        println!(
            "Unavailable image correlation_id={} session_id={} at position={} from {}",
            image.correlation_id(),
            image.session_id(),
            image.position(),
            image.source_identity().to_str().unwrap()
        );
    }

    pub fn on_new_subscription_handler(channel: CString, stream_id: i32, correlation_id: i64) {
        println!(
            "Subscription: {} {} {}",
            channel.to_str().unwrap(),
            stream_id,
            correlation_id
        );
    }    

    pub fn error_handler(error: AeronError) {
        println!("Error: {:?}", error);
    }
}