use std::{ffi::CString, sync::{Arc, Mutex}};

use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::{AlignedBuffer, AtomicBuffer},
        status::status_indicator_reader::channel_status_to_str,
    },
    context::Context, publication, utils::errors::AeronError,
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

pub struct AeronPublisher {
    aeron: Aeron,
    publication: Arc<Mutex<publication::Publication>>,
}

impl AeronPublisher {
    pub fn new(settings : &Settings ) -> Self {
        let mut ctx = Context::new();
        if !settings.dir_prefix.is_empty() {
            ctx.set_aeron_dir(settings.dir_prefix.clone());
        }
        ctx.set_new_exclusive_publication_handler(Box::new(Self::on_new_publication_handler));
        ctx.set_error_handler(Box::new(Self::error_handler));
        ctx.set_pre_touch_mapped_memory(true);

        let aeron = Aeron::new(ctx);
        if aeron.is_err() {
            println!("Aeron instance is closed");
            std::process::exit(1);
        }

        let mut aeron = aeron.unwrap();

        let publication_id = aeron
            .add_publication(str_to_c(&settings.channel), settings.stream_id)
            .expect("Could not add publication");

        let mut publication = aeron.find_publication(publication_id);
        while publication.is_err() {
            std::thread::sleep(std::time::Duration::from_millis(100));
            publication = aeron.find_publication(publication_id);
        }
        let publication: std::sync::Arc<std::sync::Mutex<publication::Publication>> = publication.unwrap();

        let channel_status = publication.lock().unwrap().channel_status();
        println!(
            "Publication channel status: {}",
            channel_status_to_str(channel_status)
        );

        Self {
            aeron,
            publication : publication
        }
    }

    pub fn publish(&self, data: &[u8], len: i32) {
        println!("Publishing to Aeron");

        let buffer = AlignedBuffer::with_capacity(len);
        let src_buffer = AtomicBuffer::from_aligned(&buffer);

        src_buffer.put_bytes(0, data);
        let result = self.publication.lock().unwrap().offer_part(src_buffer, 0, len);

        match result {
            Ok(_) => println!("Message sent"),
            Err(e) => println!("Error sending message: {:?}", e),
        }

        if !self.publication.lock().unwrap().is_connected() {
            println!("No active subscribers detected");
        }

    }

    pub fn on_new_publication_handler(channel: CString, stream_id: i32, session_id: i32, correlation_id: i64) {
        println!(
            "Publication: {} {} {} {}",
            channel.to_str().unwrap(),
            stream_id,
            session_id,
            correlation_id
        );
    }
    pub fn error_handler(error: AeronError) {
        println!("Error: {:?}", error);
    }
}


