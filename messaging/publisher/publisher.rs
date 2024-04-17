use std::{ffi::CString, sync::{Arc, Mutex}, f32::consts::E};

use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::{AlignedBuffer, AtomicBuffer},
        status::status_indicator_reader::channel_status_to_str,
    },
    context::Context, publication, utils::errors::AeronError,
};

use anyhow::{bail, Result };

use crate::base::utils::str_to_c;

pub struct Publisher {
    publication: Arc<Mutex<publication::Publication>>,
}

impl Publisher {
    pub fn new(aeron:  Arc<Mutex<Aeron>> , channel : &str, stream_id : i32 ) -> Self {
        
        println!("Adding publication to Aeron with channel {} and stream id {}", channel, stream_id);
        let publication_id = aeron.lock().unwrap()
            .add_publication(str_to_c(channel), stream_id)
            .expect("Could not add publication");

        let mut publication = aeron.lock().unwrap().find_publication(publication_id);
        while publication.is_err() {
            std::thread::sleep(std::time::Duration::from_millis(100));
            publication = aeron.lock().unwrap().find_publication(publication_id);
        }
        let publication: std::sync::Arc<std::sync::Mutex<publication::Publication>> = publication.unwrap();

        let channel_status = publication.lock().unwrap().channel_status();
        println!(
            "Publication channel status: {}",
            channel_status_to_str(channel_status)
        );

        println!("Publication {} created", stream_id);
        Self {
            publication : publication
        }
    }

    pub fn is_connected(&self) -> bool {
        self.publication.lock().unwrap().is_connected()
    }

    pub fn publish(&self, data: &[u8], len: i32) -> anyhow::Result<()> {
        println!("Publishing to Aeron, len: {}", len);

        let buffer = AlignedBuffer::with_capacity(len);
        let src_buffer = AtomicBuffer::from_aligned(&buffer);

        src_buffer.put_bytes(0, data);

        let mut count = 0;
        loop {
            match self.publication.lock().unwrap().offer_part(src_buffer, 0, len) {
                Ok(_) => {
                    //println!("Published to Aeron");
                    break;
                }
                Err(e) => {
                    std::thread::sleep(std::time::Duration::from_millis(1000));
                    println!("Publish Error: {:?}", e);
                    if count > 100 {
                        bail!("After 100 times retry, puublish still error: {:?}", e);
                    }
                }
            }
            count += 1;
        }

        if !self.publication.lock().unwrap().is_connected() {
            println!("No active subscribers detected");
            bail!("No active subscribers detected")
        }
        Ok(())

    }

    pub fn release(&self) {
        println!("Stopping publisher");
        self.publication.lock().unwrap().release();
    }

}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.release();
    }
}

