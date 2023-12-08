use dashmap::DashMap;
use anyhow::{bail, Result };
use std::{ffi::CString, sync::{Arc, Mutex}, slice};
use crossbeam::channel::{Sender, Receiver};
use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::{AlignedBuffer, AtomicBuffer},
        status::status_indicator_reader::channel_status_to_str,logbuffer::header::Header,
    },
    context::Context, publication,subscription, image::Image,
    utils::{errors::AeronError, types::Index},
};
use crate::base::settings::Settings;
use crate::publisher::publisher::Publisher;
use crate::subscriber::subscriber::Subscriber;
use lazy_static::lazy_static;
use once_cell::sync::Lazy;

//lazy_static!{
//    pub static ref SUBSCRIBER_SENDERS: DashMap<i32, Sender<Vec<u8>>> = DashMap::new();
//}


pub struct  Messaging
{
    pub settings : Option<Settings>,
    pub aeron : Option<Arc<Mutex<Aeron>>>,
    pub publishers : DashMap<i32, Publisher>,
    pub subscribers : DashMap<i32, Subscriber>,
}

impl Messaging
{
    pub fn new( ) -> Self {
        Self {
            settings : None,
            aeron : None,
            publishers : DashMap::new(),
            subscribers : DashMap::new(),
        }
    }

    pub fn connect(&mut self, settings : &Settings) -> anyhow::Result<()> {

        let mut ctx = Context::new();
        if !settings.dir_prefix.is_empty() {
            ctx.set_aeron_dir(settings.dir_prefix.clone());
        }
        ctx.set_new_exclusive_publication_handler(Box::new(Self::on_new_publication_handler));
        ctx.set_new_subscription_handler(Box::new(Self::on_new_subscription_handler));
        ctx.set_available_image_handler(Box::new(Self::available_image_handler));
        ctx.set_unavailable_image_handler(Box::new(Self::unavailable_image_handler));
        ctx.set_error_handler(Box::new(Self::error_handler));
        ctx.set_pre_touch_mapped_memory(true);

        let aeron = Aeron::new(ctx);
        if aeron.is_err() {
            bail!("Could not connect to Aeron media driver");
        }

        let aeron = aeron.unwrap();
        let mut count = 0;
        while !aeron.is_closed() {
            std::thread::sleep(std::time::Duration::from_millis(100));
            count += 1;
            if count > 100 {
                bail!("Aeron instance is closed");
            }
        }
        self.settings = Some(settings.clone());
        self.aeron = Some(Arc::new(Mutex::new( aeron )));
        Ok(())
    }

    pub fn init_publish( &self, stream_id: i32 ) ->  anyhow::Result<()> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!");
        }

        let p = Publisher::new(self.aeron.as_ref().unwrap().clone(), self.settings.as_ref().unwrap().channel.as_str(), stream_id);
        self.publishers.insert(stream_id, p);
        Ok(())
    }

    pub fn publish(&self, stream_id: i32, data: &[u8], len: i32) -> anyhow::Result<()> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }

        let p = self.publishers.get(&stream_id);
        if p.is_none() {
            bail!("Publisher not found for channel {}, please call init_publish in advance!", stream_id)
        }
        let p = p.unwrap();
        p.publish(data, len);
        Ok(())
    }

    pub fn subscribe(&mut self, stream_id: i32, sender : Sender<Vec<u8>>) -> anyhow::Result<()>{
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }

        let mut s = Subscriber::new(self.aeron.as_ref().unwrap().clone(), self.settings.as_ref().unwrap().channel.as_str(), stream_id, sender);
        s.subscribe();
        self.subscribers.insert(stream_id, s);
        //SUBSCRIBER_SENDERS.insert(stream_id, sender);
        Ok(())
    }

   
    pub fn request() {

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

    fn error_handler(error: AeronError) {
        println!("Error: {:?}", error);
    }
}

