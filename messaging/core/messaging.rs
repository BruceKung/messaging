use crate::publisher::publisher::Publisher;
use crate::subscriber::subscriber::Subscriber;
use crate::{
    base::settings::Settings, requester::requester::Requester, responser::responser::Responser,
};
use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::{AlignedBuffer, AtomicBuffer},
        logbuffer::header::Header,
        status::status_indicator_reader::channel_status_to_str,
    },
    context::Context,
    image::Image,
    publication, subscription,
    utils::{errors::AeronError, types::Index},
};
use anyhow::{bail, Result};
use crossbeam::channel::{Receiver, Sender};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use lazy_static::lazy_static;
use once_cell::sync::Lazy;
use std::{
    ffi::CString,
    hash::{self, Hash, Hasher},
    slice,
    sync::{Arc, Mutex},
    collections::hash_map::DefaultHasher,
};

//lazy_static!{
//    pub static ref SUBSCRIBER_SENDERS: DashMap<i32, Sender<Vec<u8>>> = DashMap::new();
//}

pub struct Messaging {
    pub settings: Option<Settings>,
    pub aeron: Option<Arc<Mutex<Aeron>>>,
    pub publishers: DashMap<i32, Arc<Publisher>>,
    pub subscribers: DashMap<i32, Subscriber>,
    pub requesters: DashMap<String, Arc<Requester>>,
    pub responsers: DashMap<String, Arc<Responser>>,
}

impl Messaging {
    pub fn new() -> Self {
        Self {
            settings: None,
            aeron: None,
            publishers: DashMap::new(),
            subscribers: DashMap::new(),
            requesters: DashMap::new(),
            responsers: DashMap::new(),
        }
    }

    pub fn connect(&mut self, settings: &Settings) -> anyhow::Result<()> {
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

        self.settings = Some(settings.clone());
        self.aeron = Some(Arc::new(Mutex::new(aeron.unwrap())));

        print!("Aeron instance is closed");
        Ok(())
    }

    pub fn new_publisher(&self, topic: &str) -> anyhow::Result<Arc<Publisher>> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!");
        }

        let stream_id = Self::hash_stream_id(topic);

        let p = Arc::new(Publisher::new(
            self.aeron.as_ref().unwrap().clone(),
            self.settings.as_ref().unwrap().channel.as_str(),
            stream_id,
        ));
        self.publishers.insert(stream_id, p.clone());
        Ok(p.clone())
    }

    pub fn publish(&self, topic: &str, data: &[u8], len: i32) -> anyhow::Result<()> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }
        let stream_id = Self::hash_stream_id(topic);

        match self.publishers.entry(stream_id) {
            Entry::Occupied(mut o) => {
                o.get_mut().publish(data, len);
            }
            Entry::Vacant(_) => {
                bail!(
                    "Publisher not found for channel {}, please call init_publish in advance!",
                    stream_id
                )
            }
        }
        Ok(())
    }

    pub fn unpublish(&self, topic: &str) -> anyhow::Result<()> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }

        let stream_id = Self::hash_stream_id(topic);
        match self.publishers.entry(stream_id) {
            Entry::Occupied(mut o) => {
                o.get_mut().release();
                o.remove_entry();
            }
            Entry::Vacant(_) => {
                bail!(
                    "Publisher not found for channel {}, please call init_publish in advance!",
                    stream_id
                )
            }
        }
        Ok(())
    }

    pub fn subscribe(&mut self, topic : &str, sender: Sender<Vec<u8>>) -> anyhow::Result<()> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }

        let stream_id = Self::hash_stream_id(topic);

        let mut s = Subscriber::new(
            self.aeron.as_ref().unwrap().clone(),
            self.settings.as_ref().unwrap().channel.as_str(),
            stream_id,
            sender,
        );
        s.subscribe();
        self.subscribers.insert(stream_id, s);
        Ok(())
    }

    pub fn unsubscribe(&mut self, topic : &str) -> anyhow::Result<()> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }
        let stream_id = Self::hash_stream_id(topic);

        match self.subscribers.entry(stream_id) {
            Entry::Occupied(mut o) => {
                o.get_mut().release();
                o.remove_entry();
            }
            Entry::Vacant(_) => {
                bail!(
                    "Subscriber not found for stream {}, please call subscribe in advance!",
                    stream_id
                )
            }
        }
        Ok(())
    }

    pub fn request(&mut self, topic: &str, sender: Sender<Vec<u8>>) -> anyhow::Result<Arc<Requester>> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }

        let pub_stream_id = Self::hash_stream_id(format!("{}_{}", topic, "requester").as_str());
        let sub_stream_id = Self::hash_stream_id(format!("{}_{}", topic, "responser").as_str());

        let mut s = Arc::new(Requester::new(
            self.aeron.as_ref().unwrap().clone(),
            self.settings.as_ref().unwrap().channel.as_str(),
            pub_stream_id,
            sub_stream_id,
            sender,
        ));

        self.requesters.insert(topic.to_owned(), s.clone());

        Ok(s.clone())
    }

    pub fn response(&mut self, topic: &str, sender: Sender<Vec<u8>>) -> anyhow::Result<Arc<Responser>> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }

        let pub_stream_id = Self::hash_stream_id(format!("{}_{}", topic, "requester").as_str());
        let sub_stream_id = Self::hash_stream_id(format!("{}_{}", topic, "responser").as_str());

        let mut s = Arc::new(Responser::new(
            self.aeron.as_ref().unwrap().clone(),
            self.settings.as_ref().unwrap().channel.as_str(),
            sub_stream_id,
            pub_stream_id,
            sender,
        ));

        self.responsers.insert(topic.to_owned(), s.clone());

        Ok(s.clone())
    }

    fn hash_stream_id(topic: &str) -> i32 {
        let mut state = DefaultHasher::new();
        topic.hash(&mut state) ;
        state.finish() as i32
    }

    pub fn on_new_publication_handler(
        channel: CString,
        stream_id: i32,
        session_id: i32,
        correlation_id: i64,
    ) {
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

impl Drop for Messaging {
    fn drop(&mut self) {
        if self.settings.is_none() || self.aeron.is_none() {
            eprintln!("Please call connect first!")
        }

        self.publishers.clear();
        self.subscribers.clear();
        self.requesters.clear();
        self.responsers.clear();
    }
}