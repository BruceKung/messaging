use std::{
    ffi::CString,
    slice,
    sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}},
};
use std::thread;
use crate::base::settings::Settings;
use crate::base::utils::str_to_c;
use crate::core::messaging::Messaging;
use crate::publisher::publisher::Publisher;
use crate::subscriber::subscriber::Subscriber;

use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::{AlignedBuffer, AtomicBuffer},
        logbuffer::header::Header,
        status::status_indicator_reader::channel_status_to_str,
        strategies::BusySpinIdleStrategy,
    },
    context::Context,
    fragment_assembler::{Fragment, FragmentAssembler},
    image::Image,
    subscription,
    utils::{errors::AeronError, types::Index},
};
use anyhow::{bail, Result};
use crossbeam::channel::Sender;

pub struct Requester {
    pub publisher : Arc<Publisher>,
    pub subscriber: Arc<Subscriber>,
}

impl Requester {
    pub fn new(
        aeron: Arc<Mutex<Aeron>>,
        channel: &str,
        request_stream_id: i32,
        response_stream_id: i32,
        sender: Sender<Vec<u8>>,
    ) -> Self {

        println!("Adding requester to Aeron with channel {} and stream id {},{}", channel, request_stream_id,response_stream_id);
        let publisher = Arc::new(Publisher::new(aeron.clone(), channel, request_stream_id));
        let subscriber = Arc::new(Subscriber::new(aeron.clone(), channel, response_stream_id, sender));

        Self {
            publisher,
            subscriber,
        }
    }

    pub fn is_connected(&self) -> bool {
        self.publisher.is_connected() && self.subscriber.is_connected()
    }
    
    pub fn send(&self, data: &[u8], len: i32) {
        println!("Publishing to Aeron");
        self.publisher.clone().publish(data, len);
    }


}

