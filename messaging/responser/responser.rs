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

pub struct Responser {
    pub publisher : Publisher,
    pub subscriber: Subscriber,
}

impl Responser {
    pub fn new(
        aeron: Arc<Mutex<Aeron>>,
        channel: &str,
        response_stream_id: i32,
        request_stream_id: i32,
        sender : Sender<Vec<u8>>,
    ) -> Self {

        let publisher = Publisher::new(aeron.clone(), channel, request_stream_id);
        let subscriber = Subscriber::new(aeron.clone(), channel, response_stream_id, sender);

        Self {
            publisher,
            subscriber,
        }
    }

    pub fn release(&mut self) {
        self.publisher.release();
        self.subscriber.release();
    }
    
    pub fn response(&self, data: &[u8], len: i32) {
        println!("Publishing to Aeron");
        self.publisher.publish(data, len);
    }

}