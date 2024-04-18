use std::{
    ffi::CString,
    slice,
    sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}},
};
use std::thread;
use crate::base::settings::Settings;
use crate::base::utils::str_to_c;
use crate::core::messaging::Messaging;
use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::{AlignedBuffer, AtomicBuffer},
        logbuffer::header::Header,
        status::status_indicator_reader::channel_status_to_str,
        strategies::{BackOffIdleStrategy, BusySpinIdleStrategy, SleepingIdleStrategy, Strategy},
    },
    context::Context,
    fragment_assembler::{Fragment, FragmentAssembler},
    image::Image,
    subscription::{self, Subscription},
    utils::{errors::{AeronError, GenericError}, types::Index},
};
use anyhow::{bail, Result};
use crossbeam::channel::Sender;

static DEFAULT_FRAGMENT_COUNT_LIMIT : i32 = 10;

pub struct Subscriber {
    pub stop : Arc<AtomicBool>,
    pub poll_thread : Arc<Option<thread::JoinHandle<()>>>,
}

impl Subscriber {
    pub fn new(
        aeron: Arc<Mutex<Aeron>>,
        channel: &str,
        stream_id: i32,
        sender: Sender<Vec<u8>>,
        stop : Arc<AtomicBool>
    ) -> Self {
        let mut subscription : Arc<Mutex<Subscription>>;
        {
            println!(
                "Adding subscription to Aeron with channel {} and stream id {}",
                channel, stream_id
            );

            let mut subscription_id = 0;
            
            {
                subscription_id = aeron.lock().unwrap()
                .add_subscription(str_to_c(channel), stream_id)
                .expect("Could not add subscription");
            } 

            let mut sub : Result<Arc<Mutex<Subscription>>, AeronError> = Err(GenericError::SubscriptionNotFound.into());
            loop {
                if sub.is_ok() {
                    break;
                }          
                if let Ok(mut aeron) = aeron.lock() {
                    sub = aeron.find_subscription(subscription_id);
                }
            }

            println!("Subscription channel {} stream {} is found", channel, stream_id);
            subscription = sub.unwrap();
        }

        let poll_idle_strategy = SleepingIdleStrategy::new(1);
        let mut poll_thread = Arc::new(None);
        {
            let subscription = subscription.clone();
            let stop = stop.clone();
            match thread::Builder::new()
            .name(String::from("Poll thread"))
            .spawn(move || {
    
                let mut handler = move |buffer: &AtomicBuffer, offset: Index, length: Index, header: &Header| {
                    unsafe {
                        let slice_msg =
                            slice::from_raw_parts_mut(buffer.buffer().offset(offset as isize), length as usize);
                        //let msg = CString::new(slice_msg).unwrap();
                        println!(
                            "Message to stream {} from session {} ({}@{})",
                            header.stream_id(),
                            header.session_id(),
                            length,
                            offset,
                        );
            
                        match sender.send(slice_msg.to_vec()) {
                            Ok(_) => {}, //println!("Message sent"),
                            Err(e) => println!("Error sending message: {:?}", e),
                        }
                    }
                };
    
                let mut fragment_assembler = FragmentAssembler::new(&mut handler, None);
                let mut fragment_handler = fragment_assembler.handler();
    
                while !stop.load(Ordering::SeqCst) {
                    let fragments_read = subscription.lock().unwrap().poll(&mut fragment_handler, DEFAULT_FRAGMENT_COUNT_LIMIT);
                    poll_idle_strategy.idle_opt(fragments_read);
                }
            }) {
                Ok(thread) => {
                    println!("Poll thread started");
                    poll_thread = Arc::new(Some(thread));
                }
                Err(e) => {
                    println!("Error starting poll thread: {:?}", e);
                }
            }
        }

        println!("Subscriber {} created", stream_id);
        Self {
            stop,
            poll_thread,
        }

    }

    pub fn is_connected(&self) -> bool {
        true
    }
    pub fn subscribe(&self) -> anyhow::Result<()> {
        println!("Subscribing to Aeron");
        Ok(())
    }

    pub fn release(&self) -> anyhow::Result<()> {
        println!("Unsubscribing from Aeron");
        Ok(())
    }
 
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.release();
    }
}