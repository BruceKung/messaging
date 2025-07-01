
use crate::base::utils::str_to_c;
use aeron_rs::{
    aeron::Aeron,
    concurrent::{
        atomic_buffer::AtomicBuffer,
        logbuffer::header::Header,
        strategies::{
            SleepingIdleStrategy, Strategy,
        },
    },
    fragment_assembler::FragmentAssembler,
    subscription::Subscription,
    utils::{
        errors::{AeronError, GenericError},
        types::Index,
    },
    log,
};
use anyhow::Result;
use crossbeam::channel::Sender;
use std::thread;
use std::{
    slice,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

static DEFAULT_FRAGMENT_COUNT_LIMIT: i32 = 10;

pub struct Subscriber {
    pub stop: Arc<AtomicBool>, //状态标志位
    pub poll_thread: Arc<Option<thread::JoinHandle<()>>>, //poll消息线程
}
//Subscriber实例创建后即支持接受pub对端的数据传输，异常情况都直接panic
impl Subscriber {
    pub fn new(
        aeron: Arc<Mutex<Aeron>>,
        channel: &str,
        stream_id: i32,
        sender: Sender<Vec<u8>>,
        stop: Arc<AtomicBool>,
    ) -> Self {
        let mut subscription: Arc<Mutex<Subscription>>;
        {
            log!(trace,
                "Adding subscription to Aeron with channel {} and stream id {}",
                channel, stream_id
            );
            //往aeron注册订阅
            let subscription_id = match aeron.lock() {
                Ok(mut aeron_locked) => {
                    match aeron_locked.add_subscription(str_to_c(channel), stream_id) {
                        Ok(id) => id,
                        Err(e) => {
                            panic!("[error] Could not add subscription,e:{:?}", e);
                        }
                    }
                }
                Err(e) => {
                    panic!("[error] lock aeron error");
                }
            };

            let mut sub: Result<Arc<Mutex<Subscription>>, AeronError> =
                Err(GenericError::SubscriptionNotFound.into());
            //需要从aeron中通过注册ID获取到Aeron:: Subscription实例才算分配正常，循环检查直到获取正常
            loop {
                if sub.is_ok() {
                    break;
                }
                if let Ok(mut aeron) = aeron.lock() {
                    sub = aeron.find_subscription(subscription_id);
                }
            }
            log!(trace,
                "Subscription channel {} stream {} is found",
                channel, stream_id
            );
            match sub {
                Ok(sub_) => subscription = sub_,
                Err(e) => {
                    panic!("[error] Subscription unwrap error");
                }
            }
        }

        let poll_idle_strategy = SleepingIdleStrategy::new(1);
        /*
            启动订阅线程
            1.创建消息流组装器,处理接收到的消息片段
            2.将接收到的消息通过chanel的sender给接收方receiver端
            
         */
        let mut poll_thread = Arc::new(None);
        {
            let subscription = subscription.clone();
            let stop = stop.clone();
            match thread::Builder::new()
                .name(String::from("Poll thread"))
                .spawn(move || {
                    //定义aeron::FragmentAssembler消息处理方法
                    let mut handler = move |buffer: &AtomicBuffer,
                                            offset: Index,
                                            length: Index,
                                            header: &Header| {
                        unsafe {
                            let slice_msg = slice::from_raw_parts_mut(
                                buffer.buffer().offset(offset as isize),
                                length as usize,
                            );

                            match sender.send(slice_msg.to_vec()) {
                                Ok(_) => {} //println!("Message sent"),
                                Err(e) => {
                                    log!(error, "Error sending message: {:?}", e);
                                }
                            }
                        }
                    };

                    let mut fragment_assembler = FragmentAssembler::new(&mut handler, None);
                    let mut fragment_handler = fragment_assembler.handler();
                    //while循环调用sub实例的 `poll` 方法，处理收到的消息片段
                    while !stop.load(Ordering::SeqCst) {
                        match subscription.lock() {
                            Ok(mut subscription_locked) => {
                                let fragments_read = subscription_locked
                                    .poll(&mut fragment_handler, DEFAULT_FRAGMENT_COUNT_LIMIT);
                                poll_idle_strategy.idle_opt(fragments_read);
                            }
                            Err(e) => {
                                log!(error, "[error] lock subscription error");
                            }
                        }
                    }
                }) {
                // 线程启动成功，打印成功消息并将线程句柄保存在 poll_thread
                Ok(thread) => {
                    log!(trace, "Poll thread started");
                    poll_thread = Arc::new(Some(thread));
                }
                 // 如果线程启动失败，打印错误信息
                Err(e) => {
                    log!(error, "Error starting poll thread: {:?}", e);
                }
            }
        }
    
        log!(trace, "Subscriber {} created", stream_id);
        Self { stop, poll_thread }
    }
    //sub端不检查connect状态 默认true
    pub fn is_connected(&self) -> bool {
        true
    }
    pub fn subscribe(&self) -> anyhow::Result<()> {
        log!(trace, "Subscribing to Aeron");
        Ok(())
    }
    //退出订阅
    pub fn release(&self) -> anyhow::Result<()> {
        log!(trace, "Unsubscribing from Aeron");
        Ok(())
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.release();
    }
}
