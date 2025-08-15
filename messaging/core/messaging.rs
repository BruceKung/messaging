use crate::publisher::publisher::Publisher;
use crate::subscriber::subscriber::Subscriber;
use crate::{
    base::settings::Settings, requester::requester::Requester, responser::responser::Responser,
};
use aeron_rs::{
    aeron::Aeron,
    context::Context,
    utils::errors::AeronError,
    log,
};
use anyhow::bail;
use crossbeam::channel::Sender;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use std::sync::atomic::AtomicBool;
use std::{
    collections::hash_map::DefaultHasher,
    
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
};



/*
    上游调用封装类
    1.包含aeron客户端实例
    2.包含多个推送和订阅端map结构
    3.创建aeron客户端实例的配置文件
*/
pub struct Messaging {
    pub settings: Option<Settings>, //配置文件
    pub aeron: Option<Arc<Mutex<Aeron>>>,   //aeron_client 
    pub publishers: DashMap<i32, Arc<Publisher>>, //publication实例集合
    pub subscribers: DashMap<i32, Arc<Subscriber>>, //subscription实例集合
    pub requesters: DashMap<String, Arc<Requester>>,    //上层封装Requester实例集合 基于publish和subscriber进一步封装 目前未使用 考虑去掉
    pub responsers: DashMap<String, Arc<Responser>>,    //上层封装Responser实例集合 基于publish和subscriber进一步封装 目前未使用 考虑去掉
    publisher_session_ids: DashMap<i32, i32>, // stream_id -> session_id
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
            publisher_session_ids: DashMap::new(),
        }
    }
    /*
        调用者使用顺序:
        1. 读取配置获取完整的Setting
        2. 调用执行connect链接Aeron_dirver
        3. 调用new_publisher注册push端
        4. 调用subscribe订阅其他服务注册的push对端
        5. 轮询注册的push端是否有对端订阅
    */
    pub fn connect(&mut self, settings: &Settings) -> anyhow::Result<()> {
        let mut ctx: Context = Context::new(); //实例化Aeron的Context配置类
                                               //设置访问共享内存映射目录路径，如果为空则默认访问/dev/shm/aeron-
        if !settings.dir_prefix.is_empty() {
            ctx.set_aeron_dir(settings.dir_prefix.clone());
        }
        ctx.set_error_handler(Box::new(Self::error_handler)); //设置错误处理函数
        ctx.set_pre_touch_mapped_memory(true); //设置预加载内存映射文件

        //传入配置项Context,创建Aeron实例
        let aeron = Aeron::new(ctx);
        if aeron.is_err() {
            let err = aeron.err().expect("[error] Nothing error here");
            log!(info, "panic: Aeron connection failed, dir_prefix:{}, err:{}", 
                settings.dir_prefix, err
            );
            bail!(
                "Could not connect to Aeron media driver dir_prefix:{}, err:{}",
                settings.dir_prefix, err
            );
        }
        
        let aeron_instance = match aeron {
            Ok(instance) => instance,
            Err(_) => unreachable!("Aeron should be Ok after is_err check"),
        };
        log!(info, "Aeron client connected with client_id: {}", aeron_instance.client_id());
        
        //保存配置信息
        self.settings = Some(settings.clone());
        //上述有有aeron.is_err判断
        self.aeron = Some(Arc::new(Mutex::new(aeron_instance)));

        Ok(())
    }

    //创建并初始化push实例对象
    pub fn new_publisher(&self, topic: &str) -> anyhow::Result<Arc<Publisher>> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!");
        }

        let stream_id = Self::hash_stream_id(topic);

        let p = Arc::new(Publisher::new(
            self.aeron.as_ref().expect("[error] aeron is None").clone(),
            self.settings
                .as_ref()
                .expect("[error] setting is None")
                .channel
                .as_str(),
            stream_id,
        ));
        
        // 记录 session_id
        let session_id = p.get_session_id();
        self.publisher_session_ids.insert(stream_id, session_id);
        
        self.publishers.insert(stream_id, p.clone());
        Ok(p.clone())
    }
    //消息推送方法
    pub fn publish(&self, topic: &str, data: &[u8], len: i32) -> anyhow::Result<()> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }
        let stream_id = Self::hash_stream_id(topic);
        //从dashmap找到对应的publish对象进行发送
        match self.publishers.entry(stream_id) {
            Entry::Occupied(mut o) => {
                o.get_mut().publish(data, len)?
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
    //释放publish实例
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
    //订阅其他服务的push对端
    pub fn subscribe(
        &mut self,
        topic: &str,
        sender: Sender<Vec<u8>>,
        stop: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }

        let stream_id = Self::hash_stream_id(topic);

        let s = Arc::new(Subscriber::new_with_ignore_session_id(
            self.aeron.as_ref().expect("[error] aeron is None").clone(),
            self.settings
                .as_ref()
                .expect("[error] settings is None")
                .channel
                .as_str(),
            stream_id,
            sender,
            stop,
            self.publisher_session_ids.get(&stream_id).map(|entry| *entry.value()),
        ));
        s.subscribe();
        self.subscribers.insert(stream_id, s);
        Ok(())
    }
    //取消订阅实例
    pub fn unsubscribe(&mut self, topic: &str) -> anyhow::Result<()> {
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
    //使用一个topic配置生成push和sub，用于单topic收发信息，为publish和subscriber的进一步封装
    pub fn new_requester(
        &mut self,
        topic: &str,
        sender: Sender<Vec<u8>>,
        stop: Arc<AtomicBool>,
    ) -> anyhow::Result<Arc<Requester>> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }
        //获取push和sub的数据流stream_id作为标识
        let pub_stream_id = Self::hash_stream_id(format!("{}_{}", topic, "requester").as_str());
        let sub_stream_id = Self::hash_stream_id(format!("{}_{}", topic, "responser").as_str());

        let s = Arc::new(Requester::new(
            self.aeron.as_ref().expect("[error] Aeron is None").clone(),
            self.settings
                .as_ref()
                .expect("[error] setting is None")
                .channel
                .as_str(),
            pub_stream_id,
            sub_stream_id,
            sender,
            stop,
        ));

        self.requesters.insert(topic.to_owned(), s.clone());

        Ok(s.clone())
    }
    
    //request和response需要通信双方实例化一个，使用一个topic配置生成push和sub，用于单topic收发信息，为publish和subscriber的进一步封装
    pub fn new_responser(
        &mut self,
        topic: &str,
        sender: Sender<Vec<u8>>,
        stop: Arc<AtomicBool>,
    ) -> anyhow::Result<Arc<Responser>> {
        if self.settings.is_none() || self.aeron.is_none() {
            bail!("Please call connect first!")
        }
        //获取push和sub的数据流stream_id作为标识 request和response的id设置相反 
        /*
            request和response的id设置相反
            request的request_stream_id = response的response_stream_id
            request的response_stream_id = response的request_stream_id

         */
        let pub_stream_id = Self::hash_stream_id(format!("{}_{}", topic, "requester").as_str());
        let sub_stream_id = Self::hash_stream_id(format!("{}_{}", topic, "responser").as_str());

        let s = Arc::new(Responser::new(
            self.aeron.as_ref().expect("[error] Aeron is None").clone(),
            self.settings
                .as_ref()
                .expect("[error] setting is None")
                .channel
                .as_str(),
            sub_stream_id,
            pub_stream_id,
            sender,
            stop,
        ));

        self.responsers.insert(topic.to_owned(), s.clone());

        Ok(s.clone())
    }
    //释放topic订阅推送map中的实例资源
    pub fn release(&mut self) {
        if self.settings.is_none() || self.aeron.is_none() {
            eprintln!("Please call connect first!")
        }
        // 释放DashMap<i32, Arc<Publisher>>,
        self.publishers.iter_mut().for_each(|p| {
            p.value().release();
        });
        //释放DashMap<i32, Arc<Subscriber>>,
        self.subscribers.iter_mut().for_each(|s| {
            s.value().release();
        });
        //释放DashMap<String, Arc<Requester>>,
        self.requesters.iter_mut().for_each(|r| {
            r.value().release();
        });
        //释放DashMap<String, Arc<Responser>>,
        self.responsers.iter_mut().for_each(|r| {
            r.value().release();
        });

        self.publishers.clear();
        self.subscribers.clear();
        self.requesters.clear();
        self.responsers.clear();
        self.publisher_session_ids.clear();
    }
    //传入topic计算hash值
    fn hash_stream_id(topic: &str) -> i32 {
        let mut state = DefaultHasher::new();
        topic.hash(&mut state);
        static MAX_ID: u64 = 100_000_000;
        (state.finish() % MAX_ID) as i32
    }
    //错误处理 aeron客户端层出现异常时执行
    fn error_handler(err: AeronError) {
        log!(error, "Error: {:?}", err);
    }
}
// 自定义实现资源释放
impl Drop for Messaging {
    fn drop(&mut self) {
        self.release();
    }
}
