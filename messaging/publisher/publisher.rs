use std::sync::{Arc, Mutex};

use aeron_rs::{
    aeron::Aeron,
    concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer},
    publication,
    log,
};

use anyhow::bail;

use crate::base::utils::str_to_c;

pub struct Publisher {
    publication: Arc<Mutex<publication::Publication>>,
}

impl Publisher {
    pub fn new(aeron: Arc<Mutex<Aeron>>, channel: &str, stream_id: i32) -> Self {
        log!(debug,
            "Adding publication to Aeron with channel {} and stream id {}",
            channel, stream_id
        );
        //publication_id为 aeron给Aeron::Publisher分配的注册ID
        let publication_id = match aeron.lock() {
            Ok(mut aeron_locked) => {
                match aeron_locked.add_publication(str_to_c(channel), stream_id) {
                    Ok(id) => id,
                    Err(e) => {
                        panic!("Error adding publication: {:?}", e);
                    }
                }
            }
            Err(e) => {
                panic!("Error locking aeron: {:?}", e);
            }
        };
        //需要从aeron中通过注册ID获取到Aeron::Publisher实例才算分配正常
        let mut last_error_time = std::time::Instant::now();
        let publication: Arc<Mutex<publication::Publication>> = loop {
            match aeron.lock() {
                Ok(mut aeron_locked) => match aeron_locked.find_publication(publication_id) {
                    Ok(pub_found) => break pub_found, // 找到后退出循环
                    Err(e) => {
                        let now = std::time::Instant::now();
                        if now.duration_since(last_error_time).as_secs() >= 1 {
                            log!(error, "Error finding publication: {:?}", e);
                            last_error_time = now;
                        }
                        // 如果找不到 publication，一直重试
                    }
                },
                Err(e) => {
                    panic!("Error locking aeron: {:?}", e);
                }
            }
        };
        // 由于 find_publication 返回值已经是 Arc<Mutex<Publication>> 类型，
        let publication = publication;
        log!(debug, "Publication {} created", stream_id);
        Self {
            publication: publication,
        }
    }
    // 判断pub对端是否有订阅
    pub fn is_connected(&self) -> bool {
        match self.publication.lock() {
            Ok(publication_locked) => {
                return publication_locked.is_connected();
            }
            Err(e) => {
                log!(error, "[error]locking publication: {:?}", e);
                return false;
            }
        }
    }
    //消息推送 
    pub fn publish(&self, data: &[u8], len: i32) -> anyhow::Result<()> {
        let buffer = AlignedBuffer::with_capacity(len);
        let src_buffer = AtomicBuffer::from_aligned(&buffer);

        src_buffer.put_bytes(0, data);

        let mut count = 0;
        //最多尝试100次
        loop {
            match self.publication.lock() {
                Ok(publication_locked) => match publication_locked.offer_part(src_buffer, 0, len) {
                    Ok(_) => {
                        break;
                    }
                    Err(e) => {
                        std::thread::sleep(std::time::Duration::from_millis(1000));
                        log!(error, "Publish Error: {:?}", e);
                        if count > 100 {
                            bail!("After 100 times retry, publish still error: {:?}", e);
                        }
                    }
                },
                Err(e) => {
                    log!(error, "[error]locking publication: {:?}", e);
                }
            }
            count += 1;
        }
        //每次调用都会检查对端是否存在sub订阅 如果不存在订阅则向上返回报错
        match self.publication.lock() {
            Ok(publication_locked) => {
                if !publication_locked.is_connected() {
                    log!(debug, "No active subscribers detected");
                    bail!("No active subscribers detected")
                }
            }
            Err(e) => {
                log!(error, "[error]locking publication: {:?}", e);
            }
        }
        Ok(())
    }
    //Aeron::Publisher资源释放
    pub fn release(&self) {
        log!(debug, "Stopping publisher");
        match self.publication.lock() {
            Ok(publication_locked) => {
                publication_locked.release();
            }
            Err(e) => {
                log!(error, "[error]locking publication: {:?}", e);
            }
        }
    }
}

// 自定义资源释放动作
impl Drop for Publisher {
    fn drop(&mut self) {
        self.release();
    }
}
