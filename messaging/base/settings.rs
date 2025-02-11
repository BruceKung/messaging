#[derive(Clone)]
pub struct Settings {
    pub dir_prefix: String,      //共享内存映射目录
    pub channel: String, //通道，一般包含具体的协议，网络地址，传输方式等，当前我们仅使用贡献内存通信，默认赋值aeron::ipc
    pub stream_id: i32,  // 标识符，表示同一通道不同对应发布和订阅者使用
    pub number_of_messages: i64, // 最大消息数
    pub linger_timeout_ms: u64, //系统关闭前push端等待的时间
}
