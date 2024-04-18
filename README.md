# Version 1.1.0 优化

## 增加退出机制
由于aeron会实际分配共享内存，建议增加退出机制，保证aeron分配的内存可以正常释放。具体步骤如下：

1) 在cargo.toml 中增加依赖
```
[dependencies]
//本次升级
messaging = { git = "ssh://git@code.non-convex.com/gateway/messaging.git", tag="1.1.0" }
//增加退出机制需要的依赖
signal-hook = { version = "0.3.17", features = ["extended-siginfo"] }
// 也建议增加pretty_env_logger依赖，可以根据环境变量，决定打印 aeron-rs日志
pretty_env_logger = "0.4"
```

2) 增加退出机制，处理退出信号(可选，如果选择不支持，默认传false到messaging相关接口即可)
```
use signal_hook::flag;

fn main() {
    pretty_env_logger::init();

    let stop = Arc::new(AtomicBool::new(false));


    let mut message_service = MessagingService::new(stop.clone());

    let stop_clone = stop.clone();

    flag::register(signal_hook::consts::SIGTERM, stop_clone).unwrap_or_else(|_| {
        eprintln!("Could not register signal handler");
        std::process::exit(1);
    });


    //正常业务流程.....


    let stop_clone = stop.clone();
    while ! stop_clone.load(std::sync::atomic::Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    // 当退出信号已经被触发，相关线程和资源释放，需要等待几秒钟
    std::thread::sleep(std::time::Duration::from_secs(10));
}
```

## 修复启动延时
由于 aeron-rs 内部 conductor 在多线程间 共享， aeron runner 线程在获取锁后，会在未释放锁的情况下，调用sleep， 导致其他线程无法获取锁 而卡住。 



