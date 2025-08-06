/*
 * Copyright 2020 UT OVERSEAS INC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
    thread,
};

use crate::concurrent::{logbuffer::term_reader::ErrorHandler, strategies::Strategy};
use crate::utils::errors::{AeronError, GenericError};
use crate::log;

/// The trait to be implemented by agents run within AgentRunner
pub trait Agent {
    fn on_start(&mut self) -> Result<(), AeronError>;
    fn do_work(&mut self) -> Result<i32, AeronError>;
    fn on_close(&mut self) -> Result<(), AeronError>;
}

pub struct AgentStopper {
    thread: Option<thread::JoinHandle<()>>,
    tx: Sender<bool>,
}

impl AgentStopper {
    pub fn new(thread: thread::JoinHandle<()>, tx: Sender<bool>) -> Self {
        Self {
            thread: Some(thread),
            tx,
        }
    }

    pub fn stop(&mut self) {
        self.tx
            .send(true)
            .expect("Can't send stop command to AgentRunner");

        let _b = self.thread.take().unwrap().join();
    }
}

pub struct AgentRunner<
    A: 'static + std::marker::Send + std::marker::Sync + Agent,
    I: 'static + std::marker::Send + std::marker::Sync + Strategy,
> {
    agent: Arc<Mutex<A>>, // need mutable Agent here as AgentRunner will change Agent state while running it
    idle_strategy: Arc<I>,
    exception_handler: Box<dyn ErrorHandler + std::marker::Send>,
    name: String,
}

impl<
        A: 'static + std::marker::Send + std::marker::Sync + Agent,
        I: 'static + std::marker::Send + std::marker::Sync + Strategy,
    > AgentRunner<A, I>
{
    pub fn new(
        agent: Arc<Mutex<A>>,
        idle_strategy: Arc<I>,
        exception_handler: Box<dyn ErrorHandler + std::marker::Send>,
        name: &str,
    ) -> Self {
        Self {
            agent,
            idle_strategy,
            exception_handler,
            name: String::from(name),
        }
    }

    /**
     * Name given to the thread running the agent.
     *
     * @return the name given to the thread running the agent.
     */
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn set_name(&mut self, new_name: &str) {
        self.name = String::from(new_name);
    }

    /**
     * Start the Agent running
     *
     * Will spawn a std::thread.
     * Returns closure which is once called will shutdown the runner.
     */
    pub fn start(mut this: Self) -> Result<AgentStopper, AeronError> {
        let (tx, rx) = channel::<bool>();

        let th = thread::Builder::new()
            .name(this.name.clone())
            .spawn(move || {
                this.run(rx);
            });

        if let Ok(handle) = th {
            Ok(AgentStopper::new(handle, tx))
        } else {
            Err(GenericError::AgentStartFailed { msg: th.err() }.into())
        }
    }

    /**
     * Run the Agent duty cycle until closed
     */
    pub fn run(&mut self, stop_rx: Receiver<bool>) {
        log!(trace, "[agent_runner::run] START - entering run method");
        log!(trace, "[agent_runner::run] STEP 1 - about to call agent.on_start");
        if let Err(error) = self.agent.lock().expect("Mutex poisoned").on_start() {
            self.exception_handler.call(error);
        }
        log!(trace, "[agent_runner::run] STEP 1 - agent.on_start completed");

        log!(trace, "[agent_runner::run] STEP 2 - entering main loop");
        loop {
            // Monitor message from main thread and be ready to finish the work
            log!(trace, "[agent_runner::run] LOOP - checking stop signal");
            if let Ok(time_to_stop) = stop_rx.try_recv() {
                if time_to_stop {
                    log!(trace, "[agent_runner::run] LOOP - received stop signal, breaking loop");
                    break;
                }
            }

            let mut result: Result<i32, AeronError> = Ok(0);
            log!(trace, "[agent_runner::run] LOOP - about to lock agent and call do_work");
            {
                let mut agent = self.agent.lock().expect("Mutex poisoned");
                result = agent.do_work();
            }
            log!(trace, "[agent_runner::run] LOOP - agent.do_work completed");

            match result {
                Ok(work_cnt) => {
                    log!(trace, "[agent_runner::run] LOOP - about to call idle_strategy.idle_opt, work_cnt: {}", work_cnt);
                    self.idle_strategy.idle_opt(work_cnt);
                    log!(trace, "[agent_runner::run] LOOP - idle_strategy.idle_opt completed");
                },
                Err(error) => {
                    log!(trace, "[agent_runner::run] LOOP - about to call exception_handler.call for error");
                    self.exception_handler.call(error);
                    log!(trace, "[agent_runner::run] LOOP - exception_handler.call completed");
                },
            }
        }
        log!(trace, "[agent_runner::run] STEP 2 - main loop exited");

        log!(trace, "[agent_runner::run] STEP 3 - about to call agent.on_close");
        if let Err(error) = self.agent.lock().expect("Mutex poisoned").on_close() {
            self.exception_handler.call(error);
        }
        log!(trace, "[agent_runner::run] STEP 3 - agent.on_close completed");
        log!(trace, "[agent_runner::run] END - run method completed");
    }
}
