use tokio::sync::broadcast::{self, error::RecvError, Sender};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{error, info};

use crate::types::{Collector, Executor, Strategy};

/// The main engine of Artemis. This struct is responsible for orchestrating the
/// data flow between collectors, strategies, and executors.
pub struct Engine<E, A> {
    /// The set of collectors that the engine will use to collect events.
    collectors: Vec<Box<dyn Collector<E>>>,

    /// The set of strategies that the engine will use to process events.
    strategies: Vec<Box<dyn Strategy<E, A>>>,

    /// The set of executors that the engine will use to execute actions.
    executors: Vec<Box<dyn Executor<A>>>,

    /// The capacity of the event channel.
    event_channel_capacity: usize,

    /// The capacity of the action channel.
    action_channel_capacity: usize,
}

impl<E, A> Engine<E, A> {
    pub fn new() -> Self {
        Self {
            collectors: vec![],
            strategies: vec![],
            executors: vec![],
            event_channel_capacity: 512,
            action_channel_capacity: 512,
        }
    }

    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    pub fn with_action_channel_capacity(mut self, capacity: usize) -> Self {
        self.action_channel_capacity = capacity;
        self
    }
}

impl<E, A> Default for Engine<E, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E, A> Engine<E, A>
where
    E: Send + Clone + 'static + std::fmt::Debug,
    A: Send + Clone + 'static + std::fmt::Debug,
{
    /// Adds a collector to be used by the engine.
    pub fn add_collector(&mut self, collector: Box<dyn Collector<E>>) {
        self.collectors.push(collector);
    }

    /// Adds a strategy to be used by the engine.
    pub fn add_strategy(&mut self, strategy: Box<dyn Strategy<E, A>>) {
        self.strategies.push(strategy);
    }

    /// Adds an executor to be used by the engine.
    pub fn add_executor(&mut self, executor: Box<dyn Executor<A>>) {
        self.executors.push(executor);
    }

    /// The core run loop of the engine. This function will spawn a thread for
    /// each collector, strategy, and executor. It will then orchestrate the
    /// data flow between them.
    pub async fn run(self) -> Result<JoinSet<()>, Box<dyn std::error::Error>> {
        let (event_sender, _): (Sender<E>, _) = broadcast::channel(self.event_channel_capacity);
        let (action_sender, _): (Sender<A>, _) = broadcast::channel(self.action_channel_capacity);

        let mut set = JoinSet::new();

        // Spawn executors in separate threads.
        for executor in self.executors {
            let mut receiver = action_sender.subscribe();
            set.spawn(async move {
                info!("starting executor... ");
                loop {
                    match receiver.recv().await {
                        Ok(action) => match executor.execute(action).await {
                            Ok(_) => {}
                            Err(e) => error!("error executing action: {}", e),
                        },
                        Err(RecvError::Closed) => break,
                        Err(e) => error!("error receiving action: {}", e),
                    }
                }
            });
        }

        // Spawn strategies in separate threads.
        for mut strategy in self.strategies {
            let mut event_receiver = event_sender.subscribe();
            let action_sender = action_sender.clone();
            strategy.sync_state().await?;

            set.spawn(async move {
                info!("starting strategy... ");
                loop {
                    match event_receiver.recv().await {
                        Ok(event) => {
                            for action in strategy.process_event(event).await {
                                match action_sender.send(action) {
                                    Ok(_) => {}
                                    Err(e) => error!("error sending action: {}", e),
                                }
                            }
                        }
                        Err(RecvError::Closed) => break,
                        Err(e) => error!("error receiving event: {}", e),
                    }
                }
            });
        }

        // Spawn collectors in separate threads.
        for collector in self.collectors {
            let event_sender = event_sender.clone();
            set.spawn(async move {
                info!("starting collector... ");
                let mut event_stream = collector.get_event_stream().await.unwrap();
                while let Some(event) = event_stream.next().await {
                    match event_sender.send(event) {
                        Ok(_) => {}
                        Err(e) => error!("error sending event: {}", e),
                    }
                }
            });
        }

        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CollectorStream;
    use anyhow::Result;
    use async_trait::async_trait;
    use tokio::sync::mpsc;

    struct FiniteCollector;

    #[async_trait]
    impl Collector<u64> for FiniteCollector {
        async fn get_event_stream(&self) -> Result<CollectorStream<'_, u64>> {
            Ok(Box::pin(tokio_stream::iter([1, 2])))
        }
    }

    struct EchoStrategy;

    #[async_trait]
    impl Strategy<u64, u64> for EchoStrategy {
        async fn sync_state(&mut self) -> Result<()> {
            Ok(())
        }

        async fn process_event(&mut self, event: u64) -> Vec<u64> {
            vec![event * 10]
        }
    }

    struct RecordingExecutor(mpsc::UnboundedSender<u64>);

    #[async_trait]
    impl Executor<u64> for RecordingExecutor {
        async fn execute(&self, action: u64) -> Result<()> {
            self.0.send(action)?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn consumers_finish_after_collectors_close_without_losing_queued_work() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let mut engine = Engine::new();
        engine.add_collector(Box::new(FiniteCollector));
        engine.add_strategy(Box::new(EchoStrategy));
        engine.add_executor(Box::new(RecordingExecutor(sender)));
        let mut tasks = engine.run().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        })
        .await
        .expect("closed channels must terminate consumers");
        assert_eq!(receiver.try_recv().unwrap(), 10);
        assert_eq!(receiver.try_recv().unwrap(), 20);
        assert!(receiver.try_recv().is_err());
    }
}
