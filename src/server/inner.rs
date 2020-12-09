use super::{
    mobilization::Mobilization,
    Command,
    CommandReceiver,
    EventSender,
    SinkItem,
    WorkItem,
    WorkItemContent,
};
use crate::{
    Configuration,
    Scheduler,
};
use futures::{
    future,
    FutureExt as _,
    StreamExt as _,
};
use std::fmt::Debug;

pub struct Inner<T> {
    configuration: Configuration,
    mobilization: Option<Mobilization<T>>,
    event_sender: EventSender<T>,
    scheduler: Scheduler,
}

impl<T> Inner<T> {
    pub fn new(
        event_sender: EventSender<T>,
        scheduler: Scheduler,
    ) -> Self {
        Self {
            configuration: Configuration::default(),
            mobilization: None,
            event_sender,
            scheduler,
        }
    }

    fn process_command(
        &mut self,
        command: Command<T>,
    ) where
        T: 'static + Clone + Debug + Send,
    {
        let cancel_election_timer = match command {
            Command::Configure(configuration) => {
                self.configuration = configuration;
                self.mobilization.is_some()
            },
            Command::Demobilize(completed) => {
                self.mobilization = None;
                let _ = completed.send(());
                true
            },
            #[cfg(test)]
            Command::FetchElectionTimeoutCounter(response_sender) => {
                if let Some(mobilization) = &self.mobilization {
                    let _ = response_sender
                        .send(mobilization.election_timeout_counter);
                }
                false
            },
            Command::Mobilize(mobilize_args) => {
                self.mobilization = Some(Mobilization::new(mobilize_args));
                true
            },
            Command::ProcessSinkItem(sink_item) => {
                self.process_sink_item(sink_item)
            },
        };
        if cancel_election_timer {
            if let Some(mobilization) = &mut self.mobilization {
                mobilization.cancel_election_timer();
            }
        }
    }

    fn process_sink_item(
        &mut self,
        sink_item: SinkItem<T>,
    ) -> bool
    where
        T: 'static + Clone + Debug + Send,
    {
        if let Some(mobilization) = &mut self.mobilization {
            mobilization.process_sink_item(
                sink_item,
                &self.event_sender,
                self.configuration.rpc_timeout,
                &self.scheduler,
            )
        } else {
            false
        }
    }

    fn retransmit(
        &mut self,
        peer_id: usize,
    ) where
        T: 'static + Clone + Debug + Send,
    {
        if let Some(mobilization) = &mut self.mobilization {
            mobilization.retransmit(
                peer_id,
                &self.event_sender,
                self.configuration.rpc_timeout,
                &self.scheduler,
            );
        }
    }

    pub async fn serve(
        mut self,
        command_receiver: CommandReceiver<T>,
    ) where
        T: 'static + Clone + Debug + Send,
    {
        let mut command_receiver = Some(command_receiver);
        let mut futures = Vec::new();
        loop {
            // Make server command receiver future if we don't have one.
            if let Some(command_receiver) = command_receiver.take() {
                futures
                    .push(process_command_receiver(command_receiver).boxed());
            }

            // Make election timeout future if we don't have one and we are
            // not the leader of the cluster.
            if let Some(mobilization) = &mut self.mobilization {
                if let Some(future) = mobilization
                    .upkeep_election_timeout_future(
                        &self.configuration.election_timeout,
                        &self.scheduler,
                    )
                {
                    futures.push(future);
                }
            }

            // Add any RPC timeout futures that have been set up.
            if let Some(mobilization) = &mut self.mobilization {
                futures.extend(mobilization.take_retransmission_futures());
            }

            // Wait for the next future to complete.
            // let futures_in = futures;
            let (work_item, _, futures_remaining) =
                future::select_all(futures).await;
            futures = futures_remaining;
            match work_item.content {
                #[cfg(test)]
                WorkItemContent::Cancelled(work_item) => {
                    println!("Completed canceled future: {}", work_item);
                },
                #[cfg(not(test))]
                WorkItemContent::Cancelled => {
                    println!("Completed canceled future");
                },
                WorkItemContent::ElectionTimeout => {
                    println!("*** Election timeout! ***");
                    if let Some(mobilization) = &mut self.mobilization {
                        mobilization.election_timeout(
                            &self.event_sender,
                            self.configuration.rpc_timeout,
                            &self.scheduler,
                        );
                    }
                },
                WorkItemContent::RpcTimeout(peer_id) => {
                    println!("*** RPC timeout ({})! ***", peer_id);
                    self.retransmit(peer_id);
                },
                WorkItemContent::Command {
                    command,
                    command_receiver: command_receiver_out,
                } => {
                    println!("Command: {:?}", command);
                    self.process_command(command);
                    command_receiver.replace(command_receiver_out);
                },
                WorkItemContent::Stop => break,
            }
            #[cfg(test)]
            if let Some(ack) = work_item.ack {
                let _ = ack.send(());
            }
        }
    }
}

async fn process_command_receiver<T: 'static + Clone + Debug + Send>(
    command_receiver: CommandReceiver<T>
) -> WorkItem<T> {
    let (command, command_receiver) = command_receiver.into_future().await;
    let content = if let Some(command) = command {
        WorkItemContent::Command {
            command,
            command_receiver,
        }
    } else {
        println!("Server command channel closed");
        WorkItemContent::Stop
    };
    WorkItem {
        content,
        #[cfg(test)]
        ack: None,
    }
}
