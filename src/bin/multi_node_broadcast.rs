use anyhow::{bail, Result};
use maelstrom_rust_node::{Body, Message, Node};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
    panic,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum MultiNodeBroadcastPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    InternalMessage {
        data: HashSet<usize>,
    },
    InternalMessageOk,
    TopologyOk,
}
#[derive(Debug, Default)]
struct MultiNodeBroadcast {
    id: usize,
    node_id: String,
    neighbours: Vec<String>,
    storage: HashSet<usize>,
    known: HashMap<String, HashSet<usize>>,
    msg_communicated: HashMap<usize, HashSet<usize>>,
    topology: Vec<String>,
}

impl Node<MultiNodeBroadcastPayload> for MultiNodeBroadcast {
    fn step(
        &mut self,
        input: Message<MultiNodeBroadcastPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(self.id));

        match reply.body.payload {
            MultiNodeBroadcastPayload::Init { node_id, node_ids } => {
                self.node_id = node_id;
                self.neighbours = node_ids
                    .into_iter()
                    .filter(|n| *n != self.node_id)
                    .inspect(|node| {
                        self.known.insert(node.clone(), HashSet::new());
                    })
                    .collect();

                reply.body.payload = MultiNodeBroadcastPayload::InitOk;
                <MultiNodeBroadcast as Node<MultiNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
                self.id += 1;
            }
            MultiNodeBroadcastPayload::InitOk => bail!("Should not receive InitOk as input"),

            MultiNodeBroadcastPayload::Broadcast { message } => {
                reply.body.payload = MultiNodeBroadcastPayload::BroadcastOk;

                let propagation = self.storage.insert(message);

                <MultiNodeBroadcast as Node<MultiNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
                self.id += 1;

                if propagation {
                    let mut internal_message = Message {
                        src: self.node_id.clone(),
                        dst: String::new(),
                        body: Body {
                            id: Some(self.id),
                            in_reply_to: None,
                            payload: MultiNodeBroadcastPayload::InternalMessage {
                                data: self.storage.clone(),
                            },
                        },
                    };

                    for node in &self.neighbours {
                        let know_node_data = self.known.get(node).unwrap_or_else(|| panic!(""));
                        let data = self
                            .storage
                            .difference(know_node_data)
                            .copied()
                            .collect::<HashSet<usize>>();

                        self.msg_communicated.insert(self.id, data.clone());

                        internal_message.dst = node.to_string();
                        internal_message.body.payload =
                            MultiNodeBroadcastPayload::InternalMessage { data };

                        <MultiNodeBroadcast as Node<MultiNodeBroadcastPayload>>::send_message(
                            internal_message.clone(),
                            output,
                        )?;

                        self.id += 1;
                    }
                }
            }

            MultiNodeBroadcastPayload::Read => {
                reply.body.payload = MultiNodeBroadcastPayload::ReadOk {
                    messages: self.storage.clone(),
                };

                <MultiNodeBroadcast as Node<MultiNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
                self.id += 1;
            }
            MultiNodeBroadcastPayload::Topology { mut topology } => {
                self.topology = topology
                    .remove(&self.node_id)
                    .unwrap_or_else(|| panic!("No Topology give for node {}", self.node_id));

                reply.body.payload = MultiNodeBroadcastPayload::TopologyOk;
                <MultiNodeBroadcast as Node<MultiNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
                self.id += 1;
            }
            MultiNodeBroadcastPayload::InternalMessage { data } => {
                for value in data {
                    self.storage.insert(value);
                }

                reply.body.payload = MultiNodeBroadcastPayload::InternalMessageOk;
                <MultiNodeBroadcast as Node<MultiNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
                self.id += 1;
            }
            MultiNodeBroadcastPayload::InternalMessageOk => {
                let in_reply_to = reply.body.in_reply_to.unwrap_or_else(|| panic!(""));
                let communicated_data = self.msg_communicated.get(&in_reply_to);

                if let Some(data) = communicated_data {
                    let src_node = reply.src;
                    let known_data = self.known.entry(src_node);
                    known_data.or_insert(data.to_owned());
                }
            }
            MultiNodeBroadcastPayload::BroadcastOk
            | MultiNodeBroadcastPayload::ReadOk { .. }
            | MultiNodeBroadcastPayload::TopologyOk => {}
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    maelstrom_rust_node::main_loop::<MultiNodeBroadcast, MultiNodeBroadcastPayload>()
}
