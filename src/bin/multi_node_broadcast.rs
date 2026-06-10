use anyhow::bail;
use maelstrom_rust_node::{Body, Message, Node};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
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
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    InternalMessage {
        data: Vec<usize>,
    },
    TopologyOk,
}

pub struct MultiNodeBroadcast {
    id: usize,
    node_id: String,
    neighbours: Vec<String>,
    storage: HashSet<usize>,
}

impl Default for MultiNodeBroadcast {
    fn default() -> Self {
        Self {
            id: 0,
            node_id: String::new(),
            neighbours: Vec::new(),
            storage: HashSet::new(),
        }
    }
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
                    .collect();

                reply.body.payload = MultiNodeBroadcastPayload::InitOk;
                self.id += 1;

                <MultiNodeBroadcast as Node<MultiNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
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
                    let data = Vec::from_iter(self.storage.clone());

                    let mut internal_message = Message {
                        src: self.node_id.clone(),
                        dst: String::new(),
                        body: Body {
                            id: Some(self.id),
                            in_reply_to: None,
                            payload: MultiNodeBroadcastPayload::InternalMessage { data },
                        },
                    };

                    for node in &self.neighbours {
                        internal_message.dst = node.to_string();
                        <MultiNodeBroadcast as Node<MultiNodeBroadcastPayload>>::send_message(
                            internal_message.clone(),
                            output,
                        )?;

                        self.id += 1;
                    }
                }
            }

            MultiNodeBroadcastPayload::Read => {
                let data = Vec::from_iter(self.storage.clone());
                reply.body.payload = MultiNodeBroadcastPayload::ReadOk { messages: data };
                self.id += 1;

                <MultiNodeBroadcast as Node<MultiNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
            }
            MultiNodeBroadcastPayload::Topology { .. } => {
                reply.body.payload = MultiNodeBroadcastPayload::TopologyOk;
                self.id += 1;

                <MultiNodeBroadcast as Node<MultiNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
            }
            MultiNodeBroadcastPayload::InternalMessage { data } => {
                for value in data {
                    self.storage.insert(value);
                }
            }
            MultiNodeBroadcastPayload::BroadcastOk {}
            | MultiNodeBroadcastPayload::ReadOk { .. }
            | MultiNodeBroadcastPayload::TopologyOk => {}
        }

        Ok(())
    }
}

fn main() {
    if let Err(e) =
        maelstrom_rust_node::main_loop::<MultiNodeBroadcast, MultiNodeBroadcastPayload>()
    {
        eprintln!("Error: {e}");
    }
}
