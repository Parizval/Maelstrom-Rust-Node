use anyhow::{bail, Result};
use maelstrom_rust_node::{Message, Node};
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum SingleNodeBroadcastPayload {
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
        topology: std::collections::HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

pub struct SingleNodeBroadcast {
    id: usize,
    node_id: String,
    storage: Vec<usize>,
}

impl Default for SingleNodeBroadcast {
    fn default() -> Self {
        Self {
            id: 0,
            node_id: String::new(),
            storage: Vec::new(),
        }
    }
}

impl Node<SingleNodeBroadcastPayload> for SingleNodeBroadcast {
    fn step(
        &mut self,
        input: Message<SingleNodeBroadcastPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(self.id));
        match reply.body.payload {
            SingleNodeBroadcastPayload::Init { node_id, .. } => {
                self.node_id = node_id;

                reply.body.payload = SingleNodeBroadcastPayload::InitOk;
                self.id += 1;

                <SingleNodeBroadcast as Node<SingleNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
            }
            SingleNodeBroadcastPayload::InitOk => bail!("Should not receive InitOk as input"),

            SingleNodeBroadcastPayload::Broadcast { message } => {
                self.storage.push(message);

                reply.body.payload = SingleNodeBroadcastPayload::BroadcastOk;
                self.id += 1;

                <SingleNodeBroadcast as Node<SingleNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
            }
            SingleNodeBroadcastPayload::Read => {
                reply.body.payload = SingleNodeBroadcastPayload::ReadOk {
                    messages: self.storage.clone(),
                };
                self.id += 1;
                <SingleNodeBroadcast as Node<SingleNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
            }
            SingleNodeBroadcastPayload::Topology { .. } => {
                reply.body.payload = SingleNodeBroadcastPayload::TopologyOk;
                self.id += 1;
                <SingleNodeBroadcast as Node<SingleNodeBroadcastPayload>>::send_message(
                    reply, output,
                )?;
            }
            SingleNodeBroadcastPayload::BroadcastOk {}
            | SingleNodeBroadcastPayload::ReadOk { .. }
            | SingleNodeBroadcastPayload::TopologyOk => {}
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    maelstrom_rust_node::main_loop::<SingleNodeBroadcast, SingleNodeBroadcastPayload>()
}
