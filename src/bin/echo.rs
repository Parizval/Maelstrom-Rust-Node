use anyhow::{bail, Result};
use maelstrom_rust_node::{Message, Node};
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum EchoPayload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
}
#[derive(Debug, Default)]
pub struct EchoNode {
    id: usize,
}

impl Node<EchoPayload> for EchoNode {
    fn step(&mut self, input: Message<EchoPayload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(self.id));

        match reply.body.payload {
            EchoPayload::Init { .. } => {
                reply.body.payload = EchoPayload::InitOk;
                self.id += 1;

                <EchoNode as Node<EchoPayload>>::send_message(reply, output)?;
            }
            EchoPayload::InitOk => bail!("Should not receive InitOk as input"),
            EchoPayload::Echo { echo } => {
                reply.body.payload = EchoPayload::EchoOk { echo };
                <EchoNode as Node<EchoPayload>>::send_message(reply, output)?;

                self.id += 1;
            }
            EchoPayload::EchoOk { echo: _ } => {
                // Do nothing
            }
        }

        Ok(())
    }
}
fn main() -> Result<()> {
    maelstrom_rust_node::main_loop::<EchoNode, EchoPayload>()
}
