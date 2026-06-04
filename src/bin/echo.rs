use anyhow::bail;
use maelstrom_rust_node::{Body, Message, Node};
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

pub struct EchoNode {
    id: usize,
}

impl Default for EchoNode {
    fn default() -> Self {
        Self { id: 0 }
    }
}

impl Node<EchoPayload> for EchoNode {
    fn step(&mut self, input: Message<EchoPayload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            EchoPayload::Init { .. } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: EchoPayload::InitOk,
                    },
                };

                self.id += 1;

                <EchoNode as Node<EchoPayload>>::send_message(reply, output)?;
            }
            EchoPayload::InitOk => bail!("Should not receive InitOk as input"),
            EchoPayload::Echo { echo } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: EchoPayload::EchoOk { echo },
                    },
                };

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
fn main() {
    if let Err(e) = maelstrom_rust_node::main_loop::<EchoNode, EchoPayload>() {
        eprintln!("Error: {e}");
    }
}
