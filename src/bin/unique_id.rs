use anyhow::{bail, Context};
use maelstrom_rust_node::{Body, Message, Node};
use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum UniqueIdPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Generate,
    GenerateOk {
        id: String,
    },
}

pub struct UniqueIdNode {
    id: usize,
    node_id: String,
}

impl Default for UniqueIdNode {
    fn default() -> Self {
        Self {
            id: 0,
            node_id: String::new(),
        }
    }
}

impl Node<UniqueIdPayload> for UniqueIdNode {
    fn step(
        &mut self,
        input: Message<UniqueIdPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input.body.payload {
            UniqueIdPayload::Init { node_id, .. } => {
                self.node_id = node_id;

                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: UniqueIdPayload::InitOk,
                    },
                };

                self.id += 1;

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to Echo")?;
                output.write_all(b"\n")?;
            }
            UniqueIdPayload::InitOk => bail!("Should not receive InitOk as input"),

            UniqueIdPayload::Generate => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: UniqueIdPayload::GenerateOk {
                            id: format!("{}{}", self.node_id, self.id).to_string(),
                        },
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to Echo")?;
                output.write_all(b"\n")?;

                self.id += 1;
            }
            UniqueIdPayload::GenerateOk { id: _ } => {}
        }

        Ok(())
    }
}

fn main() {
    if let Err(e) = maelstrom_rust_node::main_loop::<UniqueIdNode, UniqueIdPayload>() {
        eprintln!("Error: {e}");
    }
}
