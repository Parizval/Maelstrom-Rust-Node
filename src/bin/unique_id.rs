use anyhow::{bail, Context, Result};
use maelstrom_rust_node::{Message, Node};
use serde::{Deserialize, Serialize};
use std::io::StdoutLock;

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
#[derive(Debug, Default)]
pub struct UniqueIdNode {
    id: usize,
    node_id: String,
}

impl Node<UniqueIdPayload> for UniqueIdNode {
    fn step(
        &mut self,
        input: Message<UniqueIdPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(self.id));
        match reply.body.payload {
            UniqueIdPayload::Init { node_id, .. } => {
                self.node_id = node_id;

                reply.body.payload = UniqueIdPayload::InitOk;
                self.id += 1;

                <UniqueIdNode as Node<UniqueIdPayload>>::send_message(reply, output)
                    .context("serialize response to Echo")?;
            }
            UniqueIdPayload::InitOk => bail!("Should not receive InitOk as input"),

            UniqueIdPayload::Generate => {
                reply.body.payload = UniqueIdPayload::GenerateOk {
                    id: format!("{}-{}", self.node_id, self.id).to_string(),
                };
                self.id += 1;
                <UniqueIdNode as Node<UniqueIdPayload>>::send_message(reply, output)
                    .context("serialize response to Echo")?;
            }
            UniqueIdPayload::GenerateOk { id: _ } => {}
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    maelstrom_rust_node::main_loop::<UniqueIdNode, UniqueIdPayload>()
}
