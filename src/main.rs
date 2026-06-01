use std::io::{StdoutLock, Write};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Body {
    // #[serde(rename = "type")]
    // ty: String,
    #[serde(rename = "msg_id")]
    id: Option<usize>,

    #[serde(rename = "in_reply_to", skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<usize>,

    #[serde(flatten)]
    payload: Payload,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
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
    Generate,
    GenerateOk {
        id: String,
    },
}

struct EchoNode {
    id: usize,
    node_id: String,
}

impl EchoNode {
    pub fn step(&mut self, input: Message, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Init { node_id, .. } => {
                self.node_id = node_id;

                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk,
                    },
                };

                self.id += 1;

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to Echo")?;
                output.write_all(b"\n")?;
            }
            Payload::InitOk => bail!("Should not receive InitOk as input"),
            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk { echo },
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to Echo")?;
                output.write_all(b"\n")?;

                self.id += 1;
            }
            Payload::EchoOk { echo: _ } => {
                // Do nothing
            }
            Payload::Generate => {
                let reply = Message {
                    src: input.dst,
                    dst: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::GenerateOk {
                            id: format!("{}{}", self.node_id, self.id).to_string(),
                        },
                    },
                };

                serde_json::to_writer(&mut *output, &reply)
                    .context("serialize response to Echo")?;
                output.write_all(b"\n")?;

                self.id += 1;
            }
            Payload::GenerateOk { id: _ } => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    // println!("Hello, world!");
    // let m1 = Message {
    //     src: "a".to_string(),
    //     dst: "b".to_string(),
    //     body: Body {
    //         payload: Payload::EchoOk {
    //             echo: "Sdas".to_string(),
    //         },
    //         id: Some(1),
    //         in_reply_to: None,
    //     },
    // };

    // // let b1 = Body {
    // //     ty:
    // // }

    // println!("{}", serde_json::to_string(&m1).unwrap());

    let stdin = std::io::stdin().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut stdout = std::io::stdout().lock();

    let mut state = EchoNode {
        id: 0,
        node_id: String::new(),
    };

    for input in inputs {
        let input = input.context("Maelstrom input from STDIN could not be deserialized")?;
        state
            .step(input, &mut stdout)
            .context("Node step function failed.")?;
    }

    Ok(())
}
