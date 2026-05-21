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
    #[serde(rename = "type")]
    ty: String,

    #[serde(rename = "msg_id")]
    id: Option<usize>,

    #[serde(rename = "in_reply_to")]
    in_reply_to: Option<usize>,
}

fn main() {
    println!("Hello, world!");
}
