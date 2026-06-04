use serde::{Deserialize, Serialize};
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Body<Payload> {
    // #[serde(rename = "type")]
    // ty: String,
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,

    #[serde(rename = "in_reply_to", skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub payload: Payload,
}

pub trait Node<Payload> {
    fn step(
        &mut self,
        input: Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()>;

    fn send_message(message: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        serde_json::to_writer(&mut *output, &message)?;
        output.write_all(b"\n")?;
        Ok(())
    }
}
pub fn main_loop<S, Payload>() -> anyhow::Result<()>
where
    S: Node<Payload> + Default,
    Payload: for<'de> Deserialize<'de>,
{
    let stdin = std::io::stdin().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<Payload>>();

    let mut stdout = std::io::stdout().lock();

    let mut state = S::default();

    for input in inputs {
        let input = input?;
        state.step(input, &mut stdout)?;
    }

    Ok(())
}
