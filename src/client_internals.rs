//! A crate private module to expose `KafkaClient` internals for use
//! within this crate but not outside of it.
// todo: not sure how to do the same, but tokio/futures can't live on traits. So not sure how to deal with it.
// for now just going to not use the trait, have to figure out how to keep it hidden

use super::client::{ProduceConfirm, ProduceMessage};

use crate::error::KafkaError;

pub trait KafkaClientInternals {
    //async fn internal_produce_messages<'a, 'b, I, J>(
    fn internal_produce_messages<'a, 'b, I, J>(
        &mut self,
        required_acks: i16,
        ack_timeout: i32,
        messages: I,
    ) -> Result<Vec<ProduceConfirm>, KafkaError>
    where
        J: AsRef<ProduceMessage<'a, 'b>>,
        I: IntoIterator<Item = J>;
}
