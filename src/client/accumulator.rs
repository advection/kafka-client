// ArcSwap
//
/*

accumulator per topic partition, use an arcswap for current to add then create a new one after it's sent




*/


use crate::protocol::produce::{MessageProduceRequest, PartitionProduceRequest};
use std::future::Future;
use std::sync::Arc;
use std::pin::Pin;
use crate::error::KafkaError;
use std::task::{Poll, Context};
use crate::protocol::ProduceResponse;
use futures::Sink;
use crate::client::KafkaClient;
use arc_swap::ArcSwap;

struct RecordOffsetInfo<'a>{
    topic: &'a str,
    partition: i32,
    offset: i64
}

pub struct Batch<'a, 'b>{
    messages: Vec<MessageProduceRequest<'a>>,
    size: usize,
    earliest_ts: i64,
    topic: &'b str,
    partition: &'a i32,
}

impl<'a, 'b> Batch<'a, 'b> {
    fn add_message(mut self, msg: MessageProduceRequest<'a>) -> bool {
//        self.size += msg.key todo: add size check
        self.messages.push(msg);

        true
    }
}

impl<'a, 'b> Sink<MessageProduceRequest<'_>> for  Accumulator<'a, 'b>{
    type Error = KafkaError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // we're always ready... for now
    }

    fn start_send(self: Pin<&mut Self>, item: MessageProduceRequest) -> Result<(), Self::Error> {
        let mut batch = self.batch.load();
        if batch.add_message(item) {
            let new_batch= Arc::new(Batch{
                messages: Vec::new(),
                size:0,
                earliest_ts: 0,
                topic: self.topic,
                partition: &self.partition
            });
            let _ = self.batch.compare_and_swap(batch, new_batch);
        }

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        unimplemented!()
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        unimplemented!()
    }
}

pub struct Accumulator<'a, 'b>{
    topic: &'a str,
    partition: i32,
    batch: ArcSwap<Batch<'a, 'b>>,
    client: &'a mut KafkaClient

}