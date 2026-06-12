use futures::StreamExt;
use rskafka::client::{ClientBuilder, consumer::{StartOffset, StreamConsumerBuilder}, partition::UnknownTopicHandling};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let client = ClientBuilder::new(vec!["127.0.0.1:9092".into()]).build().await.unwrap();
    for t in &["orders", "executions", "metrics"] {
        let pc = Arc::new(client.partition_client(*t, 0, UnknownTopicHandling::Retry).await.unwrap());
        let mut s = StreamConsumerBuilder::new(pc, StartOffset::Earliest).with_max_wait_ms(200).build();
        let mut c = 0u64;
        while let Some(Ok((ro, _))) = s.next().await {
            if c < 1 {
                let val = ro.record.value.unwrap_or_default();
                let v = String::from_utf8_lossy(&val);
                println!("  sample[{}]: {}..", t, &v[..v.len().min(100)]);
            }
            c += 1;
        }
        println!("{}: {} records", t, c);
    }
}
