use futures::StreamExt;
use rskafka::client::{
    ClientBuilder,
    consumer::{StartOffset, StreamConsumerBuilder},
    partition::UnknownTopicHandling,
};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let client = ClientBuilder::new(vec!["127.0.0.1:9092".into()]).build().await.unwrap();
    for topic in &["executions", "orders"] {
        let pc = Arc::new(
            client.partition_client(*topic, 0, UnknownTopicHandling::Retry).await.unwrap()
        );
        let mut stream = StreamConsumerBuilder::new(pc, StartOffset::Earliest)
            .with_max_wait_ms(500).build();
        let mut count = 0u64;
        while let Some(Ok((ro, _))) = stream.next().await {
            if let Some(v) = &ro.record.value {
                let s = String::from_utf8_lossy(v);
                if count < 2 { println!("[{}] {}", topic, &s[..s.len().min(200)]); }
                count += 1;
            }
        }
        println!("[{}] total: {}", topic, count);
    }
}
