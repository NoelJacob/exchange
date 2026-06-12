use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(version, about = "Telemetry ingester + verifier for the hackathon platform")]
pub struct Config {
    #[arg(long, default_value = "127.0.0.1:9092")]
    pub redpanda_brokers: String,

    #[arg(long, default_value = "127.0.0.1:8812")]
    pub questdb_pgwire: String,

    #[arg(long, default_value = "127.0.0.1:6379")]
    pub valkey_addr: String,

    #[arg(long, default_value = "test-run")]
    pub contestant_id: String,

    #[arg(long, default_value_t = 60)]
    pub drain_timeout_secs: u64,

    #[arg(long, default_value_t = 2)]
    pub poll_interval_secs: u64,

    #[arg(long, default_value_t = 10)]
    /// Maximum seconds to wait for a missing exec_seq before declaring the gap permanent.
    pub gap_timeout_secs: u64,
}
