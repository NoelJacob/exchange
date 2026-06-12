use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(version, about = "Exchange bot worker — sends FIX+WS orders and reports metrics")]
pub struct Config {
    /// Exchange target host
    #[arg(long, default_value = "127.0.0.1")]
    pub target_host: String,

    /// Exchange FIX port
    #[arg(long, default_value_t = 9090)]
    pub fix_port: u16,

    /// Exchange WS port
    #[arg(long, default_value_t = 8080)]
    pub ws_port: u16,

    /// Total RPS across all connections in this process
    #[arg(long, default_value_t = 100)]
    pub rps: u64,

    /// Starting RPS per process (ramps up to target)
    #[arg(long, default_value_t = 1)]
    pub min_rps: u64,

    /// Seconds to ramp from min_rps to rps
    #[arg(long, default_value_t = 10)]
    pub ramp_up_secs: u64,


    /// Redpanda brokers (empty = stdout only)
    #[arg(long, default_value = "")]
    pub redpanda_brokers: String,

    /// Contestant ID for multi-contestant routing
    #[arg(long, default_value = "test-run")]
    pub contestant_id: String,
    /// Test duration in seconds
    #[arg(long, default_value_t = 10)]
    pub duration_secs: u64,

    /// RNG seed for deterministic order sequence
    #[arg(long, default_value_t = 42)]
    pub seed: u64,

    /// Seconds between metrics snapshot emissions
    #[arg(long, default_value_t = 2)]
    pub report_interval_secs: u64,

    /// Number of parallel FIX sessions to open
    #[arg(long, default_value_t = 4)]
    pub fix_connections: u32,

    /// Number of parallel WS connections to open
    #[arg(long, default_value_t = 4)]
    pub ws_connections: u32,

    /// Prefix for SenderCompID — each session appends index ("BOT00", "BOT01", …)
    #[arg(long, default_value = "BOT")]
    pub sender_comp_id_prefix: String,

    /// Exchange TargetCompID
    #[arg(long, default_value = "XCANG3")]
    pub target_comp_id: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            target_host: "127.0.0.1".into(),
            fix_port: 9090,
            ws_port: 8080,
            rps: 100,
            min_rps: 1,
            ramp_up_secs: 10,
            duration_secs: 10,
            seed: 42,
            report_interval_secs: 2,
            fix_connections: 4,
            ws_connections: 4,
            sender_comp_id_prefix: "BOT".into(),
            target_comp_id: "XCANG3".into(),
            redpanda_brokers: "".into(),
            contestant_id: "test-run".into(),
        }
    }
}
