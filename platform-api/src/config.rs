use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(version, about = "Platform API — hackathon backend")]
pub struct Config {
    #[arg(long, env = "PORT", default_value = "8080")]
    pub port: u16,

    #[arg(long, env = "QUESTDB_URL", default_value = "127.0.0.1:8812")]
    pub questdb_url: String,

    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    pub redis_url: String,

    #[arg(long, env = "JWT_SECRET", default_value = "dev-secret")]
    pub jwt_secret: String,

    #[arg(long, env = "DOCKER_HOST", default_value = "unix:///var/run/docker.sock")]
    pub docker_url: String,

    #[arg(long, env = "MINIO_URL", default_value = "http://minio:9000")]
    pub minio_url: String,

    #[arg(long, env = "MINIO_BUCKET", default_value = "contestant-binaries")]
    pub minio_bucket: String,

    #[arg(long, env = "INTERNAL_TOKEN", default_value = "shared-secret-token")]
    pub internal_token: String,

    #[arg(long, env = "RUNNER_IMAGE", default_value = "infra-runner:latest")]
    pub runner_image: String,

    #[arg(long, env = "MINIO_ACCESS_KEY", default_value = "admin")]
    pub minio_access_key: String,

    #[arg(long, env = "MINIO_SECRET_KEY", default_value = "password123")]
    pub minio_secret_key: String,

    #[arg(long, env = "MINIO_REGION", default_value = "us-east-1")]
    pub minio_region: String,
}
