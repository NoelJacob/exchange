use s3::{AddressingStyle, Auth, Client, Credentials};

/// MinIO/S3 client using s3 crate with authenticated SigV4 requests.
pub struct MinioClient {
    client: Client,
    bucket_name: String,
    enabled: bool,
}

impl MinioClient {
    pub async fn new(
        endpoint: &str,
        bucket_name: &str,
        access_key: &str,
        secret_key: &str,
        region: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if endpoint.is_empty() {
            tracing::warn!("[MINIO] endpoint empty — noop mode");
            let r = if region.is_empty() { "us-east-1" } else { region };
            let client = Client::builder("http://noop.invalid")?
                .region(r)
                .auth(Auth::Anonymous)
                .build()?;
            return Ok(Self { client, bucket_name: bucket_name.to_string(), enabled: false });
        }

        let creds = Credentials::new(access_key, secret_key)?;
        let client = Client::builder(endpoint)?
            .region(region)
            .auth(Auth::Static(creds))
            .addressing_style(AddressingStyle::Path)
            .build()?;

        // Check if bucket exists
        let exists = client.buckets().head(bucket_name).send().await.is_ok();
        if !exists {
            client.buckets().create(bucket_name).send().await?;
            tracing::info!("[MINIO] created bucket {bucket_name}");
        }

        tracing::info!("[MINIO] connected to {endpoint}, bucket={bucket_name}");
        Ok(Self { client, bucket_name: bucket_name.to_string(), enabled: true })
    }

    pub async fn put_binary(
        &self, key: &str, data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self.enabled {
            tracing::info!("[MINIO-NOOP] would upload {key}");
            return Ok(());
        }
        self.client
            .objects()
            .put(&self.bucket_name, key)
            .body_bytes(data.to_vec())
            .send()
            .await?;
        tracing::info!("[MINIO] uploaded {key}");
        Ok(())
    }
}

impl Clone for MinioClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            bucket_name: self.bucket_name.clone(),
            enabled: self.enabled,
        }
    }
}
