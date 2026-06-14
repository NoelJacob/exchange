use bollard::container::{Config, CreateContainerOptions, ListContainersOptions, StartContainerOptions, StopContainerOptions};
use bollard::network::CreateNetworkOptions;
use bollard::Docker;
use uuid::Uuid;

const CONTAINER_NETWORK: &str = "infra_default";

/// Create a bollard Docker client via unix socket.
pub async fn connect(docker_url: &str) -> Result<Docker, Box<dyn std::error::Error + Send + Sync>> {
    let docker = Docker::connect_with_unix(docker_url, 120u64, &bollard::ClientVersion { major_version: 1, minor_version: 41 })?;
    Ok(docker)
}


/// Ensure an image is present locally.
pub async fn ensure_image(docker: &Docker, image: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if docker.inspect_image(image).await.is_ok() {
        return Ok(());
    }
    let opts = bollard::image::CreateImageOptions {
        from_image: image.to_string(),
        ..Default::default()
    };
    let mut stream = docker.create_image(Some(opts), None, None);
    use futures_util::stream::StreamExt;
    while let Some(result) = stream.next().await {
        let _ = result?;
    }
    Ok(())
}
/// Ensure infra_default network exists (idempotent).
pub async fn ensure_network(docker: &Docker) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use bollard::network::ListNetworksOptions;

    let opts = ListNetworksOptions::<String>::default();
    let networks = docker.list_networks(Some(opts)).await?;
    if !networks.iter().any(|n| {
        n.name.as_deref() == Some(CONTAINER_NETWORK) || n.id.as_deref() == Some(CONTAINER_NETWORK)
    }) {
        docker.create_network(CreateNetworkOptions {
            name: CONTAINER_NETWORK,
            driver: "bridge",
            ..Default::default()
        }).await?;
    }
    Ok(())
}

/// Spawn contestant-sample container. Returns container_id.
pub async fn spawn_contestant(
    docker: &Docker,
    image: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    ensure_network(docker).await?;
    let container_name = format!("contestant-{}", Uuid::new_v4().simple());

    let config = Config {
        image: Some(image.to_string()),
        env: Some(vec!["PORT_FIX=9090".to_string(), "PORT_WS=8080".to_string()]),
        ..Default::default()
    };

    let id = docker
        .create_container(Some(CreateContainerOptions { name: container_name, ..Default::default() }), config)
        .await?
        .id;

    docker.start_container(&id, None::<StartContainerOptions<String>>).await?;

    // Attach to infra_default network so containers can resolve each other by name
    docker.connect_network(CONTAINER_NETWORK, bollard::network::ConnectNetworkOptions {
        container: id.clone(),
        endpoint_config: Default::default(),
    }).await?;

    Ok(id)
}

/// Spawn bot-worker container. Returns container_id.
pub async fn spawn_bot(
    docker: &Docker,
    image: &str,
    contestant_id: &str,
    target_host: &str,
    rps: u32,
    duration_secs: u32,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    ensure_network(docker).await?;
    let container_name = format!("bot-{}-{}", contestant_id, Uuid::new_v4().simple());

    let cmd = vec![
        "--target-host".to_string(),
        target_host.to_string(),
        "--fix-port".to_string(),
        "9090".to_string(),
        "--ws-port".to_string(),
        "8080".to_string(),
        "--rps".to_string(),
        rps.to_string(),
        "--duration-secs".to_string(),
        duration_secs.to_string(),
        "--fix-connections".to_string(),
        "4".to_string(),
        "--ws-connections".to_string(),
        "4".to_string(),
        "--ramp-up-secs".to_string(),
        "2".to_string(),
        "--redpanda-brokers".to_string(),
        "redpanda:9092".to_string(),
        "--contestant-id".to_string(),
        contestant_id.to_string(),
    ];
    let config = Config {
        image: Some(image.to_string()),
        cmd: Some(cmd),
        host_config: Some(bollard::models::HostConfig {
            dns: Some(vec!["127.0.0.11".into()]),
            network_mode: Some("infra_default".into()),
            ..Default::default()
        }),
        ..Default::default()
    };

    let id = docker
        .create_container(Some(CreateContainerOptions { name: container_name, ..Default::default() }), config)
        .await?
        .id;

    docker.start_container(&id, None::<StartContainerOptions<String>>).await?;
    Ok(id)
}

/// Stop and remove a container.
pub async fn kill_container(docker: &Docker, id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = docker.stop_container(id, None).await;
    let _ = docker.remove_container(id, Some(bollard::container::RemoveContainerOptions {
        force: true,
        ..Default::default()
    })).await;
    Ok(())
}

/// Stop and remove all containers whose name starts with `prefix`.
/// Each failure is logged but iteration continues (best-effort cleanup).
/// Never panics. Never silently swallows an error.
pub async fn stop_containers_by_prefix(
    docker: &Docker,
    prefix: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let list_opts = ListContainersOptions::<String> {
        all: true,
        ..Default::default()
    };
    let containers = docker.list_containers(Some(list_opts)).await?;
    for c in containers {
        let names = match c.names {
            Some(ref names) => names.clone(),
            None => {
                tracing::warn!("[CLEANUP] container has no names field — cannot match prefix, skipping");
                continue;
            }
        };
        for name in &names {
            if !name.starts_with(&format!("/{prefix}")) {
                continue;
            }
            let id = match c.id.as_ref() {
                Some(id) => id.as_str(),
                None => {
                    tracing::warn!("[CLEANUP] container {name} matched prefix but has no id — skipping");
                    continue;
                }
            };
            tracing::info!("[CLEANUP] stopping container {id} ({name})");
            if let Err(e) = docker.stop_container(id, None::<StopContainerOptions>).await {
                tracing::warn!("[CLEANUP] stop_container {id} failed: {e}");
            }
            tracing::info!("[CLEANUP] removing container {id}");
            if let Err(e) = docker.remove_container(id, None).await {
                tracing::warn!("[CLEANUP] remove_container {id} failed: {e}");
            }
        }
    }
    Ok(())
}
