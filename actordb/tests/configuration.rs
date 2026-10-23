use actordb::configuration::ConfigurationProvider;
use actordb::node::NodeConfig;

#[tokio::test]
async fn node_config_is_an_async_static_provider() {
    let config = NodeConfig::builder().fragment_count(3).build();
    let resolved = config.resolve().await.unwrap();
    assert_eq!(resolved.fragment_count, 3);
}

#[cfg(feature = "toml")]
#[tokio::test]
async fn toml_provider_overlays_defaults_and_parses_members() {
    use actordb::configuration::TomlConfigProvider;
    use std::io::Write;

    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(
        file,
        "fragment_count = 4\nflush_interval_ms = 250\n\n[default_pool]\nworkers = 7\n\n[replication]\nbatch_size = 200\n\n[members]\n"
    )
    .unwrap();
    // TOML maps are represented as a table, not a nested NodeConfig section.
    file.as_file_mut()
        .write_all(b"node-b = \"127.0.0.1:7447\"\n")
        .unwrap();

    let config = TomlConfigProvider::from_path(file.path())
        .resolve()
        .await
        .unwrap();
    assert_eq!(config.fragment_count, 4);
    assert_eq!(config.flush_interval, std::time::Duration::from_millis(250));
    assert_eq!(config.default_pool.workers, 7);
    assert_eq!(config.replication.batch_size, 200);
    assert_eq!(config.members.len(), 1);
}
