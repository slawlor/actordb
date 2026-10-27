//! Regenerate committed bindings; --check reports drift without changing files.
fn main() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = root.join("proto");
    let destination = root.join("src/generated");
    let temporary = std::env::temp_dir().join(format!("actordb-proto-{}", std::process::id()));
    std::fs::create_dir_all(&temporary).unwrap();
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    let mut config = prost_build::Config::new();
    config.out_dir(&temporary);
    config.type_attribute(".meta.WriteConcernPolicy", "#[non_exhaustive]");
    config.type_attribute(".meta.WriteConcernResult", "#[non_exhaustive]");
    config.type_attribute(".meta.WriteOperationEvidence", "#[non_exhaustive]");
    config.type_attribute(".meta.WriteConcernEvidence", "#[non_exhaustive]");
    config.type_attribute(".meta.WriteAcknowledgement", "#[non_exhaustive]");
    config
        .compile_protos(&[source.join("meta.proto")], std::slice::from_ref(&source))
        .unwrap();
    prost_build::Config::new()
        .out_dir(&temporary)
        .extern_path(".meta", "crate::meta")
        .compile_protos(&[source.join("client.proto")], &[source])
        .unwrap();
    let check = std::env::args().any(|argument| argument == "--check");
    let mut changed = false;
    for (source, name) in [("meta.rs", "meta.rs"), ("actordb.rpc.v1.rs", "client.rs")] {
        let generated = std::fs::read(temporary.join(source)).unwrap();
        let target = destination.join(name);
        if std::fs::read(&target).ok().as_deref() != Some(&generated) {
            changed = true;
            if check {
                eprintln!("generated binding differs: {}", target.display());
            } else {
                std::fs::write(target, generated).unwrap();
            }
        }
    }
    std::fs::remove_dir_all(temporary).unwrap();
    if check && changed {
        std::process::exit(1);
    }
}
