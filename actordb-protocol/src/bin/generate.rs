//! Regenerate committed bindings; --check reports drift without changing files.
fn main() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = root.join("proto");
    let destination = root.join("src/generated");
    let temporary = std::env::temp_dir().join(format!("actordb-proto-{}", std::process::id()));
    std::fs::create_dir_all(&temporary).unwrap();
    std::env::set_var("PROTOC", protobuf_src::protoc());
    let mut config = prost_build::Config::new();
    config.out_dir(&temporary);
    config
        .compile_protos(
            &[
                source.join("meta.proto"),
                source.join("client.proto"),
                source.join("quorum.proto"),
            ],
            &[source],
        )
        .unwrap();
    let check = std::env::args().any(|argument| argument == "--check");
    let mut changed = false;
    for name in ["meta.rs", "client.rs", "quorum.rs"] {
        let generated = std::fs::read(temporary.join(name)).unwrap();
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
