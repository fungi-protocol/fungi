//! Generate Rust bindings for the RPC schema.

fn main() {
    println!("cargo:rerun-if-changed=channel.capnp");
    capnpc::CompilerCommand::new()
        .file("channel.capnp")
        .run()
        .expect("compile channel.capnp: install capnp and put it on PATH");
}
