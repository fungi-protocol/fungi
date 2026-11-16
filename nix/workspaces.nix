# Each entry owns one Cargo.lock. Its packages become separate Nix outputs.
# Builds use the repository root as their source so nested workspaces may still
# depend on shared crates by path.
{ ... }:
{
  fungi = {
    manifestPath = "Cargo.toml";
    # Fetch the pinned commit directly even when its development branch moves.
    outputHashes = {
      "git+https://github.com/fungi-protocol/concurrent-psbt/?rev=7bf09f4e6b89b38a14504a0a67f03a7c93d590ca#7bf09f4e6b89b38a14504a0a67f03a7c93d590ca" =
        "sha256-eDni68zsj9ScTsYE+NXG7vX5Ez9bjBTvV7wO9te44RE=";
    };
    packages = {
      cost-function = { };
      fungi-transport = { };
      fungi-transport-tor-socks = { };
    };
  };
}
