# Each entry owns one Cargo.lock. Its packages become separate Nix outputs.
# Builds use the repository root as their source so nested workspaces may still
# depend on shared crates by path.
{ ... }:
{
  fungi = {
    manifestPath = "Cargo.toml";
    packages = {
      cost-function = { };
      fungi-transport = { };
    };
  };
}
