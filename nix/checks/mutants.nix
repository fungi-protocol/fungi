{
  perSystem =
    {
      cargoWorkspaces,
      pkgs,
      toolchains,
      ...
    }:
    let
      workspace = import ./workspace.nix { inherit cargoWorkspaces pkgs; };
    in
    {
      workspaceChecks =
        workspace.mapWorkspaces
          {
            name = "mutants";
            tags = [ ];
          }
          (
            _: cargoWorkspace:
            toolchains.nightly.mkCargoDerivation (
              (workspace.checkArgs cargoWorkspace)
              // {
                cargoArtifacts = cargoWorkspace.cargoArtifactsDev;
                CARGO_PROFILE = "dev";
                pnameSuffix = "-mutants";
                nativeBuildInputs =
                  cargoWorkspace.commonArgs.nativeBuildInputs
                  ++ (with pkgs; [
                    cargo-mutants
                    cargo-nextest
                  ]);
                buildPhaseCargoCommand = "cargo mutants --manifest-path ${pkgs.lib.escapeShellArg "./${cargoWorkspace.cargoManifestPath}"} --workspace --all-features --in-place --test-tool nextest";
                installPhase = "mkdir -p $out";
              }
            )
          );
    };
}
