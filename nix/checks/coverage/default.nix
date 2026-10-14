{
  perSystem =
    {
      cargoWorkspaces,
      pkgs,
      toolchains,
      ...
    }:
    let
      workspace = import ../workspace.nix { inherit cargoWorkspaces pkgs; };
    in
    {
      workspaceChecks =
        workspace.mapWorkspaces
          {
            name = "coverage";
            tags = [ "nightly" ];
          }
          (
            workspaceName: cargoWorkspace:
            let
              collection = toolchains.nightly.mkCargoDerivation (
                (workspace.checkArgs cargoWorkspace)
                // {
                  cargoArtifacts = cargoWorkspace.cargoArtifactsDev;
                  pnameSuffix = "-coverage-collect";
                  nativeBuildInputs =
                    cargoWorkspace.commonArgs.nativeBuildInputs
                    ++ (with pkgs; [
                      cargo-llvm-cov
                      cargo-nextest
                    ]);
                  buildPhaseCargoCommand = "bash ${./collect.sh} $out ${pkgs.lib.escapeShellArg cargoWorkspace.cargoManifestPath}";
                  installPhase = "true";
                }
              );
            in
            pkgs.runCommand "${workspaceName}-coverage" { nativeBuildInputs = [ pkgs.lcov ]; } ''
              bash ${./gate.sh} "$out" 100 ${collection}/coverage.lcov
            ''
          );
    };
}
