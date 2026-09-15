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
      mkTest =
        cargoWorkspace: craneLib: profile: cargoArtifacts:
        craneLib.cargoNextest (
          (workspace.checkArgs cargoWorkspace)
          // {
            inherit cargoArtifacts;
            CARGO_PROFILE = profile;
            cargoNextestExtraArgs = "--user-config-file ${./nextest-record.toml}";
            nativeBuildInputs = cargoWorkspace.commonArgs.nativeBuildInputs ++ [ pkgs.unzip ];
            preCheck = ''
              export NEXTEST_STATE_DIR="$TMPDIR/nextest-state"
              mkdir -p "$NEXTEST_STATE_DIR"
            '';
            postCheck = ''
              cargo nextest store export \
                --no-pager \
                --user-config-file ${./nextest-record.toml} \
                --archive-file "$out/nextest-run.zip" \
                latest
              unzip -tqq "$out/nextest-run.zip"
            '';
          }
        );
      # nextest does not run doctests; cargo test --doc does.
      mkDocTest =
        cargoWorkspace: craneLib: profile: cargoArtifacts:
        craneLib.cargoDocTest (
          (workspace.checkArgs cargoWorkspace)
          // {
            inherit cargoArtifacts;
            CARGO_PROFILE = profile;
          }
        );
    in
    {
      workspaceChecks = pkgs.lib.mapAttrs (
        _: cargoWorkspace:
        pkgs.lib.concatMapAttrs (
          toolchainName: craneLib:
          pkgs.lib.concatMapAttrs (
            profile: cargoArtifacts:
            let
              tags = [
                "nightly"
              ]
              ++ pkgs.lib.optional (toolchainName == "nightly" && profile == "dev") "quick";
            in
            {
              "tests-${toolchainName}-${profile}" = {
                inherit tags;
                package = mkTest cargoWorkspace craneLib profile cargoArtifacts;
              };
              "doctests-${toolchainName}-${profile}" = {
                inherit tags;
                package = mkDocTest cargoWorkspace craneLib profile cargoArtifacts;
              };
            }
          ) cargoWorkspace.cargoArtifacts.${toolchainName}
        ) toolchains
      ) cargoWorkspaces;
    };
}
