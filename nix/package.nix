{ ... }:
{
  perSystem =
    {
      pkgs,
      toolchains,
      ...
    }:
    let
      craneLib = toolchains.nightly;
      workspaceSpecs = import ./workspaces.nix { inherit pkgs; };
      repoRoot = ../.;
      src = pkgs.lib.cleanSourceWith {
        src = repoRoot;
        name = "source";
        filter = path: type: craneLib.filterCargoSources path type || pkgs.lib.hasSuffix ".capnp" path;
      };

      mkWorkspace =
        name: spec:
        assert spec ? manifestPath;
        assert spec ? packages;
        assert builtins.isAttrs spec.packages;
        let
          manifest = repoRoot + "/${spec.manifestPath}";
          workspaceDir = builtins.dirOf spec.manifestPath;
          cargoManifestPath = builtins.baseNameOf spec.manifestPath;
          lockfile = repoRoot + "/${workspaceDir}/Cargo.lock";
          manifestArgs = "--locked --manifest-path ${pkgs.lib.escapeShellArg cargoManifestPath}";
          commonArgs = {
            inherit src;
            buildInputs = spec.buildInputs or [ ];
            nativeBuildInputs = spec.nativeBuildInputs or [ ];
            cargoToml = manifest;
            pname = name;
            strictDeps = true;
            version = "0.0.0";
            # Checks act on every member, not only on Cargo's default members;
            # packages narrow this down again with --package.
            cargoExtraArgs = "${manifestArgs} --workspace";
            cargoVendorDir = craneLib.vendorCargoDeps { cargoLock = lockfile; };
            postUnpack = ''
              cd "$sourceRoot/${workspaceDir}"
              sourceRoot=.
            '';
          };
          # Dependency-only artifacts for every toolchain and profile, built
          # once here so that packages and checks share them.
          cargoArtifacts = builtins.mapAttrs (
            _: toolchain:
            pkgs.lib.genAttrs [ "dev" "release" ] (
              profile:
              toolchain.buildDepsOnly (
                commonArgs
                // {
                  CARGO_PROFILE = profile;
                  # crane's dummy source only carries the repository-root
                  # Cargo.lock; the replaceCargoLock hook installs this one into
                  # the workspace directory before cargo resolves anything.
                  cargoLock = lockfile;
                }
              )
            )
          ) toolchains;
          cargoArtifactsRelease = cargoArtifacts.nightly.release;
          cargoArtifactsDev = cargoArtifacts.nightly.dev;
          mkPackage =
            outputName: packageSpec:
            let
              cargoPackage = packageSpec.cargoPackage or outputName;
              extraArgs = packageSpec.cargoExtraArgs or "";
            in
            craneLib.buildPackage (
              commonArgs
              // {
                pname = outputName;
                cargoArtifacts = cargoArtifactsRelease;
                # The tests-* checks run the test suite.
                doCheck = false;
                cargoExtraArgs = "${manifestArgs} --package ${pkgs.lib.escapeShellArg cargoPackage} ${extraArgs}";
              }
            );
        in
        assert builtins.pathExists manifest;
        assert builtins.pathExists lockfile;
        {
          inherit
            cargoManifestPath
            cargoArtifacts
            cargoArtifactsDev
            cargoArtifactsRelease
            commonArgs
            ;
          manifestPath = spec.manifestPath;
          packages = builtins.mapAttrs mkPackage spec.packages;
        };

      cargoWorkspaces = builtins.mapAttrs mkWorkspace workspaceSpecs;
    in
    {
      _module.args = { inherit cargoWorkspaces; };
      packages = pkgs.lib.concatMapAttrs (_: workspace: workspace.packages) cargoWorkspaces;
    };
}
