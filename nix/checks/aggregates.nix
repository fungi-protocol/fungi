{
  perSystem =
    {
      cargoWorkspaces,
      config,
      pkgs,
      ...
    }:
    let
      registry = config.workspaceChecks;
      entries =
        workspaceName: checks:
        pkgs.lib.mapAttrsToList (name: check: {
          name = "${workspaceName}-${name}";
          path = check.package;
        }) checks;
      tagged = tag: pkgs.lib.filterAttrs (_: check: builtins.elem tag check.tags);
      # Every workspace must register the named check; a missing name (typo or
      # rename) fails evaluation instead of silently dropping the workspace.
      selectNamed =
        checkName:
        pkgs.lib.mapAttrsToList (
          workspaceName: checks:
          let
            check =
              checks.${checkName}
                or (throw "aggregates.nix: workspace '${workspaceName}' has no check named '${checkName}'; known checks: ${toString (builtins.attrNames checks)}");
          in
          {
            name = "${workspaceName}-${checkName}";
            path = check.package;
          }
        ) registry;
      selectTagged =
        tag:
        pkgs.lib.concatLists (
          pkgs.lib.mapAttrsToList (workspaceName: checks: entries workspaceName (tagged tag checks)) registry
        );
      join = name: paths: pkgs.linkFarm name paths;
      # One repository-wide report for publishing. The per-workspace coverage
      # checks enforce the threshold; this merges their native lcov output
      # with repository-relative paths and converts it to Cobertura for
      # GitHub code coverage.
      mergeCoverage =
        reports:
        pkgs.runCommand "coverage"
          {
            nativeBuildInputs = [
              pkgs.lcov
              pkgs.python3Packages.lcov-cobertura
            ];
            reports = pkgs.lib.mapAttrsToList (
              workspaceName: report:
              "${builtins.dirOf cargoWorkspaces.${workspaceName}.manifestPath}=${report.package}"
            ) reports;
          }
          ''
            bash ${./coverage/merge.sh} "$out" $reports
          '';
    in
    {
      checks =
        pkgs.lib.attrsets.unionOfDisjoint
          {
            tests = join "tests" (selectNamed "tests-nightly-dev" ++ selectNamed "doctests-nightly-dev");
            clippy = join "clippy" (selectNamed "clippy");
            coverage = mergeCoverage (pkgs.lib.mapAttrs (_: checks: checks.coverage) registry);
            quick = join "quick" (selectTagged "quick");
            lint = join "lint" (
              [
                {
                  name = "no-todo-comments";
                  path = config.checks.no-todo-comments;
                }
              ]
              ++ selectTagged "lint"
            );
            nightly = join "nightly" (selectTagged "nightly");
          }
          (
            pkgs.lib.foldlAttrs (
              aliases: workspaceName: checks:
              pkgs.lib.attrsets.unionOfDisjoint aliases {
                ${workspaceName} = join workspaceName (entries workspaceName checks);
                "${workspaceName}-quick" = join "${workspaceName}-quick" (
                  entries workspaceName (tagged "quick" checks)
                );
              }
            ) { } registry
          );
    };
}
