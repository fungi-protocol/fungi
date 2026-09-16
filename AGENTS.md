# Notes for coding agents

[CONTRIBUTING.md](CONTRIBUTING.md) applies. In addition:

- Prefer jj. Plan by change id, build by commit hash.
- Keep descriptions up to date. Tag subject of [WIP] changes, and track
  outstanding tasks in a checklist in the change description.
- Change sequences should make sense a `git send-email` patch series, as if
  they are intended to be submitted to a mailing list such as lkml (but avoid
  trailer boilerplate).
- `nix flake check` builds everything. It's OK for checks to fail while
  iterating (WIP), but eventually every commit should pass every check.
- `nix build --no-link ".#checks.${system}.${check}"` can be used to run checks
  individually.
- Specify `?rev=` as in `nix build ".?rev=$commit_hash#checks..."` to
  authoritatively check a particular commit. This can be run in the background,
  especially when expected to pass, without interfering with the working copy,
  never gets invalidated, and doesn't pollute the local directory, so is
  usually preferable over devshell usage.
- Aggregate checks include `quick`, `lint`, `tests`, `coverage` and `nightly`;
  see `nix flake show`.
