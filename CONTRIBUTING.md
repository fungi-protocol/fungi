# Contributing

## Code

- All code is a liability, some code is an asset. Tech debt is not a thing that
  happens on its own, some kind of inevitability, but a tool, a deliberate
  decision to borrow time from the future for leverage.
- Prioritize readability. Use whitespace liberally to group lines of code
  together.
- Avoid excessive comments. A good function name is better than a comment
  explaining a messy passage. Reserve comments for nuance: explaining
  non-obvious things that the code does, or how it came to be that particular
  way when that is of significance.
- Prefer small files. Small rust modules, small nix flake-parts. Front load the
  main entry point(s) of the public API (including `pub(crate)` and
  `pub(super)`). That should fit in a few screenfuls of text, with private
  functions supporting that organized below.
- Name types for qualified imports (prefer short struct names rather than
  including the namespace in the type name).
- Prefer pure functions. Imperative code is needed to manage state, but all
  computations on that state, all logic should be pure functions unless cost
  prohibitive.
- Aim to make invalid states unrepresentable. Try to find symmetry to avoid
  combinatorial explosions. For example, an error type and the corresponding
  `Result` wrapping can often be avoided by structuring the arguments to avoid
  partiality.
- Use property tests, which tend to work especially well when the previous
  guideline is adhered to.
- Distinguish primary information from secondary (derived) information. Primary
  information should generally be retained in a minimal, append only manner.
  Derived information should be easily recomputable from the primary
  information, and make the access patterns needed for the data convenient and
  efficient.
- Format with `nix fmt`

## Commits

[jj](https://jj-vcs.github.io) encouraged.

- Avoid `feat(crap)` boilerplate. Subject should be <= 50 chars and
  descriptive, trying to decide if something is a feat or not wastes 12% of the
  subject line on noise. Prefixing with say `nix:` or the specific component
  being changed can help clarity so that is not discouraged.
- One logical change per commit. Think of the repository as a living document
  describing a shared understanding of a problem and a solution, which is
  continually refined and updated.
  - Optimize readability for efficient commit-by-commit review.
  - Avoid noise, preserve provenance and causal dependency structure, how this
    understanding evolved and what motivated its change (as opposed to an
    immutable log of every typo or brain fart).
- The full commit message should explain why a change is happening in relation
  to the parent commit(s), and why it is happening in that way.
  - When a change is non-obvious, and the seemingly obvious way turned out to
    be wrong, it's good to mention that here if there is no single obvious
    place as a comment in the code.
  - Any relevant supporting materials should be likewise linked or cited in the
    commit message if there is no logical place in the code.
- If a change is complex, the commit message should summarize what's
  being changed, but readable diffs should be preferred when possible.
- Every commit should pass `nix flake check`
  - `nix run .#validate-commits` to run locally
  - CI enforces this in pull requests

## Pull Requests

- Every PR should be thought of as a series of logical commits with a cover
  letter. The PR body should explain why those commits are being proposed, and
  the messages of the commits should explain each step in the sequence clearly.
- Address review by amending and force-pushing, or by pushing fixup commits to
  be squashed before merging.

## AI-assisted contributions

- LLM written code is allowed, but the submitter is responsible for every line, and
  must review locally first. Avoid claudish doc comments in particular.
- LLM use in replies to any comments or discussions should be disclosed.
