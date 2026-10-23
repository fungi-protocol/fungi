# Contributing

## Code

- All code is a liability, some code is an asset. Tech debt is not a thing that
  happens on its own, some kind of inevitability; it should be a deliberate
  decision to borrow time from the future for leverage.
- Prioritize readability. Use whitespace liberally to group lines of code
  together.
- Avoid excessive comments. A good function name is better than a comment
  explaining a messy passage. Reserve comments for nuance: explaining
  non-obvious things that the code does, or how it came to be that particular
  way when that is of significance.
- Prefer small files. Small Rust modules, small nix flake-parts. Front-load the
  main entry point(s) of the public API (including `pub(crate)` and
  `pub(super)`). That should fit in a few screenfuls of text, with private
  functions supporting that organized below.
- Name types for qualified imports (prefer short struct names rather than
  including the namespace in the type name, e.g. `psbt::Input`, not
  `PsbtInput`).
- Prefer pure functions. Imperative code is needed to manage state, but
  computations and logic on that state should be pure functions unless cost
  prohibitive.
- Aim to make invalid states unrepresentable. Try to find symmetry to avoid
  combinatorial explosions. For example, an error type and the corresponding
  `Result` wrapping can often be avoided by structuring the arguments to avoid
  partiality.
- Use property tests, which tend to work especially well when the previous
  guideline is adhered to.
- Distinguish primary information from secondary (derived) information. Primary
  information should generally be retained in a minimal, append-only manner.
  Derived information should be easily recomputable from the primary
  information, and make the access patterns needed for the data convenient and
  efficient.

## Commits

- Using [jj](https://jj-vcs.github.io) is encouraged.
- Avoid `feat(crap)` boilerplate. Subject should be <= 50 chars and
  descriptive. Trying to decide if something is a feat or not is a distraction,
  and `feat(crap): ` wastes 12% of the subject line on noise compared to
  `crap: `. Prefixing with, say, `nix:` or the specific component being changed
  can help clarity, so that is not discouraged.
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
  - If a change is complex, the commit message should summarize what's being
    changed, but readable diffs should be preferred when possible.
- Every commit should pass `nix flake check`.
  - `nix run .#validate-commits` to run locally.
  - CI enforces this in pull requests.
- Format with `nix fmt`.

## Pull requests

- Every PR should be thought of as a progressively refined series of commits.
  The PR body is a kind of cover letter, and will be included in the merge
  commit. The messages of the commits should explain each step in the sequence
  clearly.
- Commits record a discourse about how the understanding of the project evolves
  over time. Pull requests are therefore a meta-discourse, about whether or not
  the commits convey their own purpose clearly and correctly, getting from
  draft commits to publishable ones.
- See [REVIEWING.md](REVIEWING.md) for how review works.

## AI-assisted contributions

- LLM-assisted and LLM-written code is welcome. The human submitting it
  takes responsibility for and should be able to justify every line of change.
- Be particularly mindful of slop doc comments.
- A `Co-authored-by` crediting an LLM should be added if it was essential, i.e.
  the human submitting it does not feel that they would have authored
  something equivalent. It should be used as a signal to reviewers, indicating
  that the changes appear correct and justified to the submitter, but that they
  relied on an LLM not just for execution but also for some of the
  decision-making, and details should be provided in the pull request.
- In such cases it will be more valuable to reviewers to know how an agent was
  prompted, what approach was taken to reviewing its outputs, etc.; that
  context is more useful than particulars like which model was used.
- If the submitter takes full responsibility for the entirety of a pull
  request, and LLM usage was more as a tool to make the edits, then crediting
  the LLM just adds noise.
- Reliance on LLMs in replies to any comments or discussions should be
  disclosed.
