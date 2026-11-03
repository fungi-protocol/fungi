# Reviewing

For reviewing in general, see
[Reviewing code](https://bitcoincore.academy/reviewing.html).

## Pull requests

- Reviews are welcome on any pull request.
- Review commit by commit: each one is a single logical change, and its
  message says why.
- Prefer reviewing a stack bottom up. Each pull request shows only its own
  commits and assumes the ones below.

## Responding to review

- Address review by amending (`jj edit`) and force-pushing (or just `jj git
  push`). Range-diffs are posted in the comments, so pushing fixup commits to
  be squashed before merging is not necessary in order for reviewers to keep
  track of what has changed.

## oakagent

- `oakagent` automatically reviews pull requests.
- Its comments are suggestions. The pull request's author decides whether to
  take them.
- Address a comment by changing the line or resolving the thread.
- A 👍 or 👎 tells it whether the comment was useful.
- It ACKs a commit once every open point is fixed or answered and the head has
  been quiet for 12 hours. The ACK is a comment, not a GitHub approval or a
  human review.
- Mention `@oakagent` to give feedback on the reviewer.
