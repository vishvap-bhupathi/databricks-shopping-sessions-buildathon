# Git and GitHub workflow

Use `main` for the validated pipeline. Until a baseline passes the full correctness checks, describe it as work in progress. For substantial work, branch from the verified baseline, for example `optimize/assist-intervals`. Record the baseline SHA and its test evidence in the project journal so future benchmark runs can reproduce it.

## Before each commit

1. Review `git status --short` and `git ls-files`. Confirm generated datasets, dev answers, archives, and credentials stay local. The starter fixtures and their expected CSVs are source-controlled test material.
2. Stage specific paths with `git add -- <paths>`, then inspect `git diff --cached --stat`, `git diff --cached --check`, and `git diff --cached`. Check staged file sizes and review the diff for API keys, tokens, credentials, private URLs, and data. If a secret scanner is installed, run it against the staged changes as an additional check.
3. Run checks appropriate to the change and record the observed results. Before labeling a pipeline revision known-correct, require all 36 fixtures and both exact public dev table comparisons. Store the SHA and results in the canonical Notion journal.
4. Commit with a descriptive message, such as `feat: resolve linked shopper IDs` or `perf: merge campaign assist intervals`. Keep changes with different purposes in separate commits when practical.

## Before opening a pull request

Compare the branch to its baseline, verify both output tables again, and report performance only from comparable runs. Include the dataset seed and size, platform, resource limits, and timing scope. Record the baseline and comparison commit SHAs.

Ask for explicit approval before creating a GitHub repository, pushing any branch or tag, opening a GitHub pull request, publishing private files, or submitting a commit to the Buildathon. Before any approved publication, review all commits and refs in scope for secrets and large artifacts. An ignore rule applies to untracked files and does not remove content already committed.
