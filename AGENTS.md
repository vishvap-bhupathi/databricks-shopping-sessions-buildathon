# Shopping Sessions working agreements

Use `rules.md` as the source of truth for pipeline behavior. Preserve the supplied source, Dockerfiles, scripts, and fixtures. Keep generated datasets, downloaded development answers, benchmark artifacts, and credentials local.

## Git workflow

- Inspect the working tree before edits and preserve unrelated changes. Review the exact files and contents intended for each commit using the checklist in [docs/git-workflow.md](docs/git-workflow.md).
- Make small, descriptive commits. For substantial optimizations, use a feature branch from a proven baseline. A known-correct baseline requires an exact match on both tables for all 36 fixtures and the public development set. Record the baseline commit SHA and verification evidence before optimizing.
- Review correctness on the optimization branch against that baseline before merging. Keep external publishing, GitHub repository creation, pushes, and Buildathon submissions pending until the user explicitly approves them.

## Project journal

Document every meaningful implementation, debugging, optimization, and benchmark task through Notion MCP. Read the canonical page reference from the Git-ignored `.env.notion.local` file using `NOTION_PROJECT_PAGE_URL` or `NOTION_PROJECT_PAGE_ID`. Update the existing page, verify the write, and keep the private reference out of staged files. If the local reference is missing, use the pending-update workflow below.

Include the following in each update, or explicitly mark a field as inapplicable or unverified.

- Objective, scope, and status.
- Implementation details and relevant files.
- Technical decisions, alternatives considered, and reasons for the choice.
- Tests actually run, commands, and observed results. Mark skipped tests as unexecuted.
- Actual measured runtimes with dataset size and seed, hardware or container limits, command, and measurement scope. Mark runtime as unmeasured when no measurement was taken.
- Problems encountered, fixes or remaining blockers, and lessons learned.
- Relevant Git commit hashes. Mark uncommitted work as such and add the SHA once a commit exists.

Label completed work, planned work, and unverified assumptions clearly. Identify published expected answers as reference data. Never invent measurements, tests, implementation outcomes, or submission results. Keep secrets and private data out of the journal.

If Notion MCP is unavailable or an update fails, record the entry and sync blocker in `docs/notion-sync-pending.md`. When access returns, check the canonical page for existing entries, sync the pending work, verify the update, and mark each synced entry with its date and page link.
