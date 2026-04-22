---
id: "94c44708-89de-435d-acbb-75f63ae085ec"
schedule: "0 * * * *"
pre_check: "gh auth status"
allowed_tools: [Bash,Read,Write,Edit,Glob,Grep]
max_concurrent: 1
skip_permissions: true
---

Triage open GitHub issues in the dreamware-nz/loveliness repo.

## Workflow

1. **Check GitHub authentication:**
   - Run `gh auth status` to verify auth.
   - If not authenticated, check for `GH_TOKEN` env var and use it.
   - If no auth available, exit cleanly with a clear error message.

2. **Install OpenSpec if needed:**
   - Check if `openspec` is available (`which openspec`).
   - If not, try installing via:
     - `npm install -g openspec` (if Node/npm available)
     - `go install github.com/openspec-dev/openspec/cmd/openspec@latest` (if Go available)
     - `curl -fsSL https://openspec.dev/install.sh | sh`
   - If installation fails, manually generate structured specs in the OpenSpec format.

3. **Fetch untriaged issues:**
   - Run: `gh issue list --repo dreamware-nz/loveliness --state open --json number,title,author,body,labels`
   - Identify issues that are OPEN and do NOT have the `triaged` label.
   - Filter to issues authored by johnjansen (the repo owner).

4. **For each untriaged issue:**
   - Read the issue title and body carefully.
   - Use `openspec` to generate an actionable specification based on the issue content.
   - The spec must include:
     - Clear goal / problem statement
     - Acceptance criteria (bullet list)
     - Technical approach / implementation notes
     - Edge cases to consider
   - Update the issue body by appending the generated spec. Preserve the original content and add a separator `---\n\n## Actionable Spec` followed by the spec.
   - Add the `triaged` label: `gh issue edit {number} --add-label triaged`

5. **Report:**
   - Output a summary of which issues were triaged and which were skipped.
   - If no issues needed triaging, report that clearly.

## Important
- Do NOT modify issues that already have the `triaged` label.
- Do NOT create new issues or PRs.
- Preserve all original issue content when appending the spec.
- If `openspec` is unavailable, generate a well-structured spec manually.
- If rate limits or auth errors occur, report and exit cleanly.
