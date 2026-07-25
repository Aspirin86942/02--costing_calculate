# GitHub `main` Protection — 2026-07-25

Repository: `Aspirin86942/02--costing_calculate`

The repository owner explicitly authorized the remote protection change. The
GitHub branch-protection API returned and a separate read-back confirmed:

| Setting | Confirmed value |
| --- | --- |
| Strict required status checks | `true` |
| Required check | `Public gates (windows-latest)` |
| Required check | `Public gates (ubuntu-latest)` |
| Dismiss stale reviews | `true` |
| Require CODEOWNERS review | `true` |
| Required approving-review count | `0` |
| Require resolved conversations | `true` |
| Require linear history | `true` |
| Allow force pushes | `false` |
| Allow branch deletion | `false` |
| Enforce for administrators | `false` |

The check names match the two matrix jobs in `.github/workflows/ci.yml`.
Labeling/unlabeling a PR or editing its body retriggers CI, so the frozen
baseline policy can converge after the author supplies the required
`contract-change` evidence.

## Single-maintainer limitation

At configuration time the repository had exactly one collaborator, the
administrator and CODEOWNER `@Aspirin86942`. GitHub does not allow a PR author
to approve their own PR. Requiring one approval while enforcing the rule
against that sole administrator would therefore deadlock every owner-authored
change.

The protection intentionally uses zero general approvals, requires CODEOWNERS
review for matched baseline paths, and leaves administrator enforcement off.
This preserves an emergency path for the sole maintainer. Full two-person
enforcement requires adding a second trusted maintainer; no collaborator was
invented or added by this implementation.
