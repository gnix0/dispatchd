# Contributing to dispatchd

`dispatchd` is maintained with small, focused pull requests and release-tagged changes. Contributions are expected to preserve the repository's technical documentation, validation flow, and operational evidence.

## Workflow

1. Branch from `main` using a focused name such as `feat/<topic>`, `fix/<topic>`, or `docs/<topic>`.
2. Keep each branch scoped to one behavior change or one documentation/governance update.
3. Open a pull request with a concise summary, validation notes, and any operational impact.
4. Merge only after required checks pass and review feedback is resolved.

## Before opening a pull request

Run the relevant checks locally:

```bash
make fmt-check
make test
make build
make lint
make proto-check
```

If your change touches deployment, observability, or release behavior, also run the applicable commands:

```bash
make compose-config
make k8s-render
make argocd-render
```

## Expectations by change type

- Code changes should include tests or a clear justification for why additional coverage is not needed.
- Contract changes under `proto/` must preserve backward compatibility unless the pull request explicitly documents a pre-`1.0.0` breaking change.
- Deployment changes under `deploy/` or `.github/workflows/` should include validation steps in the pull request body.
- Documentation changes should keep the README technical, descriptive, and evidence-based.
- Performance claims or SLI/SLO updates must only be made from measured evidence checked into the repository.

## Commit and pull request guidance

- Prefer commit messages in the form `type: summary`, for example:
  - `feat: add region-aware scheduler gating`
  - `fix: harden redis deployment security context`
  - `docs: update release policy for v0.2.0`
- Keep pull requests small enough that reviewers can reason about them end to end.
- Do not combine unrelated refactors with behavioral changes.

## Versioning and breaking changes

`dispatchd` follows Semantic Versioning for repository releases.

- Patch releases are for backward-compatible fixes.
- Minor releases are for backward-compatible capabilities and hardening milestones.
- Major releases are reserved for stable public API changes once the project reaches `1.0.0`.

Before `1.0.0`, breaking changes are allowed, but they must be called out explicitly in the pull request and release notes.

## Security-sensitive changes

- Never commit real credentials, long-lived tokens, or production certificates.
- Route suspected vulnerabilities through the process documented in `.github/SECURITY.md`.
- Changes to auth, TLS, secrets, or release workflows should include extra validation detail in the pull request.
