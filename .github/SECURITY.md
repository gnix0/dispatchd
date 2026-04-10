# Security Policy

## Supported Versions

Security fixes are applied to the latest released `0.x` line on a best-effort basis until `1.0.0` is cut. Earlier pre-release states and untagged historical branches should not be considered supported.

| Version | Supported |
| ------- | --------- |
| Latest `0.x` release | Yes |
| Older `0.x` releases | Best effort |
| Unreleased branches | No |

## Reporting a Vulnerability

Do not open a public GitHub issue for a suspected vulnerability.

Report security concerns privately through GitHub Security Advisories or direct maintainer contact. Include:

- affected component or workflow
- impact and reproduction details
- logs, screenshots, or traces if they help explain the issue
- whether credentials, tokens, or secrets were involved

## Response Expectations

- Initial triage target: within 3 business days
- Severity assessment: after reproduction or sufficient evidence
- Fix coordination: based on impact, exploitability, and whether a safe mitigation exists

## Disclosure Guidance

- Prefer coordinated disclosure.
- Avoid publishing working exploit details before a fix or mitigation is available.
- If a release is required, ship the fix under a new version tag rather than modifying an existing release.
