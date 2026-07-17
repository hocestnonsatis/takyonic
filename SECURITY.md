# Security policy

## Supported versions

Takyonic is currently pre-production software. Security fixes are provided for
the latest release and the `main` branch.

| Version | Supported |
| ------- | --------- |
| 1.x     | Yes       |
| < 1.0   | No        |

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability.

Use [GitHub private vulnerability reporting][report] to send a confidential
report to the maintainers. Include:

- the affected version or commit;
- a clear description of the impact;
- reproduction steps or a proof of concept;
- any known mitigations; and
- whether the issue has been disclosed elsewhere.

You should receive an acknowledgement within seven days. The maintainers will
investigate, coordinate a fix and release, and credit the reporter unless
anonymity is requested. Please allow reasonable time for remediation before
public disclosure.

The demo PostgreSQL endpoint accepts arbitrary credentials and is not intended
to be exposed directly to untrusted networks. This documented limitation alone
is not considered a vulnerability.

[report]: https://github.com/hocestnonsatis/takyonic/security/advisories/new
