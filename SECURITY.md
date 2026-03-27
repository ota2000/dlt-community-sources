# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| latest  | Yes       |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do NOT open a public GitHub issue.**

Instead, please use [GitHub Security Advisories](https://github.com/ota2000/dlt-community-sources/security/advisories/new) to report the vulnerability privately.

You will receive a response within 7 days. Once the issue is confirmed, a fix will be released as soon as possible.

## Scope

This project is a collection of API client wrappers. Security concerns typically involve:

- Credential handling (API keys, tokens)
- Data exposure through logging
- Dependency vulnerabilities

## Best Practices for Users

- Store API credentials in environment variables, not in code
- Use `.env` files locally and add them to `.gitignore`
- Keep dependencies up to date
