# Security notes

## Dependency policy

The framework keeps its attack surface small: the core (extract, transform,
validate, load, auth, API) uses only the Python standard library. The one
runtime dependency, `redis`, is optional and pinned in `requirements.txt`.

How vulnerable versions are found and fixed without breaking anything:

1. **Detect.** The `dependency-audit` CI job runs
   [`pip-audit`](https://pypi.org/project/pip-audit/) against the pinned
   requirements on every push, every pull request, and weekly on a schedule,
   so a newly published advisory fails the build even with no code change.
2. **Update.** Dependabot opens a pull request when a pinned package (or a
   GitHub Action) has a newer release.
3. **Verify.** That pull request must pass the full test suite, including the
   Redis integration tests against a real Redis, and the end-to-end sample
   run, before it is merged. A version bump that changes behaviour shows up
   as a failing test, not as a surprise in production.

To check locally:

```bash
pip install -r requirements-dev.txt
pip-audit -r requirements.txt
```

## Application security

| Area | Control |
| --- | --- |
| API authentication | JWT, HS256 pinned (the token's `alg` can't choose the algorithm), constant-time signature check, `exp` required, optional `iss`/`aud`, 32-byte minimum secret |
| Authorisation | Roles in the token: `etl:run` to trigger runs, `etl:read` to read run records |
| File access | The API only ingests files inside `ETL_INBOX_DIR`; paths are resolved and checked, so `../` and absolute paths are refused |
| SQL injection | Table and column names must be plain identifiers (validated when the config loads); all values use placeholders |
| Concurrency | One worker per pipeline via a Redis lock with a token; release is compare-and-delete, so an expired lock can't be released by its old holder |
| Request limits | JSON bodies capped at 16 KB |
| Secrets | Read from environment / `.env` (git-ignored); `.env.example` holds placeholders only |

## Reporting

Open a private security advisory on the repository rather than a public issue.
