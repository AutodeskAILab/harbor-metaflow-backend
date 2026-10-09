# Contributing

Thanks for your interest in improving harbor-metaflow-backend.

## Development setup

You need Python 3.12+ and a Harbor with the trial backend hook.

```bash
python -m venv .venv && . .venv/bin/activate
pip install "harbor @ git+https://github.com/AutodeskAILab/harbor@feat/backend-plugins"
pip install -e ".[s3,dev]"
```

With [uv](https://docs.astral.sh/uv/) and a local Harbor checkout:

```bash
uv venv .venv
uv pip install --python .venv/bin/python -e ../harbor -e ".[s3,dev]"
```

## Checks

Run these before opening a pull request; CI runs the same:

```bash
ruff check .
ruff format --check .
python -m pytest -q
```

The tests need no AWS account, Docker or Metaflow deployment: `tests/conftest.py`
replaces the flow with an in-process launcher that runs the real worker against a
local-directory store, and replaces Harbor's `Trial` with a fake that writes a
standard trial directory. When you change behaviour, add or update a test there.

## Pull requests

- Keep changes focused, and describe the problem and the fix in the PR.
- Update `README.md` when you add or change an option, and add an entry under
  "Unreleased" in `CHANGELOG.md`.
- Do not commit credentials, account ids, bucket or queue names, or other
  deployment details. Examples should use placeholders such as `<bucket>`. Before
  pushing, check the tree with:

  ```bash
  git grep -nIE '[0-9]{12}|arn:aws:|dkr\.ecr\.|s3://[a-z0-9]|amazonaws\.com' \
      -- . ':!CONTRIBUTING.md' | grep -vE '123456789012|s3://(bucket|example-bucket|b)\b'
  ```

  It should print nothing (`123456789012` is AWS's documentation placeholder, and
  `s3://bucket`, `s3://example-bucket` and `s3://b` are the tests' placeholders).
- By contributing, you agree that your contributions are licensed under the
  Apache License, Version 2.0.

## Reporting issues

Open a GitHub issue with the Harbor and Metaflow versions, the `harbor run`
command (with placeholders for private values) and the relevant part of the
`metaflow-<token>.log` file written to the job directory. Please report security
issues privately through GitHub's "Report a vulnerability" on the repository's
Security tab rather than in a public issue.
