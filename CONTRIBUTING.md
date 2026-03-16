# Contributing to SprkLogs

Thanks for your interest in contributing to SprkLogs. This guide helps you get
started quickly and keep contributions aligned with the repository workflow.

## Getting Started

1. Fork the repository.
2. Clone your fork:

```bash
git clone https://github.com/<your-user>/sprklogs.git
cd sprklogs
```

3. Install monorepo dependencies:

```bash
npm install
```

4. Install backend dependencies:

```bash
pip install -r backend/requirements.txt
```

5. Optional: validate your setup:

```bash
npm run lint
npm run test
```

## Development Workflow

1. Create a branch from `main`:

```bash
git checkout -b feature/your-feature
```

2. Run the project in development mode:

```bash
# terminal 1: monorepo tasks
npm run dev

# terminal 2: backend API
python -m backend.app

# optional terminal 3: desktop shell
cd apps/desktop
npm start
```

3. If your change touches desktop packaging, test local desktop build:

```bash
npm run build:desktop:local
```

4. Run checks before opening a PR:

```bash
# from repository root
npm run lint
npm run test
```

5. Commit with a clear, descriptive message and open a Pull Request.

## Commit Messages

Use clear, descriptive commit messages, preferably following Conventional
Commits:

- `feat:` for new functionality
- `fix:` for bug fixes
- `docs:` for documentation changes
- `refactor:` for internal code changes without behavior change
- `test:` for test-only changes
- `chore:` for maintenance tasks

Examples:

- `feat: add stage filter to diagnosis table`
- `fix: prevent crash when zip has missing event log`
- `docs: clarify desktop local build steps`

## Running Tests

Run all workspace checks:

```bash
npm run lint
npm run test
```

Run backend tests directly:

```bash
python -m pytest backend/tests
```

Run desktop package integrity check:

```bash
cd apps/desktop
npm run check
```

## Pull Request Guidelines

- Keep each PR focused on one feature or fix.
- Include a concise description of what changed and why.
- Link related issues when applicable.
- If UI behavior changed, add screenshots or a short GIF.
- If API behavior changed, document request/response impact.
- Update docs when changing setup, workflows, or user-facing behavior.
- Ensure CI-relevant checks pass locally before opening the PR.

## Project Structure

```text
apps/desktop/     Electron shell and packaging
apps/web/         Static web workspace assets
backend/          FastAPI services and Spark log processing
infra/            Build scripts and Docker-related files
packages/         Shared monorepo packages (ui, ipc-types)
```

## Reporting Issues

When opening an issue, use the appropriate template:

- Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.md) for bugs
- Use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.md) for ideas
- Check existing issues before opening a new one

For security issues, follow [SECURITY.md](SECURITY.md) and report privately.

## License

By contributing, you agree that your contributions are licensed under the
project license described in [LICENSE](LICENSE).
