# Spark Log Analyzer - Electron Hybrid Branch

This branch contains the Electron-hybrid monorepo context.

## Scope

- Desktop app in `apps/desktop/` handles local ZIP ingestion and local Python reduction.
- Backend in `backend/` is API/governance for OAuth2, usage policy points, and LLM processing from reduced logs.
- Static web/landing assets live in `apps/web/`.

## Runtime Flow

1. User selects ZIP locally in Electron.
2. Electron runs Python reducer locally and generates `reduced_report`.
3. Electron sends only `reduced_report` + optional `.py` files to `POST /api/upload-reduced`.
4. Backend enqueues async LLM analysis and returns `job_id`.

## Run Backend

```bash
docker compose up -d
```

## Run Desktop

```bash
cd apps/desktop
npm install
npm start
```

## CI/CD (Trunk-Based)

This repository now uses a trunk-based release flow on GitHub Actions.

### Day-to-day flow

1. Create a short-lived branch from `main` (or `master`).
2. Open a pull request back to `main` (or `master`).
3. CI validates backend tests, desktop integrity, and static frontend files.
4. Merge after checks pass.
5. Release Please opens/updates a Release PR with semantic version and changelog.
6. Merging the Release PR creates tag `vX.Y.Z`.
7. Tag `vX.Y.Z` triggers Windows `.exe` build and uploads artifacts to GitHub Release.
8. Pushes to `main`/`master` also publish a rolling desktop installer release at tag `master-latest`.
9. Pushes to `main`/`master` deploy static site files from `apps/web/` to GitHub Pages.

### Workflows

- `.github/workflows/ci.yml`: Pull request quality gate.
- `.github/workflows/release-please.yml`: Semantic version + changelog automation.
- `.github/workflows/build-desktop.yml`: Windows NSIS installer generation for semantic tags and rolling `master-latest` publishing on `main`/`master` pushes.
- `.github/workflows/deploy-web.yml`: Deploys `apps/web/` static files to GitHub Pages on `main`/`master` pushes.

### GitHub Pages setup (one-time)

1. In repository settings, open **Pages** and set **Source** to **GitHub Actions**.
2. Ensure Actions permissions allow workflow runs with write access to Pages.
3. After first deploy, access:
	- `https://<owner>.github.io/<repo>/` (frontend app)
	- `https://<owner>.github.io/<repo>/sprklogs-landing.html` (institutional landing)
4. The pages automatically expose the desktop download link by reading release tag `master-latest`.

### Required repository settings (one-time)

1. Protect `main` (or `master`) and require pull request reviews.
2. Require status checks for CI workflow before merge.
3. Allow GitHub Actions read/write permissions for repository contents.
4. Add release signing secrets later when code signing is enabled.
