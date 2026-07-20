module.exports = {
  branches: ['master'],
  tagFormat: 'v${version}',
  plugins: [
    ['@semantic-release/commit-analyzer', { preset: 'conventionalcommits' }],
    ['@semantic-release/release-notes-generator', { preset: 'conventionalcommits' }],
    [
      '@semantic-release/exec',
      {
        prepareCmd:
          'powershell -NoProfile -ExecutionPolicy Bypass -File ./infra/scripts/package-desktop-win.ps1 -Version ${nextRelease.version} -Target nsis -SkipInstall',
      },
    ],
    [
      '@semantic-release/github',
      {
        assets: [
          {
            path: 'apps/desktop/dist/SprkLogs-setup-v*.exe',
            label: 'SprkLogs Windows x64 installer',
          },
          {
            path: 'apps/desktop/dist/checksums.txt',
            label: 'SHA-256 checksums',
          },
        ],
        successCommentCondition: false,
        failCommentCondition: false,
        labels: false,
        releasedLabels: false,
      },
    ],
  ],
}
