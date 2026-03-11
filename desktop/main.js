const path = require('path');
const fs = require('fs/promises');
const os = require('os');
const { execFile } = require('child_process');
const { app, BrowserWindow, shell, dialog } = require('electron');
const { ipcMain } = require('electron');

function isHttpUrl(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:';
  } catch {
    return false;
  }
}

function runFile(command, args, cwd) {
  return new Promise((resolve, reject) => {
    execFile(command, args, { cwd }, (error, stdout, stderr) => {
      if (error) {
        reject(new Error(`Command failed: ${stderr || error.message}`));
        return;
      }
      resolve({ stdout, stderr });
    });
  });
}

function createWindow() {
  const win = new BrowserWindow({
    title: 'SprkLogs',
    icon: path.join(__dirname, 'renderer', 'logo-256.png'),
    width: 1200,
    height: 820,
    minWidth: 980,
    minHeight: 680,
    backgroundColor: '#000000',
    autoHideMenuBar: false,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });

  win.webContents.setWindowOpenHandler(({ url }) => {
    if (isHttpUrl(url)) {
      shell.openExternal(url);
      return { action: 'deny' };
    }
    return { action: 'allow' };
  });

  win.webContents.on('will-navigate', (event, url) => {
    if (isHttpUrl(url)) {
      event.preventDefault();
      shell.openExternal(url);
    }
  });

  win.loadFile(path.join(__dirname, 'renderer', 'index.html'));
}

app.whenReady().then(() => {
  ipcMain.handle('reduce-zip-locally', async (_event, payload) => {
    const zipPath = payload && payload.zipPath;
    const compact = !!(payload && payload.compact);

    if (!zipPath) {
      throw new Error('zipPath is required');
    }

    const outputFile = path.join(os.tmpdir(), `spark_reduced_${Date.now()}.md`);
    const scriptPath = path.join(__dirname, 'scripts', 'reduce_log.py');
    const workspaceRoot = path.resolve(__dirname, '..');

    const { stdout } = await runFile('python', [scriptPath, '--zip', zipPath, '--out', outputFile, ...(compact ? ['--compact'] : [])], workspaceRoot);

    const reducedReport = await fs.readFile(outputFile, 'utf-8');
    await fs.unlink(outputFile).catch(() => {});

    let summary = null;
    try {
      summary = JSON.parse(stdout.trim());
    } catch (_) { /* non-fatal: summary will be null */ }

    return { reducedReport, summary };
  });

  ipcMain.handle('submit-reduced-for-analysis', async (_event, payload) => {
    const apiBaseUrl = (payload && payload.apiBaseUrl) || 'http://localhost:8000';
    const reducedReport = payload && payload.reducedReport;
    const pyFilePaths = (payload && payload.pyFilePaths) || [];
    const llmProvider = payload && payload.llmProvider;
    const apiKey = payload && payload.apiKey;
    const userId = payload && payload.userId;
    const provider = payload && payload.provider;
    const language = (payload && payload.language) || 'en';

    if (!reducedReport || !String(reducedReport).trim()) {
      throw new Error('reducedReport is required');
    }

    const form = new FormData();
    form.append('reduced_report', reducedReport);
    form.append('language', language);
    if (llmProvider) {
      form.append('llm_provider', llmProvider);
    }
    if (apiKey) {
      form.append('api_key', apiKey);
    }
    if (userId) {
      form.append('user_id', userId);
    }
    if (provider) {
      form.append('provider', provider);
    }

    for (const filePath of pyFilePaths) {
      const fileBuffer = await fs.readFile(filePath);
      const fileName = path.basename(filePath);
      form.append('pyspark_files', new Blob([fileBuffer]), fileName);
    }

    const res = await fetch(`${apiBaseUrl}/api/upload-reduced`, {
      method: 'POST',
      body: form,
    });

    if (!res.ok) {
      const errText = await res.text();
      throw new Error(`upload-reduced failed (${res.status}): ${errText}`);
    }

    return res.json();
  });

  ipcMain.handle('save-report-to-disk', async (_event, payload) => {
    const { content, suggestedName } = payload || {};
    if (!content || typeof content !== 'string') {
      throw new Error('content is required');
    }

    const defaultName = suggestedName || `spark_report_${Date.now()}.md`;

    const { filePath, canceled } = await dialog.showSaveDialog({
      title: 'Salvar relatório Markdown',
      defaultPath: path.join(app.getPath('documents'), defaultName),
      filters: [{ name: 'Markdown', extensions: ['md'] }],
    });

    if (canceled || !filePath) {
      return { saved: false };
    }

    await fs.writeFile(filePath, content, 'utf-8');
    return { saved: true, filePath };
  });

  createWindow();
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});
