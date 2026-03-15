# Desktop MVP (Electron Hybrid)

This MVP implements the first hybrid desktop flow:

1. User selects a local `.zip` event log (400MB+ supported based on machine capacity).
2. Electron executes local Python reducer (`apps/desktop/main/scripts/reduce_log.py`).
3. App sends only the reduced report plus optional `.py` files to backend API (`/api/upload-reduced`).
4. Backend runs LLM analysis asynchronously.

## Run

```bash
cd apps/desktop
npm install
npm start
```

## Notes

- Python must be available on PATH as `python`.
- Backend is expected at `http://localhost:8000` by default.
- ZIP file is never uploaded to backend in this desktop flow.
