# NSE Live Terminal (NSEKE)

- `public/index.html`: the terminal UI, served at `/`.
- `api/index.py`: FastAPI backend (`/api/symbols`, `/api/history`) deployed as a Vercel Python function.

Deploy: push to GitHub, then import the repo in Vercel (Framework: Other, no build command).
