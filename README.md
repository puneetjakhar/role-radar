# role-radar

Automated job crawler for the Netherlands. Scrapes company career pages and
LinkedIn on a schedule, scores each hit with Claude, and emails a filtered
digest twice per day plus hourly during Amsterdam waking hours.

## Pipeline

- **Crawler** — `crawl_jobs.py` runs in two parallel modes: `--no-linkedin`
  for company career portals, and `linkedin` for the LinkedIn search API.
- **Scoring** — `notify.py` calls Claude Sonnet on each new job with a
  candidate profile embedded in the module. Scored results are cached in
  `score_cache.json` so re-runs stay cheap.
- **Email** — `notify.py` sends hourly digests via Gmail SMTP and a fuller
  24-hour summary at 14:00 CEST.
- **Dashboard** — the workflow injects the fresh scored jobs into
  `job-search.html` and pushes the rendered HTML to a separate Pages repo.
- **PII hygiene** — `strip_pii.py` runs before every commit and removes any
  recruiter contact fields the crawlers may have collected. No third-party
  personal data lands in a public commit.

## Running it

Everything runs on GitHub Actions on a cron schedule. To run it against your
own inbox and profile you need:

1. Set these five secrets at `Settings → Secrets and variables → Actions`
   (or `gh secret set <NAME> --repo <owner>/<repo>`):

   | Secret | Purpose |
   |---|---|
   | `GMAIL_APP_PASSWORD` | 16-char Gmail app password for the sender account |
   | `GH_PAT` | PAT with `repo` scope, used only to push compiled HTML to your Pages repo |
   | `ANTHROPIC_API_KEY` | Claude scoring calls in `notify.py` |
   | `SENDER_EMAIL` | Gmail address the crawler sends from |
   | `NOTIFY_EMAIL` | Address that receives the digests |

2. Update `DASHBOARD_URL` in `.github/workflows/crawl.yml` and the target
   repo name in the `Deploy dashboard to public GitHub Pages repo` step to
   match your own Pages repo.
3. Update the candidate profile embedded in `notify.py` (skill list, prompt)
   to your own background — the scoring is personalised and will not be
   meaningful without it.

## Local dev

```bash
pip install -r requirements.txt
python crawl_jobs.py --force --no-linkedin
python crawl_jobs.py linkedin --force
python notify.py
```

## Notes on scraping

Third-party job aggregators change their markup and rate-limit aggressively.
Expect the crawlers to break periodically and need attention. LinkedIn in
particular has terms that restrict automated access; use accordingly.
