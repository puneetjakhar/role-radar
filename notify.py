#!/usr/bin/env python3
"""
Score jobs via Claude Sonnet, detect new ones, send emails.

Modes:
  python notify.py            -- email new jobs found since last run (runs every hour)
  python notify.py --daily    -- email full 24h summary report (runs at 12pm NL)
"""
import json, os, sys, smtplib, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
JOBS_FILE     = os.path.join(BASE_DIR, 'crawled_jobs.json')
LINKEDIN_FILE = os.path.join(BASE_DIR, 'linkedin_jobs.json')
SEEN_FILE     = os.path.join(BASE_DIR, 'seen_jobs.json')
SCORE_FILE    = os.path.join(BASE_DIR, 'score_cache.json')

SENDER_EMAIL  = os.environ.get('SENDER_EMAIL', 'puneetkumarait@gmail.com')
NOTIFY_EMAIL  = os.environ.get('NOTIFY_EMAIL', 'puneetjakhar1996@gmail.com')
GMAIL_PASS    = os.environ.get('GMAIL_APP_PASSWORD', '')
DASHBOARD_URL = os.environ.get('DASHBOARD_URL', 'https://puneetjakhar.github.io/nl-jobs-dashboard/')
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

# ── Score cache ────────────────────────────────────────────────────────────────

_score_cache: dict = {}
_score_cache_lock = threading.Lock()

def _load_score_cache():
    global _score_cache
    if os.path.exists(SCORE_FILE):
        try:
            with open(SCORE_FILE) as f:
                _score_cache = json.load(f)
        except Exception:
            _score_cache = {}

def _save_score_cache():
    with open(SCORE_FILE, 'w') as f:
        json.dump(_score_cache, f, indent=2)

# ── Rule-based fallback ────────────────────────────────────────────────────────

PUNEET_SKILLS = [
    "java", "python", "c#", "spring boot", "spring", "kotlin", "groovy",
    "microservices", "kubernetes", "k8s", "docker", "helm", "argocd",
    "ci/cd", "cicd", "kafka", "messaging", "event-driven", "postgresql",
    "mongodb", "cosmosdb", "redis", "gcp", "aws", "azure", "cloud",
    "platform", "devops", "sre", "ai", "llm", "langchain", "langgraph",
    "openai", "ml", "machine learning", "backend", "backend engineer",
    "software engineer", "platform engineer", "rest", "api", "rest api",
    "distributed", "scalability", "observability", "prometheus", "grafana",
    "elasticsearch", "fintech", "payments", "banking",
]

ROLE_MAP = {
    'senior software engineer': 30,
    'senior java':              30,
    'senior backend engineer':  28,
    'senior platform engineer': 28,
    'ai engineer':              26,
    'java engineer':            22,
    'software engineer':        20,
    'backend engineer':         18,
    'platform engineer':        18,
}

def _rule_score(job) -> int:
    title = ((job.get('job_title') or '') + ' ' +
             (job.get('notes') or '') + ' ' +
             (job.get('company') or '')).lower()
    desc  = (job.get('description') or '').lower()
    text  = title + ' ' + desc
    score = 0
    role_score = 0
    for role, pts in ROLE_MAP.items():
        if role in title:
            role_score = max(role_score, pts)
    score += role_score
    seen_skills = set()
    skill_pts = 0
    for s in PUNEET_SKILLS:
        if s not in seen_skills and s in text:
            skill_pts += 3
            seen_skills.add(s)
    score += min(skill_pts, 70)
    if 'staff ' in title or ' staff' in title or 'principal' in title:
        score -= 20
    if job.get('visa_support') is False:
        score -= 20
    if job.get('relocation_support') is False:
        score -= 10
    if any(p in text for p in ['dutch speaking', 'dutch language', 'dutch required']):
        score -= 30
    return min(max(score, 0), 99)

# ── AI scorer ──────────────────────────────────────────────────────────────────

PUNEET_PROFILE = """Puneet Kumar is a Senior Software Engineer with 8 years of experience, based in the Netherlands (Rotterdam). He is actively looking for his next role.

TECHNICAL EXPERTISE
- Languages: Java (primary), Python, Kotlin, Groovy, C#
- Frameworks: Spring Boot, Spring Cloud, Micronaut
- Architecture: Microservices, event-driven systems, distributed systems
- Cloud & DevOps: GCP, AWS, Azure, Kubernetes, Docker, Helm, ArgoCD, CI/CD
- Data & Messaging: Kafka, PostgreSQL, MongoDB, CosmosDB, Redis, Elasticsearch, Azure Service Bus
- Observability: Prometheus, Grafana, OpenTelemetry
- AI/ML: LangChain, LangGraph, LLM integration, OpenAI APIs

BACKGROUND & DOMAIN
- 5 years at Vodafone in telecom platform engineering
- Experience in fintech, payments, banking platforms
- Strong in building scalable backend systems, platform engineering, and developer tooling

TARGET ROLES (priority order)
1. Senior Software Engineer (backend/Java/Python focus)
2. Senior Backend Engineer
3. Senior Platform Engineer / Staff Engineer (individual contributor)
4. AI/ML Engineer / AI Platform Engineer
5. Software Engineer (if strong tech stack match)

PREFERRED COMPANIES: Adyen, Booking.com, Databricks, GitLab, Elastic, Backbase, Bitvavo, JetBrains, Confluent, Miro, MongoDB, Datadog, Flow Traders, Catawiki

NOT A FIT
- Staff/Principal/Engineering Manager titles (too senior, people management)
- Roles requiring Dutch language fluency
- Roles that won't sponsor visa/relocation
- Pure frontend, mobile, or QA roles
- Data Science / Analytics (not engineering)

SCORING RUBRIC
90-100: Perfect fit — Senior SWE/Backend/Platform at a top company with Java/cloud/Kafka stack
75-89:  Strong fit — right seniority + 2 of 3 (stack, domain, company tier)
60-74:  Good fit — right role type, partial stack match
45-59:  Decent — adjacent role or strong stack but wrong seniority/domain
30-44:  Weak — some relevance but significant mismatch
0-29:   Not a fit — wrong function, Dutch required, or no visa support

PENALISE heavily for: Dutch language required, staff/principal/manager title, no visa sponsorship, frontend/mobile/QA only"""

def _ai_score(job) -> int:
    try:
        import anthropic
    except ImportError:
        return _rule_score(job)

    title    = job.get('job_title') or ''
    company  = job.get('company') or ''
    location = job.get('location') or ''
    desc     = (job.get('description') or '')[:2000]

    prompt = f"""{PUNEET_PROFILE}

Score this job opportunity for Puneet on a scale of 0-100.

Job title: {title}
Company: {company}
Location: {location}
Description: {desc}

Respond with only a JSON object: {{"score": <integer 0-100>, "reason": "<one sentence>"}}"""

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        response = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=300,
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = response.content[0].text.strip()
        if text.startswith('```'):
            text = text.split('```')[1].lstrip('json').strip()
        data = json.loads(text)
        return max(0, min(99, int(data['score'])))
    except Exception as e:
        print(f'  ⚠ AI score failed for "{title}": {e} — using rules')
        return _rule_score(job)

def calc_match(job) -> int:
    url = job.get('job_url') or job.get('url') or ''
    with _score_cache_lock:
        if url and url in _score_cache:
            return _score_cache[url]

    if ANTHROPIC_KEY:
        score = _ai_score(job)
    else:
        print('  ⚠ ANTHROPIC_API_KEY not set — using rule-based scoring')
        score = _rule_score(job)

    if url:
        with _score_cache_lock:
            _score_cache[url] = score
    return score

def score_jobs_parallel(jobs: list) -> list[tuple]:
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_job = {executor.submit(calc_match, j): j for j in jobs}
        for future in as_completed(future_to_job):
            job = future_to_job[future]
            try:
                score = future.result()
            except Exception as e:
                print(f'  ⚠ Score error: {e}')
                score = _rule_score(job)
            results.append((job, score))
    return sorted(results, key=lambda x: x[1], reverse=True)

# ── Seen-jobs store ────────────────────────────────────────────────────────────

def load_seen():
    if os.path.exists(SEEN_FILE):
        with open(SEEN_FILE) as f:
            return json.load(f)
    return {}

def save_seen(seen: dict):
    with open(SEEN_FILE, 'w') as f:
        json.dump(seen, f, indent=2)

def load_all_jobs():
    jobs = []
    for path in [JOBS_FILE, LINKEDIN_FILE]:
        if os.path.exists(path):
            with open(path) as f:
                jobs.extend(json.load(f))
    return jobs

# ── Email builder ──────────────────────────────────────────────────────────────

def match_color(pct):
    if pct >= 75: return '#22c55e'
    if pct >= 55: return '#f59e0b'
    return '#94a3b8'

def build_rows(scored):
    rows = ''
    for job, pct in scored:
        url   = job.get('job_url') or job.get('url') or job.get('careers_url') or '#'
        title = job.get('job_title') or 'Unknown'
        co    = job.get('company') or ''
        loc   = job.get('location') or 'Netherlands'
        date  = job.get('date_posted') or ''
        color = match_color(pct)
        rows += (
            f'<tr>'
            f'<td style="padding:9px 8px;border-bottom:1px solid #e2e8f0;text-align:center">'
            f'<span style="background:{color};color:#fff;border-radius:12px;padding:3px 9px;font-weight:600;font-size:13px">{pct}%</span></td>'
            f'<td style="padding:9px 8px;border-bottom:1px solid #e2e8f0">'
            f'<a href="{url}" style="color:#2E74B5;text-decoration:none;font-weight:600">{title}</a></td>'
            f'<td style="padding:9px 8px;border-bottom:1px solid #e2e8f0;color:#475569">{co}</td>'
            f'<td style="padding:9px 8px;border-bottom:1px solid #e2e8f0;color:#64748b;font-size:12px">{loc}</td>'
            f'<td style="padding:9px 8px;border-bottom:1px solid #e2e8f0;color:#94a3b8;font-size:12px">{date}</td>'
            f'<td style="padding:9px 8px;border-bottom:1px solid #e2e8f0;text-align:center">'
            f'<a href="{url}" style="background:#2E74B5;color:#fff;padding:4px 12px;border-radius:6px;text-decoration:none;font-size:12px;white-space:nowrap">Apply</a></td>'
            f'</tr>'
        )
    return rows

def build_email(heading, subheading, scored):
    rows = build_rows(scored)
    header_col = '#2E74B5'
    return f'''<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;background:#f8fafc;margin:0;padding:20px">
  <div style="max-width:840px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">
    <div style="background:{header_col};padding:22px 28px">
      <h1 style="margin:0;color:#fff;font-size:20px">{heading}</h1>
      <p style="margin:5px 0 0;color:#dbeafe;font-size:13px">{subheading}</p>
    </div>
    <div style="padding:20px 28px">
      <a href="{DASHBOARD_URL}" style="display:inline-block;background:{header_col};color:#fff;padding:8px 20px;border-radius:8px;text-decoration:none;font-weight:600;margin-bottom:20px">Open Dashboard</a>
      <table style="width:100%;border-collapse:collapse">
        <thead>
          <tr style="background:#f1f5f9">
            <th style="padding:9px 8px;text-align:left;font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase">Match</th>
            <th style="padding:9px 8px;text-align:left;font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase">Role</th>
            <th style="padding:9px 8px;text-align:left;font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase">Company</th>
            <th style="padding:9px 8px;text-align:left;font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase">Location</th>
            <th style="padding:9px 8px;text-align:left;font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase">Posted</th>
            <th></th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
  </div>
</body></html>'''

def send_email(subject, html_body):
    if not GMAIL_PASS:
        print('GMAIL_APP_PASSWORD not set, skipping email')
        return
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = SENDER_EMAIL
    msg['To']      = NOTIFY_EMAIL
    msg.attach(MIMEText(html_body, 'html'))
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(SENDER_EMAIL, GMAIL_PASS)
        smtp.sendmail(SENDER_EMAIL, NOTIFY_EMAIL, msg.as_string())
    print(f'Email sent: {subject}')

# ── Modes ──────────────────────────────────────────────────────────────────────

def run_hourly(all_jobs, seen):
    now_iso = datetime.now(timezone.utc).isoformat()
    new_jobs = [j for j in all_jobs if (j.get('job_url') or j.get('url')) and
                (j.get('job_url') or j.get('url')) not in seen]
    if not new_jobs:
        print(f'No new jobs (tracking {len(seen)} seen)')
        return seen

    print(f'Scoring {len(new_jobs)} new jobs via Claude Sonnet...')
    scored = score_jobs_parallel(new_jobs)
    _save_score_cache()

    print(f'{len(scored)} new jobs found:')
    for j, pct in scored[:10]:
        print(f'  {pct}% | {j.get("job_title")} @ {j.get("company")}')

    now_str = datetime.now(timezone.utc).strftime('%b %d %H:%M')
    html = build_email(
        f'{len(scored)} new job{"s" if len(scored) != 1 else ""} found',
        f'Scored by Claude AI · Crawled at {now_str} UTC',
        scored,
    )
    send_email(f'{len(scored)} new NL jobs | {now_str} UTC', html)

    for j, _ in scored:
        url = j.get('job_url') or j.get('url')
        if url:
            seen[url] = now_iso
    return seen

def run_daily(all_jobs, seen):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    jobs_24h = [
        j for j in all_jobs
        if (j.get('job_url') or j.get('url')) and
        (j.get('job_url') or j.get('url')) in seen
        and datetime.fromisoformat(seen[j.get('job_url') or j.get('url')]).replace(tzinfo=timezone.utc) >= cutoff
    ]
    if not jobs_24h:
        print('No jobs seen in last 24h for daily report')
        return

    print(f'Scoring {len(jobs_24h)} jobs for daily report (cached scores reused)...')
    scored = score_jobs_parallel(jobs_24h)
    _save_score_cache()

    today = datetime.now(timezone.utc).strftime('%b %d, %Y')
    html = build_email(
        f'Daily Report: {len(scored)} jobs in the last 24 hours',
        f'{today} | Scored by Claude AI | Sorted by match',
        scored,
    )
    send_email(f'Daily job report | {len(scored)} jobs | {today}', html)
    print(f'Daily report sent: {len(scored)} jobs')

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    daily_mode = '--daily' in sys.argv
    _load_score_cache()
    all_jobs = load_all_jobs()
    if not all_jobs:
        print('No jobs found')
        return

    seen = load_seen()

    if daily_mode:
        run_daily(all_jobs, seen)
    else:
        seen = run_hourly(all_jobs, seen)
        save_seen(seen)

if __name__ == '__main__':
    main()
