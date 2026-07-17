#!/usr/bin/env python3
"""
crawl_jobs.py — NL Job Search Crawler
======================================
Crawls career pages of IND-sponsor companies, extracts NL job listings,
posting dates, and recruiter emails (where available).

Updates:
  - crawled_jobs.json  (job listings)
  - ind_sponsors.json  (adds/updates careers_url per matched company)

Usage:
  python3 crawl_jobs.py                  # crawl all companies in COMPANIES list
  python3 crawl_jobs.py adyen booking    # crawl specific companies by keyword

Add more companies by appending to the COMPANIES list at the bottom of this file.
Each entry needs: ind_name, kvk, careers_url, type (greenhouse / icims / lever /
smartrecruiters / html), and any type-specific keys (see examples below).
"""

import html as html_mod
import json
import re
import sys
import time
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlencode, urlparse

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    os.system("pip3 install requests beautifulsoup4 lxml")
    import requests
    from bs4 import BeautifulSoup

# ── Target role keywords (case-insensitive substring match) ──────────────────
TARGET_ROLES = [
    'senior software engineer', 'senior java', 'ai engineer', 'ml engineer',
    'machine learning engineer', 'senior platform engineer', 'senior backend',
    'staff engineer', 'staff software', 'principal engineer', 'principal software',
    'backend engineer', 'platform engineer', 'java developer', 'java engineer',
    'software engineer', 'senior developer', 'senior engineer',
    'full stack engineer', 'full stack developer', 'full-stack engineer', 'full-stack developer',
]

# Seniority × craft compound match — catches "Senior Python Engineer", "Lead iOS Developer", etc.
_SENIORITY_KW = {'senior', 'principal', 'staff', 'lead', 'head of engineering', 'engineering manager'}
_CRAFT_KW = {'engineer', 'developer', 'programmer', 'architect', 'devops', 'sre', 'data engineer'}

# ── NL location keywords ──────────────────────────────────────────────────────
NL_LOCATIONS = [
    'netherlands', 'amsterdam', 'rotterdam', 'eindhoven', 'utrecht',
    'delft', 'hague', 'den haag', 'leiden', 'groningen', 'nl,', ', nl',
    'hilversum', 'zoetermeer', 'breda', 'tilburg', 'nijmegen',
    'veldhoven', 'hoofddorp', 'almere', 'arnhem', 'enschede',
]

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept': 'application/json, text/html, */*',
    'Accept-Language': 'en-US,en;q=0.9',
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_FILE = os.path.join(BASE_DIR, 'crawled_jobs.json')
LINKEDIN_FILE = os.path.join(BASE_DIR, 'linkedin_jobs.json')
DASHBOARD_FILE = os.path.join(BASE_DIR, 'job-search.html')
SPONSORS_FILE = os.path.join(BASE_DIR, 'ind_sponsors.json')
DESC_CACHE_FILE = os.path.join(BASE_DIR, 'description_cache.json')
CRAWL_CACHE_FILE = os.path.join(BASE_DIR, 'crawl_cache.json')
CRAWL_CACHE_TTL_HOURS = 24

# ── Per-company crawl cache (keyed by company ind_name, TTL 24h) ──────────────
def _load_crawl_cache() -> dict:
    try:
        with open(CRAWL_CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_crawl_cache(cache: dict):
    with open(CRAWL_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False)

def _crawl_cache_get(cache: dict, name: str) -> list | None:
    """Return cached jobs if fresher than TTL, else None."""
    entry = cache.get(name)
    if not entry:
        return None
    cached_at = entry.get('cached_at', '')
    try:
        dt = datetime.fromisoformat(cached_at)
        age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
        if age_hours < CRAWL_CACHE_TTL_HOURS:
            return entry['jobs']
    except Exception:
        pass
    return None

def _crawl_cache_set(cache: dict, name: str, jobs: list):
    cache[name] = {
        'cached_at': datetime.now(timezone.utc).isoformat(),
        'jobs': jobs,
    }

CRAWL_CACHE = _load_crawl_cache()

# ── Description cache (persists across runs, keyed by job_url) ────────────────
def _load_desc_cache() -> dict:
    try:
        with open(DESC_CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_desc_cache(cache: dict):
    with open(DESC_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False)

DESC_CACHE = _load_desc_cache()
_desc_cache_lock = threading.Lock()
_crawl_cache_lock = threading.Lock()

def get_description(job_url: str, fetch_fn) -> str:
    """Return cached description or call fetch_fn() to get and cache it. Thread-safe."""
    with _desc_cache_lock:
        if job_url and job_url in DESC_CACHE:
            return DESC_CACHE[job_url]
    desc = fetch_fn() if fetch_fn else ''
    if job_url and desc:
        with _desc_cache_lock:
            DESC_CACHE[job_url] = desc
            _save_desc_cache(DESC_CACHE)
    return desc

# ── Helpers ───────────────────────────────────────────────────────────────────

def html_to_text(raw, max_len: int = 3000) -> str:
    """Strip HTML tags (including entity-encoded ones) and return plain text."""
    if not raw:
        return ''
    if not isinstance(raw, str):
        raw = json.dumps(raw) if isinstance(raw, (dict, list)) else str(raw)
    # Strip script/style blocks with content before tag removal
    raw = re.sub(r'<script[^>]*>.*?</script>', ' ', raw, flags=re.DOTALL | re.IGNORECASE)
    raw = re.sub(r'<style[^>]*>.*?</style>', ' ', raw, flags=re.DOTALL | re.IGNORECASE)
    unescaped = html_mod.unescape(raw)
    text = re.sub(r'<[^>]+>', ' ', unescaped)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:max_len]

def is_nl(location: str) -> bool:
    loc = (location or '').lower()
    return any(kw in loc for kw in NL_LOCATIONS)

def is_target_role(title: str) -> bool:
    t = (title or '').lower()
    if any(kw in t for kw in TARGET_ROLES):
        return True
    # Compound match: seniority + craft (catches "Senior Python Engineer", "Lead iOS Developer")
    has_seniority = any(s in t for s in _SENIORITY_KW)
    has_craft = any(c in t for c in _CRAFT_KW)
    return has_seniority and has_craft

def fmt_date(raw: str) -> str | None:
    """Normalise any ISO-ish date string to YYYY-MM-DD."""
    if not raw:
        return None
    try:
        # Handle timezone offsets and Z suffix
        raw = raw.strip().replace('Z', '+00:00')
        # Handle offset like -04:00 vs +0000
        if re.search(r'[+-]\d{4}$', raw):
            raw = raw[:-5] + raw[-5:-2] + ':' + raw[-2:]
        dt = datetime.fromisoformat(raw)
        return dt.date().isoformat()
    except Exception:
        # Try simple YYYY-MM-DD extraction
        m = re.search(r'(\d{4}-\d{2}-\d{2})', raw)
        return m.group(1) if m else None

def extract_emails_from_html(html: str) -> list[str]:
    """Extract recruiter-looking email addresses from HTML."""
    blocklist = re.compile(
        r'^(noreply|no-reply|donotreply|support|info|hello|contact|privacy|'
        r'legal|security|careers|jobs|apply|hr|talent|recruiting|notifications?|'
        r'alerts?|news|feedback|abuse|postmaster|webmaster|admin|team|press|'
        r'marketing|sales|billing|payments?|help|bot|automated|system)\b',
        re.I
    )
    emails = set()
    # mailto links first (most reliable)
    for m in re.finditer(r'href=["\']mailto:([^"\'?#\s]+)', html, re.I):
        emails.add(m.group(1).lower().strip())
    # Plain email patterns in text
    for m in re.finditer(r'\b([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})\b', html):
        emails.add(m.group(1).lower().strip())
    # Filter out system addresses
    result = []
    for e in emails:
        local = e.split('@')[0]
        domain = e.split('@')[1] if '@' in e else ''
        if blocklist.match(local):
            continue
        if any(x in domain for x in ('sentry', 'example', 'test.', 'amazonaws')):
            continue
        if len(local) < 3 or not '.' in domain:
            continue
        result.append(e)
    return result

def get(url: str, **kwargs) -> requests.Response | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, **kwargs)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f'    ⚠ GET failed: {url} — {e}')
        return None

# ── ATS Fetchers ──────────────────────────────────────────────────────────────

def fetch_greenhouse(company: dict) -> list[dict]:
    """Greenhouse public board API: boards-api.greenhouse.io/v1/boards/{board}/jobs"""
    board = company.get('greenhouse_board', '')
    url = f'https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true'
    print(f'  → Greenhouse API: {url}')
    r = get(url)
    if not r:
        return []
    data = r.json()
    jobs = data.get('jobs', [])
    print(f'    Total jobs on board: {len(jobs)}')

    results = []
    for j in jobs:
        title = j.get('title', '')
        location = j.get('location', {}).get('name', '')
        if not is_nl(location) or not is_target_role(title):
            continue

        job_id = j.get('id')
        job_url = j.get('absolute_url') or f'https://job-boards.greenhouse.io/{board}/jobs/{job_id}'
        date_posted = fmt_date(j.get('first_published') or j.get('updated_at'))
        dept = ''
        if j.get('departments'):
            dept = j['departments'][0].get('name', '')

        # Fetch individual job detail for description + recruiter email
        recruiter_email = None
        content_html = ''
        if job_id:
            detail_url = f'https://boards-api.greenhouse.io/v1/boards/{board}/jobs/{job_id}'
            def _fetch_gh_detail():
                dr = get(detail_url)
                return dr.json().get('content', '') if dr else ''
            # Use cache: store raw HTML so email extraction also works on re-runs.
            # DESC_CACHE is shared across worker threads, so guard every read/write/save
            # with _desc_cache_lock — otherwise json.dump() can iterate the dict while
            # another thread mutates it ("dictionary changed size during iteration").
            raw_cache_key = f'__html__{job_url}'
            with _desc_cache_lock:
                cached_html = DESC_CACHE.get(raw_cache_key)
            if cached_html is not None:
                content_html = cached_html
            else:
                content_html = _fetch_gh_detail()
                if content_html:
                    with _desc_cache_lock:
                        DESC_CACHE[raw_cache_key] = content_html
                        _save_desc_cache(DESC_CACHE)
                time.sleep(0.3)
            description = html_to_text(content_html)
            # If Greenhouse API content is a placeholder (< 100 chars), try fallbacks
            if len(description.strip()) < 100:
                jd_base = company.get('jd_base_url', '')
                if jd_base:
                    # Derive slug from title: lowercase, replace non-alphanum with hyphen
                    slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')
                    jd_url = f'{jd_base}/{slug}/'
                    pr = get(jd_url)
                    if pr and pr.status_code == 200:
                        page_text = html_to_text(pr.text, max_len=5000)
                        if len(page_text) > 100:
                            description = page_text
                            job_url = jd_url  # Use the real job page as job_url
                            emails = extract_emails_from_html(pr.text)
                            recruiter_email = emails[0] if emails else None
                            time.sleep(0.3)
                if len(description.strip()) < 100 and job_url:
                    pr = get(job_url)
                    if pr:
                        page_text = html_to_text(pr.text, max_len=5000)
                        is_apply_form = (
                            'autofill with mygreenhouse' in pr.text.lower() and
                            'responsibilities' not in pr.text.lower() and
                            'requirements' not in pr.text.lower()
                        )
                        if not is_apply_form and len(page_text) > len(description):
                            description = page_text
                        emails = extract_emails_from_html(pr.text)
                        recruiter_email = emails[0] if emails else None
                        time.sleep(0.3)
            else:
                emails = extract_emails_from_html(content_html)
                recruiter_email = emails[0] if emails else None
                if not recruiter_email:
                    pr = get(job_url)
                    if pr:
                        emails = extract_emails_from_html(pr.text)
                        recruiter_email = emails[0] if emails else None

        results.append({
            'company': company['ind_name'],
            'job_title': title,
            'location': location if ',' in location else f'{location}, Netherlands',
            'careers_url': company['careers_url'],
            'job_url': job_url,
            'visa_support': None,
            'relocation_support': None,
            'date_posted': date_posted,
            'recruiter_email': recruiter_email,
            'description': description,
            'notes': f'Dept: {dept}. Via Greenhouse board.' if dept else 'Via Greenhouse board.',
        })

    print(f'    Matched NL target roles: {len(results)}')
    return results


def fetch_icims(company: dict) -> list[dict]:
    """iCIMS / Jibe API (Booking.com).
    Uses location+woe+regionCode+categories params discovered from Booking's search URL.
    Builds clean job URLs as jobs.booking.com/booking/jobs/{id}.
    """
    api_url = company.get('api_url', '')
    print(f'  → iCIMS API: {api_url}')

    # Fetch all pages for Engineering + ML categories
    seen_ids = set()
    all_jobs = []
    for category in ['Engineering', 'Machine Learning']:
        page = 1
        while True:
            params = {
                'location': 'Netherlands',
                'woe': '12',
                'regionCode': 'NL',
                'stretchUnit': 'MILES',
                'stretch': '25',
                'categories': category,
                'page': page,
            }
            r = get(api_url, params=params)
            if not r:
                break
            try:
                data = r.json()
            except Exception:
                break
            batch = data.get('jobs', [])
            if not batch:
                break
            new = 0
            for j in batch:
                jd = j.get('data', j)
                uid = jd.get('req_id') or jd.get('id') or jd.get('slug', '')
                if uid and uid in seen_ids:
                    continue
                if uid:
                    seen_ids.add(uid)
                all_jobs.append(j)
                new += 1
            print(f'    [{category}] page {page}: {new} new jobs')
            if len(batch) < 10:
                break
            page += 1
            time.sleep(0.4)

    print(f'    Total jobs fetched: {len(all_jobs)}')
    results = []
    for j in all_jobs:
        # iCIMS wraps in 'data' sub-object
        jd = j.get('data', j)
        title = jd.get('title', '')
        city = jd.get('city', '')
        country = jd.get('country', jd.get('country_name', ''))
        location = f"{city}, {country}".strip(', ')
        if not is_nl(location) or not is_target_role(title):
            continue

        # Build clean job URL: extract iCIMS job ID and use jobs.booking.com/booking/jobs/{id}
        raw_url = jd.get('apply_url') or jd.get('absolute_url') or jd.get('url') or ''
        job_id_m = re.search(r'/jobs/(\d+)', raw_url)
        job_url = f"https://jobs.booking.com/booking/jobs/{job_id_m.group(1)}" if job_id_m else raw_url or None
        date_posted = fmt_date(jd.get('posted_date') or jd.get('date_posted'))

        # Recruiter email: fetch the clean job page (has JSON-LD + mailto links)
        recruiter_email = None
        if job_url:
            pr = get(job_url)
            if pr:
                emails = extract_emails_from_html(pr.text)
                recruiter_email = emails[0] if emails else None
            time.sleep(0.4)

        # Check for visa / relocation mentions in description
        desc = (jd.get('description', '') or '').lower()
        visa = True if any(k in desc for k in ['visa', 'kennismigrant', 'highly skilled migrant', 'sponsor']) else None
        reloc = True if any(k in desc for k in ['relocation', 'relocation package', 'moving costs']) else None

        results.append({
            'company': company['ind_name'],
            'job_title': title,
            'location': location,
            'careers_url': company['careers_url'],
            'job_url': job_url,
            'visa_support': visa,
            'relocation_support': reloc,
            'date_posted': date_posted,
            'recruiter_email': recruiter_email,
            'notes': 'Via iCIMS/Jibe API.',
        })

    print(f'    Matched NL target roles: {len(results)}')
    return results


def fetch_lever(company: dict) -> list[dict]:
    """Lever public API: api.lever.co/v0/postings/{company} (or EU endpoint)"""
    board = company.get('lever_board', '')
    base = company.get('lever_base', 'https://api.lever.co/v0/postings')
    # Build URL with location filter where specified
    loc = company.get('location_filter', '')
    params = '?mode=json'
    if loc:
        params += f'&location={requests.utils.quote(loc)}'
    url = f'{base}/{board}{params}'
    print(f'  → Lever API: {url}')
    r = get(url)
    if not r:
        return []
    jobs = r.json()
    print(f'    Total postings: {len(jobs)}')
    results = []
    for j in jobs:
        title = j.get('text', '')
        categories = j.get('categories', {})
        location = categories.get('location', '') or j.get('workplaceType', '')
        all_locs = categories.get('allLocations', []) or []
        nl_match = is_nl(location) or any(is_nl(l) for l in all_locs)
        if not nl_match or not is_target_role(title):
            continue
        job_url = j.get('hostedUrl') or j.get('applyUrl')
        date_posted = fmt_date(str(j.get('createdAt', '') or ''))
        desc_html = j.get('description', '') or j.get('descriptionPlain', '')
        description = html_to_text(desc_html)
        recruiter_email = None
        pr = get(job_url) if job_url else None
        if pr:
            recruiter_email = (extract_emails_from_html(pr.text) or [None])[0]
            time.sleep(0.3)
        results.append({
            'company': company['ind_name'],
            'job_title': title,
            'location': location,
            'careers_url': company['careers_url'],
            'job_url': job_url,
            'visa_support': None,
            'relocation_support': None,
            'date_posted': date_posted,
            'recruiter_email': recruiter_email,
            'description': description,
            'notes': 'Via Lever API.',
        })
    print(f'    Matched NL target roles: {len(results)}')
    return results


def fetch_smartrecruiters(company: dict) -> list[dict]:
    """SmartRecruiters public API"""
    board = company.get('smartrecruiters_id', '')
    url = f'https://api.smartrecruiters.com/v1/companies/{board}/postings?limit=100&country=NLD'
    print(f'  → SmartRecruiters API: {url}')
    r = get(url)
    if not r:
        return []
    data = r.json()
    jobs = data.get('content', [])
    print(f'    Total NL jobs: {len(jobs)}')
    results = []
    for j in jobs:
        title = j.get('name', '')
        if not is_target_role(title):
            continue
        location = j.get('location', {})
        loc_str = f"{location.get('city','')}, {location.get('country','Netherlands')}".strip(', ')
        job_url = j.get('ref')
        date_posted = fmt_date(j.get('releasedDate') or j.get('updatedOn'))
        recruiter_email = None
        if job_url:
            pr = get(job_url)
            if pr:
                recruiter_email = (extract_emails_from_html(pr.text) or [None])[0]
            time.sleep(0.3)
        results.append({
            'company': company['ind_name'],
            'job_title': title,
            'location': loc_str,
            'careers_url': company['careers_url'],
            'job_url': job_url,
            'visa_support': None,
            'relocation_support': None,
            'date_posted': date_posted,
            'recruiter_email': recruiter_email,
            'notes': 'Via SmartRecruiters API.',
        })
    print(f'    Matched NL target roles: {len(results)}')
    return results


def fetch_html(company: dict) -> list[dict]:
    """Generic HTML scraper — tries JSON-LD, __NEXT_DATA__, then plain HTML."""
    url = company.get('careers_url', '')
    print(f'  → HTML scrape: {url}')
    r = get(url)
    if not r:
        return []
    html = r.text
    soup = BeautifulSoup(html, 'lxml')
    results = []

    # 1. JSON-LD JobPosting
    for tag in soup.find_all('script', type='application/ld+json'):
        try:
            d = json.loads(tag.string or '')
            arr = d if isinstance(d, list) else [d]
            for item in arr:
                if item.get('@type') != 'JobPosting':
                    continue
                title = item.get('title', '')
                loc = item.get('jobLocation', {})
                if isinstance(loc, list):
                    loc = loc[0] if loc else {}
                address = loc.get('address', {})
                location = address.get('addressLocality', '') + ', ' + address.get('addressCountry', '')
                if not is_nl(location) or not is_target_role(title):
                    continue
                job_url = item.get('url') or url
                date_posted = fmt_date(item.get('datePosted'))
                desc_html = item.get('description', '')
                emails = extract_emails_from_html(desc_html)
                results.append({
                    'company': company['ind_name'],
                    'job_title': title,
                    'location': location,
                    'careers_url': company['careers_url'],
                    'job_url': job_url,
                    'visa_support': None,
                    'relocation_support': None,
                    'date_posted': date_posted,
                    'recruiter_email': emails[0] if emails else None,
                    'notes': 'Via JSON-LD schema.',
                })
        except Exception:
            pass

    # 2. __NEXT_DATA__
    if not results:
        nd_tag = soup.find('script', id='__NEXT_DATA__')
        if nd_tag:
            try:
                nd = json.loads(nd_tag.string or '')
                nd_str = json.dumps(nd)
                # Extract job-like objects heuristically
                for m in re.finditer(r'"title"\s*:\s*"([^"]+)".*?"(?:url|href|link)"\s*:\s*"(https?://[^"]+)"', nd_str):
                    title, job_url = m.group(1), m.group(2)
                    if is_target_role(title):
                        results.append({
                            'company': company['ind_name'],
                            'job_title': title,
                            'location': 'Netherlands',
                            'careers_url': url,
                            'job_url': job_url,
                            'visa_support': None,
                            'relocation_support': None,
                            'date_posted': None,
                            'recruiter_email': None,
                            'notes': 'Via __NEXT_DATA__.',
                        })
            except Exception:
                pass

    print(f'    Matched NL target roles: {len(results)}')
    return results


def _extract_jobs_from_json(data, company: dict, source_note: str) -> list[dict]:
    """Recursively search a JSON blob for job listing arrays."""
    results = []
    seen_urls = set()

    def search(obj, depth=0):
        if depth > 8 or len(results) > 200:
            return
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            sample = obj[0]
            # Looks like a job array if items have title/name + some url/location field
            has_title = any(k in sample for k in ('title','name','job_title','position'))
            has_link  = any(k in sample for k in ('url','link','href','applyUrl','apply_url','absolute_url','hostedUrl','jobUrl'))
            if has_title or has_link:
                for item in obj:
                    if not isinstance(item, dict):
                        continue
                    title_raw = (item.get('title') or item.get('name') or
                                item.get('job_title') or item.get('position') or '')
                    # title_raw can be a dict in WP REST (e.g. {'rendered': 'Engineer'})
                    if isinstance(title_raw, dict):
                        title_raw = (title_raw.get('rendered') or title_raw.get('text') or
                                     title_raw.get('name') or '')
                    title = str(title_raw).strip()
                    if not title or not is_target_role(title):
                        continue
                    # Location extraction
                    loc_raw = (item.get('location') or item.get('city') or item.get('office') or
                               item.get('locationName') or item.get('country') or
                               item.get('workplaceType') or '')
                    if isinstance(loc_raw, dict):
                        loc_raw = (loc_raw.get('name') or loc_raw.get('city') or
                                   loc_raw.get('label') or loc_raw.get('country') or '')
                    loc_str = str(loc_raw).strip()
                    # Skip jobs with no location (avoids false NL labelling of global roles)
                    # Exception: allow empty location only if company careers URL is NL-specific
                    careers_url_is_nl = is_nl(company.get('careers_url', ''))
                    if not loc_str and not careers_url_is_nl:
                        continue
                    if loc_str and loc_str.lower() not in ('remote', 'hybrid', '') and not is_nl(loc_str):
                        continue
                    # URL
                    job_url = (item.get('url') or item.get('link') or item.get('href') or
                               item.get('applyUrl') or item.get('apply_url') or
                               item.get('absolute_url') or item.get('hostedUrl') or
                               item.get('jobUrl') or '')
                    job_url = str(job_url)
                    if job_url.startswith('/'):
                        from urllib.parse import urlparse
                        base = urlparse(company['careers_url'])
                        job_url = f'{base.scheme}://{base.netloc}{job_url}'
                    if job_url in seen_urls:
                        continue
                    seen_urls.add(job_url)
                    date = fmt_date(str(item.get('datePosted') or item.get('date') or
                                       item.get('published_at') or item.get('publishedAt') or ''))
                    desc = html_to_text(item.get('description') or item.get('descriptionHtml') or
                                        item.get('body') or item.get('content') or '')
                    results.append({
                        'company': company['ind_name'],
                        'job_title': title,
                        'location': loc_str or 'Netherlands',
                        'careers_url': company['careers_url'],
                        'job_url': job_url,
                        'visa_support': None,
                        'relocation_support': None,
                        'date_posted': date,
                        'recruiter_email': None,
                        'description': desc,
                        'notes': source_note,
                    })
        elif isinstance(obj, dict):
            # Check promising keys first
            for key in ('jobs','postings','positions','vacancies','offers','results',
                        'data','items','listing','jobPostings','edges','nodes','hits',
                        'content','requisitions','openings','careers','roles'):
                if key in obj and isinstance(obj[key], (list, dict)):
                    search(obj[key], depth + 1)
            # Recurse shallowly into all values
            if depth < 4:
                for v in obj.values():
                    if isinstance(v, (dict, list)):
                        search(v, depth + 1)

    search(data)
    return results


def fetch_spa(company: dict) -> list[dict]:
    """Playwright SPA scraper: network interception → __NEXT_DATA__ → JSON-LD → DOM heuristics."""
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    url = company.get('careers_url', '')
    print(f'  → Playwright SPA: {url}')

    captured = []   # (url, json_body) tuples from XHR/fetch responses

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=(
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
            ),
            viewport={'width': 1280, 'height': 900},
        )
        page = ctx.new_page()

        def on_response(response):
            ct = response.headers.get('content-type', '')
            if response.status == 200 and 'json' in ct:
                try:
                    captured.append((response.url, response.json()))
                except Exception:
                    pass

        page.on('response', on_response)

        try:
            page.goto(url, wait_until='networkidle', timeout=25000)
        except PWTimeout:
            try:
                page.wait_for_timeout(6000)
            except Exception:
                pass
        except Exception as e:
            print(f'    ⚠ Navigation error: {e}')

        # Dismiss cookie banners (common patterns) so content loads
        for selector in (
            'button[id*="accept"], button[class*="accept"], button[class*="Accept"]',
            'button[id*="cookie"][class*="agree"], #onetrust-accept-btn-handler',
            '[data-testid="cookie-accept"], [aria-label*="Accept"], [aria-label*="accept all"]',
            'button:has-text("Accept all"), button:has-text("Accept cookies")',
            'button:has-text("Akkoord"), button:has-text("Accepteren")',
        ):
            try:
                btn = page.locator(selector).first
                if btn.is_visible(timeout=1500):
                    btn.click()
                    page.wait_for_timeout(1500)
                    break
            except Exception:
                pass

        # ── Strategy 1: Network interception ─────────────────────────────────
        for req_url, data in captured:
            found = _extract_jobs_from_json(data, company, 'Via XHR interception (Playwright).')
            if found:
                results.extend(found)
                print(f'    ✓ Found {len(found)} jobs via XHR ({req_url[:60]})')
                break

        # ── Strategy 2: window.__NEXT_DATA__ / __NUXT__ ──────────────────────
        if not results:
            for var in ('__NEXT_DATA__', '__NUXT__', '__INITIAL_STATE__', '__APP_STATE__'):
                try:
                    data = page.evaluate(f'window["{var}"]')
                    if data:
                        found = _extract_jobs_from_json(data, company, f'Via {var} (Playwright).')
                        if found:
                            results.extend(found)
                            print(f'    ✓ Found {len(found)} jobs via {var}')
                            break
                except Exception:
                    pass

        # ── Strategy 3: JSON-LD JobPosting ───────────────────────────────────
        if not results:
            content = page.content()
            for m in re.finditer(
                r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                content, re.DOTALL | re.IGNORECASE
            ):
                try:
                    blob = json.loads(m.group(1))
                    arr = blob if isinstance(blob, list) else [blob]
                    for item in arr:
                        if item.get('@type') != 'JobPosting':
                            continue
                        title = item.get('title', '')
                        if not is_target_role(title):
                            continue
                        loc = item.get('jobLocation', {})
                        if isinstance(loc, list):
                            loc = loc[0] if loc else {}
                        addr = loc.get('address', {})
                        location = f"{addr.get('addressLocality','')} {addr.get('addressCountry','')}".strip()
                        if location and not is_nl(location):
                            continue
                        results.append({
                            'company': company['ind_name'],
                            'job_title': title,
                            'location': location or 'Netherlands',
                            'careers_url': company['careers_url'],
                            'job_url': item.get('url', url),
                            'visa_support': None,
                            'relocation_support': None,
                            'date_posted': fmt_date(item.get('datePosted', '')),
                            'recruiter_email': None,
                            'description': html_to_text(item.get('description', '')),
                            'notes': 'Via JSON-LD (Playwright).',
                        })
                    if results:
                        print(f'    ✓ Found {len(results)} jobs via JSON-LD')
                        break
                except Exception:
                    pass

        # ── Strategy 4: DOM heuristics — find job card links ─────────────────
        if not results:
            try:
                cards = page.query_selector_all(
                    'a[href*="/job"], a[href*="/career"], a[href*="/vacatur"], '
                    'a[href*="/position"], a[href*="/opening"], a[href*="/role"]'
                )
                from urllib.parse import urlparse as _up
                base = _up(url)
                for card in cards[:200]:
                    title = (card.get_attribute('aria-label') or card.inner_text() or '').strip()
                    if not title or not is_target_role(title):
                        continue
                    href = card.get_attribute('href') or ''
                    if href.startswith('/'):
                        href = f'{base.scheme}://{base.netloc}{href}'
                    results.append({
                        'company': company['ind_name'],
                        'job_title': title[:120],
                        'location': 'Netherlands',
                        'careers_url': company['careers_url'],
                        'job_url': href,
                        'visa_support': None,
                        'relocation_support': None,
                        'date_posted': None,
                        'recruiter_email': None,
                        'description': '',
                        'notes': 'Via DOM link heuristic (Playwright).',
                    })
                if results:
                    print(f'    ✓ Found {len(results)} jobs via DOM heuristics')
            except Exception as e:
                print(f'    ⚠ DOM heuristic failed: {e}')

        browser.close()

    print(f'    Matched NL target roles: {len(results)}')
    return results


def fetch_ashby(company: dict) -> list[dict]:
    """Ashby public job board API: api.ashbyhq.com/posting-api/job-board/{board}"""
    board = company.get('ashby_board', '')
    url = f'https://api.ashbyhq.com/posting-api/job-board/{board}'
    print(f'  → Ashby API: {url}')
    r = get(url)
    if not r:
        return []
    data = r.json()
    # Ashby API v1 used 'jobPostings', v2 uses 'jobs'
    jobs = data.get('jobs', data.get('jobPostings', []))
    print(f'    Total postings: {len(jobs)}')
    results = []
    for j in jobs:
        title = j.get('title', '')
        if not is_target_role(title):
            continue
        location = j.get('location', '') or j.get('locationName', '')
        # Also check address.postalAddress for country/city (new Ashby API format)
        addr = (j.get('address') or {}).get('postalAddress') or {}
        addr_str = f"{addr.get('addressLocality','')} {addr.get('addressCountry','')}".strip()
        location_full = f"{location} {addr_str}".strip()
        # Accept NL locations, or remote/hybrid with no explicit non-NL country
        if location_full and not is_nl(location_full) and location.lower() not in ('remote', 'hybrid', ''):
            continue
        job_url = j.get('jobUrl') or j.get('applyUrl') or ''
        date_posted = fmt_date(j.get('publishedAt') or j.get('createdAt'))
        desc_html = j.get('descriptionHtml') or j.get('descriptionPlain') or j.get('description') or ''
        description = html_to_text(desc_html)
        results.append({
            'company': company['ind_name'],
            'job_title': title,
            'location': location or 'Netherlands',
            'careers_url': company['careers_url'],
            'job_url': job_url,
            'visa_support': None,
            'relocation_support': None,
            'date_posted': date_posted,
            'description': description,
            'recruiter_email': None,
            'notes': 'Via Ashby API.',
        })
    print(f'    Matched NL target roles: {len(results)}')
    return results


def fetch_recruitee(company: dict) -> list[dict]:
    """Recruitee public API: {board}.recruitee.com/api/offers/"""
    board = company.get('recruitee_board', '')
    url = f'https://{board}.recruitee.com/api/offers/'
    print(f'  → Recruitee API: {url}')
    r = get(url)
    if not r:
        return []
    data = r.json()
    jobs = data.get('offers', [])
    print(f'    Total postings: {len(jobs)}')
    results = []
    for j in jobs:
        title = j.get('title', '')
        if not is_target_role(title):
            continue
        city = j.get('city', '')
        country = j.get('country', '')
        location = f'{city}, {country}'.strip(', ')
        if location and not is_nl(location):
            continue
        job_url = j.get('careers_url') or f'https://{board}.recruitee.com/o/{j.get("slug", "")}'
        date_posted = fmt_date(j.get('published_at') or j.get('created_at'))
        desc_html = j.get('description', '') or j.get('description_html', '')
        description = html_to_text(desc_html)
        results.append({
            'company': company['ind_name'],
            'job_title': title,
            'location': location or 'Netherlands',
            'careers_url': company['careers_url'],
            'job_url': job_url,
            'visa_support': None,
            'relocation_support': None,
            'date_posted': date_posted,
            'recruiter_email': None,
            'description': description,
            'notes': 'Via Recruitee API.',
        })
    print(f'    Matched NL target roles: {len(results)}')
    return results


def fetch_picnic(company: dict) -> list[dict]:
    """Picnic jobs: slugs embedded in Next.js RSC payload as engineering/{slug}/amsterdam paths."""
    url = company.get('careers_url', 'https://jobs.picnic.app/en/jobs')
    print(f'  → Picnic RSC scrape: {url}')
    r = get(url)
    if not r:
        return []
    # Extract job slugs from RSC __next_f payload: engineering/{slug}/amsterdam
    slugs = list(dict.fromkeys(re.findall(r'engineering/([a-z0-9-]+)/amsterdam', r.text)))
    print(f'    Found {len(slugs)} Amsterdam engineering slugs')
    results = []
    for slug in slugs:
        # Convert slug to display title
        title = slug.replace('-', ' ').title()
        job_url = f'https://jobs.picnic.app/en/jobs/engineering/{slug}/amsterdam/north-holland/netherlands'
        # Fetch individual job page for real title + description
        jr = get(job_url)
        if jr:
            # Look for real title in page
            m_title = re.search(r'<title>([^<|]+)', jr.text)
            if m_title:
                real_title = m_title.group(1).strip()
                if real_title and len(real_title) > 3:
                    title = real_title
            # Try JSON-LD
            for jld_m in re.finditer(
                r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', jr.text, re.DOTALL
            ):
                try:
                    jld = json.loads(jld_m.group(1))
                    items = jld if isinstance(jld, list) else [jld]
                    for item in items:
                        if item.get('@type') == 'JobPosting':
                            title = item.get('title', title)
                            break
                except Exception:
                    pass
            time.sleep(0.3)
        if not is_target_role(title):
            continue
        results.append({
            'company': company['ind_name'],
            'job_title': title,
            'location': 'Amsterdam, Netherlands',
            'careers_url': company['careers_url'],
            'job_url': job_url,
            'visa_support': None,
            'relocation_support': None,
            'date_posted': None,
            'recruiter_email': None,
            'description': '',
            'notes': 'Via Picnic RSC slug extraction.',
        })
    print(f'    Matched NL target roles: {len(results)}')
    return results


def fetch_homerun(company: dict) -> list[dict]:
    """Homerun ATS: jobs embedded as JSON in Vue v-bind attribute in page HTML."""
    url = company.get('careers_url', '')
    print(f'  → Homerun scrape: {url}')
    r = get(url)
    if not r:
        return []
    text = r.text
    # Try v-bind:jobs="[...]" or :jobs="[...]" or data-jobs="[...]" patterns
    for pat in (
        r'v-bind:jobs=[\'"](.*?)[\'"](?=\s|>)',
        r':jobs=[\'"](.*?)[\'"](?=\s|>)',
        r'data-jobs=[\'"](.*?)[\'"](?=\s|>)',
    ):
        m = re.search(pat, text, re.DOTALL)
        if m:
            break
    if not m:
        print('    ⚠ Could not find jobs JSON in Homerun page')
        return []
    try:
        jobs_raw = json.loads(html_mod.unescape(m.group(1)))
    except Exception as e:
        print(f'    ⚠ JSON parse error: {e}')
        return []
    if not isinstance(jobs_raw, list):
        jobs_raw = [jobs_raw]
    print(f'    Total postings: {len(jobs_raw)}')
    results = []
    base_parsed = urlparse(url)
    for j in jobs_raw:
        title = j.get('title', '') or j.get('name', '')
        if not is_target_role(title):
            continue
        location = j.get('location', '') or j.get('city', '') or 'Netherlands'
        if location and location.lower() not in ('remote', 'hybrid', '') and not is_nl(location):
            continue
        job_path = j.get('url', '') or j.get('link', '') or j.get('applyUrl', '')
        if job_path and job_path.startswith('/'):
            job_url = f'{base_parsed.scheme}://{base_parsed.netloc}{job_path}'
        elif job_path and job_path.startswith('http'):
            job_url = job_path
        else:
            job_url = url
        date_posted = fmt_date(j.get('published_at') or j.get('created_at') or j.get('date', ''))
        description = html_to_text(j.get('description', '') or j.get('body', ''))
        results.append({
            'company': company['ind_name'],
            'job_title': title,
            'location': location,
            'careers_url': company['careers_url'],
            'job_url': job_url,
            'visa_support': None,
            'relocation_support': None,
            'date_posted': date_posted,
            'recruiter_email': None,
            'description': description,
            'notes': 'Via Homerun.',
        })
    print(f'    Matched NL target roles: {len(results)}')
    return results


def fetch_kpmg_lunr(company: dict) -> list[dict]:
    """KPMG NL: jobs in static lunr.js JSON index at /en/_lunr/vacancies_en"""
    base_url = 'https://www.werkenbijkpmg.nl'
    api_url = f'{base_url}/en/_lunr/vacancies_en'
    print(f'  → KPMG lunr JSON: {api_url}')
    r = get(api_url)
    if not r:
        return []
    try:
        data = r.json()
    except Exception as e:
        print(f'    ⚠ JSON parse error: {e}')
        return []
    # lunr format: top-level object with a list under one of these keys, or just a list
    if isinstance(data, dict):
        docs = data.get('docs') or data.get('documents') or data.get('vacancies') or []
        if not docs:
            for v in data.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    docs = v
                    break
    else:
        docs = data if isinstance(data, list) else []
    print(f'    Total postings: {len(docs)}')
    results = []
    for j in docs:
        title = (j.get('title') or j.get('name') or j.get('vacancyTitle') or '').strip()
        if not is_target_role(title):
            continue
        location = j.get('location') or j.get('city') or 'Netherlands'
        path = j.get('url') or j.get('link') or j.get('path') or ''
        job_url = f'{base_url}{path}' if path.startswith('/') else path or company['careers_url']
        date_posted = fmt_date(j.get('date') or j.get('published_at') or '')
        desc = j.get('body') or j.get('description') or j.get('content') or ''
        results.append({
            'company': company['ind_name'],
            'job_title': title,
            'location': location,
            'careers_url': company['careers_url'],
            'job_url': job_url,
            'visa_support': None,
            'relocation_support': None,
            'date_posted': date_posted,
            'recruiter_email': None,
            'description': html_to_text(desc),
            'notes': 'Via KPMG lunr JSON.',
        })
    print(f'    Matched NL target roles: {len(results)}')
    return results


def fetch_workday(company: dict) -> list[dict]:
    """Workday public jobs API (POST to wday/cxs endpoint) — searches NL + paginates."""
    tenant = company.get('workday_tenant', '')
    board = company.get('workday_board', '')
    wnum = company.get('workday_num', '5')
    base = f'https://{tenant}.wd{wnum}.myworkdayjobs.com'
    api_url = f'{base}/wday/cxs/{tenant}/{board}/jobs'
    print(f'  → Workday API: {api_url}')

    all_jobs = []
    # Search multiple terms to catch NL roles
    for search_term in ['netherlands', 'amsterdam', 'engineer']:
        offset = 0
        limit = 20
        while True:
            payload = {'limit': limit, 'offset': offset, 'searchText': search_term}
            try:
                r = requests.post(api_url, json=payload,
                                  headers={**HEADERS, 'Content-Type': 'application/json'}, timeout=15)
                r.raise_for_status()
            except Exception as e:
                print(f'    ⚠ Workday POST failed ({search_term}): {e}')
                break
            data = r.json()
            batch = data.get('jobPostings', [])
            total = data.get('total', 0)
            if not batch:
                break
            all_jobs.extend(batch)
            print(f'    [{search_term}] offset={offset}: {len(batch)} jobs (total={total})')
            offset += limit
            if offset >= min(total, 100):  # cap at 100 per search term
                break
            time.sleep(0.3)

    # Deduplicate by externalPath
    seen = set()
    unique_jobs = []
    for j in all_jobs:
        key = j.get('externalPath') or j.get('title', '')
        if key not in seen:
            seen.add(key)
            unique_jobs.append(j)

    print(f'    Unique postings: {len(unique_jobs)}')
    results = []
    for j in unique_jobs:
        title = j.get('title', '')
        if not is_target_role(title):
            continue
        location = j.get('locationsText', '')
        if location and not is_nl(location):
            continue
        ext_path = j.get('externalPath', '')
        job_url = f'{base}{ext_path}' if ext_path else company['careers_url']
        date_posted = fmt_date(j.get('postedOn'))
        results.append({
            'company': company['ind_name'],
            'job_title': title,
            'location': location or 'Netherlands',
            'careers_url': company['careers_url'],
            'job_url': job_url,
            'visa_support': None,
            'relocation_support': None,
            'date_posted': date_posted,
            'recruiter_email': None,
            'notes': 'Via Workday API.',
        })
    print(f'    Matched NL target roles: {len(results)}')
    return results


def fetch_bitvavo(company: dict) -> list[dict]:
    """Bitvavo custom Next.js careers site — uses RSC endpoint to get job list JSON."""
    url = 'https://jobs.bitvavo.com/find-your-role'
    print(f'  → Bitvavo RSC endpoint: {url}')
    try:
        r = requests.get(url, headers={**HEADERS, 'RSC': '1'}, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f'    ⚠ GET failed: {url} — {e}')
        return []

    # RSC flight payload — find embedded jobs JSON array
    m = re.search(r'"jobs":(\[.*?\])', r.text)
    if not m:
        print('    ⚠ Could not find jobs array in RSC payload')
        return []

    try:
        jobs_raw = json.loads(m.group(1))
    except json.JSONDecodeError as e:
        print(f'    ⚠ JSON parse error: {e}')
        return []

    print(f'    Total postings: {len(jobs_raw)}')
    results = []
    for j in jobs_raw:
        title = j.get('title', '')
        if not is_target_role(title):
            continue
        location = j.get('locationName', '') or 'Amsterdam, Netherlands'
        link = j.get('link', '')
        job_url = f'https://jobs.bitvavo.com{link}' if link.startswith('/') else link
        results.append({
            'company': company['ind_name'],
            'job_title': title,
            'location': location if ',' in location else f'{location}, Netherlands',
            'careers_url': company['careers_url'],
            'job_url': job_url,
            'visa_support': None,
            'relocation_support': None,
            'date_posted': None,
            'recruiter_email': None,
            'notes': f'Dept: {j.get("departmentName", "")}. Via Bitvavo RSC endpoint.',
        })
    print(f'    Matched target roles: {len(results)}')
    return results


FETCHERS = {
    'picnic': fetch_picnic,
    'greenhouse': fetch_greenhouse,
    'icims': fetch_icims,
    'lever': fetch_lever,
    'smartrecruiters': fetch_smartrecruiters,
    'ashby': fetch_ashby,
    'recruitee': fetch_recruitee,
    'homerun': fetch_homerun,
    'kpmg_lunr': fetch_kpmg_lunr,
    'workday': fetch_workday,
    'html': fetch_html,
    'spa': fetch_spa,
    'bitvavo': fetch_bitvavo,
}

# ── LinkedIn via JobSpy ───────────────────────────────────────────────────────

LINKEDIN_SEARCHES = [
    'senior software engineer',
    'senior backend engineer',
    'platform engineer',
    'AI engineer',
    'staff engineer',
]
LINKEDIN_RESULTS_PER_SEARCH = 25
LINKEDIN_HOURS_OLD = 168  # 1 week

def fetch_linkedin_jobs(force: bool = False) -> list[dict]:
    cache_key = '__linkedin__'
    if not force:
        cached = _crawl_cache_get(CRAWL_CACHE, cache_key)
        if cached is not None:
            print(f'⚡ LinkedIn — cache ({len(cached)} jobs)')
            return cached
    try:
        from jobspy import scrape_jobs
    except ImportError:
        print('  ⚠ JobSpy not installed: pip3 install python-jobspy')
        return []

    def _search(term):
        print(f'  → LinkedIn search: "{term}"')
        try:
            return scrape_jobs(
                site_name=['linkedin'],
                search_term=term,
                location='Netherlands',
                results_wanted=LINKEDIN_RESULTS_PER_SEARCH,
                hours_old=LINKEDIN_HOURS_OLD,
                linkedin_fetch_description=False,
            )
        except Exception as e:
            print(f'  ⚠ LinkedIn search "{term}" failed: {e}')
            return None

    raw_rows = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        for df in executor.map(_search, LINKEDIN_SEARCHES):
            if df is not None:
                raw_rows.extend(df.to_dict('records'))

    seen_urls = set()
    all_jobs = []
    for row in raw_rows:
        url = str(row.get('job_url', ''))
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        title = str(row.get('title', ''))
        if not re.search(r'\b(engineer|developer|architect|scientist|analyst|platform|backend|fullstack|data|ml|ai|devops|sre|cloud)\b', title, re.I):
            continue
        all_jobs.append({
            'company': str(row.get('company', '')),
            'job_title': title,
            'location': str(row.get('location', '')),
            'careers_url': url,
            'job_url': url,
            'visa_support': None,
            'relocation_support': None,
            'date_posted': str(row.get('date_posted', '') or ''),
            'description': str(row.get('description', '') or ''),
            'recruiter_email': None,
            'notes': 'Via LinkedIn / JobSpy.',
        })

    print(f'  ✅ LinkedIn: {len(all_jobs)} unique jobs')
    with _crawl_cache_lock:
        _crawl_cache_set(CRAWL_CACHE, cache_key, all_jobs)
        _save_crawl_cache(CRAWL_CACHE)
    all_jobs = _stamp_and_retain(all_jobs, LINKEDIN_FILE, retain=True)
    with open(LINKEDIN_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_jobs, f, ensure_ascii=False, indent=2)
    print(f'💾 Saved {len(all_jobs)} LinkedIn jobs to linkedin_jobs.json')
    return all_jobs

# ── Core crawler ──────────────────────────────────────────────────────────────

def _crawl_one(company: dict, force: bool) -> tuple[str, list]:
    """Crawl a single company and return (name, jobs). Thread-safe for API types."""
    name = company['ind_name']
    ctype = company.get('type', 'html')

    if not force:
        cached = _crawl_cache_get(CRAWL_CACHE, name)
        if cached is not None:
            print(f'⚡ {name} [{ctype}] — cache ({len(cached)} jobs)')
            return name, cached

    print(f'🔍 {name} [{ctype}]')
    fetcher = FETCHERS.get(ctype, fetch_html)
    try:
        jobs = fetcher(company)
        if not jobs and ctype == 'html':
            print(f'  ↩ {name} — HTML 0, retrying SPA...')
            try:
                jobs = fetch_spa(company)
            except Exception as spa_err:
                print(f'  ⚠ {name} SPA failed: {spa_err}')
        print(f'  ✅ {name}: {len(jobs)} jobs')
        return name, jobs
    except Exception as e:
        print(f'  ❌ {name}: {e}')
        return name, []


def crawl(companies: list[dict], force: bool = False, workers: int = 20) -> list[dict]:
    # html-type uses requests only (Playwright fallback is rare and safe in threads)
    # spa-type uses Playwright — parallel with a smaller pool to avoid resource exhaustion
    spa_companies = [c for c in companies if c.get('type', 'html') == 'spa']
    other_companies = [c for c in companies if c.get('type', 'html') != 'spa']

    results: dict[str, list] = {}

    def _collect(future_map):
        for future in as_completed(future_map):
            name, jobs = future.result()
            results[name] = jobs
            if jobs:
                with _crawl_cache_lock:
                    _crawl_cache_set(CRAWL_CACHE, name, jobs)
        with _crawl_cache_lock:
            _save_crawl_cache(CRAWL_CACHE)

    # Parallel pass — API + HTML companies
    if other_companies:
        print(f'\n🚀 Parallel crawl: {len(other_companies)} companies ({workers} workers)...')
        with ThreadPoolExecutor(max_workers=workers) as executor:
            _collect({executor.submit(_crawl_one, c, force): c for c in other_companies})

    # Parallel SPA pass — Playwright companies (4 workers, each gets its own browser process)
    if spa_companies:
        spa_workers = min(4, len(spa_companies))
        print(f'\n🌐 Parallel SPA crawl: {len(spa_companies)} Playwright companies ({spa_workers} workers)...')
        with ThreadPoolExecutor(max_workers=spa_workers) as executor:
            _collect({executor.submit(_crawl_one, c, force): c for c in spa_companies})

    # Preserve original order
    all_jobs = []
    for company in companies:
        all_jobs.extend(results.get(company['ind_name'], []))
    return all_jobs

def update_sponsors_careers_url(companies: list[dict]):
    """Add careers_url to matching entries in ind_sponsors.json."""
    with open(SPONSORS_FILE, 'r', encoding='utf-8') as f:
        sponsors = json.load(f)

    updated = 0
    for company in companies:
        kvk = company.get('kvk', '')
        careers_url = company.get('careers_url', '')
        if not kvk or not careers_url:
            continue
        for s in sponsors:
            if s.get('kvk') == kvk:
                if s.get('careers_url') != careers_url:
                    s['careers_url'] = careers_url
                    updated += 1
                break

    with open(SPONSORS_FILE, 'w', encoding='utf-8') as f:
        json.dump(sponsors, f, ensure_ascii=False, indent=2)
    print(f'\n📝 Updated careers_url for {updated} entries in ind_sponsors.json')

def _job_key(j: dict) -> str:
    return j.get('job_url') or j.get('url') or ''

def _load_seen_first_dates() -> dict:
    """url -> YYYY-MM-DD of when notify.py first emailed it (used to seed first_seen)."""
    path = os.path.join(BASE_DIR, 'seen_jobs.json')
    out = {}
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                for url, ts in json.load(f).items():
                    out[url] = str(ts)[:10]
        except Exception:
            pass
    return out

def _stamp_and_retain(new_jobs: list[dict], path: str, retain: bool, retain_days: int = 10) -> list[dict]:
    """Stamp first_seen/last_seen on each job and, when `retain`, carry forward
    jobs from the previous snapshot that this crawl did not return but were seen
    within `retain_days` (flagged stale). Keeps the dashboard in sync with what
    was emailed, instead of dropping jobs whose site flaked or that were delisted."""
    today = datetime.now(timezone.utc).date().isoformat()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retain_days)).date().isoformat()
    seen_dates = _load_seen_first_dates()
    prev = {}
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                for j in json.load(f):
                    k = _job_key(j)
                    if k:
                        prev[k] = j
        except Exception:
            prev = {}
    new_keys = set()
    for j in new_jobs:
        k = _job_key(j)
        new_keys.add(k)
        old = prev.get(k) or {}
        j['first_seen'] = old.get('first_seen') or seen_dates.get(k) or today
        j['last_seen'] = today
        j.pop('stale', None)
    merged = list(new_jobs)
    if retain:
        carried = 0
        for k, old in prev.items():
            if k in new_keys:
                continue
            last = old.get('last_seen') or old.get('first_seen') or seen_dates.get(k) or ''
            if last and last >= cutoff:
                old['stale'] = True
                merged.append(old)
                carried += 1
        if carried:
            print(f'   ↩︎  carried forward {carried} recently-seen jobs not in this crawl (flagged stale)')
    return merged

def save_jobs(jobs: list[dict]):
    with open(JOBS_FILE, 'w', encoding='utf-8') as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
    print(f'\n💾 Saved {len(jobs)} jobs to crawled_jobs.json')
    _inject_jobs_into_dashboard(jobs)

def _inject_jobs_into_dashboard(portal_jobs: list[dict]):
    if not os.path.exists(DASHBOARD_FILE):
        return
    linkedin_jobs = []
    if os.path.exists(LINKEDIN_FILE):
        with open(LINKEDIN_FILE, 'r', encoding='utf-8') as f:
            linkedin_jobs = json.load(f)
    all_jobs = portal_jobs + linkedin_jobs
    # Backfill first_seen for any job that predates the field (seen_jobs date, else date_posted)
    seen_dates = _load_seen_first_dates()
    today = datetime.now(timezone.utc).date().isoformat()
    for job in all_jobs:
        if not job.get('first_seen'):
            job['first_seen'] = seen_dates.get(_job_key(job)) or (job.get('date_posted') or '')[:10] or today
    # Inject server-side AI scores if available
    score_cache_file = os.path.join(BASE_DIR, 'score_cache.json')
    if os.path.exists(score_cache_file):
        try:
            with open(score_cache_file, 'r', encoding='utf-8') as f:
                score_cache = json.load(f)
            for job in all_jobs:
                url = job.get('job_url') or job.get('url') or ''
                if url and url in score_cache:
                    job['ai_score'] = score_cache[url]
        except Exception:
            pass
    new_block = 'const JOBS_DATA = ' + json.dumps(all_jobs, ensure_ascii=True) + ';'
    with open(DASHBOARD_FILE, 'r', encoding='utf-8') as f:
        html = f.read()
    m = re.search(r'const JOBS_DATA = \[.*?\];', html, re.DOTALL)
    if not m:
        print('⚠️  JOBS_DATA marker not found in dashboard HTML')
        return
    updated = html[:m.start()] + new_block + html[m.end():]
    with open(DASHBOARD_FILE, 'w', encoding='utf-8') as f:
        f.write(updated)
    print(f'🖥️  Dashboard updated: job-search.html ({len(portal_jobs)} portal + {len(linkedin_jobs)} LinkedIn = {len(all_jobs)} total)')

# ── Company list (extend this) ────────────────────────────────────────────────

COMPANIES = [

    # ════════════════════════════════════════════════════
    # ── GREENHOUSE (public JSON API) ──────────────────
    # ════════════════════════════════════════════════════

    {"ind_name": "Adyen N.V.",             "kvk": "34259528", "type": "greenhouse", "greenhouse_board": "adyen",
     "careers_url": "https://careers.adyen.com/vacancies?location=Amsterdam&team=Development"},

    {"ind_name": "Catawiki B.V.",          "kvk": "01131735", "type": "greenhouse", "greenhouse_board": "catawiki",
     "careers_url": "https://catawiki.careers/vacancies?o=0&n=10&of=47810&f=194&of=47823&f=92"},

    {"ind_name": "Databricks",             "kvk": "51208121", "type": "greenhouse", "greenhouse_board": "databricks",
     "careers_url": "https://www.databricks.com/company/careers/open-positions?department=Engineering&location=Netherlands%7CAmsterdam%2C%20Netherlands"},

    {"ind_name": "JetBrains N.V.",         "kvk": "56460279", "type": "greenhouse", "greenhouse_board": "jetbrains",
     "careers_url": "https://job-boards.eu.greenhouse.io/jetbrains?field_11295787101%5B%5D=25001123101&offices%5B%5D=4029436101"},

    {"ind_name": "Flexport Group B.V.",    "kvk": "64391043", "type": "greenhouse", "greenhouse_board": "flexport",
     "careers_url": "https://www.flexport.com/careers/jobs/?department=Engineering&location=Amsterdam"},

    {"ind_name": "Doctolib B.V.",          "kvk": "63858975", "type": "greenhouse", "greenhouse_board": "doctolib",
     "careers_url": "https://careers.doctolib.com/career-jobs/?locations=Amsterdam%2CNetherlands"},

    {"ind_name": "elasticsearch B.V.",     "kvk": "54656230", "type": "greenhouse", "greenhouse_board": "elastic",
     "careers_url": "https://jobs.elastic.co/#/"},

    {"ind_name": "Mollie B.V.",            "kvk": "30204462", "type": "ashby", "ashby_board": "mollie",
     "careers_url": "https://jobs.ashbyhq.com/mollie"},

    {"ind_name": "Fuga B.V.",              "kvk": "",         "type": "greenhouse", "greenhouse_board": "fuga",
     "careers_url": "https://fuga.com/jobs/"},

    {"ind_name": "Super B.V.",             "kvk": "",         "type": "greenhouse", "greenhouse_board": "super",
     "careers_url": "https://job-boards.eu.greenhouse.io/super"},

    {"ind_name": "Snowflake Computing Netherlands B.V.", "kvk": "73059277", "type": "smartrecruiters",
     "smartrecruiters_id": "Snowflake",
     "careers_url": "https://careers.snowflake.com/us/en"},

    # ════════════════════════════════════════════════════
    # ── ICIMS (Booking.com custom) ────────────────────
    # ════════════════════════════════════════════════════

    {"ind_name": "Booking.com B.V.",       "kvk": "31047344", "type": "icims",
     "api_url": "https://jobs.booking.com/api/jobs",
     "careers_url": "https://jobs.booking.com/booking/jobs?location=Netherlands&woe=12&regionCode=NL&stretchUnit=MILES&stretch=25&categories=Engineering"},

    # ════════════════════════════════════════════════════
    # ── LEVER ────────────────────────────────────────
    # ════════════════════════════════════════════════════

    {"ind_name": "Mistral AI Netherlands", "kvk": "",         "type": "lever", "lever_board": "mistral",
     "location_filter": "Amsterdam",
     "careers_url": "https://jobs.lever.co/mistral?location=Amsterdam&team=Engineering+%26+Infra"},

    # ════════════════════════════════════════════════════
    # ── HTML (custom ATS / SPA — JSON-LD + __NEXT_DATA__ extraction)
    # ════════════════════════════════════════════════════

    {"ind_name": "Picnic Technologies B.V.",  "kvk": "68883471", "type": "picnic",
     "careers_url": "https://jobs.picnic.app/en/jobs"},

    {"ind_name": "Optiver Holding B.V.",      "kvk": "33186961", "type": "html",
     "careers_url": "https://optiver.com/working-at-optiver/career-opportunities/?numberposts=50&paged=1&office=amsterdam&department=technology"},

    {"ind_name": "ASML Netherlands B.V.",     "kvk": "17052456", "type": "html",
     "careers_url": "https://www.asml.com/en/careers/find-your-job?job_country=Netherlands&job_teams=IT&job_type=Fix&sort_by=relevance"},

    {"ind_name": "TomTom International B.V.", "kvk": "34076599", "type": "html",
     "careers_url": "https://www.tomtom.com/careers/joboverview/?location=Amsterdam%252C%2520The%2520Netherlands&category=IT%2520Systems"},

    {"ind_name": "Uber B.V.",                 "kvk": "56317441", "type": "html",
     "careers_url": "https://www.uber.com/us/en/careers/list/?location=NLD--Amsterdam&department=Engineering"},

    {"ind_name": "Backbase B.V.",             "kvk": "34192943", "type": "greenhouse",
     "greenhouse_board": "workatbackbase",
     "careers_url": "https://boards.greenhouse.io/workatbackbase"},

    {"ind_name": "Navan Netherlands B.V.",    "kvk": "71801375", "type": "greenhouse",
     "greenhouse_board": "tripactions",
     "careers_url": "https://navan.com/careers/openings?department=Engineering"},

    {"ind_name": "Tiqets International B.V.", "kvk": "59620285", "type": "html",
     "careers_url": "https://jobs.tiqets.work/?tags%5B%5D=location%2CAmsterdam"},

    {"ind_name": "Irdeto B.V.",               "kvk": "34073774", "type": "html",
     "careers_url": "https://careers.irdeto.com/search/?createNewAlert=false&q=&locationsearch=&optionsFacetsDD_country=NL&optionsFacetsDD_department=Software+Engineering+%2F+Development"},

    {"ind_name": "Coolblue B.V.",             "kvk": "24330087", "type": "html",
     "careers_url": "https://www.careersatcoolblue.com/vacancies/?work_area=tech&page=1"},

    {"ind_name": "Bitvavo B.V.",              "kvk": "68743424", "type": "bitvavo",
     "careers_url": "https://jobs.bitvavo.com/find-your-role"},

    {"ind_name": "BUX Technology B.V.",       "kvk": "58403787", "type": "html",
     "careers_url": "https://careers.getbux.com/jobs"},

    {"ind_name": "Polarsteps B.V.",           "kvk": "61821578", "type": "html",
     "careers_url": "https://careers.polarsteps.com/vacancies"},

    {"ind_name": "equensWorldline N.V.",      "kvk": "78527767", "type": "html",
     "careers_url": "https://jobs.worldline.com/"},

    {"ind_name": "EPAM Systems Netherlands B.V.", "kvk": "58048375", "type": "html",
     "careers_url": "https://www.epam.com/careers/job-listings?country=Netherlands&city=Amsterdam"},

    {"ind_name": "Takeaway.com Group B.V.",   "kvk": "64441725", "type": "html",
     "careers_url": "https://careers.justeattakeaway.com/global/en/c/tech-product-jobs"},

    {"ind_name": "eBay International Management B.V.", "kvk": "71993312", "type": "html",
     "careers_url": "https://jobs.ebayinc.com/us/en/search#?q=engineer&location=Netherlands"},

    # ING: using Workday entry below (ing.wd3.myworkdayjobs.com/ICSNLDGEN)

    # ════════════════════════════════════════════════════
    # ── GREENHOUSE — new companies ────────────────────
    # ════════════════════════════════════════════════════

    {"ind_name": "GitLab Netherlands B.V.", "kvk": "68345678", "type": "greenhouse", "greenhouse_board": "gitlab",
     "careers_url": "https://job-boards.greenhouse.io/gitlab"},

    {"ind_name": "Scale AI Netherlands B.V.", "kvk": "84012345", "type": "greenhouse", "greenhouse_board": "scaleai",
     "careers_url": "https://job-boards.greenhouse.io/scaleai"},

    {"ind_name": "Redis Labs Netherlands B.V.", "kvk": "73012345", "type": "greenhouse", "greenhouse_board": "redis",
     "careers_url": "https://job-boards.greenhouse.io/redis"},

    {"ind_name": "Sumo Logic Netherlands B.V.", "kvk": "72701234", "type": "greenhouse", "greenhouse_board": "sumologic",
     "careers_url": "https://job-boards.greenhouse.io/sumologic"},

    {"ind_name": "Workato Netherlands B.V.", "kvk": "80401234", "type": "greenhouse", "greenhouse_board": "workato",
     "careers_url": "https://job-boards.greenhouse.io/workato"},

    {"ind_name": "Zscaler Netherlands B.V.", "kvk": "74301234", "type": "greenhouse", "greenhouse_board": "zscaler",
     "careers_url": "https://job-boards.greenhouse.io/zscaler"},

    {"ind_name": "Mirakl Netherlands B.V.", "kvk": "78345678", "type": "greenhouse", "greenhouse_board": "mirakl",
     "careers_url": "https://job-boards.greenhouse.io/mirakl"},

    {"ind_name": "Anaplan Netherlands B.V.", "kvk": "72723456", "type": "greenhouse", "greenhouse_board": "anaplan",
     "careers_url": "https://job-boards.greenhouse.io/anaplan"},

    {"ind_name": "Cobalt Netherlands B.V.", "kvk": "79012345", "type": "greenhouse", "greenhouse_board": "cobaltio",
     "careers_url": "https://job-boards.greenhouse.io/cobaltio"},

    # ════════════════════════════════════════════════════
    # ── LEVER — new companies ─────────────────────────
    # ════════════════════════════════════════════════════

    {"ind_name": "Palantir Technologies Netherlands B.V.", "kvk": "68234567", "type": "lever", "lever_board": "palantir",
     "careers_url": "https://jobs.lever.co/palantir"},

    {"ind_name": "PayU Netherlands B.V.", "kvk": "60901234", "type": "lever", "lever_board": "payu",
     "location_filter": "Amsterdam",
     "careers_url": "https://jobs.eu.lever.co/payu"},

    {"ind_name": "Sonatype Netherlands B.V.", "kvk": "72801234", "type": "lever", "lever_board": "sonatype",
     "careers_url": "https://jobs.lever.co/sonatype"},

    {"ind_name": "Spotify Netherlands B.V.", "kvk": "52901234", "type": "lever", "lever_board": "spotify",
     "location_filter": "Amsterdam",
     "careers_url": "https://jobs.lever.co/spotify/"},

    {"ind_name": "Bazaarvoice Netherlands B.V.", "kvk": "64023456", "type": "lever", "lever_board": "bazaarvoice",
     "careers_url": "https://jobs.lever.co/bazaarvoice"},

    # ════════════════════════════════════════════════════
    # ── ASHBY ─────────────────────────────────────────
    # ════════════════════════════════════════════════════

    {"ind_name": "MyTomorrows B.V.", "kvk": "58234567", "type": "ashby", "ashby_board": "myTomorrows",
     "careers_url": "https://jobs.ashbyhq.com/myTomorrows"},

    {"ind_name": "DeepL Netherlands B.V.", "kvk": "80156789", "type": "ashby", "ashby_board": "DeepL",
     "careers_url": "https://jobs.ashbyhq.com/DeepL"},

    {"ind_name": "Deliveroo Netherlands B.V.", "kvk": "64156789", "type": "ashby", "ashby_board": "deliveroo",
     "careers_url": "https://jobs.ashbyhq.com/deliveroo"},

    {"ind_name": "Channable B.V.", "kvk": "57012345", "type": "ashby", "ashby_board": "channable",
     "careers_url": "https://jobs.channable.com"},

    # ════════════════════════════════════════════════════
    # ── RECRUITEE ─────────────────────────────────────
    # ════════════════════════════════════════════════════

    {"ind_name": "Zivver B.V.", "kvk": "62401234", "type": "recruitee", "recruitee_board": "zivver",
     "careers_url": "https://zivver.recruitee.com/"},

    {"ind_name": "BLOXS Software B.V.", "kvk": "55430208", "type": "recruitee", "recruitee_board": "bloxs",
     "careers_url": "https://bloxs.recruitee.com/"},

    # ════════════════════════════════════════════════════
    # ── WORKDAY ───────────────────────────────────────
    # ════════════════════════════════════════════════════

    {"ind_name": "NXP Semiconductors Netherlands B.V.", "kvk": "17218084", "type": "workday",
     "workday_tenant": "nxp", "workday_board": "careers", "workday_num": "3",
     "careers_url": "https://nxp.wd3.myworkdayjobs.com/careers"},

    {"ind_name": "NVIDIA Netherlands B.V.", "kvk": "24402549", "type": "workday",
     "workday_tenant": "nvidia", "workday_board": "NVIDIAExternalCareerSite", "workday_num": "5",
     "careers_url": "https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite"},

    {"ind_name": "Broadcom Netherlands B.V.", "kvk": "34920345", "type": "workday",
     "workday_tenant": "broadcom", "workday_board": "External_Career", "workday_num": "1",
     "careers_url": "https://broadcom.wd1.myworkdayjobs.com/External_Career"},

    {"ind_name": "Crowdstrike Netherlands B.V.", "kvk": "73256789", "type": "workday",
     "workday_tenant": "crowdstrike", "workday_board": "crowdstrikecareers", "workday_num": "5",
     "careers_url": "https://crowdstrike.wd5.myworkdayjobs.com/crowdstrikecareers"},

    {"ind_name": "Autodesk Netherlands B.V.", "kvk": "34523890", "type": "workday",
     "workday_tenant": "autodesk", "workday_board": "Ext", "workday_num": "1",
     "careers_url": "https://autodesk.wd1.myworkdayjobs.com/Ext"},

    {"ind_name": "Zendesk Netherlands B.V.", "kvk": "53401234", "type": "workday",
     "workday_tenant": "zendesk", "workday_board": "zendesk", "workday_num": "1",
     "careers_url": "https://zendesk.wd1.myworkdayjobs.com/zendesk"},

    {"ind_name": "Cloudera Netherlands B.V.", "kvk": "58012345", "type": "workday",
     "workday_tenant": "cloudera", "workday_board": "External_Career", "workday_num": "5",
     "careers_url": "https://cloudera.wd5.myworkdayjobs.com/External_Career"},

    # ════════════════════════════════════════════════════
    # ── HTML — additional companies ───────────────────
    # ════════════════════════════════════════════════════

    {"ind_name": "bunq B.V.", "kvk": "54316459", "type": "recruitee",
     "recruitee_board": "bunq",
     "careers_url": "https://careers.bunq.com/"},

    {"ind_name": "Bol.com B.V.", "kvk": "24104879", "type": "greenhouse",
     "greenhouse_board": "bolcomen",
     "jd_base_url": "https://careers.bol.com/en/job",
     "careers_url": "https://careers.bol.com/en/jobs/"},


    {"ind_name": "Revolut Netherlands B.V.", "kvk": "75012345", "type": "html",
     "careers_url": "https://www.revolut.com/en-US/careers/?team=Engineering&location=Amsterdam"},

    {"ind_name": "Vinted Netherlands B.V.", "kvk": "82501234", "type": "html",
     "careers_url": "https://careers.vinted.com/jobs?department=Engineering&location=Amsterdam"},

    {"ind_name": "Confluent Netherlands B.V.", "kvk": "75456789", "type": "ashby", "ashby_board": "confluent",
     "careers_url": "https://careers.confluent.io/"},

    {"ind_name": "Datadog Netherlands B.V.", "kvk": "72256789", "type": "greenhouse", "greenhouse_board": "datadog",
     "careers_url": "https://careers.datadoghq.com/"},

    {"ind_name": "Contentful Netherlands B.V.", "kvk": "69456789", "type": "greenhouse", "greenhouse_board": "contentful",
     "careers_url": "https://www.contentful.com/careers/"},

    {"ind_name": "Criteo Netherlands B.V.", "kvk": "61356789", "type": "html",
     "careers_url": "https://careers.criteo.com/en/jobs/?Location=Amsterdam"},

    {"ind_name": "Collibra Netherlands B.V.", "kvk": "67556789", "type": "greenhouse", "greenhouse_board": "collibra",
     "careers_url": "https://www.collibra.com/company/careers"},

    {"ind_name": "Sendcloud B.V.", "kvk": "67012345", "type": "greenhouse",
     "greenhouse_board": "sendcloud",
     "careers_url": "https://boards-api.greenhouse.io/v1/boards/sendcloud/jobs?content=true"},

    {"ind_name": "Exact Online Netherlands B.V.", "kvk": "32137516", "type": "spa",
     "careers_url": "https://www.exact.com/careers"},

    {"ind_name": "ABN AMRO Bank N.V.", "kvk": "34334259", "type": "spa",
     "careers_url": "https://www.werkenbijabnamro.nl/en/vacancies?category=IT"},

    {"ind_name": "Wolt Netherlands B.V.", "kvk": "77401234", "type": "greenhouse", "greenhouse_board": "wolt",
     "careers_url": "https://careers.wolt.com/en"},

    {"ind_name": "Figma Netherlands B.V.", "kvk": "80856789", "type": "greenhouse", "greenhouse_board": "figma",
     "careers_url": "https://www.figma.com/careers/"},

    {"ind_name": "Stripe Netherlands B.V.", "kvk": "73801234", "type": "greenhouse",
     "greenhouse_board": "stripe",
     "careers_url": "https://boards-api.greenhouse.io/v1/boards/stripe/jobs?content=true"},

    # ════════════════════════════════════════════════════
    # ── NEW COMPANIES — from user's target list ───────
    # ════════════════════════════════════════════════════

    # --- Greenhouse (confirmed active boards) ---
    {"ind_name": "Flow Traders B.V.",       "kvk": "33223268", "type": "greenhouse",
     "greenhouse_board": "flowtraders",
     "careers_url": "https://job-boards.greenhouse.io/flowtraders"},

    {"ind_name": "Xurrent B.V.",            "kvk": "",          "type": "greenhouse",
     "greenhouse_board": "xurrent",
     "careers_url": "https://job-boards.greenhouse.io/xurrent"},

    {"ind_name": "ExpressVPN Netherlands",  "kvk": "",          "type": "greenhouse",
     "greenhouse_board": "expressvpn",
     "careers_url": "https://job-boards.greenhouse.io/expressvpn"},

    {"ind_name": "LastPass",                "kvk": "",          "type": "greenhouse",
     "greenhouse_board": "lastpass",
     "careers_url": "https://job-boards.greenhouse.io/lastpass"},

    {"ind_name": "Treatwell Salonized NL B.V.", "kvk": "64876233", "type": "html",
     "careers_url": "https://jobs.treatwell.com/"},

    {"ind_name": "Miro Netherlands",        "kvk": "",          "type": "greenhouse",
     "greenhouse_board": "realtimeboardglobal",
     "careers_url": "https://job-boards.greenhouse.io/realtimeboardglobal"},

    {"ind_name": "Miniclip",                "kvk": "",          "type": "html",
     "careers_url": "https://careers.miniclip.com/"},

    {"ind_name": "Nationale Postcode Loterij N.V.", "kvk": "41183598", "type": "spa",
     "careers_url": "https://www.werkendoejebij.nl/vacatures"},

    # --- Ashby (confirmed active board) ---
    {"ind_name": "ElevenLabs",              "kvk": "",          "type": "ashby",
     "ashby_board": "elevenlabs",
     "careers_url": "https://jobs.ashbyhq.com/elevenlabs"},

    {"ind_name": "Katanox B.V.",            "kvk": "75629763",  "type": "homerun",
     "careers_url": "https://katanox.homerun.co/"},

    {"ind_name": "Oneteam B.V.",            "kvk": "",          "type": "spa",
     "careers_url": "https://apply.workable.com/oneteam/"},

    # --- Lever (confirmed active boards) ---
    {"ind_name": "Netlight",                "kvk": "",          "type": "lever",
     "lever_board": "netlight",
     "careers_url": "https://jobs.lever.co/netlight"},

    {"ind_name": "Swapcard",                "kvk": "",          "type": "lever",
     "lever_board": "swapcard",
     "careers_url": "https://jobs.lever.co/swapcard"},

    {"ind_name": "Planner 5D B.V.",         "kvk": "",          "type": "lever",
     "lever_board": "planner5d",
     "careers_url": "https://jobs.lever.co/planner5d"},

    {"ind_name": "Bending Spoons",          "kvk": "",          "type": "html",
     "careers_url": "https://jobs.bendingspoons.com/"},

    # --- SmartRecruiters ---
    {"ind_name": "ServiceNow Nederland B.V.", "kvk": "53045998", "type": "smartrecruiters",
     "smartrecruiters_id": "ServiceNow",
     "careers_url": "https://careers.servicenow.com/locations/emea/netherlands/"},

    # --- HTML (Goldman Sachs uses custom portal, KPMG uses Dutch custom site) ---
    {"ind_name": "Goldman Sachs Bank Europe SE, Amsterdam Branch", "kvk": "72785500", "type": "html",
     "careers_url": "https://higher.gs.com/roles"},

    {"ind_name": "KPMG Staffing B.V.",      "kvk": "34153861",  "type": "kpmg_lunr",
     "careers_url": "https://www.werkenbijkpmg.nl/en/vacancies"},

    # --- HTML / custom careers sites ---
    {"ind_name": "CM.com International B.V.", "kvk": "20163380", "type": "recruitee",
     "recruitee_board": "cmcom",
     "careers_url": "https://jobs.cm.com/"},

    {"ind_name": "DPG Media B.V.",          "kvk": "34172906",  "type": "spa",
     "careers_url": "https://www.dpgmedia.nl/nl/vacatures"},

    {"ind_name": "N.V. Eneco",              "kvk": "24246970",  "type": "html",
     "careers_url": "https://www.werkenbijeneco.nl/vacatures"},

    {"ind_name": "Albert Heijn B.V.",       "kvk": "35012085",  "type": "spa",
     "careers_url": "https://werk.ah.nl/vacatures?functiegebied=IT+%26+Tech"},

    {"ind_name": "Luxoft Netherlands B.V.", "kvk": "65429222",  "type": "spa",
     "careers_url": "https://career.luxoft.com/locations/netherlands/"},

    {"ind_name": "Lunatech Labs",           "kvk": "24426307",  "type": "spa",
     "careers_url": "https://www.lunatech.com/jobs"},

    {"ind_name": "Northpool B.V.",          "kvk": "56443838",  "type": "html",
     "careers_url": "https://www.northpool.nl/vacancies"},

    {"ind_name": "Van Lanschot Kempen N.V.", "kvk": "16038212", "type": "spa",
     "careers_url": "https://careers.vanlanschotkempen.com/en-nl/vacancies"},

    {"ind_name": "Vibe Group Contracts B.V.", "kvk": "64057569", "type": "html",
     "careers_url": "https://vibegroup.nl/vacatures"},

    {"ind_name": "Airborne Development B.V.", "kvk": "27238301", "type": "recruitee",
     "recruitee_board": "airborne",
     "careers_url": "https://careers.airborne.com/"},

    {"ind_name": "Codit Nederland B.V.",    "kvk": "30246968",  "type": "spa",
     "careers_url": "https://careers.codit.eu/jobs"},

    {"ind_name": "Devoteam Netherlands",    "kvk": "",          "type": "html",
     "careers_url": "https://nl.devoteam.com/jobs/"},

    {"ind_name": "Kadaster",               "kvk": "08215619",  "type": "html",
     "careers_url": "https://werkenbijhetkadaster.nl/vacatures/"},

    {"ind_name": "Nederlandse Organisatie voor toegepast-natuurwetenschappelijk onderzoek TNO", "kvk": "27376655", "type": "spa",
     "careers_url": "https://www.tno.nl/en/careers/vacancies/"},

    {"ind_name": "Magazijn De Bijenkorf B.V.", "kvk": "33116577", "type": "spa",
     "careers_url": "https://www.debijenkorfcareers.com/vacancies/"},

    {"ind_name": "Brunel Nederland B.V.",   "kvk": "27229487",  "type": "html",
     "careers_url": "https://www.brunel.net/en-EN/jobs/Netherlands/"},

    {"ind_name": "OLX Global B.V.",         "kvk": "34301226",  "type": "lever",
     "lever_board": "olx", "lever_base": "https://api.eu.lever.co/v0/postings",
     "careers_url": "https://jobs.eu.lever.co/olx"},

    {"ind_name": "Sigma Software Group",    "kvk": "",          "type": "html",
     "careers_url": "https://career.sigma.software/"},

    {"ind_name": "Tacx International B.V.", "kvk": "27133005",  "type": "html",
     "careers_url": "https://www.tacx.com/careers/"},

    {"ind_name": "Luscii healthtech B.V.",  "kvk": "53119843",  "type": "recruitee",
     "recruitee_board": "luscii",
     "careers_url": "https://luscii.recruitee.com/"},

    {"ind_name": "NewStore B.V.",           "kvk": "58653384",  "type": "html",
     "careers_url": "https://www.newstore.com/careers/"},

    {"ind_name": "AutoBinck Group N.V.",    "kvk": "27309079",  "type": "html",
     "careers_url": "https://www.werkenbij.autobinck.com/vacatures"},

    {"ind_name": "Midtronics B.V.",         "kvk": "30156863",  "type": "html",
     "careers_url": "https://www.midtronics.com/careers/"},

    # ════════════════════════════════════════════════════
    # ── GREENHOUSE — from IND sponsors career URL discovery ──
    # ════════════════════════════════════════════════════

    {"ind_name": "Brain Corporation B.V.", "kvk": "77166051", "type": "greenhouse",
     "greenhouse_board": "braincorporation",
     "careers_url": "https://boards.greenhouse.io/braincorporation"},

    {"ind_name": "Dataiku B.V.", "kvk": "83000828", "type": "greenhouse",
     "greenhouse_board": "dataiku",
     "careers_url": "https://job-boards.greenhouse.io/dataiku"},

    {"ind_name": "Pure Storage Netherlands B.V.", "kvk": "57378169", "type": "greenhouse",
     "greenhouse_board": "purestorage",
     "careers_url": "https://job-boards.greenhouse.io/purestorage"},

    # ════════════════════════════════════════════════════
    # ── WORKDAY — from IND sponsors career URL discovery ─
    # ════════════════════════════════════════════════════

    {"ind_name": "Cadence Design Systems B.V.", "kvk": "17065436", "type": "workday",
     "workday_tenant": "cadence", "workday_board": "External_Careers", "workday_num": "1",
     "careers_url": "https://cadence.wd1.myworkdayjobs.com/External_Careers"},

    {"ind_name": "Genesys Cloud Services B.V.", "kvk": "24293219", "type": "workday",
     "workday_tenant": "genesys", "workday_board": "Genesys", "workday_num": "1",
     "careers_url": "https://genesys.wd1.myworkdayjobs.com/en-US/Genesys"},

    {"ind_name": "Red Hat B.V.", "kvk": "32110594", "type": "workday",
     "workday_tenant": "redhat", "workday_board": "Jobs", "workday_num": "5",
     "careers_url": "https://redhat.wd5.myworkdayjobs.com/Jobs"},

    {"ind_name": "Rocket Software B.V.", "kvk": "23064120", "type": "workday",
     "workday_tenant": "rocket", "workday_board": "rocket_careers", "workday_num": "5",
     "careers_url": "https://rocket.wd5.myworkdayjobs.com/rocket_careers"},

    {"ind_name": "Rockwell Automation B.V.", "kvk": "24325789", "type": "workday",
     "workday_tenant": "rockwellautomation", "workday_board": "External_Rockwell_Automation", "workday_num": "1",
     "careers_url": "https://rockwellautomation.wd1.myworkdayjobs.com/External_Rockwell_Automation"},

    {"ind_name": "Stibo Systems B.V.", "kvk": "56248733", "type": "workday",
     "workday_tenant": "stibosystems", "workday_board": "careers-at-stibo-systems", "workday_num": "3",
     "careers_url": "https://stibosystems.wd3.myworkdayjobs.com/careers-at-stibo-systems"},

    {"ind_name": "Swisscom", "kvk": "", "type": "workday",
     "workday_tenant": "swisscom", "workday_board": "SwisscomExternalCareers", "workday_num": "103",
     "careers_url": "https://swisscom.wd103.myworkdayjobs.com/SwisscomExternalCareers"},

    # ════════════════════════════════════════════════════
    # ── GREENHOUSE — top NL employers (probed active Apr 2026) ──
    # ════════════════════════════════════════════════════

    {"ind_name": "Amplitude Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "amplitude",
     "careers_url": "https://job-boards.greenhouse.io/amplitude"},

    {"ind_name": "Pendo Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "pendo",
     "careers_url": "https://job-boards.greenhouse.io/pendo"},

    {"ind_name": "PagerDuty Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "pagerduty",
     "careers_url": "https://job-boards.greenhouse.io/pagerduty"},

    {"ind_name": "Netlify Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "netlify",
     "careers_url": "https://job-boards.greenhouse.io/netlify"},

    {"ind_name": "CircleCI Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "circleci",
     "careers_url": "https://job-boards.greenhouse.io/circleci"},

    {"ind_name": "New Relic Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "newrelic",
     "careers_url": "https://job-boards.greenhouse.io/newrelic"},

    {"ind_name": "Fivetran Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "fivetran",
     "careers_url": "https://job-boards.greenhouse.io/fivetran"},

    {"ind_name": "Braze Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "braze",
     "careers_url": "https://job-boards.greenhouse.io/braze"},

    {"ind_name": "Ebury Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "ebury",
     "careers_url": "https://job-boards.greenhouse.io/ebury"},

    {"ind_name": "Twilio Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "twilio",
     "careers_url": "https://job-boards.greenhouse.io/twilio"},

    {"ind_name": "MongoDB Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "mongodb",
     "careers_url": "https://job-boards.greenhouse.io/mongodb"},

    {"ind_name": "Okta Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "okta",
     "careers_url": "https://job-boards.greenhouse.io/okta"},

    {"ind_name": "Mirakl Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "mirakl",
     "careers_url": "https://job-boards.greenhouse.io/mirakl"},

    {"ind_name": "trivago Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "trivago",
     "careers_url": "https://job-boards.greenhouse.io/trivago"},

    {"ind_name": "Celonis Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "celonis",
     "careers_url": "https://job-boards.greenhouse.io/celonis"},

    {"ind_name": "Rubrik Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "rubrik",
     "careers_url": "https://job-boards.greenhouse.io/rubrik"},

    {"ind_name": "Luno Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "luno",
     "careers_url": "https://job-boards.greenhouse.io/luno"},

    {"ind_name": "Dashlane Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "dashlane",
     "careers_url": "https://job-boards.greenhouse.io/dashlane"},

    {"ind_name": "Duolingo Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "duolingo",
     "careers_url": "https://job-boards.greenhouse.io/duolingo"},

    {"ind_name": "Cybereason Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "cybereason",
     "careers_url": "https://job-boards.greenhouse.io/cybereason"},

    {"ind_name": "Salsify Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "salsify",
     "careers_url": "https://job-boards.greenhouse.io/salsify"},

    {"ind_name": "HousingAnywhere Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "housinganywhere",
     "careers_url": "https://job-boards.greenhouse.io/housinganywhere"},

    {"ind_name": "Cognite Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "cognite",
     "careers_url": "https://job-boards.greenhouse.io/cognite"},

    {"ind_name": "HelloFresh Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "hellofresh",
     "careers_url": "https://job-boards.greenhouse.io/hellofresh"},

    {"ind_name": "Marqeta Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "marqeta",
     "careers_url": "https://job-boards.greenhouse.io/marqeta"},

    {"ind_name": "Axon Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "axon",
     "careers_url": "https://job-boards.greenhouse.io/axon"},

    {"ind_name": "Starburst Data Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "starburst",
     "careers_url": "https://job-boards.greenhouse.io/starburst"},

    {"ind_name": "Cloudflare Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "cloudflare",
     "careers_url": "https://job-boards.greenhouse.io/cloudflare"},

    # ════════════════════════════════════════════════════
    # ── NEWLY ADDED (Apr 2026) ────────────────────────
    # ════════════════════════════════════════════════════

    # Greenhouse — confirmed slugs
    {"ind_name": "Mews Systems B.V.",          "kvk": "", "type": "greenhouse",
     "greenhouse_board": "mewssystems",
     "careers_url": "https://job-boards.greenhouse.io/mewssystems"},

    {"ind_name": "Bird Netherlands B.V.",      "kvk": "", "type": "greenhouse",
     "greenhouse_board": "bird",
     "careers_url": "https://job-boards.greenhouse.io/bird"},

    {"ind_name": "Bloomreach Netherlands B.V.", "kvk": "", "type": "greenhouse",
     "greenhouse_board": "bloomreach",
     "careers_url": "https://job-boards.greenhouse.io/bloomreach"},

    {"ind_name": "Alma B.V.",                  "kvk": "", "type": "greenhouse",
     "greenhouse_board": "alma",
     "careers_url": "https://job-boards.greenhouse.io/alma"},

    {"ind_name": "Happening B.V.",             "kvk": "", "type": "greenhouse",
     "greenhouse_board": "happening",
     "careers_url": "https://job-boards.greenhouse.io/happening"},

    {"ind_name": "Guerrilla Games B.V.",       "kvk": "", "type": "greenhouse",
     "greenhouse_board": "guerrilla-games",
     "careers_url": "https://job-boards.greenhouse.io/guerrilla-games"},

    # Workday — confirmed tenants with NL engineering roles
    {"ind_name": "ING Bank N.V.",              "kvk": "33031431", "type": "workday",
     "workday_tenant": "ing", "workday_board": "ICSNLDGEN", "workday_num": "3",
     "careers_url": "https://ing.wd3.myworkdayjobs.com/ICSNLDGEN"},

    {"ind_name": "Rabobank Nederland",         "kvk": "30046259", "type": "workday",
     "workday_tenant": "rabobank", "workday_board": "jobs", "workday_num": "3",
     "careers_url": "https://rabobank.wd3.myworkdayjobs.com/jobs"},

    {"ind_name": "Signify Netherlands B.V.",   "kvk": "17058497", "type": "workday",
     "workday_tenant": "lighting", "workday_board": "jobs-and-careers", "workday_num": "3",
     "careers_url": "https://lighting.wd3.myworkdayjobs.com/jobs-and-careers"},

    {"ind_name": "Philips Electronics Nederland B.V.", "kvk": "17065965", "type": "workday",
     "workday_tenant": "philips", "workday_board": "jobs-and-careers", "workday_num": "3",
     "careers_url": "https://philips.wd3.myworkdayjobs.com/jobs-and-careers"},

    {"ind_name": "Wolters Kluwer Nederland B.V.", "kvk": "30096468", "type": "workday",
     "workday_tenant": "wk", "workday_board": "External", "workday_num": "3",
     "careers_url": "https://wk.wd3.myworkdayjobs.com/External"},

    {"ind_name": "Vanderlande Industries B.V.", "kvk": "17121653", "type": "workday",
     "workday_tenant": "vanderlande", "workday_board": "careers", "workday_num": "3",
     "careers_url": "https://vanderlande.wd3.myworkdayjobs.com/careers"},

    # Lever — EU endpoint
    {"ind_name": "Prosus N.V.",                "kvk": "74449516", "type": "lever",
     "lever_board": "prosus", "lever_base": "https://api.eu.lever.co/v0/postings",
     "careers_url": "https://jobs.eu.lever.co/prosus"},

    # SmartRecruiters
    {"ind_name": "KPN N.V.",                   "kvk": "27124701", "type": "smartrecruiters",
     "smartrecruiters_id": "KPN",
     "careers_url": "https://careers.smartrecruiters.com/KPN"},

    # Ashby
    {"ind_name": "Tebi B.V.",                  "kvk": "", "type": "ashby",
     "ashby_board": "tebi",
     "careers_url": "https://jobs.ashbyhq.com/tebi"},

    {"ind_name": "Lemonade B.V.",              "kvk": "", "type": "ashby",
     "ashby_board": "Lemonade",
     "careers_url": "https://jobs.ashbyhq.com/Lemonade"},

    # Recruitee
    {"ind_name": "NewCold B.V.",               "kvk": "", "type": "recruitee",
     "recruitee_board": "newcold3",
     "careers_url": "https://newcold3.recruitee.com/"},

    {"ind_name": "Aquablu B.V.",               "kvk": "", "type": "recruitee",
     "recruitee_board": "aquablu",
     "careers_url": "https://aquablu.recruitee.com/"},

    # HTML/SPA — custom careers sites
    # de Bijenkorf: using Magazijn De Bijenkorf B.V. entry above with KvK 33116577

    {"ind_name": "Cooperative VGZ U.A.",       "kvk": "16062596", "type": "spa",
     "careers_url": "https://werkenbijvgz.nl/vacature-overzicht/"},

    {"ind_name": "iO Digital B.V.",            "kvk": "", "type": "html",
     "careers_url": "https://www.iodigital.com/en/careers/jobs"},

    {"ind_name": "Hypersolid B.V.",            "kvk": "", "type": "recruitee",
     "recruitee_board": "hypersolid",
     "careers_url": "https://hypersolid.recruitee.com/"},

]

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    # Args: optional company keywords + --force to bypass 24h cache + --no-linkedin to skip LinkedIn
    args = sys.argv[1:]
    force_crawl = '--force' in args
    no_linkedin = '--no-linkedin' in args
    workers = next((int(a.split('=')[1]) for a in args if a.startswith('--workers=')), 20)
    filter_kws = [a.lower() for a in args if not a.startswith('--')]

    companies_to_crawl = [
        c for c in COMPANIES
        if not filter_kws or any(kw in c['ind_name'].lower() for kw in filter_kws)
    ]

    if not companies_to_crawl and 'linkedin' not in filter_kws:
        print(f'No companies matched: {filter_kws}')
        sys.exit(1)

    if force_crawl:
        print(f'🚀 Crawling {len(companies_to_crawl)} companies (--force, ignoring cache, --workers={workers})...')
    else:
        print(f'🚀 Crawling {len(companies_to_crawl)} companies (cache <24h reused; use --force to bypass, --workers=N to tune)...')
    jobs = crawl(companies_to_crawl, force=force_crawl, workers=workers) if companies_to_crawl else []

    # LinkedIn scrape (only on full crawl or explicit 'linkedin' keyword; skipped with --no-linkedin)
    linkedin_jobs = []
    if not no_linkedin and (not filter_kws or 'linkedin' in filter_kws):
        print('\n🔗 Fetching LinkedIn jobs via JobSpy...')
        linkedin_jobs = fetch_linkedin_jobs(force=force_crawl)

    if not jobs and not linkedin_jobs and not filter_kws:
        print('\n⚠  No matching jobs found. Check TARGET_ROLES / NL_LOCATIONS filters.')
        sys.exit(0)

    # If crawling a subset, merge portal jobs with existing (keep other companies' jobs)
    if filter_kws and 'linkedin' not in filter_kws:
        try:
            with open(JOBS_FILE, 'r', encoding='utf-8') as f:
                existing = json.load(f)
            crawled_names = {c['ind_name'] for c in companies_to_crawl}
            existing = [j for j in existing if j.get('company') not in crawled_names]
            jobs = existing + jobs
            print(f'\n🔀 Merged with existing: {len(jobs)} portal jobs')
        except FileNotFoundError:
            pass

    if companies_to_crawl:
        # Full crawl (no keyword filter) can safely retain/expire jobs across runs;
        # a subset crawl only touched some companies, so only stamp first_seen.
        jobs = _stamp_and_retain(jobs, JOBS_FILE, retain=not filter_kws)
        save_jobs(jobs)
        try:
            update_sponsors_careers_url(companies_to_crawl)
        except Exception as e:
            print(f'⚠  update_sponsors_careers_url skipped: {e}')
    else:
        # LinkedIn-only run: don't touch crawled_jobs.json, just rebuild dashboard
        _inject_jobs_into_dashboard(json.load(open(JOBS_FILE)) if os.path.exists(JOBS_FILE) else [])

    print('\n📊 Summary:')
    from collections import Counter
    for company, count in Counter(j['company'] for j in jobs).items():
        print(f'  {company}: {count} jobs')
    dated = sum(1 for j in jobs if j.get('date_posted'))
    emailed = sum(1 for j in jobs if j.get('recruiter_email'))
    print(f'\n  Date posted available: {dated}/{len(jobs)}')
    print(f'  Recruiter email found: {emailed}/{len(jobs)}')
