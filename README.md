#!/usr/bin/env python3
"""
lead_enricher.py
Input: CSV with headers (company_name, website, contact_name, title) - website optional
Output: enriched CSV with ai_readiness_score, tech_stack, ai_signals_count, has_jobs, contact_emails, homepage_title, homepage_meta_desc, notes
"""

import csv
import re
import time
import argparse
import concurrent.futures
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
import tldextract

# --- Config ---
USER_AGENT = "CapraeLeadEnricher/1.0 (+https://caprae.example) PythonRequests"
TIMEOUT = 10
MAX_WORKERS = 8
SLEEP_BETWEEN_REQUESTS = 0.2  # polite
AI_KEYWORDS = [
    "ai", "artificial intelligence", "machine learning", "ml", "data scientist",
    "data science", "predictive", "neural network", "deep learning", "nlp",
    "computer vision", "automation", "predictive analytics", "analytics platform",
    "mlops", "model", "ai platform"
]
JOB_LINK_KEYWORDS = ["career", "careers", "jobs", "openings", "vacancy", "hiring", "join-us", "joinus"]

# --- Helpers ---
def normalize_url(url):
    if not url:
        return None
    url = url.strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return "https://" + url if "." in url else None

def fetch(url):
    try:
        headers = {"User-Agent": USER_AGENT}
        r = requests.get(url, headers=headers, timeout=TIMEOUT, allow_redirects=True)
        r.raise_for_status()
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        return r.text, r.url
    except Exception:
        return None, None

def find_emails(text):
    if not text:
        return []
    emails = set(re.findall(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", text))
    return list(emails)

def detect_tech(html_lower):
    techs = set()
    if "wp-content" in html_lower or "wordpress" in html_lower or "wp-json" in html_lower:
        techs.add("WordPress")
    if "cdn.shopify" in html_lower or "myshopify.com" in html_lower:
        techs.add("Shopify")
    if "woocommerce" in html_lower:
        techs.add("WooCommerce")
    if "react" in html_lower or "data-reactroot" in html_lower:
        techs.add("React")
    if "next.js" in html_lower or "/_next/" in html_lower:
        techs.add("Next.js")
    if "angular" in html_lower or "ng-" in html_lower:
        techs.add("Angular")
    if "vue" in html_lower:
        techs.add("Vue.js")
    if "django" in html_lower or "wsgi" in html_lower:
        techs.add("Django")
    if "flask" in html_lower:
        techs.add("Flask")
    if "docker" in html_lower:
        techs.add("Docker")
    if "aws" in html_lower or "amazonaws" in html_lower:
        techs.add("AWS")
    if "azure" in html_lower:
        techs.add("Azure")
    if "googleapis" in html_lower or "google-analytics" in html_lower:
        techs.add("GCP/Google")
    return list(techs)

def count_ai_keywords(text_lower):
    c = 0
    for kw in AI_KEYWORDS:
        if kw in text_lower:
            c += 1
    return c

def find_links(soup, base_url):
    links = []
    for a in soup.find_all("a", href=True):
        try:
            href = a.get("href").strip()
            # make absolute
            href_abs = urljoin(base_url, href)
            links.append((href_abs.lower(), (a.get_text() or "").lower()))
        except Exception:
            pass
    return links

# --- Scoring ---
def compute_score(signals):
    score = 0
    # website existence
    if signals.get("homepage_html"):
        score += 10
    # AI keywords presence: up to 30
    ai_count = signals.get("ai_signals_count", 0)
    score += min(30, ai_count * 6)
    # modern tech stack
    techs = signals.get("tech_stack", [])
    modern_stack_hits = any(t in ["React", "Next.js", "Docker", "AWS", "GCP/Google", "Flask", "Django"] for t in techs)
    if modern_stack_hits:
        score += 15
    # jobs with AI mentions
    if signals.get("has_jobs"):
        score += 20
        if signals.get("jobs_ai_mentions"):
            score += 5
    # pricing / commercial page
    if signals.get("has_pricing"):
        score += 10
    # contact email
    if signals.get("contact_emails"):
        score += 5
    return min(100, int(score))

# --- Worker ---
def enrich_lead(row):
    company = row.get("company_name") or ""
    website_raw = row.get("website") or ""
    website = normalize_url(website_raw)
    result = {
        **row,
        "homepage_title": "",
        "homepage_meta_desc": "",
        "ai_signals_count": 0,
        "tech_stack": "",
        "contact_emails": "",
        "has_jobs": False,
        "jobs_ai_mentions": False,
        "has_pricing": False,
        "notes": "",
        "ai_readiness_score": 0,
    }

    if not website:
        result["notes"] = "No website supplied"
        result["ai_readiness_score"] = 0
        return result

    html, final_url = fetch(website)
    if not html:
        result["notes"] = "Failed to fetch homepage"
        return result

    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    meta = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", attrs={"property":"og:description"})
    meta_desc = meta.get("content","").strip() if meta else ""
    text_lower = soup.get_text(" ", strip=True).lower()
    ai_count = count_ai_keywords(text_lower)
    techs = detect_tech(html.lower())
    emails = find_emails(html)
    links = find_links(soup, final_url)

    # detect pricing
    has_pricing = any("pricing" in href or "pricing" in txt for href, txt in links)

    # detect jobs/careers links, fetch them and search for ai keywords
    job_links = [href for href, txt in links if any(k in href or k in txt for k in JOB_LINK_KEYWORDS)]
    has_jobs = False
    jobs_ai_mentions = False
    if job_links:
        has_jobs = True
        # Fetch up to 2 job links
        for jl in job_links[:2]:
            jhtml, _ = fetch(jl)
            if jhtml:
                jl_lower = jhtml.lower()
                if count_ai_keywords(jl_lower) > 0:
                    jobs_ai_mentions = True
                    break

    # create signals dict
    signals = {
        "homepage_html": html,
        "ai_signals_count": ai_count,
        "tech_stack": techs,
        "contact_emails": emails,
        "has_jobs": has_jobs,
        "jobs_ai_mentions": jobs_ai_mentions,
        "has_pricing": has_pricing,
    }

    score = compute_score(signals)

    # fill result
    result.update({
        "homepage_title": title,
        "homepage_meta_desc": meta_desc,
        "ai_signals_count": ai_count,
        "tech_stack": ",".join(techs),
        "contact_emails": ",".join(emails),
        "has_jobs": str(has_jobs),
        "jobs_ai_mentions": str(jobs_ai_mentions),
        "has_pricing": str(has_pricing),
        "ai_readiness_score": score,
        "notes": "ok"
    })
    return result

# --- CSV I/O and main ---
def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [r for r in reader]
    return rows

def write_csv(path, rows):
    if not rows:
        print("No rows to write.")
        return
    keys = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

def main():
    parser = argparse.ArgumentParser(description="Enrich leads with AI readiness signals.")
    parser.add_argument("--input", "-i", required=True, help="Input CSV path")
    parser.add_argument("--output", "-o", required=True, help="Output CSV path")
    parser.add_argument("--workers", "-w", type=int, default=MAX_WORKERS, help="Parallel workers")
    args = parser.parse_args()

    rows = read_csv(args.input)
    enriched = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(enrich_lead, row) for row in rows]
        for fut in concurrent.futures.as_completed(futures):
            try:
                enriched.append(fut.result())
            except Exception as e:
                print("Error processing a row:", e)

    write_csv(args.output, enriched)
    print(f"Wrote enriched data to {args.output}")

if __name__ == "__main__":
    main()
