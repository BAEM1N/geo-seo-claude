#!/usr/bin/env python3
"""
Lightweight GEO audit runner for local uv/Codex workflows.

This script orchestrates the repository's existing utilities into a single
JSON + Markdown output suitable for OMX/Codex usage.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from brand_scanner import generate_brand_report
from citability_scorer import analyze_page_citability
from fetch_page import crawl_sitemap, fetch_page, fetch_robots_txt
from llmstxt_generator import validate_llmstxt


CRITICAL_AI_CRAWLERS = [
    "GPTBot",
    "OAI-SearchBot",
    "ChatGPT-User",
    "ClaudeBot",
    "PerplexityBot",
]


def normalize_url(value: str) -> str:
    if not re.match(r"^https?://", value):
        value = f"https://{value}"
    return value.rstrip("/")


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def domain_from_url(url: str) -> str:
    return urlparse(url).netloc


def infer_brand_name(url: str, homepage: dict) -> str:
    title = homepage.get("title") or ""
    if title:
        for sep in ["|", "—", "-", "–", ":"]:
            if sep in title:
                first = title.split(sep)[0].strip()
                if first:
                    return first
        return title.strip()

    domain = domain_from_url(url)
    host = domain.split(".")
    if len(host) >= 3 and host[0] not in {"www"}:
        candidate = host[0]
    else:
        candidate = host[-2] if len(host) >= 2 else host[0]
    return candidate.replace("-", " ").title()


def extract_schema_types(item) -> list[str]:
    found: list[str] = []
    if isinstance(item, list):
        for sub in item:
            found.extend(extract_schema_types(sub))
    elif isinstance(item, dict):
        at_type = item.get("@type")
        if isinstance(at_type, list):
            found.extend(str(x) for x in at_type)
        elif at_type:
            found.append(str(at_type))
        for value in item.values():
            if isinstance(value, (dict, list)):
                found.extend(extract_schema_types(value))
    return found


def summarize_page(page: dict) -> dict:
    schema_types = extract_schema_types(page.get("structured_data", []))
    return {
        "url": page.get("url"),
        "status_code": page.get("status_code"),
        "title": page.get("title"),
        "word_count": page.get("word_count", 0),
        "h1_count": len(page.get("h1_tags", [])),
        "internal_link_count": len(page.get("internal_links", [])),
        "external_link_count": len(page.get("external_links", [])),
        "image_count": len(page.get("images", [])),
        "images_missing_alt": sum(
            1 for img in page.get("images", []) if not (img.get("alt") or "").strip()
        ),
        "schema_types": sorted(set(schema_types)),
        "has_meta_description": bool(page.get("description")),
        "has_ssr_content": page.get("has_ssr_content", True),
        "security_headers_present": [
            key for key, value in page.get("security_headers", {}).items() if value
        ],
        "errors": page.get("errors", []),
    }


def eeat_signals(page: dict) -> dict:
    text = (page.get("text_content") or "").lower()
    heading_text = " ".join(h.get("text", "") for h in page.get("heading_structure", []))
    combined = f"{text} {heading_text.lower()}"
    signals = {
        "author_signal": any(k in combined for k in ["author", "written by", "editor", "byline"]),
        "about_signal": any(k in combined for k in ["about", "company", "team", "mission"]),
        "contact_signal": any(k in combined for k in ["contact", "email", "phone", "문의", "연락처"]),
        "date_signal": bool(re.search(r"\b20\d{2}\b", combined)),
        "faq_signal": "faq" in combined or "frequently asked" in combined,
        "trust_signal": any(k in combined for k in ["privacy", "terms", "security", "trust", "policy"]),
    }
    return signals


def average(values: list[float]) -> float:
    return round(sum(values) / len(values), 1) if values else 0.0


def clamp_score(value: float) -> int:
    return max(0, min(100, int(round(value))))


def compute_scores(
    homepage_summary: dict,
    robots: dict,
    llms: dict,
    citability_results: list[dict],
    page_summaries: list[dict],
    brand_report: dict,
    eeat_summary: dict,
) -> dict:
    citability_score = average(
        [
            result.get("average_citability_score", 0)
            for result in citability_results
            if "average_citability_score" in result
        ]
    )

    robot_status = robots.get("ai_crawler_status", {})
    robot_points = 0
    for crawler in CRITICAL_AI_CRAWLERS:
        status = robot_status.get(crawler, "NOT_MENTIONED")
        if status in {"ALLOWED", "ALLOWED_BY_DEFAULT", "NOT_MENTIONED", "NO_ROBOTS_TXT"}:
            robot_points += 20
        elif status == "PARTIALLY_BLOCKED":
            robot_points += 8
    robots_score = clamp_score(robot_points)

    llms_score = 80 if llms.get("exists") and llms.get("format_valid") else (45 if llms.get("exists") else 15)

    technical_base = 0
    if homepage_summary.get("status_code") == 200:
        technical_base += 25
    elif homepage_summary.get("status_code"):
        technical_base += 10
    if homepage_summary.get("has_ssr_content"):
        technical_base += 20
    technical_base += min(len(homepage_summary.get("security_headers_present", [])) * 8, 32)
    if robots.get("exists"):
        technical_base += 10
    if robots.get("sitemaps"):
        technical_base += 13
    technical_score = clamp_score(technical_base)

    schema_counter = Counter()
    for summary in page_summaries:
        schema_counter.update(summary.get("schema_types", []))
    schema_hits = 0
    important_types = {"Organization", "WebSite", "WebPage", "Article", "FAQPage", "BreadcrumbList", "TechArticle"}
    for schema_type in important_types:
        if schema_type in schema_counter:
            schema_hits += 1
    schema_score = clamp_score(min(schema_hits * 14, 84) + (10 if schema_counter else 0))

    eeat_score = 0
    if eeat_summary["author_pages"] > 0:
        eeat_score += 20
    if eeat_summary["about_pages"] > 0:
        eeat_score += 20
    if eeat_summary["contact_pages"] > 0:
        eeat_score += 15
    if eeat_summary["dated_pages"] > 0:
        eeat_score += 15
    if eeat_summary["trust_pages"] > 0:
        eeat_score += 15
    if eeat_summary["faq_pages"] > 0:
        eeat_score += 15
    eeat_score = clamp_score(eeat_score)

    wiki = brand_report.get("platforms", {}).get("wikipedia", {})
    brand_score = 25
    if wiki.get("has_wikipedia_page"):
        brand_score += 30
    if wiki.get("has_wikidata_entry"):
        brand_score += 25
    brand_score += 10 if homepage_summary.get("internal_link_count", 0) >= 10 else 0
    brand_score += 10 if homepage_summary.get("has_meta_description") else 0
    brand_score = clamp_score(brand_score)

    platform_score = clamp_score((robots_score * 0.45) + (llms_score * 0.35) + (schema_score * 0.20))

    overall = clamp_score(
        (citability_score * 0.25)
        + (brand_score * 0.20)
        + (eeat_score * 0.20)
        + (technical_score * 0.15)
        + (schema_score * 0.10)
        + (platform_score * 0.10)
    )

    return {
        "ai_citability": clamp_score(citability_score),
        "brand_authority": brand_score,
        "content_eeat": eeat_score,
        "technical": technical_score,
        "schema": schema_score,
        "platform_optimization": platform_score,
        "ai_crawler_access": robots_score,
        "llmstxt": llms_score,
        "overall": overall,
    }


def classify_rating(score: int) -> str:
    if score >= 90:
        return "Excellent"
    if score >= 75:
        return "Good"
    if score >= 60:
        return "Fair"
    if score >= 40:
        return "Poor"
    return "Critical"


def build_findings(scores: dict, robots: dict, llms: dict, homepage_summary: dict, eeat_summary: dict, schema_counter: Counter) -> list[dict]:
    findings: list[dict] = []

    blocked_critical = [
        crawler
        for crawler in CRITICAL_AI_CRAWLERS
        if robots.get("ai_crawler_status", {}).get(crawler) in {"BLOCKED", "BLOCKED_BY_WILDCARD"}
    ]
    if blocked_critical:
        findings.append(
            {
                "severity": "critical",
                "title": "Critical AI crawlers are blocked",
                "description": f"Blocked crawlers: {', '.join(blocked_critical)}.",
            }
        )

    if not llms.get("exists"):
        findings.append(
            {
                "severity": "high",
                "title": "llms.txt is missing",
                "description": "No llms.txt file was found at the site root, limiting explicit AI crawler guidance.",
            }
        )

    if not schema_counter:
        findings.append(
            {
                "severity": "high",
                "title": "No structured data detected",
                "description": "The sampled pages exposed no JSON-LD schema, which weakens entity understanding for AI and search systems.",
            }
        )

    if not homepage_summary.get("has_meta_description"):
        findings.append(
            {
                "severity": "medium",
                "title": "Homepage meta description is missing",
                "description": "The homepage does not expose a meta description, reducing clarity for search snippets and AI previews.",
            }
        )

    if not homepage_summary.get("has_ssr_content", True):
        findings.append(
            {
                "severity": "high",
                "title": "Possible client-side rendering issue",
                "description": "The homepage appears to rely on a thin app shell, which can reduce crawler visibility.",
            }
        )

    if scores["ai_citability"] < 55:
        findings.append(
            {
                "severity": "medium",
                "title": "Citability needs stronger answer blocks",
                "description": "Sampled content scored below the desirable citability range, suggesting a need for more self-contained, directly answerable sections.",
            }
        )

    if eeat_summary["author_pages"] == 0:
        findings.append(
            {
                "severity": "medium",
                "title": "Author or expert attribution is weak",
                "description": "The sampled pages did not surface clear author/byline signals.",
            }
        )

    if eeat_summary["trust_pages"] == 0:
        findings.append(
            {
                "severity": "low",
                "title": "Trust/support signals are thin in sampled pages",
                "description": "Privacy, security, terms, or equivalent trust signals were not prominent in the sampled content set.",
            }
        )

    return findings


def build_markdown(report: dict) -> str:
    scores = report["scores"]
    findings = report["findings"]
    top_pages = report["sampled_pages"][:5]
    top_blocks = report.get("top_citable_blocks", [])[:5]

    lines = [
        f"# GEO Audit Report: {report['brand_name']}",
        "",
        f"- Audit date: {report['audit_date']}",
        f"- URL: {report['url']}",
        f"- Rating: **{classify_rating(scores['overall'])}**",
        f"- Overall GEO score: **{scores['overall']}/100**",
        f"- Pages sampled: {report['sample_page_count']}",
        "",
        "## Score breakdown",
        "",
        "| Category | Score |",
        "|---|---:|",
        f"| AI citability | {scores['ai_citability']} |",
        f"| Brand authority | {scores['brand_authority']} |",
        f"| Content E-E-A-T | {scores['content_eeat']} |",
        f"| Technical | {scores['technical']} |",
        f"| Schema | {scores['schema']} |",
        f"| Platform optimization | {scores['platform_optimization']} |",
        "",
        "## Priority findings",
        "",
    ]

    if findings:
        for finding in findings:
            lines.extend(
                [
                    f"### {finding['severity'].upper()} — {finding['title']}",
                    "",
                    finding["description"],
                    "",
                ]
            )
    else:
        lines.extend(["No major issues were detected in the sampled checks.", ""])

    lines.extend(
        [
            "## Technical snapshot",
            "",
            f"- robots.txt: {'present' if report['robots']['exists'] else 'missing'}",
            f"- Sitemap entries found: {len(report['robots'].get('sitemaps', []))}",
            f"- llms.txt: {'present' if report['llms'].get('exists') else 'missing'}",
            f"- Homepage meta description: {'yes' if report['homepage'].get('has_meta_description') else 'no'}",
            f"- Homepage SSR content: {'yes' if report['homepage'].get('has_ssr_content') else 'no'}",
            f"- Homepage security headers detected: {', '.join(report['homepage'].get('security_headers_present', [])) or 'none'}",
            "",
            "## Sampled pages",
            "",
        ]
    )

    for page in top_pages:
        lines.append(
            f"- `{page['url']}` — {page['word_count']} words, schema: {', '.join(page['schema_types']) or 'none'}"
        )

    lines.extend(["", "## Best citable blocks", ""])
    if top_blocks:
        for block in top_blocks:
            lines.extend(
                [
                    f"- **{block.get('heading') or 'Untitled'}** — {block.get('total_score', 0)}/100",
                    f"  - Preview: {block.get('preview', '')}",
                ]
            )
    else:
        lines.append("- No citable content blocks were identified in the sampled pages.")

    lines.extend(
        [
            "",
            "## Recommended next actions",
            "",
            "1. Publish or improve `llms.txt` to guide AI systems toward priority docs and product pages.",
            "2. Add or expand JSON-LD on key pages (`Organization`, `WebSite`, `BreadcrumbList`, and content-specific schema).",
            "3. Restructure important pages into short, self-contained answer blocks that are easy for AI systems to quote.",
            "4. Surface stronger trust and expertise signals (author info, update dates, team/company context, policy/security links).",
        ]
    )

    return "\n".join(lines).strip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local GEO audit")
    parser.add_argument("url", help="Target URL or domain")
    parser.add_argument("--mode", choices=["quick", "full"], default="quick")
    parser.add_argument(
        "--outdir",
        default=str(Path.home() / "Outputs" / "reports"),
        help="Directory for generated JSON/Markdown reports",
    )
    parser.add_argument("--max-pages", type=int, default=5, help="Max sampled pages in full mode")
    args = parser.parse_args()

    url = normalize_url(args.url)
    outdir = Path(args.outdir).expanduser()
    outdir.mkdir(parents=True, exist_ok=True)

    homepage = fetch_page(url)
    homepage_summary = summarize_page(homepage)
    brand_name = infer_brand_name(url, homepage)
    robots = fetch_robots_txt(url)
    llms = validate_llmstxt(url)

    sampled_urls = [url]
    if args.mode == "full":
        sitemap_urls = crawl_sitemap(url, max_pages=max(args.max_pages, 5))
        for candidate in sitemap_urls:
            if candidate not in sampled_urls:
                sampled_urls.append(candidate)
            if len(sampled_urls) >= args.max_pages:
                break

    page_summaries = []
    eeat_signals_by_page = []
    citability_results = []
    top_citable_blocks = []

    for sample_url in sampled_urls:
        page = homepage if sample_url == url else fetch_page(sample_url)
        summary = summarize_page(page)
        page_summaries.append(summary)
        eeat_signals_by_page.append(eeat_signals(page))

        citability = analyze_page_citability(sample_url)
        citability_results.append(citability)
        top_citable_blocks.extend(citability.get("top_5_citable", []))

    eeat_summary = {
        "author_pages": sum(1 for s in eeat_signals_by_page if s["author_signal"]),
        "about_pages": sum(1 for s in eeat_signals_by_page if s["about_signal"]),
        "contact_pages": sum(1 for s in eeat_signals_by_page if s["contact_signal"]),
        "dated_pages": sum(1 for s in eeat_signals_by_page if s["date_signal"]),
        "faq_pages": sum(1 for s in eeat_signals_by_page if s["faq_signal"]),
        "trust_pages": sum(1 for s in eeat_signals_by_page if s["trust_signal"]),
    }

    schema_counter = Counter()
    for summary in page_summaries:
        schema_counter.update(summary.get("schema_types", []))

    brand_report = generate_brand_report(brand_name, domain_from_url(url))
    scores = compute_scores(
        homepage_summary=homepage_summary,
        robots=robots,
        llms=llms,
        citability_results=citability_results,
        page_summaries=page_summaries,
        brand_report=brand_report,
        eeat_summary=eeat_summary,
    )
    findings = build_findings(scores, robots, llms, homepage_summary, eeat_summary, schema_counter)

    audit_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    slug = slugify(domain_from_url(url))
    report = {
        "url": url,
        "brand_name": brand_name,
        "audit_date": audit_date,
        "mode": args.mode,
        "sample_page_count": len(sampled_urls),
        "scores": scores,
        "homepage": homepage_summary,
        "robots": robots,
        "llms": llms,
        "sampled_pages": page_summaries,
        "citability": citability_results,
        "top_citable_blocks": sorted(
            top_citable_blocks, key=lambda block: block.get("total_score", 0), reverse=True
        )[:10],
        "brand_report": brand_report,
        "eeat_summary": eeat_summary,
        "schema_summary": dict(schema_counter),
        "findings": findings,
    }

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    json_path = outdir / f"geo-audit-{slug}-{timestamp}.json"
    md_path = outdir / f"geo-audit-{slug}-{timestamp}.md"

    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(build_markdown(report), encoding="utf-8")

    print(
        json.dumps(
            {
                "url": url,
                "brand_name": brand_name,
                "mode": args.mode,
                "scores": scores,
                "json_report": str(json_path),
                "markdown_report": str(md_path),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
