#!/usr/bin/python

import asyncio
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import time


async def fetch_html(url: str) -> str | None:
    """
    Fetch HTML content of a page using Playwright.
    """
    from playwright.async_api import async_playwright
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            page.set_default_timeout(30000)

            await page.goto(url, wait_until="networkidle", timeout=30000)
            time.sleep(2)  # let JS finish rendering

            html_content = await page.content()
            await browser.close()
            return html_content
    except Exception as e:
        print(f"❌ Error scraping {url}: {str(e)}")
        return None


def extract_internal_links(html: str, base_url: str) -> set[str]:
    """
    Extract internal links from a page.
    """
    soup = BeautifulSoup(html, "html.parser")
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc.lower()

    internal_links = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        full_url = urljoin(base_url, href)
        parsed_href = urlparse(full_url)

        if parsed_href.scheme in ("http", "https") and parsed_href.netloc.lower() == base_domain:
            internal_links.add(full_url)

    return internal_links


async def crawl_site(start_url: str, max_pages: int = 50) -> set[str]:
    """
    Crawl all internal links recursively (BFS).
    Limits to max_pages to avoid infinite crawling.
    """
    visited = set()
    to_visit = {start_url}
    all_links = set()

    while to_visit and len(visited) < max_pages:
        current_url = to_visit.pop()
        if current_url in visited:
            continue

        print(f"🌐 Visiting: {current_url}")
        visited.add(current_url)

        html = await fetch_html(current_url)
        if not html:
            continue

        links = extract_internal_links(html, current_url)
        new_links = links - visited
        to_visit.update(new_links)
        all_links.update(links)

        print(f"   ➡ Found {len(links)} internal links (Total collected: {len(all_links)})")

    return all_links


if __name__ == "__main__":
    url = "https://www.unilever.com/news/news-search/2025/unilevers-100-accelerator-partnership-unlocks-ai-innovation-across-supply-chain/"
    results = asyncio.run(crawl_site(url, max_pages=50))  # you can increase max_pages

    print("\n✅ Final unique internal links:")
    for link in sorted(results):
        print(link)
