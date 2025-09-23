#!/usr/bin/python

import asyncio
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import signal
import sys


async def fetch_html(page, url: str) -> str | None:
    """Fetch HTML content of a page using an existing Playwright page instance."""
    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        return await page.content()
    except Exception as e:
        print(f"❌ Error scraping {url}: {str(e)}")
        return None


def extract_internal_links(html: str, base_url: str) -> set[str]:
    """Extract internal links from a page."""
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


async def crawl_site(start_url: str, max_pages: int = 500, concurrency: int = 5) -> set[str]:
    """Crawl all internal links recursively with concurrency."""
    from playwright.async_api import async_playwright

    visited = set()
    to_visit = {start_url}
    all_links = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        semaphore = asyncio.Semaphore(concurrency)

        async def process_url(url):
            nonlocal visited, to_visit, all_links
            async with semaphore:
                if url in visited:
                    return
                visited.add(url)

                page = await context.new_page()
                html = await fetch_html(page, url)
                await page.close()

                if not html:
                    return

                links = extract_internal_links(html, url)
                new_links = links - visited
                to_visit.update(new_links)
                all_links.update(links)

                print(f"🌐 {url} -> {len(links)} links (total {len(all_links)})")

        try:
            while to_visit and len(visited) < max_pages:
                batch = list(to_visit - visited)[:concurrency]
                if not batch:
                    break
                await asyncio.gather(*(process_url(u) for u in batch))
        except asyncio.CancelledError:
            print("\n⚠️ Crawl cancelled.")
        finally:
            await browser.close()

    return all_links


if __name__ == "__main__":
    url = "https://www.unilever.com/news/news-search/2025/unilevers-100-accelerator-partnership-unlocks-ai-innovation-across-supply-chain/"

    try:
        results = asyncio.run(crawl_site(url, max_pages=500, concurrency=5))
    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user (Ctrl+C). Printing collected links...\n")
        # results may not exist if interrupted too early
        try:
            results
        except NameError:
            results = set()

    print("\n✅ Final unique internal links:")
    for link in sorted(results):
        print(link)
