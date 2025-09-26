#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
combined_scraper.py
===================

A full‑stack URL extractor + content scraper that

* extracts all internal URLs from a domain
* visits each page with Playwright (single shared context)
* extracts heading‑content blocks via a tiny JS snippet
* writes incremental and final JSON files
* logs progress in real time and handles Ctrl‑C cleanly
"""

import asyncio
import json
import logging
import os
import sys
import time
import threading
from collections import deque
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
import urllib3
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.async_api import async_playwright


# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# --------------------------------------------------------------------
#  CONFIGURATION
# --------------------------------------------------------------------
CONFIG = {
    "url_extraction": {
        "max_workers": 15,
        "batch_size": 25,
        "delay": 0.05,
        "timeout": 500,
        "max_urls": 10_000,
    },
    "content_scraping": {
        "headless": True,
        "nav_timeout": 60_000,
        "max_concurrent": 3,
        "delay_between_pages": 2.0,
        "retry_attempts": 3,
    },
    "output": {
        "incremental_save": True,
        "incremental_file": "scraped_content_incremental.json",
        "final_file": "scraped_content_final.json",
        "failed_urls_file": "failed_urls.json",
    },
}

# --------------------------------------------------------------------
#  LOGGING
# --------------------------------------------------------------------
logger = logging.getLogger("combined_scraper")
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler("scraping.log", encoding="utf-8")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s - %(message)s")
)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# --------------------------------------------------------------------
#  URL EXTRACTOR
# --------------------------------------------------------------------
class URLExtractor:
    """Walk a domain and collect every internal URL."""

    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update(self._default_headers())

        self.all_urls = set()
        self.processed = set()
        self.lock = threading.Lock()

    @staticmethod
    def _default_headers():
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
                "application/signed-exchange;v=b3;q=0.7"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }

    def _is_file(self, url: str) -> bool:
        return url.lower().endswith(
            (
                ".pdf", ".ppt", ".pptx", ".doc", ".docx",
                ".xls", ".xlsx", ".zip", ".rar",
                ".jpg", ".jpeg", ".png", ".gif",
                ".mp4", ".avi", ".mp3",
            )
        )

    def get_internal_urls(self, url: str, base_domain: str) -> set:
        try:
            response = self.session.get(url, timeout=self.config["timeout"])
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            internal = set()

            for link in soup.find_all("a", href=True):
                href = link["href"]
                full_url = urljoin(url, href)
                parsed = urlparse(full_url)

                if parsed.netloc != base_domain or self._is_file(full_url):
                    continue

                clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                if parsed.query:
                    clean += f"?{parsed.query}"
                internal.add(clean)

            if self.config["delay"] > 0:
                time.sleep(self.config["delay"])

            return internal
        except Exception as exc:
            logger.exception("Error fetching %s – %s", url, exc)
            return set()

    def process_batch(self, urls_batch, base_domain):
        new_urls = set()
        with ThreadPoolExecutor(max_workers=self.config["max_workers"]) as executor:
            futures = {
                executor.submit(self.get_internal_urls, u, base_domain): u
                for u in urls_batch
            }
            for future in as_completed(futures):
                url = futures[future]
                try:
                    found = future.result()
                    with self.lock:
                        self.processed.add(url)
                        truly_new = found - self.all_urls
                        new_urls.update(truly_new)
                        self.all_urls.update(found)
                    logger.debug("✓ %s – %d found", url, len(found))
                except Exception as exc:
                    logger.error("✗ %s – %s", url, exc)
                    with self.lock:
                        self.processed.add(url)
        return new_urls

    def extract_all_urls(self, start_url):
        parsed_start = urlparse(start_url)
        base_domain = parsed_start.netloc

        to_process = deque([start_url])
        iteration = 0
        last_5_counts = []

        logger.info("Starting parallel extraction from: %s", start_url)
        logger.info("Base domain: %s", base_domain)
        logger.info(
            "Max workers: %s, Batch size: %s",
            self.config["max_workers"],
            self.config["batch_size"],
        )
        logger.info("-" * 60)

        start_time = time.time()

        while to_process:
            iteration += 1
            current_batch = []
            batch_count = min(self.config["batch_size"], len(to_process))

            for _ in range(batch_count):
                if to_process:
                    url = to_process.popleft()
                    if url not in self.processed:
                        current_batch.append(url)

            if not current_batch:
                break

            logger.info("\nIteration %d: Processing batch of %d URLs", iteration, len(current_batch))
            batch_start = time.time()

            new_urls = self.process_batch(current_batch, base_domain)

            for url in new_urls:
                if url not in self.processed:
                    to_process.append(url)

            new_count = len(new_urls)
            last_5_counts.append(new_count)

            if len(last_5_counts) > 5:
                last_5_counts.pop(0)

            logger.info("Batch completed in %.2fs", time.time() - batch_start)
            logger.info("New URLs found: %d", new_count)
            logger.info("Total unique URLs: %d", len(self.all_urls))
            logger.info("URLs in queue: %d", len(to_process))

            if len(last_5_counts) >= 3 and sum(last_5_counts[-3:]) == 0:
                logger.info("\nStopping: No new URLs found in last 3 iterations")
                break

            if len(self.all_urls) > self.config["max_urls"]:
                logger.warning("\nSafety limit reached: %d URLs found", len(self.all_urls))
                break

        logger.info("\nExtraction completed in %.2fs", time.time() - start_time)
        return sorted(self.all_urls)

# --------------------------------------------------------------------
#  CONTENT SCRAPER
# --------------------------------------------------------------------
class ContentScraper:
    """Scrape heading‑content blocks from every URL with Playwright."""

    def __init__(self, config):
        self.config = config
        self.lock = asyncio.Lock()
        self.failed_urls = []

        # Progress counters
        self.total_to_scrape = 0
        self.total_scraped = 0
        self.total_failed = 0

    @staticmethod
    def _build_headers(target_url: str) -> dict:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,"
                "application/signed-exchange;v=b3;q=0.7"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "sec-ch-ua": '"Chromium";v="123", "Not:A-Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
        }

    async def _auto_scroll(self, page):
        """Slow scroll to trigger lazy loading."""
        try:
            await page.evaluate(
                """
                async () => {
                    const step = 300;
                    const delay = ms => new Promise(r => setTimeout(r, ms));
                    let pos = 0;
                    const height = document.body.scrollHeight;
                    while (pos < height) {
                        window.scrollBy(0, step);
                        pos += step;
                        await delay(200);
                    }
                    window.scrollTo(0,0);
                }
                """
            )
        except Exception:
            logger.debug("auto_scroll: ignore scroll script failure", exc_info=True)

    async def _extract_structured_content(self, page):
        """Execute the JS snippet that returns heading‑content blocks."""
        js = r"""
        () => {
          const containers = Array.from(document.querySelectorAll(
            "article.uol-c-card, main, section.uol-c-section, div.uol-c-section__inner"
          ));
          const results = [];
          const seen = new Set();

          function normalizeText(s){
              return (s || "").replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
          }

          for (const container of containers) {
            const cardTitle = container.querySelector(".uol-c-card__title");
            const isCard = container.tagName && container.tagName.toLowerCase() === "article" && cardTitle;
            if (isCard) {
              const title = normalizeText(cardTitle.innerText);
              if (title && !seen.has(title)) {
                seen.add(title);
                const body = container.querySelector(".uol-c-card__body");
                const content = body ? normalizeText(body.innerText) : normalizeText(container.innerText.replace(cardTitle.innerText, ""));
                results.push({ heading: title, content: content });
              }
              continue;
            }

            const headings = Array.from(container.querySelectorAll("h1,h2,h3,h4,h5,h6"));
            if (headings.length === 0) continue;

            for (let i = 0; i < headings.length; i++) {
              const h = headings[i];
              const headingText = normalizeText(h.innerText);
              if (!headingText || seen.has(headingText)) continue;

              const range = document.createRange();
              try { range.setStartAfter(h); } catch { continue; }
              const nextHeading = (i + 1 < headings.length) ? headings[i + 1] : null;
              try {
                if (nextHeading) range.setEndBefore(nextHeading);
                else range.setEndAfter(container.lastChild || h);
              } catch {}
              let content = normalizeText(range.toString());

              if (!content) {
                const cardAncestor = h.closest("article");
                if (cardAncestor) {
                  const cbody = cardAncestor.querySelector(".uol-c-card__body");
                  if (cbody) content = normalizeText(cbody.innerText);
                }
                if (!content) {
                  const p = h.nextElementSibling;
                  if (p && p.tagName && p.tagName.toLowerCase() === "p") {
                    content = normalizeText(p.innerText);
                  }
                }
              }

              if (content && content.startsWith(headingText)) {
                content = content.substring(headingText.length).trim();
              }

              seen.add(headingText);
              results.push({ heading: headingText, content: content });
            }
          }

          return results;
        }
        """
        try:
            result = await page.evaluate(js)
            return result or []
        except Exception:
            logger.exception("JS extraction failed")
            return []

    async def _save_incremental(self, result):
        """Persist a single page’s data to disk."""
        async with self.lock:
            filename = CONFIG["output"]["incremental_file"]
            existing = []
            if os.path.exists(filename):
                with open(filename, "r", encoding="utf-8") as f:
                    try:
                        existing = json.load(f)
                    except json.JSONDecodeError:
                        logger.warning("Incremental file corrupt – starting fresh")
            existing.append(result)
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(existing, f, ensure_ascii=False, indent=2)

    async def _add_failed_url(self, url, error):
        async with self.lock:
            self.failed_urls.append(
                {"url": url, "error": error, "timestamp": datetime.now().isoformat()}
            )

    async def _scrape_single_page(self, context, url, semaphore):
        """Scrape one page using the shared context."""
        async with semaphore:
            page = None
            try:
                # 1️⃣  Start -------------------------------------------------
                logger.info(
                    "STARTING: %s (%d/%d)",
                    url,
                    self.total_scraped + 1,
                    self.total_to_scrape,
                )

                # 2️⃣  New page ------------------------------------------------
                page = await context.new_page()
                page.set_default_navigation_timeout(self.config["nav_timeout"])

                # 3️⃣  Navigation ------------------------------------------------
                nav_resp = None
                for attempt in range(1, self.config["retry_attempts"] + 1):
                    try:
                        nav_resp = await page.goto(
                            url, wait_until="domcontentloaded", timeout=self.config["nav_timeout"]
                        )
                    except Exception:
                        logger.warning("Attempt %d: navigation exception for %s", attempt, url)

                    if nav_resp and nav_resp.status < 400:
                        break
                    await asyncio.sleep(attempt * 1)

                if nav_resp is None or nav_resp.status >= 400:
                    err_msg = f"HTTP {nav_resp.status if nav_resp else 'N/A'}"
                    logger.error("%s – %s", err_msg, url)
                    await self._add_failed_url(url, err_msg)
                    self.total_failed += 1
                    return None

                logger.info("NAVIGATION OK: %s – %s", url, nav_resp.status)

                # 4️⃣  Wait for lazy content ---------------------------------
                await asyncio.sleep(0.8)
                await self._auto_scroll(page)
                await asyncio.sleep(1.2)

                # 5️⃣  Extraction ---------------------------------------------
                blocks = await self._extract_structured_content(page)
                logger.info("EXTRACTION OK: %s – %d heading blocks", url, len(blocks))

                result = {
                    "url": url,
                    "title": await page.title(),
                    "scraped_at": datetime.now().isoformat(),
                    "content_blocks": blocks,
                    "total_blocks": len(blocks),
                }

                if self.config.get("incremental_save", True):
                    await self._save_incremental(result)

                self.total_scraped += 1
                return result

            except Exception as exc:
                logger.exception("FAILED: %s – %s", url, exc)
                await self._add_failed_url(url, str(exc))
                self.total_failed += 1
                return None

            finally:
                if page:
                    try: await page.close()
                    except Exception: pass

    async def scrape_all_urls(self, urls):
        """Scrape every URL with a single shared context."""
        self.total_to_scrape = len(urls)
        logger.info("Starting content scraping for %d URLs", len(urls))
        semaphore = asyncio.Semaphore(self.config["max_concurrent"])

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=self.config["headless"],
                args=["--disable-blink-features=AutomationControlled"],
            )

            # shared context (cookies survive across pages)
            context = await browser.new_context(
                user_agent=self._build_headers(None)["User-Agent"],
                extra_http_headers=self._build_headers(None),
                locale="en-US",
                timezone_id="UTC",
            )

            try:
                tasks = [
                    asyncio.create_task(
                        self._scrape_single_page(context, u, semaphore)
                    )
                    for u in urls
                ]

                # Create tasks with a slight delay to avoid burst requests
                for fut in asyncio.as_completed(tasks):
                    try:
                        await fut
                    except asyncio.CancelledError:
                        logger.warning("Task cancelled – user interrupted")
                        raise
                    except Exception as exc:
                        logger.exception("Unexpected exception: %s", exc)

                logger.info(
                    "Scraping finished – %d succeeded, %d failed",
                    self.total_scraped,
                    self.total_failed,
                )
                return [t.result() for t in tasks if t.result() is not None]

            finally:
                await context.close()
                await browser.close()

# --------------------------------------------------------------------
#  COMBINED SCRAPER
# --------------------------------------------------------------------
class CombinedScraper:
    def __init__(self, config=None):
        self.config = config or CONFIG
        self.url_extractor = URLExtractor(self.config["url_extraction"])
        self.content_scraper = ContentScraper(self.config["content_scraping"])

    async def scrape_domain(self, start_url):
        logger.info("=" * 60)
        logger.info(">> COMBINED DOMAIN SCRAPER STARTED")
        logger.info("=" * 60)
        logger.info("Target URL: %s", start_url)

        start_time = time.time()

        # --------------------------------------------------------------------
        # 1️⃣  URL EXTRACTION
        # --------------------------------------------------------------------
        logger.info("\n" + "=" * 40)
        logger.info("STEP 1: EXTRACTING URLs")
        logger.info("=" * 40)

        urls = self.url_extractor.extract_all_urls(start_url)

        if not urls:
            logger.error("No URLs found. Exiting.")
            return

        logger.info("Found %d URLs to scrape", len(urls))

        # --------------------------------------------------------------------
        # 2️⃣  CONTENT SCRAPING
        # --------------------------------------------------------------------
        logger.info("\n" + "=" * 40)
        logger.info("STEP 2: SCRAPING CONTENT")
        logger.info("=" * 40)

        scraped = await self.content_scraper.scrape_all_urls(urls)

        # --------------------------------------------------------------------
        # 3️⃣  SAVE FINAL RESULTS
        # --------------------------------------------------------------------
        logger.info("\n" + "=" * 40)
        logger.info("STEP 3: SAVING RESULTS")
        logger.info("=" * 40)

        final = {
            "domain": urlparse(start_url).netloc,
            "start_url": start_url,
            "scraping_started": datetime.now().isoformat(),
            "total_urls_found": len(urls),
            "successfully_scraped": len(scraped),
            "failed_urls": len(self.content_scraper.failed_urls),
            "results": scraped,
        }

        final_file = self.config["output"]["final_file"]
        with open(final_file, "w", encoding="utf-8") as f:
            json.dump(final, f, ensure_ascii=False, indent=2)

        if self.content_scraper.failed_urls:
            failed_file = self.config["output"]["failed_urls_file"]
            with open(failed_file, "w", encoding="utf-8") as f:
                json.dump(self.content_scraper.failed_urls, f, ensure_ascii=False, indent=2)

        total_time = time.time() - start_time
        total_blocks = sum(r["total_blocks"] for r in scraped)

        logger.info("=" * 60)
        logger.info(">> SCRAPING COMPLETE")
        logger.info("=" * 60)
        logger.info("Total time: %.2fs", total_time)
        logger.info("URLs found: %d", len(urls))
        logger.info("Successfully scraped: %d", len(scraped))
        logger.info("Failed: %d", len(self.content_scraper.failed_urls))
        logger.info("Total content blocks: %d", total_blocks)
        logger.info("Final results saved to: %s", final_file)

        if self.content_scraper.failed_urls:
            logger.info("Failed URLs saved to: %s", failed_file)

        return final

# --------------------------------------------------------------------
#  MAIN
# --------------------------------------------------------------------
async def main():
    START_URL = "https://www.tanyapepsodent.com/"

    scraper = CombinedScraper(CONFIG)

    try:
        await scraper.scrape_domain(START_URL)
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user (Ctrl‑C)")
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Scraping cancelled from the outside")
