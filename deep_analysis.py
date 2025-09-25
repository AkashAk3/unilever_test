# deep analysis

# unilever_scraper.py
import asyncio
import json
import logging
import time
from urllib.parse import urlparse
from playwright.async_api import async_playwright

# CONFIG
HEADLESS = True           # set False while debugging if you want to see the browser
OUTPUT_FILE = "deep_scraped_content_dove.json"
NAV_TIMEOUT = 60000      # ms


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    # Add other relevant headers from your browser
}


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger("deep_analysis")


def build_headers(target_url: str) -> dict:
    """Return realistic navigation headers including Referer and Client Hints."""
    try:
        parsed = urlparse(target_url)
        referer = "https://www.google.com/"
        # For same-site subsequent requests, you could use f"{parsed.scheme}://{parsed.netloc}/"
    except Exception:
        referer = "https://www.google.com/"

    merged = dict(headers)
    merged.setdefault('Referer', referer)
    merged.setdefault('sec-ch-ua', '"Chromium";v="123", "Not:A-Brand";v="99"')
    merged.setdefault('sec-ch-ua-mobile', '?0')
    merged.setdefault('sec-ch-ua-platform', '"Windows"')
    merged.setdefault('sec-fetch-dest', 'document')
    merged.setdefault('sec-fetch-mode', 'navigate')
    merged.setdefault('sec-fetch-site', 'none')
    merged.setdefault('sec-fetch-user', '?1')
    merged.setdefault('upgrade-insecure-requests', '1')
    return merged


async def auto_scroll(page):
    """Slow scroll to trigger lazy loading."""
    try:
        await page.evaluate(
            """async () => {
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
            }"""
        )
    except Exception:
        logger.debug("auto_scroll: ignore scroll script failure", exc_info=True)


async def extract_structured_content(page):
    """
    Robust extractor:
    - Finds containers: article.uol-c-card, main, section.uol-c-section, div.uol-c-section__inner
    - For each container, finds headings in document order and uses a Range between a heading and the next heading
      to extract only the text belonging to that heading.
    - For card-style containers with .uol-c-card__title/.uol-c-card__body, extracts directly if no <h*> present.
    - De-duplicates headings (keeps first occurrence).
    """
    logger.debug("inside extract_structured_content")
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
        // First, handle article cards that might not have <h*> but use .uol-c-card__title
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
          // still continue to next container (do not attempt generic headings inside card)
          continue;
        }

        // For generic containers: gather headings in-order
        const headings = Array.from(container.querySelectorAll("h1, h2, h3, h4, h5, h6"));
        if (headings.length === 0) continue;

        for (let i = 0; i < headings.length; i++) {
          const h = headings[i];
          const headingText = normalizeText(h.innerText);
          if (!headingText || seen.has(headingText)) continue;
          // Build a Range: from immediately after this heading to before the next heading in this container
          const range = document.createRange();
          try {
            range.setStartAfter(h);
          } catch (e) {
            // fallback: if can't set start after, continue
            continue;
          }
          const nextHeading = (i + 1 < headings.length) ? headings[i + 1] : null;
          try {
            if (nextHeading) {
              range.setEndBefore(nextHeading);
            } else {
              // end at end of container
              const last = container.lastChild || h;
              range.setEndAfter(last);
            }
          } catch (e) {
            // ignore range errors
          }
          let content = normalizeText(range.toString());

          // If range gave no content, try useful fallbacks:
          if (!content) {
            // 1) If heading sits inside an article/card, try its .uol-c-card__body
            const cardAncestor = h.closest("article");
            if (cardAncestor) {
              const cbody = cardAncestor.querySelector(".uol-c-card__body");
              if (cbody) content = normalizeText(cbody.innerText);
            }
            // 2) If container has a prominent summary paragraph before cards, try capturing that
            if (!content) {
              const p = h.nextElementSibling;
              if (p && p.tagName && p.tagName.toLowerCase() === "p") {
                content = normalizeText(p.innerText);
              }
            }
          }

          // Final cleanup: strip any accidental heading repetition inside content
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
        logger.debug("JS extraction returned %d blocks", len(result) if isinstance(result, list) else -1)
        return result or []
    except Exception:
        logger.exception("JS extraction failed")
        return []


async def scrape_single_page(url):
    logger.debug("inside scrape_single_page")
    async with async_playwright() as p:
        browser = None
        context = None
        page = None
        try:
            browser = await p.chromium.launch(
                headless=HEADLESS,
                args=["--disable-blink-features=AutomationControlled"]
            )
            req_headers = build_headers(url)
            context = await browser.new_context(
                user_agent=req_headers.get('User-Agent'),
                extra_http_headers=req_headers,
                locale='en-US',
                timezone_id='UTC'
            )
            page = await context.new_page()
            page.set_default_navigation_timeout(NAV_TIMEOUT)

            logger.info("Visiting %s", url)
            # Try navigation with brief retries to mitigate transient bot checks
            attempts = 3
            nav_response = None
            for attempt in range(1, attempts + 1):
                try:
                    nav_response = await page.goto(
                        url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT
                    )
                except Exception:
                    logger.exception("Navigation attempt %d failed", attempt)
                    nav_response = None

                # Validate navigation response
                ok = False
                try:
                    if nav_response is not None:
                        status_code = nav_response.status
                        ok = isinstance(status_code, int) and status_code < 400
                except Exception:
                    logger.debug("Unable to read navigation response/status", exc_info=True)

                if ok:
                    break
                if attempt < attempts:
                    wait_ms = 1000 * attempt
                    logger.warning(
                        "Nav not OK (resp=%s). Retrying in %d ms...",
                        getattr(nav_response, 'status', None), wait_ms
                    )
                    await page.wait_for_timeout(wait_ms)
            # Final check
            if nav_response is None:
                logger.error("No response received for URL: %s", url)
                return []
            try:
                status_code = nav_response.status
                if isinstance(status_code, int) and status_code >= 400:
                    logger.error("HTTP %s received for URL: %s", status_code, url)
                    return []
            except Exception:
                logger.debug("Unable to read final navigation status", exc_info=True)

            # give lazy JS a short time then scroll
            await page.wait_for_timeout(800)
            await auto_scroll(page)
            # wait a bit for lazy content to load after scroll
            await page.wait_for_timeout(1200)

            data = await extract_structured_content(page)

            # per-heading log summary
            logger.info("Extracted %d heading blocks", len(data))
            for i, item in enumerate(data, start=1):
                heading_text = item.get("heading") or ""
                content_text = item.get("content") or ""
                logger.debug("[%d] %r (content length = %d)", i, heading_text, len(content_text))

            # save results
            try:
                with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logger.info("Saved %d blocks to %s", len(data), OUTPUT_FILE)
            except Exception:
                logger.exception("Failed to write output file: %s", OUTPUT_FILE)

            return data
        except Exception:
            logger.exception("Unhandled error during scrape_single_page")
            return []
        finally:
            # best-effort cleanup
            try:
                if page is not None:
                    await page.close()
            except Exception:
                logger.debug("Ignoring page.close error", exc_info=True)
            try:
                if context is not None:
                    await context.close()
            except Exception:
                logger.debug("Ignoring context.close error", exc_info=True)
            try:
                if browser is not None:
                    await browser.close()
            except Exception:
                logger.debug("Ignoring browser.close error", exc_info=True)


if __name__ == "__main__":
    # Change this to the page you want to scrape.
    url = "https://www.tanyapepsodent.com/"
    start = time.time()
    asyncio.run(scrape_single_page(url))
    logging.info("Elapsed: %.1fs", time.time() - start)
