# unilever_scraper.py
import asyncio
import json
import time
from urllib.parse import urlparse
from playwright.async_api import async_playwright

# CONFIG
HEADLESS = True           # set False while debugging if you want to see the browser
OUTPUT_FILE = "deep_scraped_content_dove.json"
NAV_TIMEOUT = 60000      # ms


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
        pass


async def extract_structured_content(page):
    """
    Robust extractor:
    - Finds containers: article.uol-c-card, main, section.uol-c-section, div.uol-c-section__inner
    - For each container, finds headings in document order and uses a Range between a heading and the next heading
      to extract only the text belonging to that heading.
    - For card-style containers with .uol-c-card__title/.uol-c-card__body, extracts directly if no <h*> present.
    - De-duplicates headings (keeps first occurrence).
    """
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
        return result or []
    except Exception as e:
        print("❌ JS extraction failed:", e)
        return []


async def scrape_single_page(url):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS,
                                          args=["--disable-blink-features=AutomationControlled"])
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_navigation_timeout(NAV_TIMEOUT)

        print(f"🌍 Visiting {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        except Exception as e:
            print("⚠️ initial navigation timed out or failed:", e)

        # give lazy JS a short time then scroll
        await page.wait_for_timeout(800)
        await auto_scroll(page)
        # wait a bit for lazy content to load after scroll
        await page.wait_for_timeout(1200)

        data = await extract_structured_content(page)

        # pretty-print / logging per-heading
        print(f"\n🧾 Extracted {len(data)} heading blocks:")
        for i, item in enumerate(data, start=1):
            h = item.get("heading") or ""
            c = item.get("content") or ""
            print(f"[{i}] {h!r} (content length = {len(c)})")

        # save results
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"\n✅ Saved {len(data)} blocks to {OUTPUT_FILE}")

        await browser.close()
        return data


if __name__ == "__main__":
    # Change this to the page you want to scrape.
    url = "https://www.degreedeodorant.com/us/en/home.html"
    start = time.time()
    asyncio.run(scrape_single_page(url))
    print(f"Elapsed: {time.time() - start:.1f}s")
