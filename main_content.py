import asyncio
import re
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

async def fetch_html(url):
    """Fetch rendered HTML with Playwright"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(url, wait_until="networkidle", timeout=30000)
        html = await page.content()
        await browser.close()
        return html

def extract_main_article(html):
    """Extract main article title and content from Unilever page"""
    soup = BeautifulSoup(html, "html.parser")

    # Select main article container
    container = (
        soup.find("article")
        or soup.find("div", class_=re.compile(r"(uco-l-content|article-body|article-content|feature-content)", re.I))
        or soup.find("main")
        or soup.body
    )

    # Extract Title
    title_tag = container.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else ""

    # Extract content
    content_blocks = []
    headings = container.find_all(re.compile(r"^h[1-6]$", re.I))
    if not headings:
        # fallback: just paragraphs
        for p in container.find_all("p"):
            text = p.get_text(strip=True)
            if text:
                content_blocks.append({"tag": "p", "text": text})
    else:
        for heading in headings:
            heading_text = heading.get_text(strip=True)
            content_parts = []
            for sibling in heading.find_next_siblings():
                if sibling.name and re.match(r"^h[1-6]$", sibling.name, re.I):
                    break
                if sibling.name in ["p", "div"]:
                    text = sibling.get_text(strip=True)
                    if text:
                        content_parts.append(text)
            content = " ".join(content_parts).strip()
            content_blocks.append({"tag": heading.name, "text": heading_text, "content": content})

    return {"title": title, "content": content_blocks}

# -------------------------
# Usage
# -------------------------
if __name__ == "__main__":
    url = "https://www.unilever.com/"
    html = asyncio.run(fetch_html(url))
    article = extract_main_article(html)
    print(article)
    print(article)
