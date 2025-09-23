import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def fetch_main_article(url):
    try:
        # Download raw HTML
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Try to find main content
        container = (
            soup.select_one("main")
            or soup.select_one("article")
            or soup.select_one("div.uco-l-content")
            or soup.select_one("div[role='main']")
        )

        if not container:
            print("⚠️ No main container found, falling back to whole page")
            container = soup.body

        # Extract headings and text
        structured_content = []
        for heading in container.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            heading_text = heading.get_text(strip=True)
            content_parts = []
            # Collect paragraphs/divs until the next heading
            for sibling in heading.find_next_siblings():
                if sibling.name and sibling.name.startswith("h"):
                    break
                if sibling.name in ["p", "div"] and sibling.get_text(strip=True):
                    content_parts.append(sibling.get_text(strip=True))
            structured_content.append({
                "heading": heading_text,
                "content": " ".join(content_parts).strip()
            })

        return structured_content

    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    url = "https://www.unilever.com/news/news-search/2025/unilevers-100-accelerator-partnership-unlocks-ai-innovation-across-supply-chain/"
    result = fetch_main_article(url)
    for section in result:
        print(f"\n{section['heading']}...\n{section['content']}")
