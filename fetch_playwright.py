import asyncio
from playwright.async_api import async_playwright

async def scrape_page(url):
    try:
        async with async_playwright() as p:
            # Launch a headless browser
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Navigate to the URL with a timeout
            await page.goto(url, timeout=30000)
            
            # Extract data (e.g., page title and all paragraph texts)
            title = await page.title()
            paragraphs = await page.query_selector_all('p')
            paragraph_texts = [await p.inner_text() for p in paragraphs]
            
            # Close the browser
            await browser.close()
            
            # Return the scraped data
            return {
                'title': title,
                'paragraphs': paragraph_texts
            }
    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        return None

async def main():
    # Example URL to scrape
    target_url = "https://www.unilever.com/news/news-search/2025/unilevers-100-accelerator-partnership-unlocks-ai-innovation-across-supply-chain/"
    data = await scrape_page(target_url)
    
    # Print the scraped data
    if data:
        print(f"Page Title: {data['title']}")
        print("Paragraphs:")
        for i, text in enumerate(data['paragraphs'], 1):
            print(f"{i}. {text}")
    else:
        print("Failed to scrape data.")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())