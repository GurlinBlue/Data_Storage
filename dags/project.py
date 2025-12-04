import asyncio
import csv
import sqlite3
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List
from urllib.parse import urlparse

import nest_asyncio
from airflow import DAG
from airflow.operators.python import PythonOperator
from playwright.async_api import async_playwright

nest_asyncio.apply()


BASE_URL = "https://www.rottentomatoes.com/browse/movies_at_home/?page="
PAGES = list(range(0, 8))

RAW_OUTPUT = "/opt/airflow/data/movies_raw.csv"
CLEAN_OUTPUT = "/opt/airflow/data/movies_clean.csv"
DB_PATH = "/opt/airflow/data/movies.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def normalize_url(url: str) -> str:
    if not url or not isinstance(url, str):
        return ""
    
    if not url.startswith(('http://', 'https://')):
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('/'):
            url = 'https://www.rottentomatoes.com' + url
    
    try:
        parsed = urlparse(url)
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return normalized.rstrip('/')
    except:
        url = url.split('?')[0].split('#')[0]
        return url.rstrip('/')


def normalize_title(title: str) -> str:
    if not title or not isinstance(title, str):
        return ""
    
    title = title.lower().strip()
    title = re.sub(r'\s+', ' ', title)
    title = re.sub(r'[^\w\s]', '', title)
    
    return title


async def scrape_list_pages():
    movies_dict = {}
    logger.info("[LIST] Starting pagination scrape with deduplication…")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context()

        for page_num in PAGES:
            url = BASE_URL + str(page_num)
            logger.info(f"[LIST] Scraping page {page_num}: {url}")

            page = await context.new_page()
            
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(1)
                
                movie_links = page.locator("a[href*='/m/']:has(span.p--small)")
                count = await movie_links.count()
                
                if count == 0:
                    movie_links = page.locator("[data-qa='discovery-media-list'] a[href*='/m/']")
                    count = await movie_links.count()
                
                logger.info(f"[LIST] Page {page_num}: Found {count} movie links")
                
                for i in range(count):
                    try:
                        link = movie_links.nth(i)
                        
                        title_elem = link.locator("span.p--small")
                        if await title_elem.count() == 0:
                            continue
                        
                        title = await title_elem.inner_text()
                        if not title or title.strip() == "":
                            continue
                        
                        title = title.strip()
                        
                        if title.lower() in ['tomatometer', 'audience score', 'popcornmeter', 'score']:
                            continue
                        
                        href = await link.get_attribute("href")
                        if not href:
                            continue
                        
                        normalized_url = normalize_url(href)
                        normalized_title = normalize_title(title)
                        
                        if not normalized_url or not normalized_title:
                            continue
                        
                        if normalized_url in movies_dict:
                            logger.debug(f"[LIST] Duplicate skipped (URL): {title}")
                            continue
                        
                        existing_titles = {normalize_title(m['title']) for m in movies_dict.values()}
                        if normalized_title in existing_titles:
                            logger.debug(f"[LIST] Duplicate skipped (title): {title}")
                            continue
                        
                        movies_dict[normalized_url] = {
                            "title": title,
                            "url": normalized_url
                        }
                        
                    except Exception as e:
                        logger.debug(f"[LIST] Error processing link {i} on page {page_num}: {e}")
                        continue
                        
            except Exception as e:
                logger.error(f"[LIST] Error loading page {page_num}: {e}")
            finally:
                await page.close()
        
        await context.close()
        await browser.close()
    
    unique_movies = list(movies_dict.values())
    
    logger.info(f"[LIST] Scraping complete. Unique movies found: {len(unique_movies)}")
    
    if unique_movies:
        logger.info("[LIST] Sample of unique movies:")
        for movie in unique_movies[:5]:
            logger.info(f"  - {movie['title']}")
    
    return unique_movies


async def scrape_movie_metadata_efficient(url: str, title: str = "") -> Dict:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page()
        
        try:
            logger.info(f"[METADATA] Scraping: {title}")
            
            await page.set_viewport_size({"width": 1280, "height": 800})
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)
            
            metadata = {
                "url": url,
                "title": title,
                "tomatometer_score": None,
                "audience_score": None,
                "genre": None,
                "rating": None,
                "duration": None,
                "release_date": None,
                "director": None,
                "original_language": None,
                "box_office": None,
                "distributor": None,
                "error": None
            }
            
            try:
                await page.wait_for_selector('media-scorecard', timeout=10000)
            except:
                logger.debug(f"[METADATA] media-scorecard not found for {title}")
            
            try:
                critics_elem = await page.wait_for_selector('rt-text[slot="criticsScore"]', timeout=5000)
                if critics_elem:
                    critics_text = await critics_elem.text_content()
                    if critics_text:
                        critics_text = critics_text.strip()
                        logger.debug(f"[METADATA] Tomatometer raw: '{critics_text}'")
                        match = re.search(r'(\d{1,3})', critics_text)
                        if match:
                            score = int(match.group(1))
                            if 0 <= score <= 100:
                                metadata["tomatometer_score"] = score
            except Exception as e:
                logger.debug(f"[METADATA] Tomatometer extraction failed: {e}")
            
            try:
                audience_elem = await page.wait_for_selector('rt-text[slot="audienceScore"]', timeout=5000)
                if audience_elem:
                    audience_text = await audience_elem.text_content()
                    if audience_text:
                        audience_text = audience_text.strip()
                        logger.debug(f"[METADATA] Audience raw: '{audience_text}'")
                        match = re.search(r'(\d{1,3})', audience_text)
                        if match:
                            score = int(match.group(1))
                            if 0 <= score <= 100:
                                metadata["audience_score"] = score
            except Exception as e:
                logger.debug(f"[METADATA] Audience extraction failed: {e}")
            
            if metadata["tomatometer_score"] is None or metadata["audience_score"] is None:
                try:
                    scores = await page.evaluate('''() => {
                        const scores = {};
                        const criticsElem = document.querySelector('rt-text[slot="criticsScore"]');
                        if (criticsElem) {
                            const text = criticsElem.textContent.trim();
                            const match = text.match(/(\d{1,3})/);
                            if (match) scores.tomatometer = parseInt(match[1], 10);
                        }
                        const audienceElem = document.querySelector('rt-text[slot="audienceScore"]');
                        if (audienceElem) {
                            const text = audienceElem.textContent.trim();
                            const match = text.match(/(\d{1,3})/);
                            if (match) scores.audience = parseInt(match[1], 10);
                        }
                        return scores;
                    }''')
                    
                    if scores.get('tomatometer') and metadata["tomatometer_score"] is None:
                        metadata["tomatometer_score"] = scores['tomatometer']
                    if scores.get('audience') and metadata["audience_score"] is None:
                        metadata["audience_score"] = scores['audience']
                        
                except Exception as e:
                    logger.debug(f"[METADATA] JavaScript extraction failed: {e}")
            
            if metadata["tomatometer_score"] is None or metadata["audience_score"] is None:
                try:
                    rt_texts = await page.query_selector_all('rt-text')
                    potential_scores = []
                    
                    for rt_text in rt_texts:
                        text = await rt_text.text_content()
                        if text:
                            text = text.strip()
                            match = re.search(r'^(\d{1,3})%?$', text)
                            if match:
                                score = int(match.group(1))
                                if 0 <= score <= 100:
                                    potential_scores.append(score)
                    
                    if len(potential_scores) == 2:
                        potential_scores.sort(reverse=True)
                        if metadata["tomatometer_score"] is None:
                            metadata["tomatometer_score"] = potential_scores[0]
                        if metadata["audience_score"] is None:
                            metadata["audience_score"] = potential_scores[1]
                            
                except Exception as e:
                    logger.debug(f"[METADATA] Fallback extraction failed: {e}")
            
            try:
                info_section = page.locator('section.media-info')
                
                if await info_section.count() > 0:
                    field_mappings = {
                        'genre': ['genre'],
                        'rating': ['rating'],
                        'runtime': ['runtime'],
                        'release date': ['release date'],
                        'director': ['director'],
                        'original language': ['original language'],
                        'box office': ['box office'],
                        'distributor': ['distributor']
                    }
                    
                    items = info_section.locator('[data-qa="item"]')
                    
                    for i in range(await items.count()):
                        try:
                            item = items.nth(i)
                            label_elem = item.locator('[data-qa="item-label"]')
                            
                            if await label_elem.count() > 0:
                                label_text = await label_elem.first.inner_text()
                                label_lower = label_text.lower().strip()
                                
                                target_field = None
                                for field, keywords in field_mappings.items():
                                    if any(keyword in label_lower for keyword in keywords):
                                        target_field = field
                                        break
                                
                                if target_field:
                                    value_elems = item.locator('[data-qa="item-value"]')
                                    values = []
                                    
                                    for j in range(await value_elems.count()):
                                        value_text = await value_elems.nth(j).inner_text()
                                        if value_text and value_text.strip():
                                            values.append(value_text.strip())
                                    
                                    if values:
                                        if target_field == 'genre':
                                            metadata["genre"] = ', '.join(values)
                                        elif target_field == 'rating':
                                            metadata["rating"] = values[0]
                                        elif target_field == 'runtime':
                                            metadata["duration"] = values[0].replace(' ', '')
                                        elif target_field == 'release date':
                                            date_text = values[0]
                                            metadata["release_date"] = re.sub(r'\s*,\s*Wide$', '', date_text)
                                        elif target_field == 'director':
                                            metadata["director"] = ', '.join(values)
                                        elif target_field == 'original language':
                                            metadata["original_language"] = values[0]
                                        elif target_field == 'box office':
                                            metadata["box_office"] = values[0]
                                        elif target_field == 'distributor':
                                            metadata["distributor"] = values[0]
                        
                        except Exception as e:
                            logger.debug(f"[METADATA] Error processing item {i}: {e}")
                            continue
                            
            except Exception as e:
                logger.debug(f"[METADATA] Info section error for {title}: {e}")
            
            for score_field in ['tomatometer_score', 'audience_score']:
                score = metadata.get(score_field)
                if score is not None:
                    try:
                        score_int = int(score)
                        if not (0 <= score_int <= 100):
                            metadata[score_field] = None
                    except (ValueError, TypeError):
                        metadata[score_field] = None
            
            if metadata["duration"]:
                if not ('h' in metadata["duration"].lower() or 'm' in metadata["duration"].lower()):
                    match = re.search(r'(\d+)', metadata["duration"])
                    if match:
                        minutes = int(match.group(1))
                        if minutes >= 60:
                            hours = minutes // 60
                            mins = minutes % 60
                            metadata["duration"] = f"{hours}h {mins}m"
                        else:
                            metadata["duration"] = f"{minutes}m"
            
            scores_found = (metadata["tomatometer_score"] is not None or 
                          metadata["audience_score"] is not None)
            
            if scores_found:
                logger.info(f"[METADATA] {title}: "
                           f"T={metadata['tomatometer_score'] or 'N/A'}, "
                           f"A={metadata['audience_score'] or 'N/A'}")
            
            await browser.close()
            return metadata
            
        except Exception as e:
            logger.error(f"[METADATA] Error scraping {url}: {str(e)}")
            await browser.close()
            return {
                "url": url,
                "title": title,
                "error": str(e)
            }


async def scrape_all_movies_batch(movies: List[Dict], max_concurrent: int = 3) -> List[Dict]:
    logger.info(f"[BATCH] Starting batch scrape for {len(movies)} unique movies")
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def process_movie_with_semaphore(movie):
        async with semaphore:
            try:
                result = await scrape_movie_metadata_efficient(movie["url"], movie["title"])
                return result
            except Exception as e:
                logger.error(f"[BATCH] Error processing {movie['title']}: {e}")
                return {
                    "url": movie["url"],
                    "title": movie["title"],
                    "error": str(e)
                }
    
    tasks = [process_movie_with_semaphore(movie) for movie in movies]
    
    chunk_size = 10
    all_results = []
    
    for i in range(0, len(tasks), chunk_size):
        chunk = tasks[i:i + chunk_size]
        chunk_num = i // chunk_size + 1
        total_chunks = (len(tasks) + chunk_size - 1) // chunk_size
        
        logger.info(f"[BATCH] Processing chunk {chunk_num}/{total_chunks} "
                   f"({len(chunk)} movies)")
        
        try:
            chunk_results = await asyncio.gather(*chunk, return_exceptions=True)
            
            for j, result in enumerate(chunk_results):
                if isinstance(result, Exception):
                    movie = movies[i + j]
                    logger.error(f"[BATCH] Gather error for '{movie['title']}': {result}")
                    all_results.append({
                        "url": movie["url"],
                        "title": movie["title"],
                        "error": str(result)
                    })
                elif result:
                    all_results.append(result)
        
        except Exception as e:
            logger.error(f"[BATCH] Chunk processing error: {e}")
        
        if i + chunk_size < len(tasks):
            await asyncio.sleep(1)
    
    successful = len([r for r in all_results if r.get('tomatometer_score') is not None])
    logger.info(f"[BATCH] Completed. Success rate: {successful}/{len(movies)} "
               f"({(successful/len(movies)*100 if len(movies) > 0 else 0):.1f}%)")
    
    return all_results


def task_scrape_movies():
    logger.info("[TASK 1] Starting movie list scraping")
    
    movies = asyncio.run(scrape_list_pages())
    
    with open(RAW_OUTPUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "url"])
        writer.writeheader()
        writer.writerows(movies)
    
    logger.info(f"[TASK 1] Saved {len(movies)} unique movies → {RAW_OUTPUT}")
    
    if movies:
        logger.info("[TASK 1] Sample of saved movies:")
        for movie in movies[:3]:
            logger.info(f"  - {movie['title']}")
    
    return len(movies)


def task_clean_movies():
    import pandas as pd
    
    logger.info("=" * 60)
    logger.info("[TASK 2] Starting metadata extraction for unique movies")
    logger.info("=" * 60)
    
    try:
        df_raw = pd.read_csv(RAW_OUTPUT)
        logger.info(f"[TASK 2] Loaded {len(df_raw)} unique movies from {RAW_OUTPUT}")
    except Exception as e:
        logger.error(f"[TASK 2] Error reading raw data: {e}")
        return pd.DataFrame()
    
    movies = df_raw.to_dict("records")
    
    test_mode = False
    if test_mode:
        movies = movies[:3]
        logger.info(f"[TASK 2] TEST MODE: Scraping {len(movies)} movies")
    else:
        logger.info(f"[TASK 2] PRODUCTION MODE: Scraping all {len(movies)} unique movies")
    
    all_metadata = asyncio.run(scrape_all_movies_batch(movies, max_concurrent=3))
    
    df_clean = pd.DataFrame(all_metadata)
    
    expected_columns = [
        'title', 'url', 'tomatometer_score', 'audience_score', 
        'genre', 'rating', 'duration', 'release_date', 'director',
        'original_language', 'box_office', 'distributor'
    ]
    
    for col in expected_columns:
        if col not in df_clean.columns:
            df_clean[col] = None
    
    for col in ['tomatometer_score', 'audience_score']:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    
    df_clean = df_clean[expected_columns]
    
    df_clean.to_csv(CLEAN_OUTPUT, index=False, encoding='utf-8')
    
    successful_scores = len(df_clean[df_clean['tomatometer_score'].notna()])
    successful_audience = len(df_clean[df_clean['audience_score'].notna()])
    
    logger.info(f"[TASK 2] Extraction complete!")
    logger.info(f"[TASK 2] Results saved to → {CLEAN_OUTPUT}")
    logger.info(f"[TASK 2] Data completeness:")
    logger.info(f"  - Tomatometer scores: {successful_scores}/{len(df_clean)}")
    logger.info(f"  - Audience scores: {successful_audience}/{len(df_clean)}")
    logger.info(f"  - Genres: {df_clean['genre'].notna().sum()}/{len(df_clean)}")
    logger.info(f"  - Ratings: {df_clean['rating'].notna().sum()}/{len(df_clean)}")
    logger.info(f"  - Durations: {df_clean['duration'].notna().sum()}/{len(df_clean)}")
    
    if not df_clean.empty:
        logger.info("[TASK 2] Sample of scraped data:")
        sample = df_clean.head(5)
        for _, row in sample.iterrows():
            logger.info(f"  {row['title'][:40]:40} "
                       f"T:{row['tomatometer_score']:3} "
                       f"A:{row['audience_score']:3} "
                       f"Genre: {row['genre'][:20] if pd.notna(row['genre']) else 'N/A'}")
    
    return df_clean


def task_load_to_sqlite():
    import pandas as pd
    
    logger.info("[TASK 3] Loading cleaned data to SQLite")
    
    try:
        df = pd.read_csv(CLEAN_OUTPUT)
        logger.info(f"[TASK 3] Read {len(df)} rows from {CLEAN_OUTPUT}")
        
        conn = sqlite3.connect(DB_PATH)
        
        df.to_sql("movies", conn, if_exists="replace", index=False)
        
        with conn:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tomatometer ON movies(tomatometer_score)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_audience ON movies(audience_score)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_genre ON movies(genre)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rating ON movies(rating)")
        
        result = pd.read_sql_query("SELECT COUNT(*) as count FROM movies", conn)
        loaded_count = result['count'].iloc[0]
        
        logger.info(f"[TASK 3] Successfully loaded {loaded_count} movies into SQLite")
 
        conn.close()
        
        logger.info("[TASK 3] Data pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"[TASK 3] Error: {str(e)}")
        raise

#DAG
with DAG(
    dag_id="scraper",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=90),
    },
    tags=["movies", "rottentomatoes", "scores", "metadata"],
) as dag:

    t1 = PythonOperator(
        task_id="scrape_movie_list",
        python_callable=task_scrape_movies,
    )

    t2 = PythonOperator(
        task_id="scrape_movie_details",
        python_callable=task_clean_movies,
        execution_timeout=timedelta(minutes=45),
    )

    t3 = PythonOperator(
        task_id="load_to_sqlite",
        python_callable=task_load_to_sqlite,
    )

    t1 >> t2 >> t3