import os
import re
import asyncio
import logging
import time
import json
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict

try:
    import requests
    from bs4 import BeautifulSoup
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload
    from PIL import Image
except ImportError as e:
    print(f"Missing library: {e}")
    raise

logger = logging.getLogger(__name__)

# Configuration
GOOGLE_DRIVE_FOLDER_ID = os.environ.get('GOOGLE_DRIVE_FOLDER_ID', '')
STITCH_HEIGHT = 12000
STITCH_QUALITY = 100

def clean_filename(name):
    """Clean filename from invalid characters"""
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()

async def get_skip_value_from_sheet(url: str, sheet_scheduler) -> int:
    """Get skip value from Google Sheet for a given URL"""
    try:
        if not sheet_scheduler:
            return 0

        from scheduler import PROGRESS_SHEET

        # Read PROGRESS sheet
        progress_data = await sheet_scheduler.read_sheet_data(PROGRESS_SHEET, "A:M")

        if not progress_data or len(progress_data) < 2:
            return 0

        # Search for matching URL in column B (index 1)
        for row in progress_data[1:]:  # Skip header
            if len(row) > 1:
                sheet_url = row[1].strip() if row[1] else ""
                if sheet_url == url:
                    # Found matching URL, get skip value from column M (index 12)
                    skip_str = row[12].strip() if len(row) > 12 and row[12] else "0"
                    try:
                        skip_value = int(skip_str)
                        logger.info(f"Found skip value {skip_value} for URL in sheet")
                        return skip_value
                    except ValueError:
                        logger.warning(f"Invalid skip value: {skip_str}")
                        return 0

        logger.info("URL not found in sheet, using skip=0")
        return 0

    except Exception as e:
        logger.error(f"Error getting skip value from sheet: {e}")
        return 0

class GoogleDriveUploader:
    """Handle Google Drive uploads with OAuth"""

    def __init__(self):
        self.service = None
        self.credentials = None
        self.SCOPES = ['https://www.googleapis.com/auth/drive.file']

    def setup_credentials(self):
        """Setup Google Drive API credentials using OAuth"""
        try:
            token_str = os.environ.get('GOOGLE_OAUTH_TOKEN')

            if token_str:
                try:
                    token_dict = json.loads(token_str)
                    self.credentials = Credentials.from_authorized_user_info(token_dict, self.SCOPES)
                    logger.info("Loaded OAuth credentials from environment")
                except Exception as e:
                    logger.error(f"Failed to load token: {e}")
                    self.credentials = None

            if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                logger.info("Refreshing expired token...")
                self.credentials.refresh(Request())

                token_dict = {
                    'token': self.credentials.token,
                    'refresh_token': self.credentials.refresh_token,
                    'token_uri': self.credentials.token_uri,
                    'client_id': self.credentials.client_id,
                    'client_secret': self.credentials.client_secret,
                    'scopes': self.credentials.scopes
                }
                logger.warning("Token refreshed! Update GOOGLE_OAUTH_TOKEN with:")
                print(f"\n{'='*60}")
                print("UPDATE GOOGLE_OAUTH_TOKEN in Replit Secrets with:")
                print(json.dumps(token_dict))
                print(f"{'='*60}\n")

            if not self.credentials or not self.credentials.valid:
                logger.error("="*60)
                logger.error("NO VALID OAUTH TOKEN FOUND!")
                logger.error("="*60)
                return False

            self.service = build('drive', 'v3', credentials=self.credentials)
            logger.info("✅ Google Drive API initialized with OAuth")
            return True

        except Exception as e:
            logger.error(f"Failed to setup Google Drive: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def create_folder(self, folder_name: str, parent_id: str = None) -> Optional[str]:
        """Create a folder in Google Drive and return its ID"""
        try:
            if not self.service:
                if not self.setup_credentials():
                    return None

            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }

            if parent_id:
                file_metadata['parents'] = [parent_id]
            elif GOOGLE_DRIVE_FOLDER_ID:
                file_metadata['parents'] = [GOOGLE_DRIVE_FOLDER_ID]

            loop = asyncio.get_event_loop()
            folder = await loop.run_in_executor(
                None,
                lambda: self.service.files().create(
                    body=file_metadata,
                    fields='id, webViewLink'
                ).execute()
            )

            # Make folder publicly accessible
            await loop.run_in_executor(
                None,
                lambda: self.service.permissions().create(
                    fileId=folder['id'],
                    body={'type': 'anyone', 'role': 'reader'}
                ).execute()
            )

            logger.info(f"✅ Created folder: {folder_name} (ID: {folder['id']})")
            return folder['id']

        except Exception as e:
            logger.error(f"Folder creation error: {e}")
            return None

    async def upload_file(self, file_buffer: BytesIO, filename: str, folder_id: str = None, mime_type: str = 'image/jpeg') -> tuple:
        """Upload file to Google Drive and return shareable link"""
        try:
            if not self.service:
                if not self.setup_credentials():
                    return None, None

            file_buffer.seek(0)

            file_metadata = {'name': filename, 'mimeType': mime_type}

            if folder_id:
                file_metadata['parents'] = [folder_id]
            elif GOOGLE_DRIVE_FOLDER_ID:
                file_metadata['parents'] = [GOOGLE_DRIVE_FOLDER_ID]

            media = MediaIoBaseUpload(
                file_buffer,
                mimetype=mime_type,
                resumable=True
            )

            loop = asyncio.get_event_loop()
            file = await loop.run_in_executor(
                None,
                lambda: self.service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id, webViewLink'
                ).execute()
            )

            await loop.run_in_executor(
                None,
                lambda: self.service.permissions().create(
                    fileId=file['id'],
                    body={'type': 'anyone', 'role': 'reader'}
                ).execute()
            )

            file_id = file['id']
            view_link = file.get('webViewLink', f"https://drive.google.com/file/d/{file_id}/view")

            logger.info(f"✅ Uploaded {filename} to Google Drive")
            return view_link, file_id

        except Exception as e:
            logger.error(f"Google Drive upload error: {e}")
            import traceback
            traceback.print_exc()
            return None, None

    async def get_folder_link(self, folder_id: str) -> Optional[str]:
        """Get the shareable link for a folder"""
        try:
            if not self.service:
                return None

            loop = asyncio.get_event_loop()
            folder = await loop.run_in_executor(
                None,
                lambda: self.service.files().get(
                    fileId=folder_id,
                    fields='webViewLink'
                ).execute()
            )

            return folder.get('webViewLink')

        except Exception as e:
            logger.error(f"Error getting folder link: {e}")
            return None

class ImageStitcher:
    """Handle image stitching operations"""

    @staticmethod
    def stitch_images(image_data_list: List[BytesIO], max_height: int = STITCH_HEIGHT) -> List[BytesIO]:
        """
        Stitch images vertically into chunks of max_height
        Returns list of stitched image buffers
        """
        try:
            if not image_data_list:
                return []

            # Load all images
            images = []
            for img_data in image_data_list:
                img_data.seek(0)
                img = Image.open(img_data)
                # Convert to RGB if necessary
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                images.append(img)

            logger.info(f"Loaded {len(images)} images for stitching")

            # Calculate total dimensions
            max_width = max(img.width for img in images)

            # Stitch images into chunks
            stitched_images = []
            current_height = 0
            current_images = []

            for img in images:
                if current_height + img.height > max_height and current_images:
                    # Create stitched image from current batch
                    stitched = ImageStitcher._create_stitched_image(current_images, max_width, current_height)
                    stitched_images.append(stitched)

                    # Start new batch
                    current_images = [img]
                    current_height = img.height
                else:
                    current_images.append(img)
                    current_height += img.height

            # Handle remaining images
            if current_images:
                stitched = ImageStitcher._create_stitched_image(current_images, max_width, current_height)
                stitched_images.append(stitched)

            logger.info(f"Created {len(stitched_images)} stitched image(s)")
            return stitched_images

        except Exception as e:
            logger.error(f"Image stitching error: {e}")
            import traceback
            traceback.print_exc()
            return []

    @staticmethod
    def _create_stitched_image(images: List[Image.Image], width: int, height: int) -> BytesIO:
        """Create a single stitched image from a list of images"""
        # Create new image
        stitched = Image.new('RGB', (width, height), (255, 255, 255))

        # Paste images
        y_offset = 0
        for img in images:
            # Center image horizontally if it's narrower
            x_offset = (width - img.width) // 2
            stitched.paste(img, (x_offset, y_offset))
            y_offset += img.height

        # Save to buffer
        buffer = BytesIO()
        stitched.save(buffer, format='JPEG', quality=STITCH_QUALITY, optimize=True)
        buffer.seek(0)

        return buffer

class MangaDownloader:
    """Core downloader functionality"""

    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.driver = None
        self.driver_lock = asyncio.Lock()

    def init_selenium(self):
        """Initialize Selenium WebDriver for Replit"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-software-rasterizer")

            chrome_options.binary_location = "/nix/store/*-chromium-*/bin/chromium"

            try:
                service = Service()
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.info("Selenium WebDriver initialized")
            except:
                from webdriver_manager.chrome import ChromeDriverManager
                driver_path = ChromeDriverManager().install()
                service = Service(driver_path)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.info("Selenium WebDriver initialized with webdriver-manager")

            return True
        except Exception as e:
            logger.error(f"Selenium init failed: {e}")
            return False

    def close_selenium(self):
        """Close Selenium WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception as e:
                logger.error(f"Error closing driver: {e}")

    def search_naver(self, query: str) -> List[Dict]:
        """Search for webtoons on Naver"""
        try:
            search_url = f"https://comic.naver.com/search?keyword={query}"
            r = self.session.get(search_url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')

            results = []
            links = soup.find_all('a', href=re.compile(r'titleId=\d+'))
            seen_ids = set()

            for link in links[:10]:
                href = link['href']
                tid = re.search(r'titleId=(\d+)', href)
                if tid:
                    tid = tid.group(1)
                    title = link.get_text(strip=True)
                    if tid not in seen_ids and title:
                        full_link = f"https://comic.naver.com/webtoon/list?titleId={tid}"
                        results.append({
                            'title': title,
                            'url': full_link,
                            'id': tid
                        })
                        seen_ids.add(tid)

            logger.info(f"Found {len(results)} results for: {query}")
            return results
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []

    def get_chapter_url(self, site: str, base_url: str, ep_num: int) -> Optional[str]:
        """Build chapter URL based on site"""
        if "Naver" in site:
            match = re.search(r'titleId=(\d+)', base_url)
            if match:
                return f"https://comic.naver.com/webtoon/detail?titleId={match.group(1)}&no={ep_num}"

        elif "Webtoons.com" in site:
            match = re.search(r'title_no=(\d+)', base_url)
            if match:
                title_no = match.group(1)
                return f"https://www.webtoons.com/en/detail/{title_no}/{ep_num}?title_no={title_no}"

        elif "LINE" in site:
            match = re.search(r'product[/?](?:periodic\?id=)?([a-zA-Z0-9]+)', base_url)
            if match:
                return f"https://manga.line.me/product/{match.group(1)}/chapter/{ep_num}"

        return None

    def scrape_with_requests(self, url: str, site: str) -> List[str]:
        """Scrape images using requests"""
        try:
            r = self.session.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(r.text, 'html.parser')
            images = []

            if "Naver" in site:
                viewer = soup.find('div', class_='wt_viewer')
                if viewer:
                    images = [img.get('src') for img in viewer.find_all('img') if img.get('src')]

            elif "LINE" in site:
                for img in soup.find_all('img'):
                    src = img.get('data-src') or img.get('src')
                    if src and ('line-scdn' in src or 'obs.line' in src):
                        images.append(src)
                images = list(dict.fromkeys(images))

            logger.info(f"Scraped {len(images)} images")
            return images
        except Exception as e:
            logger.error(f"Scraping error: {e}")
            return []

    def scrape_with_selenium(self, url: str, site: str) -> List[str]:
        """Scrape images using Selenium"""
        try:
            if not self.driver:
                if not self.init_selenium():
                    return []

            self.driver.get(url)

            if "Webtoons.com" in site:
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.ID, "content-all"))
                )
                time.sleep(3)

            total_height = int(self.driver.execute_script("return document.body.scrollHeight"))
            for i in range(1, total_height, 800):
                self.driver.execute_script(f"window.scrollTo(0, {i});")
                time.sleep(0.1)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            images = []
            img_elements = self.driver.find_elements(By.TAG_NAME, "img")

            for img in img_elements:
                src = img.get_attribute('src') or img.get_attribute('data-src')
                if src and len(src) > 50:
                    if "Webtoons.com" in site and "cdn.webtoon" in src and "stub" not in src:
                        images.append(src)
                    elif "Webtoons.com" not in site:
                        images.append(src)

            images = list(dict.fromkeys(images))
            logger.info(f"Scraped {len(images)} images with Selenium")
            return images
        except Exception as e:
            logger.error(f"Selenium error: {e}")
            return []

    def download_image(self, args) -> Optional[BytesIO]:
        """Download a single image"""
        url, referer = args
        try:
            headers = self.headers.copy()
            headers['Referer'] = referer
            r = self.session.get(url, headers=headers, stream=True, timeout=15)
            if r.status_code == 200:
                return BytesIO(r.content)
        except Exception as e:
            logger.error(f"Image download error: {e}")
        return None

    async def download_chapter(self, site: str, base_url: str, chapter_num: int, progress_callback=None) -> tuple:
        """Download a single chapter and return stitched images"""
        chapter_url = self.get_chapter_url(site, base_url, chapter_num)
        if not chapter_url:
            return None, "Invalid URL format"

        if progress_callback:
            await progress_callback(f"🔍 Fetching chapter {chapter_num}...")

        use_selenium = "Webtoons.com" in site or "AC.QQ" in site

        loop = asyncio.get_event_loop()
        if use_selenium:
            images = await loop.run_in_executor(None, self.scrape_with_selenium, chapter_url, site)
        else:
            images = await loop.run_in_executor(None, self.scrape_with_requests, chapter_url, site)

        if not images:
            return None, "No images found or chapter locked"

        if progress_callback:
            await progress_callback(f"📥 Downloading {len(images)} images...")

        tasks = [(img_url, chapter_url) for img_url in images]

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = await loop.run_in_executor(None, lambda: list(executor.map(self.download_image, tasks)))
            image_data = [r for r in results if r is not None]

        if not image_data:
            return None, "Failed to download images"

        if progress_callback:
            await progress_callback(f"🧵 Stitching {len(image_data)} images...")

        # Stitch images
        stitched_images = await loop.run_in_executor(None, ImageStitcher.stitch_images, image_data)

        if not stitched_images:
            return None, "Failed to stitch images"

        logger.info(f"Created {len(stitched_images)} stitched image(s) for chapter {chapter_num}")
        return stitched_images, None
