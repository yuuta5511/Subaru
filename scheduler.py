import os
import asyncio
import logging
from datetime import datetime, time, timedelta
import pytz
from typing import Optional, List, Dict
from io import BytesIO
import json

try:
    from google.oauth2 import service_account
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
except ImportError as e:
    print(f"Missing library: {e}")
    print("Make sure google-api-python-client is in requirements.txt")

logger = logging.getLogger(__name__)

SHEET_ID = "1CKbgNt7yMMm3H_s6n3wxKVrDcedyEdZHDjKFUGFLlLU"
PROGRESS_SHEET = os.environ.get('PROGRESS_SHEET_NAME', 'PROGRESS')
CONFIG_SHEET = os.environ.get('CONFIG_SHEET_NAME', 'Config')
SCHEDULE_TIME = time(15, 5)  # 3:05 PM
TIMEZONE = pytz.timezone('Etc/GMT-2')  # GMT+2


class SheetScheduler:
    """Handle Google Sheets reading and scheduled downloads"""

    def __init__(self, downloader, drive_uploader):
        self.downloader = downloader
        self.drive_uploader = drive_uploader
        self.sheets_service = None
        self.credentials = None

    def setup_sheets_credentials(self):
        """Setup Google Sheets API credentials using Service Account"""
        try:
            # Try service account first
            service_account_json = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')

            if service_account_json:
                logger.info("Using Service Account for Google Sheets...")
                try:
                    service_account_info = json.loads(service_account_json)
                    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

                    self.credentials = service_account.Credentials.from_service_account_info(
                        service_account_info,
                        scopes=SCOPES
                    )

                    self.sheets_service = build('sheets', 'v4', credentials=self.credentials)
                    logger.info("✅ Google Sheets API initialized with Service Account")
                    return True

                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
                    logger.error("Make sure you copied the entire JSON content from Google Cloud Console")
                    return False
                except Exception as e:
                    logger.error(f"Failed to setup service account: {e}")
                    import traceback
                    traceback.print_exc()
                    return False

            # Fallback to OAuth if service account not available
            logger.info("No service account found, trying OAuth for Sheets...")

            # Try to reuse the drive_uploader's credentials
            if self.drive_uploader and self.drive_uploader.credentials:
                logger.info("Attempting to use existing Drive credentials for Sheets...")
                self.credentials = self.drive_uploader.credentials

                try:
                    self.sheets_service = build('sheets', 'v4', credentials=self.credentials)
                    logger.info("✅ Google Sheets API initialized with Drive credentials")
                    return True
                except Exception as e:
                    logger.warning(f"Drive credentials don't have Sheets access: {e}")

            # Fall back to separate OAuth token
            token_str = os.environ.get('GOOGLE_SHEETS_TOKEN') or os.environ.get('GOOGLE_OAUTH_TOKEN')

            if token_str:
                try:
                    token_dict = json.loads(token_str)
                    SCOPES = [
                        'https://www.googleapis.com/auth/spreadsheets.readonly',
                        'https://www.googleapis.com/auth/drive.file'
                    ]
                    self.credentials = Credentials.from_authorized_user_info(token_dict, SCOPES)
                    logger.info("Loaded OAuth credentials for Sheets from environment")
                except Exception as e:
                    logger.error(f"Failed to load sheets token: {e}")
                    return False

            if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                logger.info("Refreshing sheets token...")
                try:
                    self.credentials.refresh(Request())
                except Exception as e:
                    logger.error(f"Failed to refresh token: {e}")
                    return False

            if not self.credentials or not self.credentials.valid:
                logger.error("="*60)
                logger.error("NO VALID CREDENTIALS FOR GOOGLE SHEETS!")
                logger.error("="*60)
                logger.error("Please set up one of the following:")
                logger.error("")
                logger.error("OPTION 1 (RECOMMENDED): Service Account")
                logger.error("1. Go to: https://console.cloud.google.com/iam-admin/serviceaccounts")
                logger.error("2. Create a service account")
                logger.error("3. Create and download JSON key")
                logger.error("4. Share your Google Sheet with the service account email")
                logger.error("5. Add the entire JSON to Replit Secrets as GOOGLE_SERVICE_ACCOUNT_JSON")
                logger.error("")
                logger.error("OPTION 2: OAuth Token")
                logger.error("1. Run: python setup_oauth.py")
                logger.error("2. Follow browser authorization")
                logger.error("3. Copy token to GOOGLE_OAUTH_TOKEN in Replit Secrets")
                logger.error("="*60)
                return False

            self.sheets_service = build('sheets', 'v4', credentials=self.credentials)
            logger.info("✅ Google Sheets API initialized with OAuth")
            return True

        except Exception as e:
            logger.error(f"Failed to setup Sheets API: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def get_sheet_names(self) -> Optional[List[str]]:
        """Get all sheet names in the spreadsheet"""
        try:
            if not self.sheets_service:
                if not self.setup_sheets_credentials():
                    return None

            loop = asyncio.get_event_loop()
            sheet_metadata = await loop.run_in_executor(
                None,
                lambda: self.sheets_service.spreadsheets().get(
                    spreadsheetId=SHEET_ID
                ).execute()
            )

            sheets = sheet_metadata.get('sheets', [])
            sheet_names = [sheet['properties']['title'] for sheet in sheets]
            return sheet_names

        except Exception as e:
            logger.error(f"Error getting sheet names: {e}")
            return None

    async def read_sheet_data(self, sheet_name: str, range_spec: str) -> Optional[List[List]]:
        """Read data from Google Sheet - including hyperlinks"""
        try:
            if not self.sheets_service:
                if not self.setup_sheets_credentials():
                    return None

            full_range = f"{sheet_name}!{range_spec}"

            loop = asyncio.get_event_loop()

            # First get the values
            result = await loop.run_in_executor(
                None,
                lambda: self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=SHEET_ID,
                    range=full_range
                ).execute()
            )
            values = result.get('values', [])

            # Then get the cell data with formulas to extract hyperlinks
            sheet_data = await loop.run_in_executor(
                None,
                lambda: self.sheets_service.spreadsheets().get(
                    spreadsheetId=SHEET_ID,
                    ranges=[full_range],
                    includeGridData=True
                ).execute()
            )

            # Extract hyperlinks from cell data
            hyperlinks = {}
            sheets = sheet_data.get('sheets', [])
            if sheets:
                grid_data = sheets[0].get('data', [])
                if grid_data:
                    row_data = grid_data[0].get('rowData', [])
                    for row_idx, row in enumerate(row_data):
                        cell_data = row.get('values', [])
                        for col_idx, cell in enumerate(cell_data):
                            hyperlink = cell.get('hyperlink')
                            if hyperlink:
                                hyperlinks[(row_idx, col_idx)] = hyperlink

            # Replace display text with actual URLs where hyperlinks exist
            for row_idx, row in enumerate(values):
                for col_idx, cell in enumerate(row):
                    if (row_idx, col_idx) in hyperlinks:
                        values[row_idx][col_idx] = hyperlinks[(row_idx, col_idx)]

            logger.info(f"Read {len(values)} rows from {sheet_name} (with {len(hyperlinks)} hyperlinks)")
            return values

        except Exception as e:
            # Check if it's a 404 error (sheet not found)
            if "404" in str(e) or "not found" in str(e).lower():
                logger.error(f"Sheet '{sheet_name}' not found in the spreadsheet!")
                logger.error(f"Please check that the sheet name is exactly: '{sheet_name}'")
                logger.error("Available sheets can be checked at:")
                logger.error(f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit")
            else:
                logger.error(f"Error reading sheet: {e}")
            return None

    async def get_today_tasks(self) -> List[Dict]:
        """Get tasks scheduled for today from PROGRESS sheet"""
        try:
            # Read PROGRESS sheet (columns A to M - added column M for skip value)
            progress_data = await self.read_sheet_data(PROGRESS_SHEET, "A:M")

            if not progress_data or len(progress_data) < 2:
                logger.info("No data in PROGRESS sheet")
                return []

            # Get today's day name (e.g., "Wednesday", "Monday")
            today_name = datetime.now(TIMEZONE).strftime("%A")
            logger.info(f"Checking for tasks scheduled for: {today_name}")

            tasks = []
            # Skip header row (index 0)
            for row_idx, row in enumerate(progress_data[1:], start=2):
                # Ensure row has enough columns
                if len(row) < 11:
                    continue

                # Column K (index 10) contains the day name
                scheduled_day = row[10].strip() if len(row) > 10 and row[10] else ""

                # Case-insensitive comparison
                if scheduled_day.lower() == today_name.lower():
                    # Column A (index 0) = Title
                    # Column B (index 1) = URL
                    # Column D (index 3) = Last chapter number
                    # Column M (index 12) = Skip chapters value
                    url = row[1].strip() if len(row) > 1 and row[1] else ""
                    last_chapter_str = row[3].strip() if len(row) > 3 and row[3] else "0"
                    title = row[0].strip() if len(row) > 0 and row[0] else "Unknown"
                    skip_chapters_str = row[12].strip() if len(row) > 12 and row[12] else "0"

                    try:
                        last_chapter = int(last_chapter_str)
                        skip_chapters = int(skip_chapters_str) if skip_chapters_str else 0

                        # Calculate next chapter: (last_chapter + 1) + skip_chapters
                        next_chapter = last_chapter + 1 + skip_chapters

                        logger.info(
                            f"Task calculation: {title} - "
                            f"Last chapter: {last_chapter}, "
                            f"Skip: {skip_chapters}, "
                            f"Next chapter to download: {next_chapter}"
                        )
                    except ValueError as e:
                        logger.warning(
                            f"Invalid number in row {row_idx}: "
                            f"last_chapter='{last_chapter_str}', skip='{skip_chapters_str}' - {e}"
                        )
                        continue

                    if url:
                        tasks.append({
                            'row_number': row_idx,
                            'title': title,
                            'url': url,
                            'chapter': next_chapter,
                            'last_chapter': last_chapter,
                            'skip_chapters': skip_chapters
                        })
                        logger.info(
                            f"Found task: {title} - "
                            f"Chapter {next_chapter} (Row {row_idx}, Skip: {skip_chapters})"
                        )

            logger.info(f"Found {len(tasks)} task(s) for today")
            return tasks

        except Exception as e:
            logger.error(f"Error getting today's tasks: {e}")
            import traceback
            traceback.print_exc()
            return []

    async def get_destination_folder(self, row_number: int) -> Optional[str]:
        """Get destination folder ID from Config sheet"""
        try:
            # Read Config sheet (columns A to C)
            config_data = await self.read_sheet_data(CONFIG_SHEET, "A:C")

            if not config_data or len(config_data) < row_number:
                logger.warning(f"Config sheet doesn't have row {row_number}")
                return None

            # Get the same row number from Config sheet
            config_row = config_data[row_number - 1] if len(config_data) >= row_number else None

            if config_row and len(config_row) > 2:
                # Column C (index 2) contains the folder ID
                folder_id = config_row[2].strip() if config_row[2] else ""
                if folder_id:
                    logger.info(f"Found destination folder for row {row_number}: {folder_id}")
                    return folder_id

            logger.warning(f"No folder ID found in Config sheet row {row_number}")
            return None

        except Exception as e:
            logger.error(f"Error reading Config sheet: {e}")
            return None

    async def download_and_upload_chapter(self, task: Dict, destination_folder_id: str) -> Dict:
        """Download chapter and upload to specified folder"""
        try:
            url = task['url']
            chapter = task['chapter']  # This is the ACTUAL chapter to download (already includes skip)
            title = task['title']
            skip_chapters = task.get('skip_chapters', 0)
            last_chapter = task.get('last_chapter', 0)

            # Calculate the display chapter number (without skip)
            display_chapter = last_chapter + 1

            logger.info(
                f"Processing: {title} - Display Ch.{display_chapter}, "
                f"Downloading actual Ch.{chapter} (Skipped {skip_chapters})"
            )

            # Detect site
            if "comic.naver.com" in url:
                site = "Naver Webtoon"
            elif "webtoons.com" in url:
                site = "Webtoons.com (Global)"
            elif "manga.line.me" in url:
                site = "LINE Manga"
            else:
                logger.error(f"Unsupported site for URL: {url}")
                return {
                    'success': False,
                    'error': 'Unsupported site',
                    'task': task
                }

            # Download chapter using ACTUAL chapter number (with skip)
            logger.info(f"Downloading {title} Actual Chapter {chapter} from {site}")
            stitched_images, error = await self.downloader.download_chapter(site, url, chapter)

            if error or not stitched_images:
                logger.error(f"Failed to download: {error}")
                return {
                    'success': False,
                    'error': error or 'Download failed',
                    'task': task
                }

            # Import clean_filename function
            import re
            def clean_filename(name):
                return re.sub(r'[\\/*?:"<>|]', "", name).strip()

            # Create chapter folder using DISPLAY chapter number (without skip)
            folder_name = f"Ch{display_chapter:03d}"
            chapter_folder_id = await self.drive_uploader.create_folder(folder_name, destination_folder_id)

            if not chapter_folder_id:
                logger.error("Failed to create chapter folder")
                return {
                    'success': False,
                    'error': 'Failed to create folder',
                    'task': task
                }

            # Upload images
            logger.info(f"Uploading {len(stitched_images)} stitched image(s)")
            uploaded_count = 0
            total_size = 0

            for i, img_buffer in enumerate(stitched_images):
                filename = f"Page{i+1:02d}.jpg"
                view_link, file_id = await self.drive_uploader.upload_file(
                    img_buffer, 
                    filename, 
                    chapter_folder_id, 
                    'image/jpeg'
                )

                if view_link:
                    uploaded_count += 1
                    total_size += len(img_buffer.getvalue())

                await asyncio.sleep(0.5)

            if uploaded_count == 0:
                return {
                    'success': False,
                    'error': 'Failed to upload images',
                    'task': task
                }

            # Get folder link
            folder_link = await self.drive_uploader.get_folder_link(chapter_folder_id)
            if not folder_link:
                folder_link = f"https://drive.google.com/drive/folders/{chapter_folder_id}"

            logger.info(f"✅ Successfully uploaded {title} Ch.{display_chapter} (downloaded actual ch.{chapter})")
            return {
                'success': True,
                'task': task,
                'uploaded_count': uploaded_count,
                'total_size': total_size,
                'folder_link': folder_link,
                'display_chapter': display_chapter,  # Add this for notifications
                'actual_chapter': chapter
            }

        except Exception as e:
            logger.error(f"Error in download_and_upload_chapter: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e),
                'task': task
            }

    async def process_daily_tasks(self, notification_channel=None):
        """Process all tasks scheduled for today"""
        try:
            logger.info("Starting daily task processing...")

            tasks = await self.get_today_tasks()

            if not tasks:
                logger.info("No tasks scheduled for today")
                if notification_channel:
                    await notification_channel.send("📅 No scheduled downloads for today")
                return

            if notification_channel:
                await notification_channel.send(
                    f"🔄 Starting automated downloads: {len(tasks)} chapter(s) scheduled for today"
                )

            results = []

            for task in tasks:
                # Get destination folder
                destination_folder = await self.get_destination_folder(task['row_number'])

                if not destination_folder:
                    logger.warning(f"No destination folder for {task['title']}, using default")
                    destination_folder = os.environ.get('GOOGLE_DRIVE_FOLDER_ID', '')

                if not destination_folder:
                    logger.error(f"No destination folder available for {task['title']}")
                    if notification_channel:
                        await notification_channel.send(
                            f"❌ {task['title']} Ch.{task['chapter']}: No destination folder"
                        )
                    continue

                # Download and upload
                result = await self.download_and_upload_chapter(task, destination_folder)
                results.append(result)

                # Send notification
                if notification_channel:
                    if result['success']:
                        skip_info = f" (skipped {task['skip_chapters']})" if task.get('skip_chapters', 0) > 0 else ""
                        await notification_channel.send(
                            f"✅ **{task['title']}** - Chapter {task['chapter']}{skip_info}\n"
                            f"📁 {result['uploaded_count']} image(s) - {result['total_size']/(1024*1024):.2f}MB\n"
                            f"🔗 [Open Folder]({result['folder_link']})"
                        )
                    else:
                        await notification_channel.send(
                            f"❌ **{task['title']}** - Chapter {task['chapter']}\n"
                            f"Error: {result.get('error', 'Unknown error')}"
                        )

                # Wait between downloads
                await asyncio.sleep(5)

            # Summary
            successful = sum(1 for r in results if r['success'])
            failed = len(results) - successful

            if notification_channel:
                await notification_channel.send(
                    f"📊 **Daily Download Summary**\n"
                    f"✅ Successful: {successful}\n"
                    f"❌ Failed: {failed}\n"
                    f"📅 Total: {len(results)}"
                )

            logger.info(f"Daily task processing complete: {successful}/{len(results)} successful")

        except Exception as e:
            logger.error(f"Error in process_daily_tasks: {e}")
            import traceback
            traceback.print_exc()
            if notification_channel:
                await notification_channel.send(f"❌ Error processing daily tasks: {str(e)}")

    async def schedule_loop(self, bot):
        """Main scheduling loop - runs daily at scheduled time"""
        await bot.wait_until_ready()
        logger.info(f"Scheduler started - will run daily at {SCHEDULE_TIME} {TIMEZONE}")

        # Get notification channel
        notification_channel_id = os.environ.get('NOTIFICATION_CHANNEL_ID')
        notification_channel = None

        if notification_channel_id:
            try:
                notification_channel = bot.get_channel(int(notification_channel_id))
                if notification_channel:
                    logger.info(f"Notification channel set: {notification_channel.name}")
            except:
                logger.warning("Could not get notification channel")

        while not bot.is_closed():
            try:
                now = datetime.now(TIMEZONE)
                scheduled_time = TIMEZONE.localize(
                    datetime.combine(now.date(), SCHEDULE_TIME)
                )

                # If scheduled time has passed today, schedule for tomorrow
                if now >= scheduled_time:
                    scheduled_time = TIMEZONE.localize(
                        datetime.combine(
                            (now + timedelta(days=1)).date(),
                            SCHEDULE_TIME
                        )
                    )

                # Calculate wait time
                wait_seconds = (scheduled_time - now).total_seconds()
                logger.info(f"Next scheduled run: {scheduled_time} (in {wait_seconds/3600:.1f} hours)")

                # Wait until scheduled time
                await asyncio.sleep(wait_seconds)

                # Run daily tasks
                logger.info("⏰ Running scheduled tasks...")
                await self.process_daily_tasks(notification_channel)

                # Wait a bit to avoid running twice
                await asyncio.sleep(60)

            except Exception as e:
                logger.error(f"Error in schedule_loop: {e}")
                import traceback
                traceback.print_exc()
                # Wait 1 hour before retrying on error
                await asyncio.sleep(3600)


async def start_scheduler(bot, downloader, drive_uploader):
    """Initialize and start the scheduler"""
    scheduler = SheetScheduler(downloader, drive_uploader)

    # Setup credentials
    if not scheduler.setup_sheets_credentials():
        logger.error("Failed to setup scheduler credentials")
        return None

    # Start the scheduling loop
    bot.loop.create_task(scheduler.schedule_loop(bot))
    logger.info("✅ Scheduler task created")

    return scheduler
