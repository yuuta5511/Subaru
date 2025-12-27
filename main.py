import os
import sys
import asyncio
import logging
from typing import List, Dict
from scheduler import start_scheduler
from downloader import MangaDownloader, GoogleDriveUploader, get_skip_value_from_sheet, clean_filename

try:
    import discord
    from discord.ext import commands
    from discord import app_commands
    import requests
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"Missing library: {e}")
    sys.exit()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = os.environ.get('BOT_TOKEN')
STITCH_HEIGHT = 12000
STITCH_QUALITY = 100

# Discord Bot Setup
intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
downloader = MangaDownloader()
drive_uploader = GoogleDriveUploader()
sheet_scheduler = None  # Will be set when scheduler starts

class SearchResultView(discord.ui.View):
    """View for search results with buttons"""
    def __init__(self, results: List[Dict], timeout=180):
        super().__init__(timeout=timeout)
        self.results = results

        for i, result in enumerate(results[:25]):
            button = discord.ui.Button(
                label=result['title'][:80],
                style=discord.ButtonStyle.primary,
                custom_id=f"select_{i}"
            )
            button.callback = self.create_callback(i)
            self.add_item(button)

    def create_callback(self, index):
        async def callback(interaction: discord.Interaction):
            await interaction.response.edit_message(
                content=f"✅ Selected: **{self.results[index]['title']}**\n\nURL: `{self.results[index]['url']}`\n\nUse `/download` with this URL!",
                view=None
            )
        return callback

@bot.event
async def on_ready():
    global sheet_scheduler
    logger.info(f'{bot.user} has connected to Discord!')
    drive_uploader.setup_credentials()
    sheet_scheduler = await start_scheduler(bot, downloader, drive_uploader)
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} command(s)")
    except Exception as e:
        logger.error(f"Failed to sync commands: {e}")

@bot.tree.command(name="help", description="Show bot commands")
async def help_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🎭 RAW Provider Bot - Stitched Images",
        description="Download manga/webtoon chapters as stitched images in Google Drive folders",
        color=discord.Color.blue()
    )
    embed.add_field(
        name="Commands",
        value=(
            "`/search <name>` - Search Naver Webtoon\n"
            "`/download <url> <ch>` - Download chapter\n"
            "`/batch <url> <s> <e>` - Download multiple\n"
            "`/sites` - Supported sites"
        ),
        inline=False
    )
    embed.add_field(
        name="📤 Features",
        value=(
            "✨ Images stitched vertically (max 12000px)\n"
            "✨ 100% JPEG quality\n"
            "✨ Auto-reads skip value from Google Sheet\n"
            "✨ Uploaded to Google Drive folders"
        ),
        inline=False
    )
    embed.set_footer(text="Stitched images for better reading experience")
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="sites", description="Show supported sites")
async def sites_command(interaction: discord.Interaction):
    embed = discord.Embed(title="🌐 Supported Sites", color=discord.Color.green())
    embed.add_field(name="Naver Webtoon", value="URL needs `titleId=`", inline=False)
    embed.add_field(name="Webtoons.com", value="URL needs `title_no=`", inline=False)
    embed.add_field(name="LINE Manga", value="URL needs product ID", inline=False)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="search", description="Search for webtoons")
@app_commands.describe(name="Webtoon name")
async def search_command(interaction: discord.Interaction, name: str):
    await interaction.response.defer()

    results = await asyncio.get_event_loop().run_in_executor(None, downloader.search_naver, name)

    if not results:
        await interaction.followup.send("😔 No results found")
        return

    embed = discord.Embed(
        title=f"🔍 Results for '{name}'",
        description=f"Found {len(results)} results",
        color=discord.Color.blue()
    )

    view = SearchResultView(results)
    await interaction.followup.send(embed=embed, view=view)

@bot.tree.command(name="download", description="Download chapter as stitched images to Google Drive")
@app_commands.describe(url="Series URL", chapter="Chapter number", site="Website")
@app_commands.choices(site=[
    app_commands.Choice(name="Auto-detect", value="auto"),
    app_commands.Choice(name="Naver", value="Naver Webtoon"),
    app_commands.Choice(name="Webtoons.com", value="Webtoons.com (Global)"),
    app_commands.Choice(name="LINE", value="LINE Manga"),
])
async def download_command(interaction: discord.Interaction, url: str, chapter: int, site: str = "auto"):
    await interaction.response.defer()

    if chapter < 1:
        await interaction.followup.send("❌ Invalid chapter number")
        return

    if site == "auto":
        if "comic.naver.com" in url:
            site = "Naver Webtoon"
        elif "webtoons.com" in url:
            site = "Webtoons.com (Global)"
        elif "manga.line.me" in url:
            site = "LINE Manga"
        else:
            await interaction.followup.send("❌ Could not detect site")
            return

    try:
        # Get skip value from Google Sheet
        skip_value = await get_skip_value_from_sheet(url, sheet_scheduler)
        actual_chapter = chapter + skip_value

        manga_name = "Chapter"
        try:
            r = requests.get(url, headers=downloader.headers, timeout=5)
            soup = BeautifulSoup(r.text, 'html.parser')
            title_tag = soup.find('title')
            if title_tag:
                manga_name = clean_filename(title_tag.text.split('|')[0].split('-')[0].strip())
        except:
            pass

        skip_info = f" (skip: {skip_value}, downloading actual ch: {actual_chapter})" if skip_value > 0 else ""
        status_msg = await interaction.followup.send(
            f"⏳ Starting download: **{manga_name}** Ch.{chapter}{skip_info}"
        )

        async def update_status(msg):
            try:
                await status_msg.edit(content=f"⏳ {msg}")
            except:
                pass

        # Download using actual chapter number (with skip)
        stitched_images, error = await downloader.download_chapter(site, url, actual_chapter, update_status)

        if error or not stitched_images:
            await status_msg.edit(content=f"❌ {error or 'Failed'}")
            return

        # Create folder with REQUESTED chapter number (without skip)
        folder_name = f"{manga_name}_Ch{chapter:03d}"
        await status_msg.edit(content="📁 Creating folder in Google Drive...")

        folder_id = await drive_uploader.create_folder(folder_name)

        if not folder_id:
            await status_msg.edit(content="❌ Failed to create Google Drive folder")
            return

        # Upload stitched images to folder
        await status_msg.edit(content=f"☁️ Uploading {len(stitched_images)} stitched image(s)...")

        uploaded_count = 0
        total_size = 0

        for i, img_buffer in enumerate(stitched_images):
            # Use REQUESTED chapter number in filename (without skip)
            filename = f"{manga_name}_Ch{chapter:03d}_Part{i+1:02d}.jpg"
            view_link, file_id = await drive_uploader.upload_file(img_buffer, filename, folder_id, 'image/jpeg')

            if view_link:
                uploaded_count += 1
                total_size += len(img_buffer.getvalue())

            await asyncio.sleep(0.5)

        # Get folder link
        folder_link = await drive_uploader.get_folder_link(folder_id)

        if not folder_link:
            folder_link = f"https://drive.google.com/drive/folders/{folder_id}"

        # Send success message
        embed = discord.Embed(
            title="✅ Upload Complete!",
            description=f"**{manga_name}** - Chapter {chapter}",
            color=discord.Color.green()
        )
        embed.add_field(name="📁 Images", value=f"{uploaded_count} stitched image(s)", inline=True)
        embed.add_field(name="💾 Total Size", value=f"{total_size/(1024*1024):.2f}MB", inline=True)
        embed.add_field(name="📏 Max Height", value=f"{STITCH_HEIGHT}px", inline=True)
        if skip_value > 0:
            embed.add_field(name="⏭️ Skip Applied", value=f"Downloaded actual chapter {actual_chapter}", inline=True)
        embed.add_field(
            name="🔗 Google Drive Folder",
            value=f"[Open Folder]({folder_link})",
            inline=False
        )
        embed.set_footer(text=f"Downloaded from {site} | Quality: {STITCH_QUALITY}%")

        await status_msg.edit(content=None, embed=embed)

    except Exception as e:
        logger.error(f"Download error: {e}", exc_info=True)
        await interaction.followup.send(f"❌ Error: {str(e)}")

@bot.tree.command(name="batch", description="Download multiple chapters as stitched images")
@app_commands.describe(url="Series URL", start_chapter="Start", end_chapter="End")
async def batch_command(interaction: discord.Interaction, url: str, start_chapter: int, end_chapter: int):
    await interaction.response.defer()

    if start_chapter < 1 or end_chapter < start_chapter:
        await interaction.followup.send("❌ Invalid range")
        return

    if end_chapter - start_chapter > 10:
        await interaction.followup.send("❌ Max 10 chapters per batch")
        return

    if "comic.naver.com" in url:
        site = "Naver Webtoon"
    elif "webtoons.com" in url:
        site = "Webtoons.com (Global)"
    elif "manga.line.me" in url:
        site = "LINE Manga"
    else:
        await interaction.followup.send("❌ Unsupported site")
        return

    try:
        # Get skip value from Google Sheet
        skip_value = await get_skip_value_from_sheet(url, sheet_scheduler)

        manga_name = "Manga"
        try:
            r = requests.get(url, headers=downloader.headers, timeout=5)
            soup = BeautifulSoup(r.text, 'html.parser')
            title_tag = soup.find('title')
            if title_tag:
                manga_name = clean_filename(title_tag.text.split('|')[0].split('-')[0].strip())
        except:
            pass

        skip_info = f" (skip: {skip_value} applied)" if skip_value > 0 else ""
        status_msg = await interaction.followup.send(
            f"⏳ Batch download: **{manga_name}** (Ch.{start_chapter}-{end_chapter}){skip_info}"
        )

        # Create main batch folder
        batch_folder_name = f"{manga_name}_Ch{start_chapter:03d}-{end_chapter:03d}"
        main_folder_id = await drive_uploader.create_folder(batch_folder_name)

        if not main_folder_id:
            await status_msg.edit(content="❌ Failed to create main folder")
            return

        uploaded_chapters = []

        for ch in range(start_chapter, end_chapter + 1):
            actual_ch = ch + skip_value
            await status_msg.edit(content=f"⏳ Downloading Chapter {ch}/{end_chapter} (actual: {actual_ch})...")

            stitched_images, error = await downloader.download_chapter(site, url, actual_ch)

            if error or not stitched_images:
                await interaction.followup.send(f"⚠️ Skipped Ch.{ch}: {error or 'Failed'}")
                continue

            # Create subfolder with DISPLAY chapter number (without skip)
            chapter_folder_name = f"Ch{ch:03d}"
            chapter_folder_id = await drive_uploader.create_folder(chapter_folder_name, main_folder_id)

            if not chapter_folder_id:
                await interaction.followup.send(f"⚠️ Failed to create folder for Ch.{ch}")
                continue

            await status_msg.edit(content=f"☁️ Uploading Chapter {ch} ({len(stitched_images)} part(s))...")

            uploaded_count = 0
            total_size = 0

            for i, img_buffer in enumerate(stitched_images):
                # Use DISPLAY chapter number in filename (without skip)
                filename = f"{manga_name}_Ch{ch:03d}_Part{i+1:02d}.jpg"
                view_link, file_id = await drive_uploader.upload_file(img_buffer, filename, chapter_folder_id, 'image/jpeg')

                if view_link:
                    uploaded_count += 1
                    total_size += len(img_buffer.getvalue())

                await asyncio.sleep(0.5)

            if uploaded_count > 0:
                folder_link = await drive_uploader.get_folder_link(chapter_folder_id)
                if not folder_link:
                    folder_link = f"https://drive.google.com/drive/folders/{chapter_folder_id}"

                uploaded_chapters.append({
                    'chapter': ch,
                    'actual_chapter': actual_ch,
                    'folder_link': folder_link,
                    'images': uploaded_count,
                    'size': total_size
                })

                skip_note = f" (actual: {actual_ch})" if skip_value > 0 else ""
                await interaction.followup.send(
                    f"✅ Chapter {ch}{skip_note} uploaded! ({uploaded_count} image(s), {total_size/(1024*1024):.2f}MB)\n🔗 [Open Folder]({folder_link})"
                )
            else:
                await interaction.followup.send(f"❌ Failed to upload Chapter {ch}")

            await asyncio.sleep(2)

        # Send summary
        if uploaded_chapters:
            main_folder_link = await drive_uploader.get_folder_link(main_folder_id)
            if not main_folder_link:
                main_folder_link = f"https://drive.google.com/drive/folders/{main_folder_id}"

            embed = discord.Embed(
                title="✅ Batch Upload Complete!",
                description=f"**{manga_name}** - Chapters {start_chapter}-{end_chapter}",
                color=discord.Color.green()
            )

            chapters_text = "\n".join([
                f"Ch.{item['chapter']}: {item['images']} image(s) - {item['size']/(1024*1024):.1f}MB"
                for item in uploaded_chapters
            ])

            embed.add_field(name="📚 Uploaded Chapters", value=chapters_text, inline=False)
            embed.add_field(
                name="📁 Main Folder",
                value=f"[Open All Chapters]({main_folder_link})",
                inline=False
            )
            if skip_value > 0:
                embed.add_field(name="⏭️ Skip Applied", value=f"{skip_value} chapters", inline=True)
            embed.set_footer(text=f"Uploaded {len(uploaded_chapters)} chapters with stitched images")

            await status_msg.edit(content=None, embed=embed)
        else:
            await status_msg.edit(content="❌ No chapters were successfully uploaded")

    except Exception as e:
        logger.error(f"Batch error: {e}", exc_info=True)
        await interaction.followup.send(f"❌ Error: {str(e)}")

@bot.tree.command(name="list_sheets", description="List all sheet names in the Google Spreadsheet")
@app_commands.default_permissions(administrator=True)
async def list_sheets_command(interaction: discord.Interaction):
    """Debug command to see all available sheet names"""
    await interaction.response.defer()

    try:
        from scheduler import SheetScheduler, SHEET_ID, PROGRESS_SHEET, CONFIG_SHEET

        scheduler = SheetScheduler(downloader, drive_uploader)

        if not scheduler.setup_sheets_credentials():
            await interaction.followup.send("❌ Failed to setup Sheets credentials")
            return

        sheet_names = await scheduler.get_sheet_names()

        if not sheet_names:
            await interaction.followup.send("❌ Could not read sheet names from spreadsheet")
            return

        sheets_list = "\n".join([f"• `{name}`" for name in sheet_names])

        embed = discord.Embed(
            title="📋 Google Spreadsheet Sheets",
            description=f"Spreadsheet ID: `{SHEET_ID}`",
            color=discord.Color.blue()
        )

        embed.add_field(name="Available Sheets", value=sheets_list, inline=False)
        embed.add_field(
            name="Currently Looking For",
            value=f"• Progress: `{PROGRESS_SHEET}`\n• Config: `{CONFIG_SHEET}`",
            inline=False
        )
        embed.set_footer(text="Sheet names are case-sensitive!")

        await interaction.followup.send(embed=embed)

    except Exception as e:
        logger.error(f"Error in list_sheets command: {e}")
        import traceback
        traceback.print_exc()
        await interaction.followup.send(f"❌ Error: {str(e)}")

@bot.tree.command(name="test_schedule", description="Manually trigger scheduled downloads")
@app_commands.default_permissions(administrator=True)
async def test_schedule_command(interaction: discord.Interaction):
    """Manually test the scheduler"""
    await interaction.response.defer()

    try:
        from scheduler import SheetScheduler
        from datetime import datetime
        import pytz

        scheduler = SheetScheduler(downloader, drive_uploader)

        if not scheduler.setup_sheets_credentials():
            await interaction.followup.send("❌ Failed to setup Sheets credentials")
            return

        TIMEZONE = pytz.timezone('Etc/GMT-2')
        today_name = datetime.now(TIMEZONE).strftime("%A")

        await interaction.followup.send(
            f"⏳ Running scheduled tasks...\n"
            f"📅 Looking for tasks scheduled for: **{today_name}**"
        )

        await scheduler.process_daily_tasks(interaction.channel)

    except Exception as e:
        logger.error(f"Error in test_schedule command: {e}")
        import traceback
        traceback.print_exc()
        await interaction.followup.send(f"❌ Error: {str(e)}")

# Run the bot
if __name__ == "__main__":
    if not BOT_TOKEN:
        print("❌ Bot token not found!")
        print("Add BOT_TOKEN to Replit Secrets")
        sys.exit()

    try:
        bot.run(BOT_TOKEN)
    except Exception as e:
        logger.error(f"Bot error: {e}", exc_info=True)
    finally:
        downloader.close_selenium()
