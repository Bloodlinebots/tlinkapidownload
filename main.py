import os
import asyncio
import logging
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from urllib.parse import urlparse
from motor.motor_asyncio import AsyncIOMotorClient

# --- ENV VARIABLES ---
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
SESSION_STRING = os.environ.get("SESSION_STRING")
MONGO_URI = os.environ.get("MONGO_URI")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "Tharki_hub_bot")

WORKER_URL_TEMPLATE = "https://icy-brook12.arjunavai273.workers.dev/?id="
TELEGRAM_LIMIT_BYTES = 2 * 1024 * 1024 * 1024
START_INDEX = 1000  # Start from the 1000th link

logging.basicConfig(level=logging.INFO)

# --- MongoDB Setup ---
mongo = AsyncIOMotorClient(MONGO_URI)
db = mongo['terabox']
all_links = db['all_links']
processed = db['processed_urls']

# --- Telethon Client ---
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# --- Helper Functions ---
def extract_id_from_url(url):
    try:
        parsed_url = urlparse(url)
        parts = parsed_url.path.strip("/").split("/")
        return parts[-1] if parts else None
    except:
        return None

def get_file_size(path):
    return os.path.getsize(path) if os.path.exists(path) else 0

async def run_ffmpeg_async(cmd):
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    await proc.communicate()
    return proc.returncode

async def store_links_in_mongo(urls):
    existing = await all_links.count_documents({})
    new_entries = [{"index": i + 1 + existing, "url": url} for i, url in enumerate(urls)]
    if new_entries:
        await all_links.insert_many(new_entries)

async def already_done(url):
    return await processed.find_one({"url": url}) is not None

async def mark_done(url):
    await processed.insert_one({"url": url})

async def process_url(url, target_entity, index):
    video_id = extract_id_from_url(url)
    if not video_id:
        print(f"[{index}] Invalid URL: {url}")
        return

    worker_url = f"{WORKER_URL_TEMPLATE}{video_id}"
    raw_file = f"{video_id}.mp4"
    compressed_file = f"compressed_{video_id}.mp4"
    final_file = raw_file

    cmd = ["ffmpeg", "-rw_timeout", "5000000", "-reconnect", "1", "-reconnect_at_eof", "1",
           "-reconnect_streamed", "1", "-reconnect_delay_max", "5", "-i", worker_url,
           "-c", "copy", "-y", raw_file]

    if await run_ffmpeg_async(cmd) != 0:
        print(f"[{index}] ❌ Failed to download.")
        return

    if get_file_size(raw_file) > TELEGRAM_LIMIT_BYTES:
        crf = 24
        while get_file_size(final_file) > TELEGRAM_LIMIT_BYTES and crf <= 35:
            compress_cmd = ["ffmpeg", "-i", raw_file, "-c:v", "libx264", "-crf", str(crf),
                            "-preset", "fast", "-c:a", "copy", "-y", compressed_file]
            if await run_ffmpeg_async(compress_cmd) != 0:
                print(f"[{index}] ❌ Compression failed.")
                return
            final_file = compressed_file
            crf += 2

    if os.path.exists(final_file):
        await client.send_file(target_entity, final_file)
        await mark_done(url)
        print(f"[{index}] ✅ Uploaded!")
        os.remove(final_file)
    if os.path.exists(raw_file): os.remove(raw_file)
    if os.path.exists(compressed_file): os.remove(compressed_file)

# --- Telegram Message Handler ---
@client.on(events.NewMessage)
async def txt_handler(event):
    if not event.file or not event.file.name.endswith(".txt"):
        return

    path = await event.download_media()
    with open(path, 'r') as f:
        urls = [line.strip() for line in f if line.strip()]

    await store_links_in_mongo(urls)
    await event.reply(f"📥 Stored {len(urls)} links in MongoDB!")

    target_entity = await client.get_entity(BOT_USERNAME)
    cursor = all_links.find().sort("index", 1).skip(START_INDEX - 1)

    i = START_INDEX
    async for link_doc in cursor:
        url = link_doc["url"]
        if await already_done(url):
            print(f"[{i}] Skipped (already processed)")
            i += 1
            continue

        try:
            print(f"[{i}] 🔄 Processing: {url}")
            await process_url(url, target_entity, i)
        except Exception as e:
            print(f"[{i}] ❌ Error: {e}")
        i += 1

    await event.reply("✅ All pending links processed.")

# --- Start the Bot ---
async def main():
    await client.start()
    print("✅ Bot started and waiting for .txt file...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
