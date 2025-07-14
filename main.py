import os
import asyncio
import logging
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from urllib.parse import urlparse
from motor.motor_asyncio import AsyncIOMotorClient

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
SESSION_STRING = os.environ.get("SESSION_STRING")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")
WORKER_URL_TEMPLATE = "https://icy-brook12.arjunavai273.workers.dev/?id="
TELEGRAM_LIMIT_BYTES = 2 * 1024 * 1024 * 1024

logging.basicConfig(level=logging.INFO)

# --- MongoDB Setup ---
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client['terabox']
collection = db['processed_links']

# --- Telethon Setup ---
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# --- Helper Functions ---
def extract_id_from_url(url):
    try:
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.strip('/').split('/')
        return path_parts[-1] if path_parts else None
    except:
        return None

def get_file_size(filepath):
    return os.path.getsize(filepath) if os.path.exists(filepath) else 0

async def run_ffmpeg_async(command):
    process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    await process.communicate()
    return process.returncode

async def already_processed(url):
    return await collection.find_one({"url": url}) is not None

async def mark_as_processed(url):
    await collection.insert_one({"url": url})

async def process_url(url, target_entity):
    video_id = extract_id_from_url(url)
    if not video_id:
        return

    worker_url = f"{WORKER_URL_TEMPLATE}{video_id}"
    output_file = f"{video_id}.mp4"
    compressed_file = f"compressed_{video_id}.mp4"
    final_file = output_file

    command = ["ffmpeg", "-rw_timeout", "5000000", "-reconnect", "1", "-reconnect_at_eof", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5", "-i", worker_url, "-c", "copy", "-y", output_file]
    if await run_ffmpeg_async(command) != 0:
        return

    if get_file_size(output_file) > TELEGRAM_LIMIT_BYTES:
        crf = 24
        while get_file_size(final_file) > TELEGRAM_LIMIT_BYTES:
            if crf > 35:
                return
            compress_command = ["ffmpeg", "-i", output_file, "-c:v", "libx264", "-crf", str(crf), "-preset", "fast", "-c:a", "copy", "-y", compressed_file]
            if await run_ffmpeg_async(compress_command) != 0:
                return
            final_file = compressed_file
            crf += 2

    if os.path.exists(final_file):
        await client.send_file(target_entity, final_file)
        await mark_as_processed(url)
        os.remove(final_file)
    if os.path.exists(output_file): os.remove(output_file)
    if os.path.exists(compressed_file): os.remove(compressed_file)

@client.on(events.NewMessage(pattern='/upload'))
async def handle_upload(event):
    if event.file and event.file.name.endswith(".txt"):
        path = await event.download_media()
        with open(path, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        await event.reply(f"📥 Found {len(urls)} links. Starting processing...")

        for idx, url in enumerate(urls):
            if not await already_processed(url):
                await event.respond(f"➡️ Processing link {idx+1}: {url[:50]}...")
                try:
                    await process_url(url, event.chat_id)
                except Exception as e:
                    await event.respond(f"❌ Error processing link {url[:50]}: {e}")
        await event.respond("✅ All links processed.")
    else:
        await event.reply("Please send a `.txt` file containing links.")

async def main():
    await client.start(bot_token=BOT_TOKEN)
    print("Bot is running!")
    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
