import os
import shutil
import asyncio
import aiohttp
import aiofiles
import py7zr
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv
import logging

# Load .env file
load_dotenv()

TOPROCESS_FOLDER = os.getenv("TOPROCESS_FOLDER")
FAILED_FOLDER = os.getenv("FAILED_FOLDER")
PROCESSED_FOLDER = os.getenv("PROCESSED_FOLDER")
ORTHANC_ENDPOINT = os.getenv("ORTHANC_ENDPOINT")
MAX_CONCURRENT_UPLOADS = int(os.getenv("MAX_CONCURRENT_UPLOADS", 10))

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Function to decompress .7z files
async def decompress_7z(file_path, output_dir):
    try:
        with py7zr.SevenZipFile(file_path, mode='r') as archive:
            archive.extractall(path=output_dir)
        return True
    except Exception as e:
        logging.error(f"Failed to decompress {file_path}: {e}")
        return False

# Function to upload a single file
async def upload_file(file_path, semaphore, session):
    async with semaphore:
        try:
            async with aiofiles.open(file_path, "rb") as f:
                data = await f.read()
            async with session.post(ORTHANC_ENDPOINT, data=data) as resp:
                if resp.status in {200, 201}:
                    logging.info(f"Uploaded {file_path}")
                else:
                    logging.error(f"Failed to upload {file_path}, status code: {resp.status}")
                    return False
        except Exception as e:
            logging.error(f"Error uploading {file_path}: {e}")
            return False
    return True

# Process a single .7z file
async def process_file(file_path):
    logging.info(f"Processing file: {file_path}")
    temp_dir = Path(file_path).stem + "_temp"
    temp_dir_path = Path(TOPROCESS_FOLDER) / temp_dir

    try:
        # Step 1: Decompress the .7z file
        if not await decompress_7z(file_path, temp_dir_path):
            shutil.move(file_path, FAILED_FOLDER)
            return

        # Step 2: Upload all decompressed files asynchronously
        tasks = []
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
        async with aiohttp.ClientSession() as session:
            for root, _, files in os.walk(temp_dir_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    tasks.append(upload_file(file_path, semaphore, session))
            results = await asyncio.gather(*tasks)

        # Step 3: Move the .7z file to the appropriate folder
        if all(results):
            shutil.move(file_path, PROCESSED_FOLDER)
            logging.info(f"Successfully processed {file_path}")
        else:
            shutil.move(file_path, FAILED_FOLDER)
            logging.warning(f"Processing partially failed for {file_path}")

    finally:
        # Clean up temp directory
        shutil.rmtree(temp_dir_path, ignore_errors=True)

# Watchdog event handler
class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(".7z"):
            asyncio.run(process_file(event.src_path))

# Start monitoring
def start_monitoring():
    observer = Observer()
    handler = FileHandler()
    observer.schedule(handler, TOPROCESS_FOLDER, recursive=False)
    observer.start()
    logging.info("Monitoring started...")
    try:
        while True:
            pass  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    start_monitoring()
