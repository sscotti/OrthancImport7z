import os
import shutil
import asyncio
import aiohttp
import aiofiles
import py7zr
import zipfile
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
def decompress_7z(archive_path, output_dir):
    try:
        with py7zr.SevenZipFile(archive_path, mode='r') as archive:
            archive.extractall(path=output_dir)
        return True
    except Exception as e:
        logging.error(f"Failed to decompress {archive_path}: {e}")
        return False

# Function to compress files into a .zip archive
def compress_to_zip(input_dir, zip_path):
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(input_dir):
                for file in files:
                    full_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_file_path, input_dir)
                    zipf.write(full_file_path, arcname=relative_path)
                    
        return True
    except Exception as e:
        logging.error(f"Failed to compress {input_dir} into {zip_path}: {e}")
        return False

# Function to upload a .zip file
async def upload_zip_file(zip_file_path):
    try:
        async with aiohttp.ClientSession() as session:
            async with aiofiles.open(zip_file_path, "rb") as f:
                data = await f.read()
            headers = {'Content-Type': 'application/zip'}
            async with session.post(ORTHANC_ENDPOINT, data=data, headers=headers) as resp:
                if resp.status in {200, 201}:
                    logging.info(f"Uploaded {zip_file_path}")
                    return True
                else:
                    logging.error(f"Failed to upload {zip_file_path}, status code: {resp.status}")
                    return False
    except Exception as e:
        logging.error(f"Error uploading {zip_file_path}: {e}")
        return False

# Process a single .7z file
async def process_file(original_file_path):
    logging.info(f"Processing file: {original_file_path}")
    temp_dir_name = Path(original_file_path).stem + "_temp"
    temp_dir_path = Path(TOPROCESS_FOLDER) / temp_dir_name
    zip_file_name = Path(original_file_path).stem + ".zip"
    zip_file_path = Path(TOPROCESS_FOLDER) / zip_file_name

    try:
        # Step 1: Decompress the .7z file
        if not decompress_7z(original_file_path, temp_dir_path):
            shutil.move(original_file_path, FAILED_FOLDER)
            return

        # Step 2: Compress the decompressed files into a .zip archive
        if not compress_to_zip(temp_dir_path, zip_file_path):
            shutil.move(original_file_path, FAILED_FOLDER)
            return
        # Step 3: Upload the .zip file
        if await upload_zip_file(zip_file_path):
            # Move the original .7z file to the PROCESSED_FOLDER
            # Overwrite the existing file in PROCESSED_FOLDER
            processed_file_path = os.path.join(PROCESSED_FOLDER, os.path.basename(original_file_path))
            if os.path.exists(processed_file_path):
                os.remove(processed_file_path)
            shutil.move(original_file_path, processed_file_path)
            logging.info(f"Successfully processed {original_file_path}")
            logging.info(f"Moved to {processed_file_path}")

        else:
            shutil.move(original_file_path, FAILED_FOLDER)
            logging.warning(f"Processing failed for {original_file_path}")

    finally:
        # Clean up temp directory and zip file
        shutil.rmtree(temp_dir_path, ignore_errors=True)
        if zip_file_path.exists():
            zip_file_path.unlink()

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
