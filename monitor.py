import os
import shutil
import requests
import py7zr
import zipfile
import tempfile
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import logging

# Load .env file
load_dotenv()

TOPROCESS_FOLDER = os.getenv("TOPROCESS_FOLDER")
FAILED_FOLDER = os.getenv("FAILED_FOLDER")
PROCESSED_FOLDER = os.getenv("PROCESSED_FOLDER")
ORTHANC_ENDPOINT = os.getenv("ORTHANC_ENDPOINT")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 5))

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Create a ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

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

# Function to upload a file
def upload_file(file_path, content_type):
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        headers = {'Content-Type': content_type}
        response = requests.post(ORTHANC_ENDPOINT, data=data, headers=headers)
        if response.status_code in {200, 201}:
            logging.info(f"Uploaded {file_path}")
            return True
        else:
            logging.error(f"Failed to upload {file_path}, status code: {response.status_code}")
            return False
    except Exception as e:
        logging.error(f"Error uploading {file_path}: {e}")
        return False

# Process a single file
def process_file(original_file_path):
    logging.info(f"Processing file: {original_file_path}")
    extension = os.path.splitext(original_file_path)[1].lower()

    if extension == '.7z':
        # Use a temporary directory outside of the monitored folders
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            zip_file_name = Path(original_file_path).stem + ".zip"
            zip_file_path = temp_dir_path / zip_file_name

            try:
                # Step 1: Decompress the .7z file into the temporary directory
                if not decompress_7z(original_file_path, temp_dir_path):
                    shutil.move(original_file_path, FAILED_FOLDER)
                    return

                # Step 2: Compress the decompressed files into a .zip archive in the temporary directory
                if not compress_to_zip(temp_dir_path, zip_file_path):
                    shutil.move(original_file_path, FAILED_FOLDER)
                    return

                # Step 3: Upload the .zip file
                if upload_file(zip_file_path, 'application/zip'):
                    # Move the original .7z file to the PROCESSED_FOLDER
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
                # Temporary directory and files are automatically cleaned up
                pass
    elif extension == '.zip':
        # Upload the zip file directly
        if upload_file(original_file_path, 'application/zip'):
            # Move the original .zip file to the PROCESSED_FOLDER
            processed_file_path = os.path.join(PROCESSED_FOLDER, os.path.basename(original_file_path))
            if os.path.exists(processed_file_path):
                os.remove(processed_file_path)
            shutil.move(original_file_path, processed_file_path)
            logging.info(f"Successfully processed {original_file_path}")
            logging.info(f"Moved to {processed_file_path}")
        else:
            shutil.move(original_file_path, FAILED_FOLDER)
            logging.warning(f"Processing failed for {original_file_path}")
    elif extension == '.dcm':
        # Upload the .dcm file directly
        if upload_file(original_file_path, 'application/dicom'):
            # Move the original .dcm file to the PROCESSED_FOLDER
            processed_file_path = os.path.join(PROCESSED_FOLDER, os.path.basename(original_file_path))
            if os.path.exists(processed_file_path):
                os.remove(processed_file_path)
            shutil.move(original_file_path, processed_file_path)
            logging.info(f"Successfully processed {original_file_path}")
            logging.info(f"Moved to {processed_file_path}")
        else:
            shutil.move(original_file_path, FAILED_FOLDER)
            logging.warning(f"Processing failed for {original_file_path}")
    else:
        logging.warning(f"Unsupported file type: {original_file_path}")
        # Optionally, move to failed folder
        shutil.move(original_file_path, FAILED_FOLDER)

# Process a path (file or directory)
def process_path(path):
    if os.path.isdir(path):
        # It's a directory; traverse its contents
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                file_path = os.path.join(root, name)
                executor.submit(process_file, file_path)
            for name in dirs:
                dir_path = os.path.join(root, name)
                # Remove empty directories
                if os.path.exists(dir_path):
                    try:
                        os.rmdir(dir_path)
                    except Exception as e:
                        logging.error(f"Failed to remove directory {dir_path}: {e}")
        # After processing all files and directories, remove the root directory
        if os.path.exists(path):
            try:
                os.rmdir(path)
            except Exception as e:
                logging.error(f"Failed to remove directory {path}: {e}")
    else:
        # It's a file
        executor.submit(process_file, path)

# Watchdog event handler
class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            process_path(event.src_path)
        else:
            process_path(event.src_path)

# Process existing files at startup
def process_existing_files():
    for root, dirs, files in os.walk(TOPROCESS_FOLDER):
        for name in files:
            file_path = os.path.join(root, name)
            process_path(file_path)
        for name in dirs:
            dir_path = os.path.join(root, name)
            process_path(dir_path)

# Start monitoring
def start_monitoring():
    # Process existing files and directories at startup
    process_existing_files()

    observer = Observer()
    handler = FileHandler()
    observer.schedule(handler, TOPROCESS_FOLDER, recursive=True)
    observer.start()
    logging.info("Monitoring started...")
    try:
        while True:
            pass  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    # Shutdown the executor
    executor.shutdown(wait=True)

if __name__ == "__main__":
    start_monitoring()
