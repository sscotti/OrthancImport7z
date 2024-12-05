import os
import shutil
import requests
import py7zr
import zipfile
import tempfile
import magic
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

# Helper functions to identify MacOSX files and directories
def is_macosx_file(filename):
    return filename == '.DS_Store' or filename.startswith('._')

def is_macosx_dir(dirname):
    return dirname == '__MACOSX'

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
    logging.info(f"Starting compression from {input_dir} to {zip_path}")
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(input_dir):
                # Skip __MACOSX directories
                dirs[:] = [d for d in dirs if not is_macosx_dir(d)]
                for file in files:
                    if is_macosx_file(file):
                        continue  # Skip MacOSX files
                    full_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_file_path, input_dir)
                    zipf.write(full_file_path, arcname=relative_path)
        logging.info(f"Compression successful: {zip_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to compress {input_dir} into {zip_path}: {e}")
        return False

# Function to upload a file
def upload_file(file_path, content_type):
    logging.info(f"Uploading file {file_path} with content type {content_type}")
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        headers = {'Content-Type': content_type}
        response = requests.post(ORTHANC_ENDPOINT, data=data, headers=headers)
        if response.status_code in {200, 201}:
            logging.info(f"Uploaded {file_path} successfully with status code {response.status_code}")
            return True
        else:
            logging.error(f"Failed to upload {file_path}, status code: {response.status_code}, response: {response.text}")
            return False
    except Exception as e:
        logging.error(f"Error uploading {file_path}: {e}")
        return False

# Move file to processed folder
def move_to_processed(original_file_path):
    try:
        processed_file_path = os.path.join(PROCESSED_FOLDER, os.path.basename(original_file_path))
        if os.path.exists(processed_file_path):
            os.remove(processed_file_path)
        shutil.move(original_file_path, processed_file_path)
        logging.info(f"Successfully moved {original_file_path} to {processed_file_path}")
    except Exception as e:
        logging.error(f"Failed to move {original_file_path} to {PROCESSED_FOLDER}: {e}")
        # Move to FAILED_FOLDER if moving to PROCESSED_FOLDER fails
        try:
            shutil.move(original_file_path, FAILED_FOLDER)
            logging.warning(f"Moved {original_file_path} to FAILED_FOLDER due to move error")
        except Exception as e2:
            logging.error(f"Failed to move {original_file_path} to {FAILED_FOLDER}: {e2}")

# Process .7z files
def process_7z_file(original_file_path):
    logging.info(f"Starting to process 7z file: {original_file_path}")
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
            logging.info(f"Uploading ZIP file: {zip_file_path}")
            if upload_file(zip_file_path, 'application/zip'):
                logging.info(f"Upload successful for {zip_file_path}")
                move_to_processed(original_file_path)
            else:
                logging.warning(f"Upload failed for {zip_file_path}")
                shutil.move(original_file_path, FAILED_FOLDER)
                logging.warning(f"Moved {original_file_path} to FAILED_FOLDER")
        except Exception as e:
            logging.error(f"Unexpected error processing 7z file {original_file_path}: {e}")
            shutil.move(original_file_path, FAILED_FOLDER)
            logging.warning(f"Moved {original_file_path} to FAILED_FOLDER due to error")

# Process .zip files
def process_zip_file(original_file_path):
    logging.info(f"Starting to process ZIP file: {original_file_path}")
    try:
        # Upload the zip file directly without modifying it
        logging.info(f"Uploading ZIP file: {original_file_path}")
        if upload_file(original_file_path, 'application/zip'):
            logging.info(f"Upload successful for {original_file_path}")
            move_to_processed(original_file_path)
        else:
            logging.warning(f"Upload failed for {original_file_path}")
            shutil.move(original_file_path, FAILED_FOLDER)
            logging.warning(f"Moved {original_file_path} to FAILED_FOLDER")
    except Exception as e:
        logging.error(f"Unexpected error processing zip file {original_file_path}: {e}")
        shutil.move(original_file_path, FAILED_FOLDER)
        logging.warning(f"Moved {original_file_path} to FAILED_FOLDER due to error")

# Process .dcm files
def process_dcm_file(original_file_path):
    logging.info(f"Starting to process DICOM file: {original_file_path}")
    try:
        if upload_file(original_file_path, 'application/dicom'):
            logging.info(f"Upload successful for {original_file_path}")
            move_to_processed(original_file_path)
        else:
            logging.warning(f"Upload failed for {original_file_path}")
            shutil.move(original_file_path, FAILED_FOLDER)
            logging.warning(f"Moved {original_file_path} to FAILED_FOLDER")
    except Exception as e:
        logging.error(f"Unexpected error processing DICOM file {original_file_path}: {e}")
        shutil.move(original_file_path, FAILED_FOLDER)
        logging.warning(f"Moved {original_file_path} to FAILED_FOLDER due to error")

# Process a single file
def process_file(original_file_path):
    logging.info(f"Processing file: {original_file_path}")

    # Detect the MIME type of the file
    try:
        mime = magic.from_file(original_file_path, mime=True)
    except Exception as e:
        logging.error(f"Failed to determine file type for {original_file_path}: {e}")
        shutil.move(original_file_path, FAILED_FOLDER)
        return

    logging.info(f"Detected MIME type: {mime}")

    try:
        if mime == 'application/x-7z-compressed':
            # Handle .7z files
            process_7z_file(original_file_path)
        elif mime == 'application/zip':
            # Handle .zip files
            process_zip_file(original_file_path)
        elif mime in ('application/dicom', 'application/octet-stream'):
            # Handle .dcm files
            process_dcm_file(original_file_path)
        else:
            logging.warning(f"Unsupported file type ({mime}): {original_file_path}")
            shutil.move(original_file_path, FAILED_FOLDER)
    except Exception as e:
        logging.error(f"Error processing file {original_file_path}: {e}")
        shutil.move(original_file_path, FAILED_FOLDER)

# Process a path (file or directory)
def process_path(path):
    if os.path.isdir(path):
        # It's a directory; traverse its contents
        for root, dirs, files in os.walk(path, topdown=False):
            # Skip __MACOSX directories
            dirs[:] = [d for d in dirs if not is_macosx_dir(d)]
            for name in files:
                if is_macosx_file(name):
                    continue  # Skip MacOSX files
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
        if is_macosx_file(os.path.basename(path)):
            return  # Skip MacOSX files
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
        # Skip __MACOSX directories
        dirs[:] = [d for d in dirs if not is_macosx_dir(d)]
        for name in files:
            if is_macosx_file(name):
                continue  # Skip MacOSX files
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
