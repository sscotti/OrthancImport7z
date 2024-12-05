## Docker package consisting of an Orthanc Instance and a Python container designed to monitor a drop folder for .dcm, .zip and .7z files.  The script monitors the folder and then processes the dropped files by.

1.  Decompressing and converting the  .7z file to a .zip format file and then sending to Orthanc.
2.  Sending any .zip files to the designated Orthanc API endpoint.
3.  Sending any .dcm files to the designated Orthanc API endpoint.
3.  Cleaning up by moving the original file to a FAILED or PROCESSED folder.

Orthanc can process .zip files on its end, so if your bandwidth is limited it makes more sense to send the files as a .zip archive.

Feel free to add more advanced logging, file handling, etc.

As it is, the script examines the file extension to detect the file type.  It might be better to detect the MIME type itself:
```
# Detect the MIME type of the file
    try:
        mime = magic.from_file(original_file_path, mime=True)
    except Exception as e:
        logging.error(f"Failed to determine file type for {original_file_path}: {e}")
        shutil.move(original_file_path, FAILED_FOLDER)
        return

    logging.info(f"Detected MIME type: {mime}")

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
        ```