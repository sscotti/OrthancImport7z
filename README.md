## Docker package consisting of an Orthanc Instance and a Python container designed to monitor a drop folder for .7z compressed files consisting of .dcm files.  The script monitors the folder and then processes the dropped files by.

1.  Decompressing and converting the  .7z file to a .zip format file.
2.  Sending the .zip file to the designated Orthanc API endpoint.
3.  Cleaning up by moving the original file to a FAILED or PROCESSED folder.

Orthanc can process .zip files on its end, so if your bandwidth is limited it makes more sense to send the files as a .zip archive.

Feel free to add more advanced logging, file handling, etc.