#!/bin/bash
# atmoz/sftp scans /etc/sftp.d/*.sh on startup. Ensure ods can write to upload dir.
set -e
mkdir -p /home/ods/upload
chown -R ods:users /home/ods/upload
chmod 750 /home/ods/upload
