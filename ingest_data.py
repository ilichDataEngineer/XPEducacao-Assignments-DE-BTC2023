import os
import threading
import sys
import pandas as pd
import boto3
from boto3.s3.transfer import TransferConfig, S3Transfer

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write("\r%s  %s / %s  (%.2f%%)" % (self._filename, self._seen_so_far, self._size, percentage))
            sys.stdout.flush()

# Criar um cliente para interagir com o AWS S3
s3_client = boto3.client('s3')

# Path with the data
folder_path = r'C:\Users\idbs9\edc-bc-xpe\edc-mod1-desafio-ib\data\ftp.mtps.gov.br\pdet\microdados\RAIS\2020'

all_files = os.listdir(folder_path)
txt_files = [f for f in all_files if f.endswith(".txt")]

bucket_name = "dl-ib-igti-edc-des-mod1"
folder_name = "raw-data"

transfer_config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                                  multipart_chunksize=1024 * 25, use_threads=True)
transfer_manager = S3Transfer(s3_client, transfer_config)

for file in txt_files:
    if file.startswith("RAIS_VINC_PUB"):
        file_path = os.path.join(folder_path, file)
        key = folder_name + "/" + file
        transfer_manager.upload_file(file_path, bucket_name, key, callback=ProgressPercentage(file_path))