# -*- coding: utf-8 -*-
"""
This code lists all the files/files with a certain starting name, and sends the names one by one to the queue.
This is important in a datapipeline, when you want to process some files further using a queue triggered function.

"""

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import (BinaryBase64EncodePolicy,QueueServiceClient)
       
def send_blobnames_to_queue(prefix_blob,abs_acct_url,abs_container_name):
    try:
        
        blob_service_client = BlobServiceClient(account_url=abs_acct_url, credential=DefaultAzureCredential())
        container_client = blob_service_client.get_container_client(abs_container_name)

        blob_list = container_client.list_blobs(prefix_blob)
                
        queuename="<queuename>"
        queueserviceclient = QueueServiceClient(account_url="<queueurl>", credential=DefaultAzureCredential())
        queueClient=queueserviceclient.get_queue_client(queuename)
        queueClient.message_encode_policy = BinaryBase64EncodePolicy()
        
        for blob in blob_list:
            blob_=str(blob.name).split("/")[3]
            print(blob_)
            queueClient.send_message(queueClient.message_encode_policy.encode((blob_).encode('utf-8')))

    except Exception as e:
        print(e)
    

if __name__ == '__main__':
    # Parameters/Configurations
    abs_acct_name="<storageaccountname>"
    abs_acct_url=f'https://{abs_acct_name}.blob.core.windows.net/'
    abs_container_name="<containername>"