#!/usr/bin/env python3

import datetime
import io
import os

from google.cloud import storage
from google.oauth2 import service_account

class gCloudStorage():
    googleCredentials = None
    storageClient = None

    def __init__(self, configfile=None):
        if not isinstance(configfile, str):
            raise ValueError("configfile must be a {} it was a {}".format(str, type(configfile)))
        
        if not os.access(configfile, os.R_OK):
            raise FileNotFoundError("Could not read configfile. Verify path and permissions")

        self.__loadSettings(configfile)
        self.storageClient = storage.Client(credentials=self.googleCredentials, project=self.googleCredentials.project_id)

    def __loadSettings(self, configfile):
        if isinstance(configfile, str):
            self.googleCredentials = service_account.Credentials.from_service_account_file(configfile)
            return  

        raise TypeError("Could not load Configfile")

    def fileExists(self, bucket, remotename):
        return self.storageClient.bucket(bucket).blob(remotename).exists()

    def uploadFile(self, filepath, bucket, remotename, overwrite=False):
        if not isinstance(filepath, str):
            raise ValueError("filepath must be presented as a {} it was a {}".format(str, type(filepath)))

        if not os.access(filepath, os.R_OK):
            raise FileNotFoundError("Could not read the file {}. Verify path and permissions".format(filepath))

        fileexists = self.fileExists(bucket, remotename)

        if fileexists:
            if not overwrite:
                raise FileExistsError("File exists!")
            else:
                self.storageClient.bucket(bucket).blob(remotename).delete()

        with open(filepath, 'r') as file_pointer:
            self.storageClient.bucket(bucket).blob(remotename).upload_from_file(file_pointer)
        return

    def deleteFile(self, bucket, remotename):
        self.storageClient.bucket(bucket).blob(remotename).delete()
        return True

    def createUploadURI(self, bucket, remotename, expiration=None):
        """Generates a URL for a blob of the name Remotename.
        
        NOTE up to the generator to not overwrite stuff!"""
        if expiration is None:
            expiration = datetime.timedelta(minutes=5)

        if not isinstance(expiration, datetime.timedelta):
            raise TypeError("Timeout should be {}, it was {}".format(datetime.datetime, type(expiration)))
    
        return self.storageClient.bucket(bucket).blob(remotename).generate_signed_url(method="PUT", expiration=expiration)

if __name__ == '__main__':
    pass