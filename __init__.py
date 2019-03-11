#!/usr/bin/env python3

import io
import os

from google.cloud import storage
from google.oauth2 import service_account

class GCloudStorage():
    googleCredentials = None
    storageClient = None

    def __init__(self, configfile=None, projectID=None):
        if not isinstance(projectID, str):
            raise ValueError("projectID must be a {} it was a {}".format(str, type(projectID)))

        if not isinstance(configfile, str):
            raise ValueError("configfile must be a {} it was a {}".format(str, type(configfile)))
        
        if not os.access(configfile, os.R_OK):
            raise FileNotFoundError("Could not read configfile. Verify path and permissions")

        self.__loadSettings(configfile)
        self.storageClient = storage.Client(credentials=self.googleCredentials, project=projectID)

    def __loadSettings(self, configfile):
        if isinstance(configfile, str):
            self.googleCredentials = service_account.Credentials.from_service_account_file(configfile)
            return  

        raise TypeError("Could not load Configfile")

    def uploadFile(self, filepath, bucket, remotename):
        if not isinstance(filepath, str):
            raise ValueError("filepath must be presented as a {} it was a {}".format(str, type(filepath)))

        if not os.access(filepath, os.R_OK):
            raise FileNotFoundError("Could not read the file {}. Verify path and permissions".format(filepath))

        fileexists = self.storageClient.bucket(bucket).blob(remotename).exists()

        if fileexists:
            raise FileExistsError("File exists!")

        with open(filepath, 'r') as file_pointer:
            self.storageClient.bucket(bucket).blob(remotename).upload_from_file(file_pointer)
        return

    def createUploadURI(self, bucket, remotename):
        
        #https://googleapis.github.io/google-cloud-dotnet/docs/Google.Cloud.Storage.V1/index.html#signed-urls
        pass

"""UrlSigner urlSigner = UrlSigner.FromServiceAccountCredential(credential);
var destination = "places/world.txt";
string url = urlSigner.Sign(
    bucketName,
    destination,
    TimeSpan.FromHours(1),
    HttpMethod.Put,
    contentHeaders: new Dictionary<string, IEnumerable<string>> {
        { "Content-Type", new[] { "text/plain" } }
    });"""

if __name__ == '__main__':
    pass