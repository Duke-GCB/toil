from contextlib import contextmanager
import os
import uuid
from StringIO import StringIO
from bd2k.util.threading import ExceptionalThread
import boto
import logging
import cPickle
import gcs_oauth2_boto_plugin
from toil.jobStores.abstractJobStore import (AbstractJobStore, NoSuchJobException,
                                             ConcurrentFileModificationException,
                                             NoSuchFileException)
from toil.jobWrapper import JobWrapper

log = logging.getLogger(__name__)

GOOGLE_STORAGE = 'gs'

class GoogleJobStore(AbstractJobStore):
    sharedFileOwnerID = uuid.UUID('891f7db6-e4d9-4221-a58e-ab6cc4395f94')

    @classmethod
    def createJobStore(cls, jobStoreString, config=None):

        try:
            namePrefix, projectID = jobStoreString.split(":",1)
        except(ValueError):
            # we don't have a specified projectID
            namePrefix=jobStoreString
            projectID=None
        return cls(namePrefix, projectID, config)

    def __init__(self, namePrefix, projectID=None, config=None):
        #  create 2 buckets
        self.projectID = projectID #part of jobstorestring= gs:project_id:bucket
        self.bucketName = namePrefix+"--toil"
        self.gsBucketURL = "gs://"+self.bucketName
        self.headerValues = {"x-goog-project-id": projectID} if projectID else None
        self.uri = boto.storage_uri(self.gsBucketURL, GOOGLE_STORAGE)
        try:
            self.files = self.uri.create_bucket(headers=self.headerValues)  # GSCreateError
            # self.files.make_public()
        except:
            self.files = self._call_with_headers(self.uri.get_bucket)
        super(GoogleJobStore, self).__init__(config=config)

    def _call_with_headers(self,fn):
        # many fns need google headers. use this fn to handle that for you
        return fn(headers=self.headerValues) if self.headerValues else fn()
        #return fn()

    def _newID(self, file=False, ownerID=None):
        if file:
            assert ownerID is not None
            return str(uuid.uuid4())
        else:
            return "job"+str(uuid.uuid4())

    def _fileURI(self,jobStoreId):
        # generate uri given file
        try:
            return boto.storage_uri(self.gsBucketURL+'/'+jobStoreId, GOOGLE_STORAGE)
        except:
            raise NoSuchFileException(jobStoreId)

    def _writeFile(self, jobStoreID, fileObj, update=False):
        key = None
        fileURI = self._fileURI(jobStoreID)
        if update:
            key = fileURI.get_key()
        else:
            key = fileURI.new_key()
        key.set_contents_from_file(fileObj)

    def _writeString(self, jobStoreID, stringToUpload, update=False):
        self._writeFile(jobStoreID,StringIO(stringToUpload))

    def _readContents(self, jobStoreID):
        fileURI = self._fileURI(jobStoreID)
        try:
            return fileURI.get_contents_as_string()
        except:
            raise NoSuchFileException(jobStoreID)

    def deleteJobStore(self):
        print ("deleted")
        while True:
            for obj in self._call_with_headers(self.uri.get_bucket):  # what if bucket doesn't exist?
                obj.delete() # try except here too
            try:
                self.uri.delete_bucket()
            except Exception as e:  # TODO: make exception more specific
                # keep trying- we could have failed because of eventual consistency in 2 places
                # 1) missing objects in bucket that are meant to be deleted
                # 2) listing of ghost objects when trying to delete bucket itself
                pass
            else:
                return  # break loop

    def create(self, command, memory, cores, disk, predecessorNumber=0):
        jobStoreID = self._newID()
        job = GoogleJob(jobStoreID=jobStoreID,
                        command=command, memory=memory, cores=cores, disk=disk,
                        remainingRetryCount=self._defaultTryCount(), logJobStoreFileID=None,
                        predecessorNumber=predecessorNumber)
        # TODO: Determind if new_key require our headers.
        self._writeString(jobStoreID, cPickle.dumps(job))
        return job

    def exists(self, jobStoreID):
        return self._fileURI(jobStoreID).exists()

    def getPublicUrl(self, fileName):
        # correct format but our objects are not publically accesible...
        if self.exists(fileName):
            return "https://{}.storage.googleapis.com/{}".format(self.bucketName,fileName)
        else:
            raise NoSuchFileException(fileName)

    def getSharedPublicUrl(self, sharedFileName):
        return self.getPublicUrl(sharedFileName)

    def load(self, jobStoreID):
        try:
            jobString = self._readContents(jobStoreID)
        except NoSuchFileException:
            raise NoSuchJobException(jobStoreID)
        return cPickle.loads(jobString)

    def update(self, job):
        self._writeString(job.jobStoreID, cPickle.dumps(job), update=True)

    def delete(self, jobStoreID):
        try:
            self._get_key(jobStoreID).delete()
        except NoSuchFileException:
            pass

    def _get_key(self, jobStoreID):
        try:
            return self._fileURI(jobStoreID).get_key()
        except:
            raise NoSuchFileException(jobStoreID)

    def jobs(self):
        for key in self.uri.list_bucket():
            jobStoreID = key.name
            if jobStoreID.startswith("job"):
                yield self.load(jobStoreID)

    def writeFile(self, localFilePath, jobStoreID=None):  # TODO: Fix this to handle jobStore None
        fileID = self._newID(file=True, ownerID=jobStoreID)
        with open(localFilePath) as f:
            self._writeFile(fileID, f)
        return fileID

    @contextmanager
    def writeFileStream(self, jobStoreID=None):
        key = self._new_key(self._newID(file=True, ownerID=jobStoreID))
        with self._upload_stream(key) as writable:
            yield writable, key.name

    def _new_key(self, jobStoreID):
        return self._fileURI(jobStoreID).new_key()

    def getEmptyFileStoreID(self, jobStoreID=None):
        jobStoreID =  self._newID(file=True, ownerID=jobStoreID)
        self._writeFile(jobStoreID, StringIO(""))
        return jobStoreID

    def readFile(self, jobStoreFileID, localFilePath):  # TODO: download to file directly
        with open(localFilePath, mode="w") as f:
            f.write(self._readContents(jobStoreFileID))

    @contextmanager
    def readFileStream(self, jobStoreFileID):
        key = self._get_key(jobStoreFileID)
        with self._download_stream(key) as readable:
            yield readable

    @contextmanager
    def _download_stream(self,key):
        readable_fh, writable_fh = os.pipe()
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def writer():
                    try:
                        key.get_file(writable)
                    finally:
                        # This close() will send EOF to the reading end and ultimately cause
                        # the yield to return. It also makes the implict .close() done by the
                        #  enclosing "with" context redundant but that should be ok since
                        # .close() on file objects are idempotent.
                        writable.close()

                thread = ExceptionalThread(target=writer)
                thread.start()
                yield readable
            thread.join()

    def deleteFile(self, jobStoreFileID):
        self._get_key(jobStoreFileID).delete()

    def fileExists(self, jobStoreFileID):
        try:
            self._get_key(jobStoreFileID)
            return True
        except:
            return False

    def updateFile(self, jobStoreFileID, localFilePath):
        with open(localFilePath) as f:
            self._writeFile(jobStoreFileID, f, update=True)

    @contextmanager
    def updateFileStream(self, jobStoreFileID):
        key = self._get_key(jobStoreFileID)
        with self._upload_stream(key) as readable:
            yield readable

    @contextmanager
    def writeSharedFileStream(self, sharedFileName, isProtected=None):
        # import sys as _sys
        # _sys.path.append('/Applications/PyCharm.app/Contents/debug-eggs/pycharm-debug.egg')
        # import pydevd
        # pydevd.settrace('127.0.0.1', port=21212, suspend=True, stdoutToServer=True, stderrToServer=True, trace_only_current_thread=False)
        key = self._new_key(sharedFileName)
        with self._upload_stream(key) as readable:
            yield readable

    @contextmanager
    def _upload_stream(self,key):
        readable_fh, writable_fh = os.pipe()
        with os.fdopen(readable_fh, 'r') as readable:
            with os.fdopen(writable_fh, 'w') as writable:
                def writer():
                    key.set_contents_from_stream(readable)  # TODO: resumable download
                thread = ExceptionalThread(target=writer)
                thread.start()
                yield writable
            thread.join()


    @contextmanager
    def readSharedFileStream(self, sharedFileName):
        with self.readFileStream(sharedFileName) as readable:
            yield readable

    def writeStatsAndLogging(self, statsAndLoggingString):
        pass

    def readStatsAndLogging(self, callback, readAll=False):
        pass


class GoogleJob(JobWrapper):
    pass
