import luigi
import subprocess
from luigi.s3 import S3Target, S3PathTask
from luigi.mock import MockFile

"""
    Twinder
        This gets a file from the Twitter Monthly archive hosted on the WWW
        This is ran by the user

    How to test:
       luigi --module web_to_s3 SaveToS3Task --tar https://www.dropbox.com/s/lda2zid5vvqyyxa/test_twitter.tar?dl=0  --year 2013 --month 08 --local-scheduler 
        - make sure to delete the test files from S3 after testing

    How to run:
       luigi --module web_to_s3 SaveToS3Task --tar "complete path to tar"  --year 2015 --month 08 --local-scheduler 

    Limitations: Only up to month, lost the day structure
                 Did not use global scheduler

    Author:  Loryfel T. Nunez
"""

class GetFromHTTPTask(luigi.Task):

    tar_name = luigi.Parameter()

    # this does not work, the result of the subprocess is saved in CWD
    def output(self):
        #return luigi.LocalTarget("/home/ubuntu/%s" % self.tar_name)
        return MockFile("SimpleTask", mirror_on_stderr=True)

    def run(self):
        command = "wget " + self.tar_name
        subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
        # Should change to get actual file name of the file downloaded
        command = "tar -xvf *.tar"
        subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)


class SaveToS3Task(luigi.Task):

    # Example parameter for our task: a 
    # date for which a report should be run
    tar = luigi.Parameter()
    year = luigi.Parameter()
    month = luigi.Parameter()
    extract = luigi.Parameter()

    def requires(self):
        """
        Need to download the tar file from web 
        """
        return [GetFromHTTPTask(tar_name=self.tar)]

    def output(self):
        """
        Upload to S3 on the year/month folder
        """
        s3_path = 's3://loryfel-twitter/tweets/' + self.year + '/' + self.month
        return S3Target(s3_path)


    def run(self):
        """
        Extracts the remaining bz2 files and uploads the extracted files to S3
        """
        # iterate through the directory structure year/month/day/hour/*.bz2
        count = 0
        print 'EXTRACT ', extract
        for root, dirs, files in os.walk(extract):
            print 'root ', root
            for name in files:
                print 'name ', name
                if name.endswith(("bz2")):
                    command = 'bzip2 -d ' + name
                    done = subprocess.check_output([command])
                    count = count + 1
            #if count % 100 == 0:
            print "Processed  files "


