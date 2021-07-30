
import pyodbc 
import time
from datetime import timezone 
import datetime
import sys
import glob
import boto3
from botocore.errorfactory import ClientError
import pandas as pd
import csv
import s3fs
import os

## Fetch the arguments
#server = sys.argv[1]
DBName = sys.argv[1]
LSBucket=sys.argv[2]

## I am hard-coing the server name here, but you can choose to pass it as an argument 
server = '<servername>'
srvname = server.partition(".")[0]
database = 'master' 
username = '<username>' 
password = '<pwd>'
CSVLogfile = srvname+"_"+DBName+"_CSVLog.csv"

### Create a CSV Log file and upload it to S3 #####
s3 = boto3.client('s3')

try:
    s3.head_object(Bucket=LSBucket, Key=CSVLogfile)
except ClientError:
    # Not found
    with open(CSVLogfile, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["ServerName", "DBName", "RunTime(UTC)", "LastRestoredFile", "LastRestoredDate", "OutputLog", "ReplicationLag(mins)"])
    with open(CSVLogfile, "rb") as f:
        s3.upload_fileobj(f, LSBucket, CSVLogfile)

###Function write to csv
def write_to_csv(server, DBName, utc_time, lastrestoredobject, lastrestoreddate, OutputMSG, time_Delta, LSBucket, CSVLogfile): 

    #download file from s3 to tmp 
    s3 = boto3.resource('s3')
    obj = s3.Object(LSBucket, CSVLogfile)
    obj.download_file('templog.csv')

    my_bucket = s3.Bucket(LSBucket)
    # list you want to append
    lists = [server, DBName, utc_time, lastrestoredobject, lastrestoreddate, OutputMSG, time_Delta]

    with open('templog.csv','r') as infile:
        reader = list(csv.reader(infile))
        reader = reader[::-1] # the date is ascending order in file
        reader.insert(0,lists)
    with open('templog.csv', 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for line in reversed(reader): # reverse order
            writer.writerow(line)
 
    #upload file from tmp to s3 key
    my_bucket.upload_file('templog.csv', CSVLogfile)
    os.remove('templog.csv') 

s3 = boto3.resource('s3')
my_bucket = s3.Bucket(LSBucket)
dt = datetime.datetime.now() 
utc_time = dt.replace(tzinfo = timezone.utc) 

### Connect to SQL 
try:
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password,autocommit=True)
    cursor = cnxn.cursor()

except: 
   e = sys.exc_info()[0]
   print( "Error: %s" % e )
   lastrestoredobject = "N\A"
   lastrestoreddate = "N\A"
   OutputMSG = "Failed to connect to target SQL Server"
   time_Delta = "N\A" 
   write_to_csv(server, DBName, utc_time, lastrestoredobject, lastrestoreddate, OutputMSG, time_Delta, LSBucket, CSVLogfile)
   raise

       
###### Get the last file restored on target database ###############
query = ("exec msdb.dbo.rds_task_status @db_name = ?")
         
# execute the query and read to a dataframe in Python
data = pd.read_sql(sql=query, con=cnxn, params=[DBName])

# filter data with query method 
data.query('lifecycle == "SUCCESS"', inplace = True)
maxClm = data['task_id'].max()
data.query ('task_id == @maxClm', inplace = True) 
lastrestoredobject = data.S3_object_arn.values[0]
lastrestoredfile = lastrestoredobject.partition("/")[2]

# Get the date of this restored file
for my_bucket_object in my_bucket.objects.all():
     if my_bucket_object.key == lastrestoredfile:
        lastrestoreddate = my_bucket_object.last_modified

###### Get all the files in S3 dated after above file ################
unsortedS3 = []
suffix = 'trn'
for my_bucket_object in my_bucket.objects.all():
     if my_bucket_object.last_modified > lastrestoreddate:
         if my_bucket_object.key.endswith(suffix):
            #print('{0}:{1}'.format(my_bucket.name, my_bucket_object.key))
            unsortedS3.append(my_bucket_object)

if not unsortedS3:
  OutputMSG = "There are no new files to restore"
else:
    ###### find the oldest file ################
    sortedS3 = [obj.key for obj in sorted(unsortedS3, key=lambda x: x.last_modified)][0:9]
    oldesttrnfile = sortedS3[0]
    nextrestorefile = 'arn:aws:s3:::'+LSBucket+'/'+oldesttrnfile
    ####### Restore the file ##############
    ## Check if there is any other process in progress ? 
    restore_query = """exec msdb.dbo.rds_restore_log
                        @restore_db_name=?,
                        @s3_arn_to_restore_from=?,
                        @with_norecovery=1;"""
    restore_args = (DBName,nextrestorefile)
    cursor.execute(restore_query,restore_args)
    OutputMSG = "Ran restore for file: "+nextrestorefile

time_delta = (utc_time - lastrestoreddate)
time_Delta= ((time_delta.total_seconds())/60)

write_to_csv(server, DBName, utc_time, lastrestoredobject, lastrestoreddate, OutputMSG, time_Delta, LSBucket, CSVLogfile)

###### Alert if > 15 mins ###########
sns = boto3.client('sns')
snsmessage = (server+".\n"+"LogShipping is out of sync for database: "+DBName+".\n"+"Lag time (mins):"+str(time_Delta))

if time_Delta > 15:
        response = sns.publish(
            TopicArn='<topicname>',
            Subject=("LOGSHIPPING IS OUT OF SYNC"),
            Message=(snsmessage)    
        )


