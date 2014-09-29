PageRank-Hadoop
===============

PageRank Implementation Over Hadoop

Cacluate page Rank of wiki dump.
Sample of the wiki dump : sampleWikidump.xml

Can be run on local as well as Amazon AWS cluster.

How to run:

1. Amazon aws emr CLI command

create a job folder in your s3 bucket 
upload your jar file to the job folder
create an input folder in s3 bucket
upload the sample dump to your input folder
You can create an output folder but you can just give some output name and aws emr will create for you in your bucket automatically

elastic-mapreduce --create --name "PageRank" --ami-version 2.4.2 --instance-type m1.small --num-instances <numberofnodes> --log-uri s3://<bucket name>/logs --jar s3://<bucket name>/job/PageRank.jar --arg s3://<bucket name>/<path to input folder> s3://<bucket name>/<path to output folder>

2. local installation

Copy the sample dump to some input folder 

hadoop jar ~/Path/to/PageRank.jar /Path/to/input/folder/ /Path/to/output/folder/
