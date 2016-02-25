SeqInCloud is a highly scalable parallel implementation of a popular
genetic variant pipeline called genome analysis toolkit (GATK). 
SeqInCloud uses the Hadoop MapReduce framework and runs the
workflow in a distributed fashion using multiple compute nodes
provisioned either on-premises or in Microsoft Azure cloud.

SeqInCloud is built on top of GATK version 1.6, which is freely 
available under a MIT License via the Broad Institute.

This README contains instructions to build and run SeqInCloud.

Building SeqInCloud
===================

1. Make sure that "java" and "ant" are installed and that the 
   PATH environment is set correctly.

2. Run "ant" from the directory containing the build.xml file 
in order to create the following two jar files in the "binaries" 
directory:
    
   a. SeqInCloud.jar, which is the main application jar file
   b. SICdeps.jar, which is the dependent jar file

3. To rebuild, you can run "ant clean" followed by "ant"

Running SeqInCloud
==================

1. Conigure a Hadoop cluster

2. Load the dataset and the dependent jar (SICdeps.jar) into HDFS

3. Run SeqInCloud with the command below. This provides a list of 
   command-line options to use:

   # hadoop jar SeqInCloud.jar

NOTE
====

The alignment stage in the pipeline is NOT optimized to use memory efficiently.
The subsequent stages of the pipeline run fine, and hence, it is recommended to
run SeqInCloud on an aligned BAM file.
