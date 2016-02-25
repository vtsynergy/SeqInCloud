/* Copyright (c) 2012-2013 by Virginia Polytechnic Institute and State
 * University
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT REPRESENTATIONS 
 * OR WARRANTIES OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT 
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS 
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT, OR THE 
 * ABSENCE OF LATENT OR OTHER DEFECTS, WHETHER OR NOT 
 * DISCOVERABLE. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT 
 * HOLDERS BE LIABLE FOR ANY CLAIM, INCIDENTAL OR CONSEQUENTIAL 
 * DAMAGES OF ANY KIND, OR OTHER LIABILITY, WHETHER IN AN ACTION 
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
 * IN THE SOFTWARE.
 */

package org.broadinstitute.sting.gatk.hadoop;

import java.io.*;
import java.net.*;
import java.lang.Integer;
import java.util.List;
import java.util.Map;

import org.broadinstitute.sting.gatk.hadoop.SplittingBAMIndexer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.broadinstitute.sting.gatk.hadoop.NLineXInputFormat;
import org.broadinstitute.sting.gatk.hadoop.hadoopsrc.InputSampler;
import org.broadinstitute.sting.gatk.hadoop.hadoopsrc.TotalOrderPartitioner;
import org.broadinstitute.sting.gatk.hadoop.hbamsrc.SAMRecordWritable;
import org.broadinstitute.sting.gatk.hadoop.IndelMapReduce.*;
import org.broadinstitute.sting.gatk.hadoop.RecalMapReduce.*;
import org.broadinstitute.sting.gatk.hadoop.RecalCovMapReduce.*;
import org.broadinstitute.sting.gatk.hadoop.VariantMapReduce.*;
import org.broadinstitute.sting.gatk.hadoop.BWAMapReduce.*;
import org.broadinstitute.sting.gatk.hadoop.ContigMapReduce.*;
import org.broadinstitute.sting.gatk.hadoop.IndexMapReduce.*;
import org.broadinstitute.sting.gatk.hadoop.SortOutputFormat;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.GenericOptionsParser;
import net.sf.samtools.SAMFileReader.ValidationStringency;
import org.apache.commons.cli.*;

import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.BAMIndexer;
import net.sf.samtools.util.BlockCompressedStreamConstants;
import net.sf.samtools.SAMSequenceDictionary;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMSequenceRecord;


public class GATKJobClient extends Configured implements Tool {

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    public static String BWAOutPath, SortBWAOutPath, BAMInputPath, IndelOutPath,
                         RmdupOutPath, RecalOutPath, FinalBAMPath,
                         readFile1, readFile2, outputDir, refFileName, gatk_binary_loc, 
                         bwa_binary_loc, refFileLoc, knownSitesLoc, fqInput,
                         bamIndexFile, platform;

    public static boolean is_azure = true, printUsage = true, noalign = false,
                          norealign = false, nomarkdup = false, noqrecab = false, 
                          novariant = false, nofvariant = false, nomresults = false,
                          xVariantCall = false;

    public static int fq_read_size, nReducers = 0, nThreads = 0;

    public static long reads_per_split = 0; // If 0, calculated automatically

    static void validatePath(String path, Configuration conf) 
                                       throws ParseException, IOException {
        Path checkPath = new Path(path);
        if (! checkPath.getFileSystem(conf).exists(checkPath)) {
            printUsage = false;
            throw new ParseException("File " + checkPath + " does not exist");
        }
    }

    static void parseCommandLineArgs(String[] argv, Configuration conf) {

        CommandLineParser parser = new PosixParser();

        Options options = new Options();

        Option gatkdLocOpt = OptionBuilder.withArgName("depjar_loc")
                                        .hasArg()
                                        .withDescription( "Complete HDFS path of gatk dependency jar")
                                        .create("djarloc");
        options.addOption(gatkdLocOpt);
        Option bwaLocOpt = OptionBuilder.withArgName("bwa_loc")
                                        .hasArg()
                                        .withDescription( "Complete HDFS path of bwa binary or bwa.exe file")
                                        .create("bwaloc");
        options.addOption(bwaLocOpt);
        Option fq1Opt = OptionBuilder.withArgName("fastq_file1")
                                        .hasArg()
                                        .withDescription( "Complete HDFS path or path relative to user directory for 1st fastq file")
                                        .create("r1");
        options.addOption(fq1Opt);
        Option fq2Opt = OptionBuilder.withArgName("fastq_file2")
                                        .hasArg()
                                        .withDescription( "Complete HDFS path or path relative to user directory for 2nd fastq file")
                                        .create("r2");
        options.addOption(fq2Opt);
        Option bamOpt = OptionBuilder.withArgName("bam_directory")
                                        .hasArg()
                                        .withDescription( "Complete HDFS directory path or path relative to user directory for input BAM file")
                                        .create("b");
        options.addOption(bamOpt);
        Option outOpt = OptionBuilder.withArgName("output_directory")
                                        .hasArg()
                                        .withDescription( "Complete HDFS path or path relative to user directory for output directory")
                                        .create("o");
        options.addOption(outOpt);
        Option rSizeOpt = OptionBuilder.withArgName("fastq_read_size")
                                        .hasArg()
                                        .withDescription( "Number of bytes of a read sequence in input FastQ file")
                                        .create("rsize");
        options.addOption(rSizeOpt);
        Option rPSplitOpt = OptionBuilder.withArgName("reads_per_map_split")
                                        .hasArg()
                                        .withDescription( "Optional number of reads to be processed by a mapper")
                                        .create("reads_per_split");
        options.addOption(rPSplitOpt);
        Option nRedOpt = OptionBuilder.withArgName("number_of_reducers")
                                        .hasArg()
                                        .withDescription( "Optional number of reducers")
                                        .create("nred");
        options.addOption(nRedOpt);
        Option nThreadOpt = OptionBuilder.withArgName("number_of_threads")
                                        .hasArg()
                                        .withDescription( "Optional number of threads")
                                        .create("nthreads");
        options.addOption(nThreadOpt);
        Option refFileOpt = OptionBuilder.withArgName("path_to_reference_dir")
                                        .hasArg()
                                        .withDescription( "Complete HDFS path of reference directory")
                                        .create("ref");
        options.addOption(refFileOpt);
        Option kSiteFileOpt = OptionBuilder.withArgName("path_to_knownsites_dir")
                                        .hasArg()
                                        .withDescription( "Complete HDFS path of known-sites db directory")
                                        .create("dbfile");
        options.addOption(kSiteFileOpt);

        Option platformOpt = OptionBuilder.withArgName("Linux/Windows")
                                        .hasArg()
                                        .withDescription( "Platform to run on")
                                        .create("p");
        options.addOption(platformOpt);

        Option noAlignOpt = new Option("na", "noalign", false, "Don't run Alignment stage");
        options.addOption(noAlignOpt);

        Option noReAlignOpt = new Option("nra", "norealign", false, "Do not run Local Realignment stage");
        options.addOption(noReAlignOpt);

        Option noMarkDupOpt = new Option("nmd", "nomarkdup", false, "Do not run Mark Duplicates stage");
        options.addOption(noMarkDupOpt);

        Option noQRecabOpt = new Option("nqr", "noqrecab", false, "Do not run Quality Recalibration stage");
        options.addOption(noQRecabOpt);

        Option noVarOpt = new Option("nv", "novariant", false, "Do not run Structural Variant stage");
        options.addOption(noVarOpt);

        Option noFVarOpt = new Option("nfv", "nofvariant", false, "Do not run Filter Variant stage");
        options.addOption(noFVarOpt);

        Option noMerOpt = new Option("nm", "nomresults", false, "Do not Merge Results");
        options.addOption(noMerOpt);

        Option isXVariantOpt = new Option("xv", "xvariant", false, "enable flag, if variant calling should be done independently for INDELs and SNPs");
        options.addOption(isXVariantOpt);

        try {
            // parse the command line arguments
            String[] args = new GenericOptionsParser(conf, options, argv).getRemainingArgs();
            CommandLine line = parser.parse(options, args);

            if (line.hasOption(noAlignOpt.getOpt()))
                noalign = true;
            if (line.hasOption(noReAlignOpt.getOpt()))
                norealign = true;
            if (line.hasOption(noMarkDupOpt.getOpt()))
                nomarkdup = true;
            if (line.hasOption(noQRecabOpt.getOpt()))
                noqrecab = true;
            if (line.hasOption(noVarOpt.getOpt()))
                novariant = true;
            if (line.hasOption(noFVarOpt.getOpt()))
                nofvariant = true;
            if (line.hasOption(noMerOpt.getOpt()))
                nomresults = true;
          
            if( line.hasOption(fq1Opt.getOpt()) && line.hasOption(bamOpt.getOpt()) ) {
                throw new ParseException("Invalid Usage: fastq file and BAM file cannot be given together as input");
            }
            if( line.hasOption(fq2Opt.getOpt()) && !line.hasOption(fq1Opt.getOpt()) ) {
                throw new ParseException("Invalid Usage: fastq file2 is invalid without fastq file1");
            }
            if( !line.hasOption(fq2Opt.getOpt()) && !line.hasOption(fq1Opt.getOpt()) && !line.hasOption(bamOpt.getOpt())) {
                throw new ParseException("Invalid Usage: Either the fastq file or BAM file has to be provided as input");
            }
            if( line.hasOption(gatkdLocOpt.getOpt()) ) {
                gatk_binary_loc = line.getOptionValue(gatkdLocOpt.getOpt());
                validatePath(gatk_binary_loc, conf);
            } else {
                    throw new ParseException("Invalid Usage: GATK dependency jar location (-djarloc) is mandatory for running the pipeline");
            }
            
            if (!noalign) {
                if( line.hasOption(fq1Opt.getOpt()) ) {
                    readFile1 = line.getOptionValue(fq1Opt.getOpt());
                    validatePath(readFile1, conf);
                    fqInput = (new Path(readFile1).getParent()).toString();
                }
                if( line.hasOption(fq2Opt.getOpt()) ) {
                    readFile2 = line.getOptionValue(fq2Opt.getOpt());
                    conf.setBoolean("gatk.hadoop.pairedend", true);
                    validatePath(readFile2, conf);
                    conf.set("gatk.hadoop.readfile2", readFile2);;
                }
                if( line.hasOption(rSizeOpt.getOpt()) ) {
                    fq_read_size = Integer.parseInt(line.getOptionValue(rSizeOpt.getOpt()));
                } else {
                    throw new ParseException("Invalid Usage: read size (-rsize) is mandatory for Alignment");
                }
                if( line.hasOption(bwaLocOpt.getOpt()) ) {
                    bwa_binary_loc = line.getOptionValue(bwaLocOpt.getOpt());
                    validatePath(bwa_binary_loc, conf);
                } else {
                    throw new ParseException("Invalid Usage: bwa binary/exe location (-bwaloc) is mandatory for Alignment");
                }
                if (line.hasOption(rPSplitOpt.getOpt())) {
                    reads_per_split = Integer.parseInt(line.getOptionValue(rPSplitOpt.getOpt()));
                }
            }
            if (line.hasOption(nRedOpt.getOpt())) {
                nReducers = Integer.parseInt(line.getOptionValue(nRedOpt.getOpt()));
            }
            if (line.hasOption(nThreadOpt.getOpt())) {
                nThreads = Integer.parseInt(line.getOptionValue(nThreadOpt.getOpt()));
                conf.setInt("gatk.hadoop.nthreads", nThreads);
            }
            if (line.hasOption(bamOpt.getOpt())) {
                int rcount = 0;
                BAMInputPath = line.getOptionValue(bamOpt.getOpt());
                validatePath(BAMInputPath, conf);
                Path BAMPath = new Path(BAMInputPath);
                FileSystem fs = BAMPath.getFileSystem(conf);
                FileStatus[] content = fs.listStatus(BAMPath);
                for (int i = 0; i < content.length; i++) {
                    String filename = content[i].getPath().getName();
                    if (filename.endsWith(".bam")) {
                        String prefix = filename.substring(0,6);
                        try {
                            Long value = Long.valueOf(prefix);
                        } catch (NumberFormatException e) {
                            String tmpFile = BAMInputPath + Path.SEPARATOR + String.format("%06d", rcount) +
                                                       "-" + filename;
                            boolean rename = fs.rename(content[i].getPath(), new Path(tmpFile));
                        }
                        rcount++;
                    }
                }
            }
            if (line.hasOption(outOpt.getOpt())) {
                outputDir = line.getOptionValue(outOpt.getOpt());
                if (!(new Path(outputDir).getFileSystem(conf).mkdirs(new Path(outputDir)))) {
                    throw new Exception("MKDIR failure");
                }
                if (!noalign) {
                    BWAOutPath = outputDir + Path.SEPARATOR + "AlignerOut";
                    SortBWAOutPath = outputDir + Path.SEPARATOR + "SortedAlignerOut";
                    BAMInputPath = outputDir + Path.SEPARATOR + "BAMInput";
                }
                IndelOutPath = outputDir + Path.SEPARATOR + "IndelRealignOut";
                RmdupOutPath = outputDir + Path.SEPARATOR + "DedupOut";
                RecalOutPath = outputDir + Path.SEPARATOR + "RecalibrationOut";
                FinalBAMPath = outputDir + Path.SEPARATOR + "FinalBAMOut";
            } else {
                throw new ParseException("Invalid Usage: output directory is mandatory");
            }
            if (line.hasOption(refFileOpt.getOpt())) {
                Path refFileDir = new Path(line.getOptionValue(refFileOpt.getOpt()));
                FileSystem fs = refFileDir.getFileSystem(conf);
                FileStatus[] content = fs.listStatus(refFileDir);
                for (int i = 0; i < content.length; i++) {
                    if( (content[i].getPath().getName()).endsWith(".fa") ||
                           (content[i].getPath().getName()).endsWith(".fasta")) {
                        refFileLoc = content[i].getPath().toString();
                    }
                }
                validatePath(refFileLoc, conf);
                refFileName = refFileLoc.substring(0, refFileLoc.lastIndexOf("."));
            } else {
                throw new ParseException("Invalid Usage: reference fasta file is mandatory");
            }
            if (line.hasOption(kSiteFileOpt.getOpt())) {
                Path knownSitesDir = new Path(line.getOptionValue(kSiteFileOpt.getOpt()));
                FileSystem fs = knownSitesDir.getFileSystem(conf);
                FileStatus[] content = fs.listStatus(knownSitesDir);
                for (int i = 0; i < content.length; i++) {
                    if( (content[i].getPath().getName()).endsWith(".vcf")) {
                        knownSitesLoc = content[i].getPath().toString();
                    }
                }
                validatePath(knownSitesLoc, conf);
            }
            if (line.hasOption(platformOpt.getOpt())) {
                platform = line.getOptionValue(platformOpt.getOpt());
                if (platform.equalsIgnoreCase("Linux")) {
                    is_azure = false;
                    conf.setBoolean("gatk.hadoop.isazure", false);
                }
            }
            if (line.hasOption(isXVariantOpt.getOpt())) {
                xVariantCall = true;
            }
        }
        catch( ParseException exp ) {
            System.out.println(exp.getMessage());
            if (printUsage) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("hadoop jar {/local/path/to/SeqInCloud.jar} {options}", options);
            }
            System.exit(-1);
        } catch ( Exception exp) {
            System.out.println("Command line parsing error: " + exp.getMessage());
            System.exit(-1);
        }
    }

    @Override
    public int run(String[] argv) throws Exception {
        try {
            Configuration conf;
            FileSystem srcFs, outFs, fs;
            Path inputPath = null, mergeOutFile, inputDir, partition = null, outputPath;
            int maxMapTasks, maxReduceTasks, max_splits = Integer.MAX_VALUE, granularity = 100;
            FileStatus[] content;
            ClusterStatus status;
            int numNodes, mapSlotsPerNode;
            long mapOutputBytes, iMBytesPerRed, mapOutBufSize, inputSize, cacheSize, startTime, 
                                                                    blockSize, endTime, splitSize;
            float inputBufpcnt;
            FSDataOutputStream out;
            FSDataInputStream in;
            SAMFileReader fileReader;
            InputSampler.Sampler<LongWritable, SAMRecordWritable> sampler;
            double sampling_frequency = 0.01;

            // Job object can be used for Aligner job if enabled
            conf = getConf();
            Job job = new Job(conf);

            parseCommandLineArgs(argv, conf);

            maxMapTasks = new JobClient(new JobConf(conf))
                                                .getClusterStatus().getMaxMapTasks();

            maxReduceTasks = new JobClient(new JobConf(conf))
                                                .getClusterStatus().getMaxReduceTasks();
            if (!noalign) {
                System.out.println("Starting Alignment Job");
                startTime = System.currentTimeMillis();

                status = new JobClient(new JobConf(conf)).getClusterStatus();
                numNodes = status.getTaskTrackers();
                // Job specific setting of number of Reducers..
                if (nReducers == 0)
                    nReducers = numNodes;
                conf.setInt("mapred.reduce.tasks", nReducers);

                Path refPath = new Path(refFileLoc);
                fs = refPath.getFileSystem(conf);
                blockSize = fs.getFileStatus(refPath).getBlockSize();
                splitSize = Math.round(fs.getFileStatus(refPath).getBlockSize());

                if (reads_per_split == 0) {
                    inputPath = new Path(readFile1);
                    long readSize = (inputPath.getFileSystem(conf)).getFileStatus(inputPath).getLen();
                    long numSplits = Math.round(readSize / splitSize);

                    if (numSplits < maxMapTasks)
                        numSplits = maxMapTasks;

                    if (numSplits < nReducers)
                        numSplits = nReducers;

                    long numReads = Math.round(readSize / (long) fq_read_size);
                    reads_per_split = numReads / numSplits;

                    // Total Order Partitioner
                    if ((double) reads_per_split <= (1/sampling_frequency)) {
                        sampling_frequency = 1; 
                        granularity = 1;
                    } else if (((double) reads_per_split > (1/sampling_frequency)) &&
                                ((double) reads_per_split <= (1/sampling_frequency * 100)) ) {
                        sampling_frequency = 0.1; 
                        granularity = 10;
                    }
                }

                job.setJarByClass(GATKJobClient.class);
                job.setInputFormatClass(NLineXInputFormat.class);
                FileInputFormat.addInputPath(job, new Path(fqInput));
                FileOutputFormat.setOutputPath(job, new Path(BWAOutPath));

                DistributedCache.addCacheFile(new URI(refFileLoc + "#" + "ref.fa"), job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileLoc + ".amb#" + "ref.fa.amb"), job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileLoc + ".ann#" + "ref.fa.ann"), job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileLoc + ".bwt#" + "ref.fa.bwt"), job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileLoc + ".pac#" + "ref.fa.pac"), job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileLoc + ".sa#" + "ref.fa.sa"), job.getConfiguration());
                if (!is_azure) {
                    DistributedCache.addCacheFile(new URI(bwa_binary_loc +"#" + "bwa"), job.getConfiguration());
                    DistributedCache.addCacheFile(new URI(refFileLoc + ".rbwt#" + "ref.fa.rbwt"), job.getConfiguration());
                    DistributedCache.addCacheFile(new URI(refFileLoc + ".rpac#" + "ref.fa.rpac"), job.getConfiguration());
                    DistributedCache.addCacheFile(new URI(refFileLoc + ".rsa#" + "ref.fa.rsa"), job.getConfiguration());
                } else {
                    DistributedCache.addCacheFile(new URI(bwa_binary_loc +"#" + "bwa.exe"), job.getConfiguration());
                }
                DistributedCache.createSymlink(job.getConfiguration());

                // Setting local.cache.size - Add up the size of the files
                // distributed through the cache

                cacheSize = fs.getFileStatus(new Path(refFileLoc)).getLen()
                                + fs.getFileStatus(new Path(refFileLoc + ".amb")).getLen()
                                + fs.getFileStatus(new Path(refFileLoc + ".ann")).getLen()
                                + fs.getFileStatus(new Path(refFileLoc + ".bwt")).getLen()
                                + fs.getFileStatus(new Path(refFileLoc + ".pac")).getLen()
                                + fs.getFileStatus(new Path(refFileLoc + ".sa")).getLen();
                if (!is_azure) {
                    cacheSize = cacheSize
                                    + fs.getFileStatus(new Path(refFileLoc + ".rbwt")).getLen()
                                    + fs.getFileStatus(new Path(refFileLoc + ".rpac")).getLen()
                                    + fs.getFileStatus(new Path(refFileLoc + ".rsa")).getLen();
                }

                if (cacheSize > 8 * 1024 * 1024 * 1024) {
                        conf.setLong("local.cache.size", cacheSize
                                        + (1 * 1024 * 1024 * 1024));
                }

                conf.setLong("mapred.task.timeout", 86400000L); // 24 hrs..
                conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
                conf.setLong("mapred.line.input.format.linespermap", reads_per_split * 4);
                conf.setInt("dfs.datanode.socket.write.timeout", 600000);
                conf.setInt("dfs.socket.timeout", 600000);
                // conf.setBoolean("mapred.map.tasks.speculative.execution", false);
                // conf.setBoolean("mapred.compress.map.output", true); // Default compression ratio 3.5:1

                job.setMapOutputKeyClass(LongWritable.class);
                job.setMapOutputValueClass(Text.class);
                job.setPartitionerClass(BWAPartitioner.class);
                job.setReducerClass(BWAReducer.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(NullWritable.class);

                if (job.waitForCompletion(true)) {
                        System.out.println("BWA Alignment done");
                }

                content = fs.listStatus(new Path(BWAOutPath));

                for (int i = 0; i < content.length; i++) {
                        if (!((content[i].getPath().getName()).endsWith(".bam"))
                                        && !((content[i].getPath().getName()).startsWith("_"))) {
                                fs.delete(content[i].getPath(), false);
                        }
                }
                endTime = System.currentTimeMillis();
                System.out.println("BWA Alignment took: "
                                                    + (endTime - startTime));
                startTime = System.currentTimeMillis();
                System.out.println("Starting Splitting BAM Indexing Job");
                job = new Job();
                job.setJarByClass(GATKJobClient.class);
                conf = job.getConfiguration();
                inputPath = new Path(BWAOutPath);
                FileInputFormat.addInputPath(job, inputPath);
                job.setInputFormatClass(WholeFileInputFormat.class);

                Path output = new Path(outputDir + Path.SEPARATOR + "DeleteThisDir1");
                FileOutputFormat.setOutputPath(job, output);
                conf.setBoolean("mapred.map.tasks.speculative.execution", false);
                conf.setInt("mapred.reduce.tasks", 0);
                conf.setInt("gatk.hadoop.granularity", granularity);
                conf.setBoolean("gatk.hadoop.issindex", true);
                conf.setBoolean("gatk.hadoop.isindex", false);
                conf.setBoolean("gatk.hadoop.ismarkdup", false);

                job.setMapperClass(IndexMapper.class);
                job.setMapOutputKeyClass(NullWritable.class);
                job.setMapOutputValueClass(NullWritable.class);

                DistributedCache.addArchiveToClassPath(new Path(gatk_binary_loc),
                                job.getConfiguration());

                if (job.waitForCompletion(true)) {
                        System.out.println("SplittingBAM Indexing job done");
                }
                output.getFileSystem(conf).delete(output, true);

                endTime = System.currentTimeMillis();
                System.out.println("Splitting BAM Indexing took: "
                                                + (endTime - startTime));

                startTime = System.currentTimeMillis();
                System.out.println("Starting Sort Job");
                job = new Job();
                job.setJarByClass(GATKJobClient.class);
                conf = job.getConfiguration();

                if (norealign && nomarkdup && noqrecab && novariant && !nomresults)
                    conf.setBoolean("gatk.hadoop.ismerge", true);
                inputPath = new Path(BWAOutPath);
                FileInputFormat.addInputPath(job, inputPath);
                FileOutputFormat.setOutputPath(job, new Path(SortBWAOutPath));
                job.setInputFormatClass(ContigInputFormat.class);
                job.setPartitionerClass(ContigPartitioner.class);

                DistributedCache.addArchiveToClassPath(new Path(gatk_binary_loc),
                                job.getConfiguration());

                fs = inputPath.getFileSystem(conf);
                content = fs.listStatus(inputPath);
                for (int i = 0; i < content.length; i++) {
                    if (content[i].getPath().getName().endsWith(".bam")) {
                        in = fs.open(content[i].getPath());
                        List<SAMSequenceRecord> sequences = (new SAMFileReader(in).getFileHeader()).
                                                                    getSequenceDictionary().getSequences();
                        conf.setInt("mapred.reduce.tasks", sequences.size());
                       
                        break;
                    }
                }

                conf.setLong("mapred.task.timeout", 86400000L);
                conf.setInt("dfs.datanode.socket.write.timeout", 600000);
                conf.setInt("dfs.socket.timeout", 600000);
                //conf.setBoolean("mapred.map.tasks.speculative.execution", false);
                //conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
                //conf.setBoolean("mapred.compress.map.output", true); // Default compression ratio 3.5:1

                job.setReducerClass(SortReducer.class);
                job.setMapOutputKeyClass(LongWritable.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(SAMRecordWritable.class);
                job.setOutputFormatClass(SortOutputFormat.class);

                if (job.waitForCompletion(true)) {
                        System.out.println("Sort completed successfully");
                }
                endTime = System.currentTimeMillis();
                System.out.println("Sort job took: " + (endTime - startTime));
            } 

            if (!norealign) {
                if (!noalign)
                    BAMInputPath = SortBWAOutPath;

                startTime = System.currentTimeMillis();
                System.out.println("Starting Indexing Job");
                job = new Job();
                job.setJarByClass(GATKJobClient.class);
                conf = job.getConfiguration();
                inputPath = new Path(BAMInputPath);
                FileInputFormat.addInputPath(job, inputPath);
                job.setInputFormatClass(WholeFileInputFormat.class);
                Path output = new Path(outputDir + Path.SEPARATOR + "DeleteThisDir2");
                FileOutputFormat.setOutputPath(job, output);

                conf.setLong("mapred.task.timeout", 86400000L);
                conf.setBoolean("mapred.map.tasks.speculative.execution", false);
                conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
                conf.setInt("mapred.reduce.tasks", 0);
                conf.setBoolean("gatk.hadoop.isindex", true);
                conf.setBoolean("gatk.hadoop.issindex", true);
                conf.setBoolean("gatk.hadoop.ismarkdup", false);

                job.setMapperClass(IndexMapper.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(NullWritable.class);

                DistributedCache.addArchiveToClassPath(new Path(gatk_binary_loc),
                                                                    job.getConfiguration());
                if (job.waitForCompletion(true)) {
                        System.out.println("Indexing job done");
                }
                output.getFileSystem(conf).delete(output, true);

                endTime = System.currentTimeMillis();
                System.out.println("Indexing job took: " + (endTime - startTime));

                startTime = System.currentTimeMillis();
                System.out.println("Starting Realigner Job");
                job = new Job();
                job.setJarByClass(GATKJobClient.class);
                conf = job.getConfiguration();

                inputPath = new Path(BAMInputPath);
                FileInputFormat.addInputPath(job, inputPath);

                job.setInputFormatClass(BAMInputFormat.class);

                srcFs = new Path(outputDir).getFileSystem(conf);
                if (!srcFs.mkdirs(new Path(outputDir + Path.SEPARATOR + "Partition")))
                    System.out.println("mkdir failed");
                inputDir = new Path(outputDir + Path.SEPARATOR + "Partition");
                inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));
                partition = new Path(inputDir, "_partition");
                job.setPartitionerClass(TotalOrderPartitioner.class);
                TotalOrderPartitioner.setPartitionFile(conf, partition);

                try {
                        URI partitionURI = new URI(partition.toString() + "#_partition");
                        DistributedCache.addCacheFile(partitionURI, conf);
                } catch (URISyntaxException e) {
                        assert false;
                }

                if (nReducers == 0) {
                    if (!nomarkdup || !noqrecab || !novariant) {
                        conf.setInt("mapred.reduce.tasks", maxMapTasks);
                    } else {
                        conf.setInt("mapred.reduce.tasks", Math.max(1,
                                                            maxReduceTasks * 9 / 10));
                    }
                } else {
                    conf.setInt("mapred.reduce.tasks", nReducers);
                }

                conf.setLong("mapred.task.timeout", 86400000L);
                conf.setInt("dfs.datanode.socket.write.timeout", 600000);
                conf.setInt("dfs.socket.timeout", 600000);
                conf.setBoolean("mapred.compress.map.output", true); // Default compression ratio 3.5:1

                if (nomarkdup && noqrecab && novariant && !nomresults)
                    conf.setBoolean("gatk.hadoop.ismerge", true);
                conf.setBoolean("gatk.hadoop", true);
                conf.setBoolean("gatk.hadoop.isazure", is_azure);
                job.setMapperClass(IndelMapper.class);
                job.setReducerClass(SortReducer.class);
                job.setMapOutputKeyClass(LongWritable.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(SAMRecordWritable.class);
                job.setOutputFormatClass(SortOutputFormat.class);
                FileOutputFormat.setOutputPath(job, new Path(IndelOutPath));

                sampler = new InputSampler.IntervalSampler<LongWritable, SAMRecordWritable>(
                                                                    sampling_frequency, max_splits);
                InputSampler.<LongWritable, SAMRecordWritable> writePartitionFile(job, sampler);
                job.setInputFormatClass(LociInputFormat.class);
                
                DistributedCache.addArchiveToClassPath(new Path(gatk_binary_loc),
                                                                    job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileLoc + "#" + "ref.fa"),
                                                                    job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileLoc + ".fai#"
                                                        + "ref.fa.fai"), job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileName + ".dict#"
                                                        + "ref.dict"), job.getConfiguration());
                DistributedCache.createSymlink(job.getConfiguration());

                if (job.waitForCompletion(true)) {
                    System.out.println("Indel realignment done");
                }
                endTime = System.currentTimeMillis();
                System.out
                        .println("Indel Realigner took: " + (endTime - startTime));
            }

            if (!nomarkdup || !noqrecab || !novariant) {
                /* 
                 * MarkDuplicate and Indexing Job 
                 * FixMateInformation is not required as it is handled
                 * automatically by GATK after IndelRealignment.
                 */
                System.out.println("Starting MarkDup/Indexing job");
                startTime = System.currentTimeMillis();
                job = new Job();
                job.setJarByClass(GATKJobClient.class);
                conf = job.getConfiguration();
                if (!norealign)
                    inputPath = new Path(IndelOutPath);
                else if (!noalign)
                    inputPath = new Path(SortBWAOutPath);
                else
                    inputPath = new Path(BAMInputPath);
                FileInputFormat.addInputPath(job, inputPath);
                job.setInputFormatClass(WholeFileInputFormat.class);

                conf.setLong("mapred.task.timeout", 86400000L);
                conf.setBoolean("mapred.map.tasks.speculative.execution", false);
                conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
                conf.setInt("mapred.reduce.tasks", 0);
                if (!nomarkdup) {
                    System.out.println("Starting MarkDuplicates job");
                    conf.setBoolean("gatk.hadoop.ismarkdup", true);
                    FileOutputFormat.setOutputPath(job, new Path(RmdupOutPath));
                }
                if(!noqrecab || !novariant) {
                    conf.setBoolean("gatk.hadoop.issindex", true);
                    conf.setBoolean("gatk.hadoop.isindex", true);
                    if (nomarkdup) {
                        System.out.println("Starting Indexing job");
                        FileOutputFormat.setOutputPath(job, new Path(outputDir + Path.SEPARATOR + "DeleteThisDir3"));
                    }
                }
                job.setMapperClass(IndexMapper.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(NullWritable.class);

                DistributedCache.addArchiveToClassPath(new Path(gatk_binary_loc),
                                                                    job.getConfiguration());
                if (job.waitForCompletion(true)) {
                        System.out.println("Markdup/Indexing job done !!!");
                }
                Path toDelete = new Path(outputDir + Path.SEPARATOR + "DeleteThisDir3");
                fs = toDelete.getFileSystem(conf);
                if (fs.exists(toDelete)) {
                    fs.delete(toDelete, true);
                }

                if (!nomarkdup) {
                    Path rmdupOutPath = new Path(RmdupOutPath);
                    fs = rmdupOutPath.getFileSystem(conf);
                    content = fs.listStatus(rmdupOutPath);

                    for (int i = 0; i < content.length; i++) {
                            if ((content[i].getPath().getName()).startsWith("part")) {
                                    fs.delete(content[i].getPath(), false);
                            }
                    }
                    endTime = System.currentTimeMillis();
                    System.out.println("MarkDuplicates took: " + (endTime - startTime));
                }
                else {
                    endTime = System.currentTimeMillis();
                    System.out.println("Indexing took: " + (endTime - startTime));
                }
            }

            if (!noqrecab) {
                startTime = System.currentTimeMillis();
                System.out.println("Starting Recal - Count Covariates Job");
                job = new Job();
                job.setJarByClass(GATKJobClient.class);
                conf = job.getConfiguration();
                if (!nomarkdup)
                    inputPath = new Path(RmdupOutPath);
                else if (!norealign)
                    inputPath = new Path(IndelOutPath);
                else if (!noalign)
                    inputPath = new Path(SortBWAOutPath);
                else
                    inputPath = new Path(BAMInputPath);
                FileInputFormat.addInputPath(job, inputPath);
                job.setInputFormatClass(LociInputFormat.class);

                conf.setLong("local.cache.size", 20106127360L);
                conf.setInt("mapred.reduce.tasks", 1);
                conf.setLong("mapred.task.timeout", 86400000L);
                conf.set("gatk.hadoop.outputpath", outputDir);
                // conf.setInt("mapred.tasktracker.map.tasks.maximum", 1);
                // conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 1);
                // conf.setBoolean("mapred.map.tasks.speculative.execution", false);
                // conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
                // conf.setBoolean("mapred.compress.map.output", true); // Default compression ration 3.5:1

                conf.setBoolean("gatk.hadoop", true);
                conf.setBoolean("gatk.hadoop.isazure", is_azure);
                job.setMapperClass(RecalCovMapper.class);
                job.setCombinerClass(RecalCovCombiner.class);
                job.setReducerClass(RecalCovReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);
                FileOutputFormat.setOutputPath(job, new Path(outputDir + Path.SEPARATOR + "CovariateOut"));

                DistributedCache.addArchiveToClassPath(new Path(gatk_binary_loc),
                                                                  job.getConfiguration());
                // Standard inputs
                DistributedCache.addCacheFile(new URI(knownSitesLoc + "#"
                                                    + "ref.vcf"), job.getConfiguration());
                DistributedCache.addCacheFile(new URI(knownSitesLoc + ".idx#"
                                                + "ref.vcf.idx"), job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileLoc + "#" + "ref.fa"),
                                                                  job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileLoc + ".fai#"
                                                 + "ref.fa.fai"), job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileName + ".dict#"
                                                   + "ref.dict"), job.getConfiguration());

                DistributedCache.createSymlink(job.getConfiguration());

                if (job.waitForCompletion(true)) {
                        System.out.println("CountCovariates done");
                }
                endTime = System.currentTimeMillis();
                System.out
                        .println("CountCovariates took: " + (endTime - startTime));
            }

            if (!noqrecab || !novariant) {
                startTime = System.currentTimeMillis();
                System.out.println("Starting Table Recalibration / Unified Genotyper Job");
                if (!nomarkdup)
                    inputPath = new Path(RmdupOutPath);
                else if (!norealign)
                    inputPath = new Path(IndelOutPath);
                else if (!noalign)
                    inputPath = new Path(SortBWAOutPath);
                else
                    inputPath = new Path(BAMInputPath);
                job = new Job();
                job.setJarByClass(GATKJobClient.class);
                conf = job.getConfiguration();
                FileInputFormat.addInputPath(job, inputPath);

                if (!noqrecab) {
                    conf.setBoolean("gatk.hadoop.recab", true);
                    if (norealign) {
                        job.setInputFormatClass(BAMInputFormat.class);
                        srcFs = new Path(outputDir).getFileSystem(conf);
                        if (!srcFs.mkdirs(new Path(outputDir + "/" + "Partition")))
                            System.out.println("mkdir failed");
                    } else {
                        job.setInputFormatClass(LociInputFormat.class);
                    }
                    inputDir = new Path(outputDir + "/" + "Partition");
                    inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));
                    partition = new Path(inputDir, "_partition");
                    job.setPartitionerClass(TotalOrderPartitioner.class);
                    TotalOrderPartitioner.setPartitionFile(conf, partition);
                    try {
                            URI partitionURI = new URI(partition.toString() + "#_partition");
                            DistributedCache.addCacheFile(partitionURI, conf);
                    } catch (URISyntaxException e) {
                            assert false;
                    }

                    if (nReducers == 0) {
                        conf.setInt("mapred.reduce.tasks", maxMapTasks);
                    } else {
                        conf.setInt("mapred.reduce.tasks", nReducers);
                    }
                    conf.setBoolean("mapred.compress.map.output", true); // Default compression ratio 3.5:1
                    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
                    if (!nomresults)
                        conf.setBoolean("gatk.hadoop.ismerge", true);
                    job.setReducerClass(SortReducer.class);
                    job.setMapOutputKeyClass(LongWritable.class);
                    job.setOutputKeyClass(NullWritable.class);
                    job.setOutputValueClass(SAMRecordWritable.class);
                    job.setOutputFormatClass(SortOutputFormat.class);
                    FileOutputFormat.setOutputPath(job, new Path(RecalOutPath));
                } else {
                    job.setInputFormatClass(LociInputFormat.class);
                    conf.setInt("mapred.reduce.tasks", 0);
                    FileOutputFormat.setOutputPath(job, new Path(outputDir + Path.SEPARATOR + "DeleteThisDir4"));
                }

                job.setMapperClass(RecalMapper.class);
                conf.setLong("mapred.task.timeout", 86400000L);
                conf.setBoolean("mapred.map.tasks.speculative.execution", false);
                conf.setInt("dfs.datanode.socket.write.timeout", 600000);
                conf.setInt("dfs.socket.timeout", 600000);

                conf.set("gatk.hadoop.outputpath", outputDir);
                conf.setBoolean("gatk.hadoop", true);
                conf.setBoolean("gatk.hadoop.isazure", is_azure);
                if (!novariant) {
                    conf.setBoolean("gatk.hadoop.variant", true);
                    if (!nofvariant)
                        conf.setBoolean("gatk.hadoop.fvariant", true);
                    conf.setInt("gatk.hadoop.nthreads", nThreads);
                    conf.setBoolean("gatk.hadoop.xvariant", xVariantCall);
                }

                if (!noqrecab && norealign) {
                    sampler =
                            new InputSampler.IntervalSampler<LongWritable,SAMRecordWritable>(
                                                                sampling_frequency, max_splits);
                    InputSampler.<LongWritable,SAMRecordWritable>writePartitionFile(
                                                                            job, sampler);
                    job.setInputFormatClass(LociInputFormat.class);
                }

                DistributedCache.addArchiveToClassPath(new Path(gatk_binary_loc),
                                                                    job.getConfiguration());
                // Standard inputs
                DistributedCache.addCacheFile(new URI(refFileLoc + "#" + "ref.fa"),
                                                                    job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileLoc + ".fai#"
                                                   + "ref.fa.fai"), job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileName + ".dict#"
                                                     + "ref.dict"), job.getConfiguration());

                DistributedCache.createSymlink(job.getConfiguration());

                if (job.waitForCompletion(true)) {
                        System.out.println("TableRecalibration Job done !!");
                }
                endTime = System.currentTimeMillis();
                Path toDelete = new Path(outputDir + Path.SEPARATOR + "DeleteThisDir4");
                fs = toDelete.getFileSystem(conf);
                if (fs.exists(toDelete)) {
                    fs.delete(toDelete, true);
                }
                System.out.println("TableRecalibraion / UnifiedGenotyper job took: "
                                              + (endTime - startTime));
            }
            if (!novariant && !nomresults) {
                startTime = System.currentTimeMillis();
                System.out.println("Merge Variant Job");
                job = new Job();
                job.setJarByClass(GATKJobClient.class);
                conf = job.getConfiguration();
                inputPath = new Path(outputDir + Path.SEPARATOR + "VariantOut");
                FileInputFormat.addInputPath(job, inputPath);
                job.setInputFormatClass(WholeFileInputFormat.class);

                conf.setInt("mapred.reduce.tasks", 1);
                conf.setLong("mapred.task.timeout", 86400000L);
                conf.setBoolean("mapred.map.tasks.speculative.execution", false);
                conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

                conf.setBoolean("gatk.hadoop", true);
                conf.setBoolean("gatk.hadoop.isazure", is_azure);
                job.setReducerClass(VariantReducer.class);
                job.setMapOutputKeyClass(LongWritable.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(NullWritable.class);
                FileOutputFormat.setOutputPath(job, new Path(outputDir + Path.SEPARATOR + "FinalVariantOut"));

                DistributedCache.addArchiveToClassPath(new Path(gatk_binary_loc),
                                                                job.getConfiguration());
                // Standard inputs
                DistributedCache.addCacheFile(new URI(refFileLoc + "#" + "ref.fa"),
                                                                job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileLoc + ".fai#"
                                               + "ref.fa.fai"), job.getConfiguration());
                DistributedCache.addCacheFile(new URI(refFileName + ".dict#"
                                                 + "ref.dict"), job.getConfiguration());

                DistributedCache.createSymlink(job.getConfiguration());

                if (job.waitForCompletion(true)) {
                        System.out.println("Merge Variants done");
                }
                endTime = System.currentTimeMillis();
                System.out.println("MergeVariant job took: "
                                                + (endTime - startTime));

                if (xVariantCall && !novariant && !nomresults) {
                    startTime = System.currentTimeMillis();

                    System.out.println("Merge INDEL Variant Job");
                    job = new Job();
                    job.setJarByClass(GATKJobClient.class);
                    conf = job.getConfiguration();
                    inputPath = new Path(outputDir + Path.SEPARATOR + "IVariantOut");
                    FileInputFormat.addInputPath(job, inputPath);
                    job.setInputFormatClass(WholeFileInputFormat.class);

                    conf.setInt("mapred.reduce.tasks", 1);
                    conf.setLong("mapred.task.timeout", 86400000L);
                    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
                    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

                    conf.setBoolean("gatk.hadoop", true);
                    conf.setBoolean("gatk.hadoop.isazure", is_azure);
                    job.setReducerClass(VariantReducer.class);
                    job.setMapOutputKeyClass(LongWritable.class);
                    job.setMapOutputValueClass(Text.class);
                    job.setOutputKeyClass(NullWritable.class);
                    job.setOutputValueClass(NullWritable.class);
                    FileOutputFormat.setOutputPath(job, new Path(outputDir + Path.SEPARATOR + "FinalIVariantOut"));

                    DistributedCache.addArchiveToClassPath(new Path(gatk_binary_loc),
                                    job.getConfiguration());
                    // Standard inputs
                    DistributedCache.addCacheFile(new URI(refFileLoc + "#" + "ref.fa"),
                                    job.getConfiguration());
                    DistributedCache.addCacheFile(new URI(refFileLoc + ".fai#"
                                    + "ref.fa.fai"), job.getConfiguration());
                    DistributedCache.addCacheFile(new URI(refFileName + ".dict#"
                                    + "ref.dict"), job.getConfiguration());

                    DistributedCache.createSymlink(job.getConfiguration());

                    if (job.waitForCompletion(true)) {
                            System.out.println("Merge INDEL Variants done");
                    }
                    endTime = System.currentTimeMillis();
                    System.out.println("MergeINDELVariant job took: "
                                                    + (endTime - startTime));
                }
            }

            if (!nomresults) {
                startTime = System.currentTimeMillis();
                System.out.println("Starting Merge BAM Job");

                outputPath = new Path(FinalBAMPath);
                outFs = outputPath.getFileSystem(conf);

                if (!outFs.mkdirs(outputPath))
                    System.out.println("mkdir failed");
                // Currently no support to merge output from MarkDuplicates 
                // from Job Client. Need to have a separate MR job for it.
                if (!noqrecab)
                    inputPath = new Path(RecalOutPath);
                else if (!norealign)
                    inputPath = new Path(IndelOutPath);
                else if (!noalign)
                    inputPath = new Path(SortBWAOutPath);
                else if (!nomarkdup)
                    throw new Exception("Merge not implemented MarkDuplicates output.");
                else if (noqrecab && noalign && norealign && novariant && nomarkdup && nofvariant)
                    inputPath = new Path(BAMInputPath);

                fs = inputPath.getFileSystem(conf);

                content = fs.listStatus(inputPath);
                mergeOutFile = new Path(FinalBAMPath, "GATKAnalysisResult.bam");

                Path p = null;
                int nfiles = 0;
                for (int i = 0; i < content.length; i++) {
                    p = content[i].getPath();
                    ++nfiles;
                }

                if (nfiles == 1) {
                    boolean rename = fs.rename(p, mergeOutFile);
                } else {
                    out = outFs.create(mergeOutFile, true);

                    for (int i = 0; i < content.length; i++) {
                        p = content[i].getPath();
                        if ((p.getName()).endsWith(".bam")) {
                                in = fs.open(p);
                                IOUtils.copyBytes(in, out, conf, false);
                                in.close();
                        }
                    }

                    out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK);
                    out.close();
                }

                endTime = System.currentTimeMillis();
                System.out.println("Final Merge took: " + (endTime - startTime));
            }
            System.out.println("JobCompleted");
        } catch (IOException e) {
            System.err.printf("Hadoop Error : %s\n", e);
            return -1;
        } catch (InterruptedException e) {
            System.err.printf("Hadoop Error : %s\n", e);
            return -1;
        } catch (ClassNotFoundException e) {
            System.err.printf("Hadoop Error : %s\n", e);
            return -1;
        } catch (Exception e) {
            System.err.printf("Hadoop Error : %s\n", e);
            return -1;
        }
        return 0;
    }

    public static void main(String[] argv) throws Exception
    {
        int exitCode = ToolRunner.run(new GATKJobClient(), argv);
        System.exit(exitCode);
    }
}
