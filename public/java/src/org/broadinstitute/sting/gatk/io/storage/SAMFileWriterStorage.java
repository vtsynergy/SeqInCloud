/*
 * Copyright (c) 2009 The Broad Institute
 * Copyright (c) 2012-2013 by Virginia Polytechnic Institute and State
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
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.broadinstitute.sting.gatk.io.storage;

import net.sf.samtools.*;
import net.sf.samtools.util.CloseableIterator;
import net.sf.samtools.util.RuntimeIOException;
import org.apache.log4j.Logger;
import org.broadinstitute.sting.gatk.io.stubs.SAMFileWriterStub;
import org.broadinstitute.sting.utils.exceptions.UserException;
import org.broadinstitute.sting.utils.sam.SimplifyingSAMFileWriter;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.broadinstitute.sting.gatk.hadoop.BAMRecordWriterLite;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Provides temporary storage for SAMFileWriters.
 *
 * @author mhanna
 * @version 0.1
 */
public class SAMFileWriterStorage implements SAMFileWriter, Storage<SAMFileWriter> {
    private final File file;
    private SAMFileWriter writer;
    private BAMRecordWriterLite bamWriter;
    private boolean writeToHDFS = false;

    private static Logger logger = Logger.getLogger(SAMFileWriterStorage.class);

    public SAMFileWriterStorage( SAMFileWriterStub stub ) {
        this(stub,stub.getSAMFile());   
    }

    public SAMFileWriterStorage( SAMFileWriterStub stub, File file ) {
        this.file = file;
        SAMFileWriterFactory factory = new SAMFileWriterFactory();
        // Enable automatic index creation for pre-sorted BAMs.
        if (stub.getFileHeader().getSortOrder().equals(SAMFileHeader.SortOrder.coordinate) && stub.getIndexOnTheFly())
            factory.setCreateIndex(true);
        if (stub.getGenerateMD5())
            factory.setCreateMd5File(true);
        // Adjust max records in RAM.
        if(stub.getMaxRecordsInRam() != null)
            factory.setMaxRecordsInRam(stub.getMaxRecordsInRam());

        if(stub.getSAMFile() != null) {
            try {
		if (file.toString().endsWith(".bam")) {		
                this.writer = createBAMWriter(factory,stub.getFileHeader(),stub.isPresorted(),file,stub.getCompressionLevel());
		} else if (file.toString().endsWith(".sam")) {
                	this.writer = factory.makeSAMWriter(stub.getFileHeader(), stub.isPresorted(), file);
            }
            } catch(RuntimeIOException ex) {
                throw new UserException.CouldNotCreateOutputFile(file,"file could not be created",ex);
            }
        }
        else if(stub.getSAMOutputStream() != null){
            this.writer = factory.makeSAMWriter( stub.getFileHeader(), stub.isPresorted(), stub.getSAMOutputStream());
        }
	else if (stub.getSAMPath() != null) {
	    try {
		Path p = stub.getSAMPath();
		FileSystem fs = p.getFileSystem(new Configuration());
		if (fs.exists(p))
			fs.delete(p, false);
		FSDataOutputStream output = fs.create(p, true);
		//FSDataOutputStream output = fs.create(p, true, fs.getConf().getInt("io.file.buffer.size", 4096), (short) 2, fs.getDefaultBlockSize());
              	this.bamWriter = new BAMRecordWriterLite(output, stub.getFileHeader(), true);
               	this.writeToHDFS = true;
	    } catch (Exception e) { 
		System.out.println("SAMFileWriterStorage exception: " + e.getMessage());
	    }
	}
        else
            throw new UserException("Unable to write to SAM file; neither a target file nor a stream has been specified");

        // if we want to send the BAM file through the simplifying writer, wrap it here
        if ( stub.simplifyBAM() ) {
            this.writer = new SimplifyingSAMFileWriter(this.writer);
        }
    }

    public SAMFileHeader getFileHeader() {
	if (writeToHDFS)
		return bamWriter.getFileHeader();
	else
        return writer.getFileHeader();
    }

    public void addAlignment( SAMRecord read ) {
	if (writeToHDFS)
		bamWriter.writeAlignment(read);
	else
        writer.addAlignment(read);
    }

    public void close() {
        try {
	    if (writeToHDFS)
		bamWriter.close();
	    else
            writer.close();
        } catch (RuntimeIOException e) {
            throw new UserException.ErrorWritingBamFile(e.getMessage());
        }
    }

    public void mergeInto( SAMFileWriter targetStream ) {
        SAMFileReader reader = new SAMFileReader( file );
        try {
            CloseableIterator<SAMRecord> iterator = reader.iterator();
            while( iterator.hasNext() )
                targetStream.addAlignment( iterator.next() );
            iterator.close();
        }
        finally {
            reader.close();
            file.delete();
        }
    }

    private SAMFileWriter createBAMWriter(final SAMFileWriterFactory factory,
                                 final SAMFileHeader header,
                                 final boolean presorted,
                                 final File outputFile,
                                 final Integer compressionLevel) {
        SAMFileWriter writer;
        if(compressionLevel != null)
            writer = factory.makeBAMWriter(header, presorted, outputFile, compressionLevel);
        else
            writer = factory.makeBAMWriter(header, presorted, outputFile);

        // mhanna - 1 Mar 2011 - temporary hack until Picard generates an index file for empty BAMs --
        //                     - do a pre-initialization of the BAM file.
        try {
            Method prepareToWriteAlignmentsMethod = writer.getClass().getDeclaredMethod("prepareToWriteAlignments");
            if(prepareToWriteAlignmentsMethod != null) {
                prepareToWriteAlignmentsMethod.setAccessible(true);
                prepareToWriteAlignmentsMethod.invoke(writer);
            }
        }
        catch(NoSuchMethodException ex) {
            logger.info("Unable to call prepareToWriteAlignments method; this should be reviewed when Picard is updated.");
        }
        catch(IllegalAccessException ex) {
            logger.info("Unable to access prepareToWriteAlignments method; this should be reviewed when Picard is updated.");
        }
        catch(InvocationTargetException ex) {
            logger.info("Unable to invoke prepareToWriteAlignments method; this should be reviewed when Picard is updated.");
        }

        return writer;
    }

}
