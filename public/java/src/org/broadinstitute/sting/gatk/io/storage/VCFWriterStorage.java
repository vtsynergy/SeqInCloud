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

import net.sf.samtools.util.BlockCompressedOutputStream;
import org.apache.log4j.Logger;
import org.broad.tribble.source.BasicFeatureSource;
import org.broadinstitute.sting.gatk.io.stubs.VCFWriterStub;
import org.broadinstitute.sting.utils.codecs.vcf.StandardVCFWriter;
import org.broadinstitute.sting.utils.codecs.vcf.VCFCodec;
import org.broadinstitute.sting.utils.codecs.vcf.VCFHeader;
import org.broadinstitute.sting.utils.codecs.vcf.VCFWriter;
import org.broadinstitute.sting.utils.exceptions.ReviewedStingException;
import org.broadinstitute.sting.utils.exceptions.UserException;
import org.broadinstitute.sting.utils.variantcontext.VariantContext;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;

/**
 * Provides temporary and permanent storage for genotypes in VCF format.
 *
 * @author mhanna
 * @version 0.1
 */
public class VCFWriterStorage implements Storage<VCFWriterStorage>, VCFWriter {
    /**
     * our log, which we want to capture anything from this class
     */
    private static Logger logger = Logger.getLogger(VCFWriterStorage.class);

    protected final File file;
    protected Path path;
    protected OutputStream stream;
    protected final VCFWriter writer;

    /**
     * Constructs an object which will write directly into the output file provided by the stub.
     * Intentionally delaying the writing of the header -- this should be filled in by the walker.
     * @param stub Stub to use when constructing the output file.
     */
    public VCFWriterStorage( VCFWriterStub stub )  {
        if ( stub.getFile() != null ) {
            this.file = stub.getFile();
	    this.path = null;
            writer = vcfWriterToFile(stub,stub.getFile(),true);
        }
        else if ( stub.getOutputStream() != null ) {
            this.file = null;
	    this.path = null;
            this.stream = stub.getOutputStream();
            writer = new StandardVCFWriter(stream, stub.getMasterSequenceDictionary(), stub.doNotWriteGenotypes());
        }
        else if ( stub.getPath() != null ) {
	    this.path = stub.getPath();
	    this.file = null;
            writer = vcfWriterToPath(stub,stub.getPath(),true);
	} else
            throw new ReviewedStingException("Unable to create target to which to write; storage was provided with neither a file nor a stream.");
    }

    /**
     * common initialization routine for multiple constructors
     * @param stub Stub to use when constructing the output file.
     * @param file Target file into which to write VCF records.
     * @param indexOnTheFly true to index the file on the fly.  NOTE: will be forced to false for compressed files.
     * @return A VCF writer for use with this class
     */
    private StandardVCFWriter vcfWriterToFile(VCFWriterStub stub, File file, boolean indexOnTheFly) {
        try {
            if ( stub.isCompressed() )
                stream = new BlockCompressedOutputStream(file);
            else
                stream = new PrintStream(file);
        }
        catch(IOException ex) {
            throw new UserException.CouldNotCreateOutputFile(file, "Unable to open target output stream", ex);
        }

        // The GATK/Tribble can't currently index block-compressed files on the fly.  Disable OTF indexing even if the user explicitly asked for it.
        return new StandardVCFWriter(file, this.stream, stub.getMasterSequenceDictionary(), indexOnTheFly && !stub.isCompressed(), stub.doNotWriteGenotypes());
    }

    @SuppressWarnings("unchecked")
	private StandardVCFWriter vcfWriterToPath(VCFWriterStub stub, Path path, boolean indexOnTheFly) {
	try {
	    FileSystem fs = path.getFileSystem(new Configuration());
            if (fs.exists(path))
            	fs.delete(path, false);
	    final FSDataOutputStream out = fs.create(path, true);
	    //final FSDataOutputStream out = fs.create(path, true, fs.getConf().getInt("io.file.buffer.size", 4096), (short) 2, fs.getDefaultBlockSize());
            if ( stub.isCompressed() )
        	stream = new BlockCompressedOutputStream(out, null);
            else
                stream = new PrintStream(out);
        }
        catch(IOException ex) {
            throw new UserException.CouldNotCreateOutputFile(file, "Unable to open target output stream", ex);
        }

        // The GATK/Tribble can't currently index block-compressed files on the fly.  Disable OTF indexing even if the user explicitly asked for it.
        return new StandardVCFWriter(path, this.stream, stub.getMasterSequenceDictionary(), indexOnTheFly && !stub.isCompressed(), stub.doNotWriteGenotypes());
     }

    /**
     * Constructs an object which will redirect into a different file.
     * @param stub Stub to use when synthesizing file / header info.
     * @param tempFile File into which to direct the output data.
     */
    public VCFWriterStorage(VCFWriterStub stub, File tempFile) {
        logger.debug("Creating temporary VCF file " + tempFile.getAbsolutePath() + " for VCF output.");
        this.file = tempFile;
        this.writer = vcfWriterToFile(stub, file, false);
        writer.writeHeader(stub.getVCFHeader());
    }

    public void add(VariantContext vc) {
        writer.add(vc);
    }

    /**
     * initialize this VCF header
     *
     * @param header  the header
     */
    public void writeHeader(VCFHeader header) {
        writer.writeHeader(header);
    }

    /**
     * Close the VCF storage object.
     */
    public void close() {
        if(file != null)
            logger.debug("Closing temporary file " + file.getAbsolutePath());
        writer.close();
    }

    @SuppressWarnings("unchecked")
	public void mergeInto(VCFWriterStorage target) {
        try {
            String sourceFilePath = file.getAbsolutePath();
            String targetFilePath = target.file != null ? target.file.getAbsolutePath() : "/dev/stdin";
            logger.debug(String.format("Merging %s into %s",sourceFilePath,targetFilePath));
            BasicFeatureSource<VariantContext> source = BasicFeatureSource.getFeatureSource(file.getAbsolutePath(), new VCFCodec(), false);
            
            for ( VariantContext vc : source.iterator() ) {
                target.writer.add(vc);
            }

            source.close();
        } catch (IOException e) {
            throw new UserException.CouldNotReadInputFile(file, "Error reading file in VCFWriterStorage: ", e);
        }
    }
}
