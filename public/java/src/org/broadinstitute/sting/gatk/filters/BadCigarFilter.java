/*
 * Copyright (c) 2009 The Broad Institute
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

package org.broadinstitute.sting.gatk.filters;

import net.sf.samtools.Cigar;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMRecord;

/**
 * Filter out reads with wonky cigar strings.
 *
 * @author ebanks
 * @version 0.1
 */

public class BadCigarFilter extends ReadFilter {

    public boolean filterOut(final SAMRecord rec) {
        Cigar c = rec.getCigar();
        boolean previousElementWasIndel = false;
        CigarOperator lastOp = c.getCigarElement(0).getOperator(); 

        if (lastOp == CigarOperator.D)                                                                                  // filter out reads starting with deletion
            return true;
        
        for (CigarElement ce : c.getCigarElements()) {
            CigarOperator op = ce.getOperator();
            if (op == CigarOperator.D || op == CigarOperator.I) {
                if (previousElementWasIndel)
                    return true;                                                                                        // filter out reads with adjacent I/D

                previousElementWasIndel = true;
            }
            else                                                                                                        // this is a regular base (match/mismatch/hard or soft clip)
                previousElementWasIndel = false;                                                                        // reset the previous element

            lastOp = op;
        }

        return lastOp == CigarOperator.D;
    }
}