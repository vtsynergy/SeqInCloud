/*
 * Copyright (c) 2012, The Broad Institute
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
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.broadinstitute.sting.gatk.report;

import org.broadinstitute.sting.utils.exceptions.ReviewedStingException;

import java.util.*;

/**
 * Tracks a linked list of GATKReportColumn in order by name.
 */
public class GATKReportColumns extends LinkedHashMap<String, GATKReportColumn> implements Iterable<GATKReportColumn> {
    private final List<String> columnNames = new ArrayList<String>();

    /**
     * Returns the column by index
     *
     * @param i the index
     * @return The column
     */
    public GATKReportColumn getByIndex(int i) {
        return get(columnNames.get(i));
    }

    @Override
    public GATKReportColumn remove(Object columnName) {
        if ( !(columnName instanceof String) ) {
            throw new ReviewedStingException("The column name must be a String!");
        }
        columnNames.remove(columnName.toString());
        return super.remove(columnName);
    }

    @Override
    public GATKReportColumn put(String key, GATKReportColumn value) {
        columnNames.add(key);
        return super.put(key, value);
    }

    @Override
    public Iterator<GATKReportColumn> iterator() {
        return new Iterator<GATKReportColumn>() {
            int offset = 0;

            public boolean hasNext() {
                return offset < columnNames.size();
            }

            public GATKReportColumn next() {
                return getByIndex(offset++);
            }

            public void remove() {
                throw new UnsupportedOperationException("Cannot remove from a GATKReportColumn iterator");
            }
        };
    }

    public boolean isSameFormat(GATKReportColumns that) {
        if (!columnNames.equals(that.columnNames)) {
            return false;
        }
        for (String columnName : columnNames) {
            if (!this.get(columnName).isSameFormat(that.get(columnName))) {
                return false;
            }
        }
        return true;
    }

    boolean equals(GATKReportColumns that) {
        for (Map.Entry<String, GATKReportColumn> pair : entrySet()) {
            // Make sure that every column is the same, we know that the # of columns
            // is the same from isSameFormat()
            String key = pair.getKey();

            if (!get(key).equals(that.get(key))) {
                return false;
            }
        }

        return true;
    }
}
