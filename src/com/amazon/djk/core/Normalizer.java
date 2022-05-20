package com.amazon.djk.core;

import java.io.IOException;

import com.amazon.djk.core.DenormRecord.Visibility;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.record.Record;

/**
 * Used to normalize records as child records into a parent record.  If the child record
 * is a DenormRecord and getIsFields()=true, the fields of the child will be added
 */
public class Normalizer {
    private final String child;
    private long numNormalized;

    /**
     * Adds the children as subrecords to the output record
     *
     * @param child the name of the subrecord field
     */
    public Normalizer(String child) {
        this.child = child;
    }

    /**
     * @return
     */
    public long getNumNormalized() {
        return numNormalized;
    }

    /**
     * @param output
     * @param children
     * @throws IOException
     */
    public void normalize(Record output, RecordSource children) throws IOException {
        numNormalized = 0;

        while (true) {
            Record rec = children.next();
            boolean asFields = false;

            if (rec == null) break;

            numNormalized++;

            if (rec instanceof DenormRecord) {
                // turn off the remnant added for execution
                ((DenormRecord) rec).setVisibility(Visibility.PART_TWO);
                asFields = ((DenormRecord) rec).getIsField();
            }

            if (asFields) {
                output.addFields(rec); // as the fields of the child
            } else {
                output.addField(child, rec); // as a child subrecord

            }
        }
    }
}
