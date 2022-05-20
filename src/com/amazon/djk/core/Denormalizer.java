package com.amazon.djk.core;

import java.io.IOException;

import com.amazon.djk.core.DenormRecord.Visibility;
import com.amazon.djk.record.FieldIterator;
import com.amazon.djk.record.FieldType;
import com.amazon.djk.record.NamedFieldIterator;
import com.amazon.djk.record.NotIterator;
import com.amazon.djk.record.Record;

/**
 * denormalize the subrecords of a record.
 * <p>
 * The challenge here is within a subexpression like foreach, to not pollute
 * the downstream records with the 'remnant' fields of parent records, while
 * at the same time, making these fields appear to visible during execution.
 */
public class Denormalizer extends MinimalRecordSource {
    public enum AddMode {CHILD_FIELDS_ONLY, INCLUDE_REMNANT_FIELDS}

    ;

    public enum Context {SUBEXP, SIMPLE_DENORM}

    ;

    private final AddMode mode;
    private final Record outrec;
    private final DenormRecord remnant = new DenormRecord();
    private Record inRec;

    private final NotIterator notChildFiter;
    private final FieldIterator childFiter;
    private final Context expContext;

    private boolean ready = false;

    /**
     * @param child   the name of the fields representing child records
     * @param mode    either CHILD_FIELDS_ONLY or INCLUDE_REMNANT_FIELDS
     * @param context either SUBEXP or SIMPLE_DENORM
     * @throws IOException
     */
    public Denormalizer(String child, AddMode mode, Context context) throws IOException {
        this.mode = mode;
        this.notChildFiter = new NotIterator(child);
        this.childFiter = new NamedFieldIterator(child);
        this.expContext = context;
        this.outrec = context == Context.SUBEXP ? new DenormRecord() : new Record();
    }

    /**
     * @param input incoming record
     * @return a remnant record containing all but the fields
     * @throws IOException
     */
    public Record init(Record input) throws IOException {
        inRec = input;
        remnant.reset();

        // resize the remnant to the input because we don't want to have
        // to grow incrementally.  rate changes from 8MB/sec -> 400MB/sec
        remnant.resize(input.size());

        if (input instanceof DenormRecord) {
            // make invisible the remant from higher level denormalization.
            // this will exclude higher level remnants from the remnant created here
            ((DenormRecord) input).setVisibility(Visibility.PART_TWO);
        }

        // does all fields other than children
        notChildFiter.init(inRec);
        while (notChildFiter.next()) {
            remnant.addField(notChildFiter);
        }

        // initialize fiter to iterate over children remaining in inrec
        childFiter.init(inRec);
        ready = true;
        return remnant;
    }

    @Override
    public Record next() throws IOException {
        if (!ready || !childFiter.next()) {
            ready = false; // leave in a non-ready state for when used inside subexpressions
            return null;
        }

        outrec.reset();

        // remnants always added first
        if (mode == AddMode.INCLUDE_REMNANT_FIELDS) {
            if (inRec instanceof DenormRecord) {
                // add the higher level remnant
                ((DenormRecord) inRec).setVisibility(Visibility.PART_ONE);
                outrec.addFields(inRec);
            }

            // this level remnant
            outrec.addFields(remnant);
            if (expContext == Context.SUBEXP) {
                ((DenormRecord) outrec).setPartOne(); // set as part one to define this level (and above) remnant
            }
        }

        if (childFiter.getType() == FieldType.RECORD) { // the subrecord case
            childFiter.getValueAsRecord(outrec, false); // denormalizes into
        } else { // the field case
            if (outrec instanceof DenormRecord) {
                ((DenormRecord) outrec).setIsField(true);
            }
            outrec.addField(childFiter);
        }

        return outrec;
    }
}
