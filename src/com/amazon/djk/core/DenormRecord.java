package com.amazon.djk.core;

import com.amazon.djk.record.Record;

public class DenormRecord extends Record {
    private int partOneEnd = 0;
    private int trueLength = -1;
    private boolean isField = false; // true if this represents a scalar field instead of a record

    @Override
    public Record getCopy() {
        DenormRecord copy = new DenormRecord();
        copy.putBytes(this);
        copy.partOneEnd = partOneEnd;
        copy.trueLength = trueLength;
        return copy;
    }

    /**
     * After the call, the current fields of the record will represent part one.
     * Any subsequently added fields will be contained in part two.
     */
    public void setPartOne() {
        partOneEnd = length;
    }

    public enum Visibility {BOTH_PARTS, PART_ONE, PART_TWO}

    ;

    public void setIsField(boolean value) {
        isField = value;
    }

    public boolean getIsField() {
        return isField;
    }

    /**
     * Makes invisible the remnants from higher level denormalization.
     * The remnant returned by Denomalizer.init() will exclude higher level remnants from previous denormalizations.
     *
     * @param visibility
     */
    public void setVisibility(Visibility visibility) {
        switch (visibility) {
            case BOTH_PARTS:
                trueLength = trueLength != -1 ? trueLength : length; // assure trueLength is always actual true length

                offset = 0;
                length = trueLength;
                break;

            case PART_ONE:
                trueLength = trueLength != -1 ? trueLength : length; // assure trueLength is always actual true length

                offset = 0;
                length = partOneEnd;
                break;

            case PART_TWO:
                trueLength = trueLength != -1 ? trueLength : length; // assure trueLength is always actual true length

                offset = partOneEnd;
                length = trueLength - partOneEnd;
        }
    }


    @Override
    public void resize(int add) {
        super.resize(add);
    }

    @Override
    public void reset() {
        partOneEnd = 0;
        trueLength = -1;
        isField = false;
        super.reset();
    }
}
