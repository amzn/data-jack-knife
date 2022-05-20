package com.amazon.djk.test;

import java.io.IOException;
import java.util.List;

import com.amazon.djk.source.InlineRecords;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.processor.JackKnife;
import com.amazon.djk.record.Record;
import com.amazon.djk.record.SlowComparableRecord;

public class ExpressionResponseTest implements ExpressionTest {
    private final JackKnife knife;
    private final String exp;
    private final String resp;

    /**
     * 
     * @param knife
     * @param exp the test expression
     * @param resp the response expression or null if non-existent
     */
    public ExpressionResponseTest(JackKnife knife, String exp, String resp) {
        this.knife = knife;
        this.exp = exp;
        this.resp = resp;
    }

    @Override
    public boolean isSuccessful() throws IOException, SyntaxError {
        List<Record> recs = knife.collectMain(exp);
        if (resp != null) {
            List<Record> answer = knife.collectMain(resp);
            if (!SlowComparableRecord.areEqual(answer, recs)) {
                System.err.println("incorrect response for: " + exp);
                return false;
            }

            return true;
        }
        
        System.out.println();
        System.out.println("missing response to: " + exp);
        System.out.print("R [ ");
        for (Record rec : recs) {
            System.out.print(InlineRecords.getAsSingleLine(rec) + " ");
        }
        System.out.println("]");
        
        return false;
    }
}

