package com.amazon.djk.pipe;

import java.io.IOException;

import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.pipe.IfPipe.BaseIfOp;

/**
 * Not a subclass of IfPipe, never instantiated. The Op returns version of IfPipe
 *
 */
public class IfNotPipe extends RecordPipe implements Keyword { // NOT a subclass of IfPipe
    public IfNotPipe(RecordPipe root) throws IOException {
        super(root);
    }

    @Description(
            text={"Inverse of 'if'.  Since 'nonBoolean' CONDITIONALS are treated as false, allows for simplier expressions involving fields that are not always present. See 'if', 'acceptIf' and 'rejectIf'."},
            contexts={"[ TRUE_EXP ifNot:CONDITIONAL", "[ TRUE_EXP else FALSE_EXP ifNot:CONDITIONAL"})
    @Example(expr="[ id:1 id:2 color:blue ] [ add:foo:bar ifNot:'{l.id == 2;}'", type=ExampleType.EXECUTABLE_GRAPHED)
    public static class Op extends BaseIfOp {
        public Op() {
            super(true);
        }
    }
}
