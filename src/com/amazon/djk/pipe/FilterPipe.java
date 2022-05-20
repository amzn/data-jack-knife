package com.amazon.djk.pipe;

import java.io.IOException;

import com.amazon.djk.core.Keyword;
import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParserOperands;
import com.amazon.djk.expression.PipeOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;
import com.amazon.djk.manual.Gloss;

/**
 * Exposes JoinPipe implementation as the 'filter' predicate. This is really just an alias for 'join'
 *
 */
public class FilterPipe extends RecordPipe implements Keyword {
    public final static String NAME = "filter";

    // unused
    public FilterPipe(RecordPipe root) throws IOException {
		super(root);
	}

    @Description(text={"Filters the LEFT_SRC by the keyed RIGHT_SRC acccording to HOW.",
    "The keyed right source defines which fields comprise the KEY."},
    contexts={"LEFT_SRC RIGHT_SRC filter:HOW"})

    @Gloss(entry="-", def="eliminate left records that have KEY in common with a right record")
    @Gloss(entry="+", def="retain left records that have KEY in common with a right record")
    @Gloss(entry="+diff", def="retain left records that have KEY in common with a right record and diff the records (SLOW!)")
    @Gloss(entry="once", def="retain left records ONLY ONCE that have KEY in common with a right record")    

    @Example(expr="[ id:1,color:blue id:2,color:red ] [ id:1 ] map:id filter:+", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1,color:blue id:2,color:red ] [ id:1 ] map:id filter:-", type=ExampleType.EXECUTABLE)
    @Example(expr="[ id:1,color:blue id:1,color:red ] [ id:1 ] map:id filter:once", type=ExampleType.EXECUTABLE)    
    @Example(expr="[ id:1,color:blue,x:y ] [ x:y,id:1,size:big,color:green ] map:id filter:+diff", type=ExampleType.EXECUTABLE)
    
    @Arg(name="HOW", gloss="+ | - | once", type = ArgType.STRING, eg = "+")
    public static class Op extends PipeOperator {
    	public Op() {
    		super(NAME + ":HOW");
    	}

		@Override
		public RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
			JoinPipe.Op joinOp = new JoinPipe.Op();
			return joinOp.getAsPipe(operands, args, true);
		}
	 }
}
