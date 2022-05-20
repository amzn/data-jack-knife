package com.amazon.djk.expression;

import java.io.IOException;
import java.util.List;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.processor.MacroPipe;


public abstract class MacroOperator extends PipeOperator {
    public static final String MACRO_SOURCE_VARIABLE = "?";
    
    public MacroOperator(String usage) {
        super(usage);
    }
    
    public abstract List<String> getRawMacroLines(OpArgs args) throws IOException, SyntaxError;
    
    @Override
    public final RecordPipe getAsPipe(ParserOperands operands, OpArgs args) throws IOException, SyntaxError {
        MacroPipe macro = new MacroPipe(getRawMacroLines(args)); 
        for (int i = 0; i < macro.getArity(); i++) {
            // popAsIs since don't want to cause lazy instantiation, this is too early
            macro.addSource(operands.popSourceAsIs());
        }
        
        return macro; 
    }
    
    @Override
    public Class<?> getMostSpecificPredicateType() {
        return MacroPipe.class;
    }
}