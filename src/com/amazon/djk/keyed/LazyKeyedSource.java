package com.amazon.djk.keyed;

import java.io.FileNotFoundException;
import java.io.IOException;

import com.amazon.djk.core.MinimalRecordSource;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.ParseToken;
import com.amazon.djk.expression.SourceOperator;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.processor.InnerKnife;
import com.amazon.djk.processor.NeedsSource;
import com.amazon.djk.processor.WithInnerSink;
import com.amazon.djk.report.DisplayMode;

/**
 * This class is really SourceOperator masquerading as a RecordSource in order to
 * delay the instantiation of KeyedSources until the predicate that needs it 
 * asks for it from ParseOperands.
 * 
 * For keyed sources we need to know if it's in a KeyedSource context or in 
 * RecordSource Context, but we can't know that until the very end due to macros.
 * Data is never pulled from this.
 * 
 * would like to make this final but for the extending class below.
 * 
 * For the PlusInnerSink subclass, it also is hard-coded to implement NeedsSource
 * because we can't wait until instantiation of the class to find out if we need
 * to grab a source from the operands.  However we could expand the subclasses here
 * to provide the matrix of possibilities and use reflection accordingly.
 */
public class LazyKeyedSource extends MinimalRecordSource {
    private final SourceOperator op;
    private final OpArgs accessArgs;
    
    /**
     * factory method for hiding the messiness of constructing these wrapper classes
     * 
     * @param op
     * @param args
     * @return
     * @throws SyntaxError
     * @throws IOException
     */
    public static RecordSource create(SourceOperator op, OpArgs args) throws SyntaxError, IOException {        
        // if this is a keyed source then we need to provide a lazy accessor
        if (op.getMostSpecificPredicateType() == KeyedSource.class) {
            boolean hasInner = op.hasInnerSink();
            return hasInner ? 
                    new LazyKeyedSource.PlusInnerSink(op, args) : new LazyKeyedSource(op, args);
        } 
        
        else { // nope, just return the source
            return op.getSource(args);
        }
    }
    
    private LazyKeyedSource(SourceOperator op, OpArgs accessArgs) throws SyntaxError, IOException {
        this.op = op;
        this.accessArgs = accessArgs;
    }
   
    public KeyedSource getKeyedSource() throws IOException, SyntaxError {
        KeyedSource source = null;
        try {
            source = op.getKeyedSource(accessArgs);
        } 
        
        catch (SyntaxError | FileNotFoundException e) {
            throw new SyntaxError(accessArgs.getToken(), e.getMessage());
        }
        
        if (source == null) {
            throw new SyntaxError(accessArgs.getToken(), "component has not implemented getKeyedSource()");
        }
        
        return source;
    }
    
    public RecordSource getSource() throws IOException, SyntaxError {
        return op.getSource(accessArgs);
    }
    
    /**
     * WithInnerSink version of the above
     *
     */
    private final static class PlusInnerSink extends LazyKeyedSource implements WithInnerSink, NeedsSource {
        private RecordSource sourceForInnerSink = null;
        
        private PlusInnerSink(SourceOperator op, OpArgs accessArgs) throws SyntaxError, IOException {
            super(op, accessArgs);
        }

        @Override
        public KeyedSource getKeyedSource() throws IOException, SyntaxError {
            KeyedSource source = super.getKeyedSource();
            if (! (source instanceof WithInnerSink) ) {
                throw new RuntimeException("programmer exception");
            }

            ((NeedsSource)source).addSource(sourceForInnerSink);
            return source;
        }
        
        @Override
        public RecordSource getSource() throws IOException, SyntaxError {
            RecordSource source = super.getSource();
            if (! (source instanceof WithInnerSink) ) {
                throw new RuntimeException("programmer exception");
            }

            ((NeedsSource)source).addSource(sourceForInnerSink);
            return source;
        }
        
        @Override
        public void addSource(RecordSource source) {
            sourceForInnerSink = source;
        }

        @Override
        public RecordSink getSink() {
            throw new UnsupportedOperationException("this class should never occur with an execution chain");
        }

        @Override
        public void finishSinking(InnerKnife processor) throws IOException, SyntaxError {
            throw new UnsupportedOperationException("this class should never occur with an execution chain");
        }
    }
}
