package com.amazon.djk.expression;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.amazon.djk.core.RecordPipe;
import com.amazon.djk.core.RecordSink;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.file.FileSystem;
import com.amazon.djk.format.FormatOperator;
import com.amazon.djk.format.WriterOperator;
import com.amazon.djk.keyed.KeyedSink;
import com.amazon.djk.keyed.LazyKeyedSource;
import com.amazon.djk.processor.KnifeProperties.Namespace;
import com.amazon.djk.processor.MacroPipe;
import com.amazon.djk.processor.NeedsSource;
import com.amazon.djk.reducer.Reducer;
import com.amazon.djk.sink.PrintSink;
import com.amazon.djk.source.FormatFactory;
import com.amazon.djk.source.InlineRecords;
import com.amazon.djk.source.ReceiverSource;

public class ExpressionParser {
    private final Namespace propertiesNamespace;
    private final FormatFactory formatFactory;
    private final Map<String, PipeOperator> pipeOps;
    private final Map<String, SourceOperator> sourceOps; // prefixed-name-only (not formats)

    /**
     * @param formatFactory
     * @param sourceOps
     * @param pipeOps
     */
    public ExpressionParser(Namespace propertiesNamespace, FormatFactory formatFactory, Map<String, SourceOperator> sourceOps, Map<String, PipeOperator> pipeOps) {
        this.propertiesNamespace = propertiesNamespace;
        this.formatFactory = formatFactory;
        this.pipeOps = pipeOps;
        this.sourceOps = sourceOps;
    }

    public Map<String, PipeOperator> getPipeOps() {
        return pipeOps;
    }

    public Map<String, SourceOperator> getSourceOps() {
        return sourceOps;
    }

    /**
     * @return
     */
    public Map<String, FormatOperator> getFormatOps() {
        return formatFactory.getFormatOperators();
    }

    /**
     * @return
     */
    public Map<String, WriterOperator> getWriterOps() {
        return formatFactory.getWriterOperators();
    }

    public Map<String, Operator> getAllOps() {
        Map<String, Operator> map = new HashMap<>();
        map.putAll(pipeOps);
        map.putAll(sourceOps);
        map.putAll(formatFactory.getFormatOperators());
        map.putAll(formatFactory.getWriterOperators());
        return map;
    }

    public Map<String, FileSystem> getFileSystems() {
        return formatFactory.getFileSystems();
    }

    /**
     * @param expression
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    public RecordSink getSink(ExpressionChunks expression) throws IOException, SyntaxError {
        return getSink(null, expression);
    }

    /**
     * @param upstream         record source supplying records to the downstream expression
     * @param expressionChunks
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    public RecordSink getSink(RecordSource upstream, ExpressionChunks expressionChunks) throws IOException, SyntaxError {
        // properties have been set in the ExecutionContext
        expressionChunks.resolveProperties(propertiesNamespace);
        Stack<ParseToken> tokStack = TokenResolver.getAsStack(expressionChunks);
        ParserOperands operands = new ParserOperands();
        MacroPipe macro = null;
        RecordPipe pipe = null;

        if (upstream != null) {
            operands.add(upstream);
        }

        if (operands.peek() instanceof MacroPipe) {
            macro = (MacroPipe) operands.pop();
        }

        while (!tokStack.isEmpty()) {
            ParseToken token = tokStack.pop();
            String opName = token.getOperator();

            if (opName.equals(MacroOperator.MACRO_SOURCE_VARIABLE)) {
                if (macro == null) {
                    throw new SyntaxError(token, MacroPipe.MACRO_SOURCE_VARIABLE + " only valid within macro");
                }

                operands.add(macro.popSource());
                continue;
            }

            // special case InlineRecordSource syntax of:
            // [ id:1,color:red id:2,color:blue rec ...
            if (opName.equals(ReceiverSource.LEFT_SCOPE)) {
                RecordSource inline = getInlineSource(tokStack);
                if (inline != null) {
                    operands.add(inline);
                    continue;
                }
            }

            try {
                pipe = getPipe(token, operands);
            } catch (FileNotFoundException e) {
                SyntaxError error = new SyntaxError(token, "bad path: " + token);
                throw error;
            }

            // we're a record source
            if (pipe == null) {
                continue;
            } else if (pipe instanceof MacroPipe) {
                pipe = resolveMacro((MacroPipe) pipe);
            }

            if (pipe instanceof RecordSink) {
                if (!token.isLast()) {
                    throw new SyntaxError("non-final sink");
                }

                if (operands.size() != 0) {
                    throw new SyntaxError("extra source operands on stack:" + operands);
                }
            } else { // is RecordPipe
                operands.add(pipe);
            }
        }

        // if one operand and its a source, default final sink is print to stdout
        if (operands.size() == 1) {
            pipe = new PrintSink(System.out);
            pipe.addSource(operands.pop());
        }

        if (operands.size() != 0) {
            throw new SyntaxError("extra source operands on stack:" + operands);
        }

        return (RecordSink) pipe;
    }

    /**
     * recursively resolve macros
     *
     * @param macro
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    private RecordPipe resolveMacro(MacroPipe macro) throws IOException, SyntaxError {
        RecordSink macroSink = getSink(macro, macro.getMainChunks());

        if (macroSink instanceof PrintSink) { // we ended in the default printSink, discard it.
            return (RecordPipe) macroSink.getSource();
        }

        return macroSink;
    }

    /**
     * @param token
     * @param operands
     * @return
     * @throws SyntaxError
     * @throws IOException
     */
    private RecordSource getSource(ParseToken token, ParserOperands operands) throws SyntaxError, IOException {
        String maybeOp = token.getOperator();

        // could this be a sink or pipe? (pipeOps contain both)
        PipeOperator pop = pipeOps.get(maybeOp);

        // enter this section and return null if pipe or sink
        if (pop != null) {
            Class<?> clazz = pop.getMostSpecificPredicateType();
            // no ambiguity concerning pipes/reducers/macros so if these classes return
            if (clazz == RecordPipe.class || clazz == Reducer.class || clazz == MacroPipe.class) {
                return null;
            }

            // there can be sink/source ambiguity, but must be the sink if ...
            if ((clazz == RecordSink.class || clazz == KeyedSink.class) && token.isLast() && operands.size() != 0) {
                return null;
            }
        }

        // we allow source/sink ambiguity in operator names
        SourceOperator sop = sourceOps.get(maybeOp);

        if (sop != null) {
            // non stream sources like 'devnull' 'devinf' 'else' 'groupDB'
            final OpArgs soArgs = new OpArgs(sop, token, operands);
            return LazyKeyedSource.create(sop, soArgs);
        }

        // at this point we can only be either a FormatFactory-sink or FormatFactory-source
        boolean mustBeSink = operands.size() == 1 && token.isLast();
        if (mustBeSink) return null;

        return formatFactory.createFormatSource(token, operands);
    }

    /**
     * @param operands
     * @param token
     * @return a pipe or sink or null if a source.
     * @throws IOException
     * @throws SyntaxError
     */
    private RecordPipe getPipe(ParseToken token, ParserOperands operands) throws IOException, SyntaxError {
        try {
            RecordSource operand = getSource(token, operands);

            if (operand != null) {
                // currently the decision is to not allow suppression of source reports

                if (operand instanceof NeedsSource) {
                    NeedsSource ns = (NeedsSource) operand;
                    // assume that WithInnerSources don't constitute
                    // a KeyedSource context (i.e. like JoinPipe).
                    // Therefore, we can pop the non-keyed version now
                    ns.addSource(operands.pop());
                }

                operands.add(operand);

                return null; // we're not a pipe, return null.
            }

            // get the pipe or sink
            String maybeOp = token.getOperator();
            PipeOperator pop = pipeOps.get(maybeOp);

            RecordPipe ret = null;
            if (pop != null) { // we are pipe or sink
                OpArgs args = new OpArgs(pop, token, operands);

                if (pop instanceof PipeOperatorFormatHack) {
                    // this hack should go away when S3 sink goes away via proper S3FileSystem.
                    ret = ((PipeOperatorFormatHack) pop).getAsPipe(operands, args, formatFactory);
                } else {
                    ret = pop.getAsPipe(operands, args);
                }
            }

            // are we a format based sink?
            else if (token.isLast() && operands.size() != 0) {
                ret = formatFactory.createFormatSink(token, operands);
            }

            if (ret == null) {
                throw new SyntaxError(token, "unknown operator");
            }
            // currently the decision is to not allow suppression of the sink report
            if (!(ret instanceof RecordSink)) {
                // if ends with tilde or already suppressed
                ret.suppressReport(token.endsWithTilde() | ret.isReportSuppressed());
            }

            return ret;
        } catch (SyntaxError e) {
            throw new SyntaxError(token, e.getMessage());
        }
    }

    /**
     * syntactic sugar for inline records.
     * <p>
     * rewrites: [ id:1,size:big id:2,size:large... ]
     * <p>
     * the input syntax is easier to read the individual records
     * the output syntax is easier to parse
     * <p>
     * single record: [ id:1 ]
     *
     * @param tokStack
     * @return null on syntax error else rewritten expression
     * @throws SyntaxError
     * @throws IOException
     */
    private RecordSource getInlineSource(Stack<ParseToken> tokStack) throws SyntaxError, IOException {
        Stack<ParseToken> peekStack = new Stack<>();
        while (!tokStack.isEmpty()) {
            ParseToken token = tokStack.pop();
            peekStack.add(token);

            String opName = token.getOperator();

            // an intervening [ cannot exists, if this token belongs to an inlineRecord
            if (opName.equals(ReceiverSource.LEFT_SCOPE)) {
                break;
            }

            if (opName.equals(InlineRecords.RIGHT_SCOPE)) {
                return create(peekStack);
            }
        }

        // nothing here, put them back
        while (!peekStack.isEmpty()) {
            tokStack.push(peekStack.pop());
        }

        return null;
    }

    /**
     * for inline records
     *
     * @param tokens
     * @return
     * @throws IOException
     * @throws SyntaxError
     */
    private static RecordSource create(List<ParseToken> tokens) throws SyntaxError, IOException {
        if (tokens.size() == 1) {
            throw new SyntaxError("inline record error");
        } else {
            return InlineRecords.create(tokens);
        }
    }

    /**
     * parses a string into parse tokens
     *
     * @param expression a string representing a djk expression
     * @return the parse tokens
     * @throws SyntaxError
     */
    public static List<ParseToken> parseExpression(String expression) {
        String[] chunks = ChunkTokenizer.split(expression);

        List<ParseToken> tokens = new ArrayList<>();
        for (int i = 0; i < chunks.length; i++) {
            tokens.add(new ParseToken(chunks[i], i, i == chunks.length - 1));
        }

        return tokens;
    }

    /**
     * @throws IOException
     */

    public void close() throws IOException {
        Map<String, FileSystem> systems = formatFactory.getFileSystems();
        for (FileSystem fs : systems.values()) {
            fs.close();
        }
    }
}
