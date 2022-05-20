package com.amazon.djk.processor;

import java.io.IOException;

import com.amazon.djk.chartjs.HTMLChartSink;
import com.amazon.djk.concurrent.UniquePipe;
import com.amazon.djk.core.DenormalizePipe;
import com.amazon.djk.expression.FieldDeclaration;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.format.JsonFormatParser;
import com.amazon.djk.format.JsonFormatWriter;
import com.amazon.djk.format.LineFormatParser;
import com.amazon.djk.format.LineFormatWriter;
import com.amazon.djk.format.NV2FormatParser;
import com.amazon.djk.format.NV2FormatWriter;
import com.amazon.djk.format.NativeFormatParser;
import com.amazon.djk.format.NativeFormatWriter;
import com.amazon.djk.format.TSVFormatParser;
import com.amazon.djk.format.TSVFormatWriter;
import com.amazon.djk.legacy.DJFFileParser;
import com.amazon.djk.legacy.NVPFormatParser;
import com.amazon.djk.natdb.MemDBKeyedSource;
import com.amazon.djk.natdb.NativeDBKeyedSource;
import com.amazon.djk.natdb.NativeDBSink;
import com.amazon.djk.pipe.AcceptIf;
import com.amazon.djk.pipe.AddFieldsPipe;
import com.amazon.djk.pipe.AddRegexFieldsPipe;
import com.amazon.djk.pipe.BinValuePipe;
import com.amazon.djk.pipe.CatPipe;
import com.amazon.djk.pipe.FileBasedMacroOp;
import com.amazon.djk.pipe.FilterPipe;
import com.amazon.djk.pipe.FlattenPipe;
import com.amazon.djk.pipe.ForeachPipe;
import com.amazon.djk.pipe.HashPipe;
import com.amazon.djk.pipe.HeadPipe;
import com.amazon.djk.pipe.IfNotPipe;
import com.amazon.djk.pipe.IfPipe;
import com.amazon.djk.pipe.InjectPipe;
import com.amazon.djk.pipe.JoinPipe;
import com.amazon.djk.pipe.KeepFieldsPipe;
import com.amazon.djk.pipe.MergeFieldPipe;
import com.amazon.djk.pipe.MoveFieldsPipe;
import com.amazon.djk.pipe.NoOpPipe;
import com.amazon.djk.pipe.RandomSamplePipe;
import com.amazon.djk.pipe.RejectIf;
import com.amazon.djk.pipe.RemoveFieldsPipe;
import com.amazon.djk.pipe.RestPipe;
import com.amazon.djk.pipe.TailPipe;
import com.amazon.djk.pipe.TextCountToksPipe;
import com.amazon.djk.pipe.TextMatchPipe;
import com.amazon.djk.pipe.TextSplitPipe;
import com.amazon.djk.pipe.ThrottlePipe;
import com.amazon.djk.record.Value;
import com.amazon.djk.reducer.BooleanReducer;
import com.amazon.djk.reducer.RecordCountReducer;
import com.amazon.djk.reducer.SumReducer;
import com.amazon.djk.reducer.TextCatReducer;
import com.amazon.djk.sink.DevnullSink;
import com.amazon.djk.sink.PrintSink;
import com.amazon.djk.sort.SortPipe;
import com.amazon.djk.source.BlanksSource;
import com.amazon.djk.source.ElseReceiver;
import com.amazon.djk.source.EmptyKeyedSource;
import com.amazon.djk.source.FormatOriginSource;
import com.amazon.djk.source.InlineRecords;
import com.amazon.djk.source.ReceiverSource;
import com.amazon.djk.stats.DivergenceReducer;
import com.amazon.djk.stats.StatsOfReducer;

public class DataJackKnife extends JackKnife {

    public DataJackKnife() throws DJKInitializationException {
    	// add core operators
        registerOp(AddFieldsPipe.Op.class);
        registerOp(DenormalizePipe.Op.class);
        registerOp(DevnullSink.Op.class);
        registerOp(ForeachPipe.Op.class);
        registerOp(FlattenPipe.Op.class);
        registerOp(InjectPipe.Op.class);
        registerOp(HeadPipe.Op.class);
        registerOp(NoOpPipe.Op.class);
        registerOp(IfPipe.Op.class);
        registerOp(IfNotPipe.Op.class);
        registerOp(NativeDBSink.GroupOp.class);
        registerOp(NativeDBSink.MapOp.class);
        registerOp(TailPipe.Op.class);
        registerOp(JoinPipe.Op.class);
        registerOp(FilterPipe.Op.class);
        registerOp(FileBasedMacroOp.class);
        
        
        registerOp(ReceiverSource.Op.class);
        registerOp(InlineRecords.Op.class);
        registerOp(ElseReceiver.Op.class);
        registerOp(EmptyKeyedSource.Op.class);
        registerOp(BlanksSource.Op.class);
        registerOp(NativeDBKeyedSource.Op.class);
        registerOp(MemDBKeyedSource.MapOp.class);
        registerOp(MemDBKeyedSource.GroupOp.class);
        registerOp(FormatOriginSource.Op.class);
        
        // formats
        registerOp(NV2FormatWriter.Op.class);
        registerOp(NV2FormatParser.Op.class);
        registerOp(JsonFormatWriter.Op.class);
        registerOp(JsonFormatParser.Op.class);
        registerOp(NativeFormatParser.Op.class);
        registerOp(NativeFormatWriter.Op.class);

        // reducers
        registerOp(RecordCountReducer.Op.class);

        registerManPage(Value.Entry.class);
        registerManPage(KnifeProperties.Entry.class);
        registerManPage(FieldDeclaration.Entry.class);
        // example internal macro see:com.amazon.djk.processor
        //registerInternalMacro("helloworld");
    	
        // pipes/sinks
        registerOp(BinValuePipe.Op.class);
        registerOp(CatPipe.Op.class);
        registerOp(DivergenceReducer.Op.class);
        registerOp(HashPipe.Op.class);
        registerOp(RejectIf.Op.class);
        registerOp(AcceptIf.Op.class);
        registerOp(KeepFieldsPipe.Op.class);
        registerOp(RandomSamplePipe.Op.class);
        registerOp(AddRegexFieldsPipe.Op.class);
        registerOp(RemoveFieldsPipe.Op.class);
        registerOp(SortPipe.Op.class);
        registerOp(UniquePipe.Op.class);
        registerOp(MoveFieldsPipe.Op.class);
        registerOp(MergeFieldPipe.Op.class);
        registerOp(SumReducer.Op.class);
        registerOp(BooleanReducer.OrOp.class);
        registerOp(BooleanReducer.AndOp.class);
        registerOp(TextCatReducer.Op.class);
        registerOp(TextSplitPipe.Op.class);
        registerOp(TextCountToksPipe.Op.class);
        registerOp(TextMatchPipe.Op.class);
        registerOp(RestPipe.Op.class);
        registerOp(StatsOfReducer.Op.class);
        registerOp(ThrottlePipe.Op.class);

        // sources/formats
        registerOp(NVPFormatParser.Op.class);
        registerOp(DJFFileParser.Op.class);
        registerOp(TSVFormatParser.Op.class);
        registerOp(TSVFormatWriter.Op.class);
        registerOp(LineFormatParser.Op.class);
        registerOp(LineFormatWriter.Op.class);
        
        // sink
        registerOp(HTMLChartSink.Op.class);
        registerOp(PrintSink.Op.class);
    }
    
    public static void main(String[] args) throws IOException, SyntaxError {
        DataJackKnife djk = new DataJackKnife();

        if (args.length == 0) {
            djk.printUsage();
            System.exit(0);
        }
        
        djk.executeMain(args);
    }
}