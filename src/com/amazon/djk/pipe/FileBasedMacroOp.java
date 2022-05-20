package com.amazon.djk.pipe;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.ArgType;
import com.amazon.djk.expression.MacroOperator;
import com.amazon.djk.expression.OpArgs;
import com.amazon.djk.expression.SyntaxError;
import com.amazon.djk.manual.Description;
import com.amazon.djk.manual.Example;
import com.amazon.djk.manual.ExampleType;

@Description(text={"Creates a macro pipe from a text file.  Ignores lines beginning with #.",
        "Lines are automatically wrapped (i.e. no need for \\ at the end of lines like bash)",
        "?'s within the macro expression will be instantiated with operands on the stack."},
        preLines={ "if 'mymacro.djk' contains the line: '? join:left'",
                   "",
                   "djk [ foo:bar,bob:cob ] [ foo:bar,num:38 ] map:foo macro:mymacro.djk",
                   "foo:bar",
                   "bob:cob",
                   "group",
                   "\tnum:38",
                   "#"
})
@Arg(name="PATH", gloss="path to the file containing the macro expression", type=ArgType.STRING, eg="/tmp/mymacro.txt")
@Example(expr = "djk [ foo:bar,bob:cob ] [ foo:bar,num:38 ] group:foo macro:mymacro.djk", type=ExampleType.DISPLAY_ONLY)
public class FileBasedMacroOp extends MacroOperator {
    public final static String MACRO = "macro";
	
    public FileBasedMacroOp() {
    	super(MACRO + ":PATH");
    }
    
    @Override
    public List<String> getRawMacroLines(OpArgs args) throws IOException, SyntaxError {
        String path = (String)args.getArg("PATH");
        File file = new File(path);        
        return getRawMacroLines(file);
    }
    
    public static List<String> getRawMacroLines(String path) throws SyntaxError, IOException {
        File file = new File(path);        
        return getRawMacroLines(file);
    }
    
    public static List<String> getRawMacroLines(File file) throws SyntaxError, IOException {
        if (!file.exists()) {
            throw new SyntaxError(file.getAbsolutePath() + " does not exist");
        }

        return FileUtils.readLines(file, "UTF-8");
    }
}
