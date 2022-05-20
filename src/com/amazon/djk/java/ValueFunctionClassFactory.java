package com.amazon.djk.java;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazon.djk.processor.CoreDefs;

public class ValueFunctionClassFactory {
    // allow dots in names, include the paren so we can disambiguate methods on strings
	// these are positive-lookbehinds: http://www.regular-expressions.info/lookaround.html
    private final static String sysPropPatternString = "(?<=^|[^a-zA-Z0-9_])P\\.(?<name>[\\w.\\(]+)"; 
    private final static String fieldPatternString = "(?<=^|[^a-zA-Z0-9_])(?<type>[PRldbsr])\\.(?<name>\\w+)"; // no dots
    
    public static Class<?> getClass(String classCode) throws Exception {
        Pattern classNamePat = Pattern.compile("public class (?<name>[^\\s]+) ");
        Matcher m = classNamePat.matcher(classCode);
        if (!m.find()) return null;
        
        String className = m.group("name");
        Class<?> valueFunctionClass = InMemoryJavaCompiler.compile("com.amazon.djk.java." + className, classCode);
        return valueFunctionClass;
    }
    
    public static String getCode(String djkJavaRightHandSide) throws Exception {
        Pattern sysPropPat = Pattern.compile(sysPropPatternString);
        StringBuilder code = new StringBuilder();

        // get a random number                                                                                                         
        int number = Math.abs(new Random().nextInt());
        String className = "VC" + number;
        code.append("package com.amazon.djk.java;\n");
        code.append("import com.amazon.djk.java.ValueFunction;\n");
        code.append("import com.amazon.djk.record.*;\n");
        code.append("import java.util.*;\n");
        code.append("import java.util.concurrent.*;\n");
        code.append("import java.util.stream.*;\n");
        code.append("import java.io.IOException;\n");
        code.append("public class " + className + " extends ValueFunction {\n");
        
        Matcher m = sysPropPat.matcher(djkJavaRightHandSide);
        List<String> sysPropDotNames = new ArrayList<>();
        while (m.find()) {
            String dotName = m.group("name");
            if (dotName.endsWith("(")) { // the last dot represents the dot before a method
                int dot = dotName.lastIndexOf('.');
                if (dot == -1) throw new Exception("improper syntax");
                dotName = dotName.substring(0, dot);
            }

            // declare the final only once
            if (!sysPropDotNames.contains(dotName)) {
                String prop = CoreDefs.get().getProperty(dotName);
                String noDotName = dotName.replace('.', '_');
                code.append(String.format("   private final String %s = \"%s\"; // System Property\n", noDotName, prop));
            }
            
            sysPropDotNames.add(dotName);            
        }
        
        code.append("   public Object get(Record rec) throws IOException {\n");

        interpret(djkJavaRightHandSide, sysPropDotNames, code);

        code.append("   }\n");
        code.append("}\n");

        return code.toString();
    }
    
    private static void interpret(String djkJavaRhs, List<String> sysPropDotNames, StringBuilder out) {
        // replace system properties first which allow dots in names
        for (String sysPropDotName : sysPropDotNames) {
            String noDotName = sysPropDotName.replace('.', '_');
        	// positive-lookbehinds: http://www.regular-expressions.info/lookaround.html
            String propNameRegex = "(?<=^|[^a-zA-Z0-9_])P\\." + sysPropDotName.replaceAll("\\.", "\\.");
            djkJavaRhs = djkJavaRhs.replaceAll(propNameRegex, noDotName);
        }
        
        // next do other fields which DONT allow dots (because of methods on String and Record)
        Pattern fieldPat = Pattern.compile(fieldPatternString);
        Matcher m = fieldPat.matcher(djkJavaRhs);
        StringBuffer realJavaRhs = new StringBuffer();
        Map<String,String> fields = new HashMap<>();
        while (m.find()) {
            String type = m.group("type");
            String name = m.group("name");

            // R stands for current Record and is a special case where name = method on Record
            if (type.equals("R")) {
                m.appendReplacement(realJavaRhs, "rec." + name);
            }
            
            else {
                m.appendReplacement(realJavaRhs, name);
                fields.put(name, type);
            }
        }
        m.appendTail(realJavaRhs);
        realJavaRhs.append("\n");
        
        // add declarations to the output
        for (Map.Entry<String,String> entry : fields.entrySet()) {
            String type;
            switch (entry.getValue()) { // type
                case "s": type = "String"; break;
                case "l": type = "Long"; break;
                case "d": type = "Double"; break;
                case "b": type = "Boolean"; break;
                case "r": type = "Record"; break; // for sub records
                default: type = "Unknown";
            }

            String name = entry.getKey(); // name     
            out.append(String.format("      %s %s = rec.getFirstAs%s(\"%s\");\n", type, name, type, name));
        }

        out.append("      return "); // return statement
        out.append(realJavaRhs);
    }
}
