package com.amazon.djk.java;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 
 *
 */
public class InMemoryJavaCompiler {
    static JavaCompiler javac = ToolProvider.getSystemJavaCompiler();

    public static Class<?> compile(String className, String sourceCodeInText) throws Exception {
        SourceCode sourceCode = new SourceCode(className, sourceCodeInText);
        CodeObject compiledCode = new CodeObject(className);
        Iterable<? extends JavaFileObject> compilationUnits = Arrays.asList(sourceCode);
        
        ClassLoader threadLoader = Thread.currentThread().getContextClassLoader();
        DynamicClassLoader dcl = new DynamicClassLoader(threadLoader);
        
        // http://stackoverflow.com/questions/1563909/how-to-set-classpath-when-i-use-javax-tools-javacompiler-compile-the-source
        List<String> options = new ArrayList<String>();
        // set compiler's classpath to be same as the runtime's
        
        // if djk compiler classpath provided, append it
        String djkClasspath = System.getProperty("djk.compiler.class.path");
        if (djkClasspath != null) {
            String classpath = System.getProperty("java.class.path") + ":" + djkClasspath;
            options.addAll(Arrays.asList("-classpath", classpath));
        }
        
        // for error reporting
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        
        ExtendedJavaFileManager fileManager = new ExtendedJavaFileManager(javac.getStandardFileManager(null, null, null), compiledCode, dcl);
        JavaCompiler.CompilationTask task = javac.getTask(printWriter, fileManager, null, options, null, compilationUnits);
        if (task.call()) {
            Class<?> retClazz = dcl.loadClass(className);
            return retClazz;
        } 
        
        else {
            String errorOutput = stringWriter.getBuffer().toString();
            throw new Exception("unable to compile: " + errorOutput + "\ndynamic code=\n"+sourceCodeInText);
        }
    }
}
