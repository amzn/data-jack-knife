package com.amazon.djk.java;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by trung on 5/3/15.
 */
public class DynamicClassLoader extends ClassLoader {

    private Map<String, CodeObject> customCompiledCode = new HashMap<>();

    public DynamicClassLoader(ClassLoader parent) {
        super(parent);
    }

    public void setCode(CodeObject cc) {
        customCompiledCode.put(cc.getName(), cc);
    }
    
    @Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
        CodeObject cc = customCompiledCode.get(name);
        if (cc == null) {
            return super.findClass(name);
        }
        
        byte[] byteCode = cc.getByteCode();
        return defineClass(name, byteCode, 0, byteCode.length);
    }
}
