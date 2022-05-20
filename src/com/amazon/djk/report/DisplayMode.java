package com.amazon.djk.report;

public enum DisplayMode {
    SHOW("show"), 
    NO_SHOW("noShow"),
    SHOW_ONCE("showOnce"), // at the end
    NO_SHOW_IF_FAST("noShowIfFast"),
    FORCE_SHOW("forceShow");

    private final String name;
    private DisplayMode(String s) {
        name = s;
    }
    
    @Override
    public String toString() {
        return name;
    }
    
    public static DisplayMode getMode(String s) {
        if (s == null) return SHOW;
        
        for (DisplayMode m : DisplayMode.values()) {
            if (s.equals(m.toString())) {
                return m;
            }
        }
        
        return SHOW; // when in doubt
    }
}
