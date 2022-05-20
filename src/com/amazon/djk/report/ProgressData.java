package com.amazon.djk.report;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.amazon.djk.record.Record;

/**
 * Progress report data collected via annotations 
 *
 */
public class ProgressData {
	private final static int NON_WRAPPING_LINE_LENGTH = 10000;
	private final List<ProgressFormat> lineFormats;
	// annotatedName --> ScalarResolver 
	private final ConcurrentHashMap<String,ScalarResolver> resolverMap;
	private final Collection<ScalarResolver> resolvers;
	private final ReportProvider provider;
	private final ProgressFormat headerFormat; 

	/**
	 * creates an up-to-date instance of progress data for given provider
	 * 
	 * @param source
	 */
	public ProgressData(ReportProvider source) {
		provider = source;
		resolverMap = ResolverMap.get(source);
		headerFormat = getHeaderFormat(source, resolverMap);
		lineFormats = getLineFormats(source, resolverMap);
		resolvers = resolverMap.size() != 0 ? resolverMap.values() : new CopyOnWriteArrayList<ScalarResolver>();
		
		set(source);
	}
	
	public String getHeader() {
		// configured not to wrap (see below)
		if (headerFormat == null) return null;
	    List<String> lines = headerFormat.resolve();
		return lines.size() > 0 ? lines.get(0) : null;
	}
	
	/**
	 * 
	 * @return the lines for use in a ProgressReport
	 */
	public List<String> getLines() {
		List<String> lines = new ArrayList<>();
		for (int i = 0; i < lineFormats.size(); i++) {
			ProgressFormat format = lineFormats.get(i);
			lines.addAll(format.resolve());
		}
		
		return lines;
	}

	/**
	 * 
	 * @param source
	 * @param resolverMap
	 * @return
	 */
    private List<ProgressFormat> getLineFormats(ReportProvider source, ConcurrentHashMap<String,ScalarResolver> resolverMap) {
        List<ProgressFormat> formats = new CopyOnWriteArrayList<>();
        try {
            ReportFormats reportFormats = source.getClass().getAnnotation(ReportFormats.class);
            String[] strings = reportFormats != null ? reportFormats.lineFormats() : new String[0];
            int maxLineLength = reportFormats != null ? reportFormats.maxLineLength() : ReportFormats.MAX_LINE_LEN;
            
            for (String string : strings) {
                formats.add(new ProgressFormat(string, resolverMap, maxLineLength));
            }
            
            // formats from subclassess
            ReportFormats2 sublines2 = source.getClass().getAnnotation(ReportFormats2.class);
            strings = sublines2 != null ? sublines2.lineFormats() : new String[0];
            maxLineLength = reportFormats != null ? reportFormats.maxLineLength() : ReportFormats.MAX_LINE_LEN;
            for (String string : strings) {
                formats.add(new ProgressFormat(string, resolverMap, maxLineLength));
            }
            
            ReportFormats3 sublines3 = source.getClass().getAnnotation(ReportFormats3.class);
            strings = sublines3 != null ? sublines3.lineFormats() : new String[0]; 
            maxLineLength = reportFormats != null ? reportFormats.maxLineLength() : ReportFormats.MAX_LINE_LEN;
            for (String string : strings) {
                formats.add(new ProgressFormat(string, resolverMap, maxLineLength));
            }
        }
        
        catch (SecurityException | IllegalArgumentException e) { }
        
        return formats;
    }

    /**
     * get the header format used to define text displayed after the name of the predicate
     * 
     * @param source
     * @param resolverMap
     * @return
     */
    private ProgressFormat getHeaderFormat(ReportProvider source, ConcurrentHashMap<String,ScalarResolver> resolverMap) {
        try {
            ReportFormats reportFormats = source.getClass().getAnnotation(ReportFormats.class);
            if (reportFormats != null) {
                String string = reportFormats.headerFormat();
                if (string.length() > 0) {
                    return new ProgressFormat(string, resolverMap, NON_WRAPPING_LINE_LENGTH);
                }
            }
            
            // try in ReportFormats2
            ReportFormats2 reportFormats2 = source.getClass().getAnnotation(ReportFormats2.class);
            if (reportFormats2 != null) {
                String string = reportFormats2.headerFormat();
                if (string.length() > 0) {
                    return new ProgressFormat(string, resolverMap, NON_WRAPPING_LINE_LENGTH);
                }
            }
        }
        
        catch (SecurityException | IllegalArgumentException e) { }
        
        return null;
    }

	/**
	 * 
	 * @param contribution to aggregate into this objects resolvers
	 */
	public void aggregate(ProgressData contribution) {
		if (contribution == null) return;
		// we need to aggregate the depending resolvers LAST so 
		// that what they depend on is already resolved.
		for (ScalarResolver resolver : resolvers) {
			if (! (resolver instanceof DependingResolver) ) {
				ScalarResolver other = contribution.resolverMap.get(resolver.getAnnotatedName());
				resolver.aggregate(other);
			}
		}
		
		for (ScalarResolver resolver : resolvers) {
			if (resolver instanceof DependingResolver) {
				ScalarResolver other = contribution.resolverMap.get(resolver.getAnnotatedName());
				resolver.aggregate(other);
			}
		}
	}

	/**
	 * 
	 * @param source
	 */
	public void set(ReportProvider source) {
		// we need to set the depending resolvers LAST so 
		// that what they depend on is already resolved.
		for (ScalarResolver resolver : resolvers) {
			if (! (resolver instanceof DependingResolver) ) {
				resolver.set(source);
			}
		}
		
		for (ScalarResolver resolver : resolvers) {
			if (resolver instanceof DependingResolver) {
				resolver.set(source);
			}
		}
	}
	
	/**
	 * gets the Progress data as a Record using the annotated names (not member names)
	 * 
	 * @return
	 * @throws IOException
	 */
	public Record getAsRecord() throws IOException {
		Record rec = new Record();

		rec.addField("name", provider.getClass().getSimpleName());

		if (headerFormat != null) {
		    headerFormat.addTo(rec);
		}
		
		for (ProgressFormat format : lineFormats) {
			format.addTo(rec);
		}
		
		return rec;
	}
}
