package com.amazon.djk.report;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResolverMap {
	public static ConcurrentHashMap<String,ScalarResolver> get(ReportProvider source) {
		ConcurrentHashMap<String,ScalarResolver> resolvers = new ConcurrentHashMap<>();

		// date  
		ScalarResolver r = new DateResolver();
		resolvers.put(r.getAnnotatedName(), r);
		// elapsed

		try {
			// first non-depending resolvers
			List<Field> fields = getFieldsIncludingInherited(source);
			for (Field field : fields) {
				if (field.isAnnotationPresent(ScalarProgress.class)) {
					ScalarProgress progress = field.getAnnotation(ScalarProgress.class);
					r = new ScalarResolver(field, 
							progress.name(),
							progress.aggregate(),
							progress.multiplier());
					resolvers.put(r.getAnnotatedName(), r);
				}
				
				if (field.isAnnotationPresent(RateProgress.class)) {
					RateProgress progress = field.getAnnotation(RateProgress.class);
					r = new RateResolver(field, 
							progress.name(),
							progress.aggregate(),
							progress.multiplier());
					resolvers.put(r.getAnnotatedName(), r);
				}
			}

			// now depending ones
			for (Field field : fields) {
				if (field.isAnnotationPresent(PercentProgress.class)) {
					PercentProgress progress = field.getAnnotation(PercentProgress.class);
					r = new PercentResolver(field,
							progress.name(),
							progress.aggregate(),
							progress.multiplier(),							
							getDependentResolver(progress.denominatorAnnotation(), resolvers));
					resolvers.put(r.getAnnotatedName(), r);
				}
				
				if (field.isAnnotationPresent(SelectProgress.class)) {
					SelectProgress progress = field.getAnnotation(SelectProgress.class);
					r = new SelectResolver(field,
							progress.name(),
							progress.aggregate(), 
							progress.multiplier(),							
							getDependentResolver(progress.otherAnnotation(), resolvers),
							progress.choice());
					resolvers.put(r.getAnnotatedName(), r);
				}
			}
		}
		
		catch (SecurityException | IllegalArgumentException e) { }
		
		return resolvers;
	}
	
	/**
	 * 
	 * @param annotatedName
	 * @param map
	 * @return
	 */
	private static ScalarResolver getDependentResolver(String annotatedName, Map<String,ScalarResolver> map) {
		ScalarResolver dependentResolver = map.get(annotatedName);

		if (dependentResolver == null) {
			throw new RuntimeException("missing annotated denominator");
		}
			
		if (dependentResolver instanceof DependingResolver) {
			throw new RuntimeException("a dependent resolver must be scalar");
		}
		
		return dependentResolver;
	}
	
	/**
	 * 
	 * @param provider
	 * @return
	 */
	private static List<Field> getFieldsIncludingInherited(ReportProvider provider) {
		Class<?> clazz = provider.getClass();
		List<Field> fields = new ArrayList<>();
		while (clazz != null) {
			Field[] f = clazz.getDeclaredFields();
			fields.addAll(Arrays.asList(f));
			clazz = clazz.getSuperclass();
		}
		
		return fields;
	}
}
