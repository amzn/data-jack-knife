package com.amazon.djk.manual;

import static com.amazon.djk.expression.Param.REQUIRED_FROM_COMMANDLINE;
import static com.amazon.djk.expression.Param.UNDEFINED_DEFAULT;

import java.util.List;

import org.apache.commons.lang.StringEscapeUtils;

import com.amazon.djk.expression.Arg;
import com.amazon.djk.expression.Operator;
import com.amazon.djk.expression.Param;
import com.amazon.djk.processor.KnifeProperties;
import com.google.common.base.Strings;

public class GlossesSection {
	private final Manual manual;

	public GlossesSection(Manual manual) {
		this.manual = manual;
	}
	/**
	 * 
	 * @param op
	 */
	private void displayArguments(Operator op) {
	    List<String> argNames = op.getArgNames();
	    List<Gloss> glosses = op.getGlosses();

	    if (argNames.isEmpty() && glosses.isEmpty()) return;
	    
		manual.addLine("where:", manual.lightblue());
		manual.addLine();

        for (String name : argNames) {
        	Arg arg = op.getArg(name);
            manual.addLine(name + " = " + arg.gloss() + " (type=" + arg.type() + ")");		    
		}
        
        for (Gloss gloss : glosses) {
            manual.addLine(gloss.entry() + " = " + gloss.def());            
        }
		
		manual.addLine();
	}
	
	/**
	 * 
	 * @param op
	 */
	private void displayOperatorParams(Operator op) {
	    List<String> names = op.getParamNames();
	    if (names.size() == 1) return;
        manual.addLine("parameters:", manual.lightblue());
        manual.addLine();
        for (String name : names) {
        	Param param = op.getParam(name);
        	String paramDescription = String.format("%s = %s. %s", name, param.defaultValue().equals(REQUIRED_FROM_COMMANDLINE) ? "REQUIRED" : "OPTIONAL", param.gloss());

        	String defaultValue = param.defaultValue();
        	if (!defaultValue.isEmpty() && !defaultValue.equals(UNDEFINED_DEFAULT) && !defaultValue.equals(REQUIRED_FROM_COMMANDLINE)){
        		String defaultValueDesc = StringEscapeUtils.escapeJava(defaultValue);

        		if (KnifeProperties.isSystemProperty(defaultValue)) {
					defaultValueDesc = String.format("System property '%s' if set", KnifeProperties.getPropertyName(defaultValue));

					if (!Strings.isNullOrEmpty(KnifeProperties.getDefaultValue(defaultValue))) {
						defaultValueDesc += String.format(", else %s", KnifeProperties.getDefaultValue(defaultValue));
					}
				}

				paramDescription += String.format(" DEFAULT=%s", defaultValueDesc);
			}
			paramDescription += String.format(" (type=%s)", param.type());
			manual.addLine(paramDescription);
        }
        manual.addLine();
	}

	/**
	 * This method should become a method on Operator: op.getUsageExample();
	 * 
	 * @param op
	 */
	private void displayUsageExample(Operator op) {
	    manual.addString("e.g. ", manual.lightblue());
        manual.addLine(op.getUsageExample());
        manual.addLine();
	}
	
	/**
	 * 
	 * @param op
	 */
	public void display(Operator op) {
		displayArguments(op);
		displayOperatorParams(op);
		displayUsageExample(op);
	}
}
