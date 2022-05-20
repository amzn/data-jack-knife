package com.amazon.djk.reducer;

import java.io.IOException;

import com.amazon.djk.record.Record;

public class PathReducer extends Reducer {
    private final Record outrec = new Record();
    private final String absolutePath;
    /**
     * base constructor
     * 
     * @param outField
     * @param counter
     * @throws IOException
     */
    private PathReducer(Reducer root, String absolutePath, String instanceName) throws IOException {
        super(root, instanceName, Type.MAIN_ONLY);
        this.absolutePath = absolutePath;
    }

    /**
     * constructor for:
     * 
     * getAsPipe mainExpression
     * subReplicate subExpression
     * 
     * @param outField
     * @throws IOException
     */
    public PathReducer(String absolutePath, String instanceName) throws IOException {
        this(null, absolutePath, instanceName);
    }
    
    @Override
    public Object replicate() throws IOException {
        return new PathReducer(this, absolutePath, getInstanceName());
    }
    
    @Override
    public boolean reset() {
        outrec.reset();
    	return true;
    }

	@Override
	public Record getChildReduction() throws IOException {
		outrec.addField("path", absolutePath);
		return outrec;
	}
}
