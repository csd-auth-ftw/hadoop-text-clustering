package csd.auth.ftw;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class IntArrayWritable extends ArrayWritable {
	public static final String SEPARATOR = " ";
	
	public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }
	
	@Override
    public IntWritable[] get() {
        return (IntWritable[]) super.get();
    }
	
	@Override
    public String toString() {
        IntWritable[] values = get();
        StringBuilder sb = new StringBuilder("[");
        for (IntWritable num: values) {
        	sb.append(num + SEPARATOR);
        }
        
        sb.deleteCharAt(sb.length() - 1).append("]");
        return sb.toString();
    }
	
	public static Text arrayToText(int[] arr) {
	    IntArrayWritable iaw = IntArrayWritable.createFromArray(arr);
	    return new Text(iaw.toString());
	}

	public static IntArrayWritable createFromArray(int[] disList) {
		IntWritable[] values = new IntWritable[disList.length];
		for (int i=0; i<disList.length; i++)
			values[i] = new IntWritable(disList[i]);
		
		return new IntArrayWritable(values);
	}
}
