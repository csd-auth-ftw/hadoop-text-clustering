package csd.auth.ftw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, IntArrayWritable> {
    private static ArrayList<String> inputFiles = null;
    
    protected void setup(Context context) throws IOException {
    	if (inputFiles != null)
            return;
        
        inputFiles = new ArrayList<>();
        
        // read the input file list
        FileSystem hdfs = FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get(TextClustering.KEY_INPUT_DIR);
        FileStatus[] filesStatuses = hdfs.listStatus(new Path(filename));
        
        // insert to inputFiles
        for (int i=0; i<filesStatuses.length; i++) {
            FileStatus fstatus = filesStatuses[i];
            inputFiles.add(fstatus.getPath().toString());
        }
        
        Collections.sort(inputFiles);
    }
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // create the array and fill it with 0
        int[] disList = new int[inputFiles.size()];
        Arrays.fill(disList, 0);
        
        for (Text filename: values) {
            int id = getFilenameId(filename.toString());
            disList[id] = 1;
        }
        
        IntArrayWritable valueList = IntArrayWritable.createFromArray(disList);
        context.write(key, valueList);
    }
    
    private int getFilenameId(String filename) {
    	for (int i=0; i<inputFiles.size(); i++) {
    		if (inputFiles.get(i).equals(filename))
    			return i;
    	}
		
    	// TODO error
		System.exit(2);
    	
    	return -1;
    }
}
