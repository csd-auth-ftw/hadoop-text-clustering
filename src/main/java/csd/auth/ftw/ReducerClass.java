package csd.auth.ftw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, Text, Text, Text> {
    private ArrayList<String> inputFiles = null;
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException {
        resolveInputFiles(context);
        
        int[] disList = new int[inputFiles.size()];
        Arrays.fill(disList, 0);
        
        for (Text filename: values) {
            int id = getFilenameId(filename);
            disList[id] = 1;
        }
        
        // TODO write distlist
//        context.write(key, arg1);
    }
    
    private void resolveInputFiles(Context context) throws IOException {
        if (inputFiles != null)
            return;
        
        inputFiles = new ArrayList<>();
        
        FileSystem fs = FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("map.input.dir");
        FileStatus[] filesStatuses = fs.listStatus(new Path(filename));
        
        for (int i=0; i<filesStatuses.length; i++) {
            FileStatus status = filesStatuses[i];
            inputFiles.add(status.getPath().toString());
        }
    }
    
    private int getFilenameId(Text filename) {
        return inputFiles.indexOf(filename.toString());
    }
}
