package csd.auth.ftw;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<Text, IntArrayWritable, Text, IntWritable> {
    
    protected void map(Text key, IntArrayWritable value, Context context) throws IOException {
        IntWritable[] intWritables = value.get();
        int[] list = new int[intWritables.length];
        for (int i=0; i<list.length; i++)
            list[i] = intWritables[i].get();
        
        int[][] centers = getCurrentCenters(context);
        
        
    }

    private int[][] getCurrentCenters(Context context) throws IOException {
        FileSystem hdfs = FileSystem.get(context.getConfiguration());
        return null;
    }
    
}
