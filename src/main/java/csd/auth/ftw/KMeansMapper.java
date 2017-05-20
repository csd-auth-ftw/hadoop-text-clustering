package csd.auth.ftw;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<Text, IntArrayWritable, IntWritable, IntWritable> {
    
    protected void map(Text key, IntArrayWritable value, Context context) throws IOException {
        IntWritable[] intWritables = value.get();
        int[] list = new int[intWritables.length];
        for (int i=0; i<list.length; i++)
            list[i] = intWritables[i].get();
        
        String word = key.toString();
        int[][] centers = getCurrentCenters(context);
        
        // TODO find least distance with a center
        
        // TODO write key:min_center value:value as is
        
    }

    private int[][] getCurrentCenters(Context context) throws IOException {
        FileSystem hdfs = FileSystem.get(context.getConfiguration());
        
        // hdfs centers.txt
        
        // if not randomize
        int k = 3;
        int n = 10;
        int[][] centers = new int[k][n];
        for (int i=0; i<k; i++) {
            for (int j=0; j<n; j++) {
//                centers[k][n] = 
            }
        }
        
        return centers;
    }
    
}
