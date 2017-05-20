package csd.auth.ftw;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class KMeansReducer extends Reducer<IntWritable, IntArrayWritable, IntWritable, IntArrayWritable> {
    
    public void reduce(IntWritable key, Iterable<IntArrayWritable> values, Context context) {
        // key: the center values: list of vectors
        
        // TODO find average
        
        // TODO write key: key value: avg_vector
        
    }
}
