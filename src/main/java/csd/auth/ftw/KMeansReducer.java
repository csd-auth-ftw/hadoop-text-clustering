package csd.auth.ftw;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static csd.auth.ftw.KMeansMapper.getVectorFromLine;
import static csd.auth.ftw.KMeansMapper.getWordFromLine;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
    	Text[] valuesArr = iterableToArray(values);
    	
    	String jobName = context.getJobName();
    	
        if (jobName.equals(TextClustering.KMEANS_LAST_JOB_NAME)) {
            String[] words = new String[valuesArr.length];
            int i =0;

            // extract all words
            for (Text temp : valuesArr) {   
                words[i] = getWordFromLine(temp.toString()); 
                i++;
            }

            // join words
            String allWords  = String.join(" ", words);

            context.write(key, new Text(allWords));
        } else if (jobName.equals(TextClustering.KMEANS_JOB_NAME)) {
            int[] average = getAverage(valuesArr);

            context.write(key, IntArrayWritable.arrayToText(average));
        } else {
            System.err.println("Wrong job type.");
            System.exit(1);
        }
    }
    
    /**
     * Converts an iterable to an array
     */
    private Text[] iterableToArray(Iterable<Text> values) {
		ArrayList<String> tmp = new ArrayList<>();
		for (Text value: values)
			tmp.add(value.toString());
		
		Text[] arr = new Text[tmp.size()];
		for (int i=0; i<tmp.size(); i++)
			arr[i] = new Text(tmp.get(i));
		
		return arr;
	}

	private int[] getAverage(Text[] values){        
        // gets the length of each vector
        int vectorLen = getVectorFromLine(values[0].toString(), false).length;
        int[] averageVector = new int[vectorLen];
        int max = 0;
        
        for (Text textVector : values) {
            int[] intVector = getVectorFromLine(textVector.toString(), false);
            for (int i = 0; i < vectorLen; i++) {
                averageVector[i] += intVector[i];
                
                if (averageVector[i] > max)
                	max = averageVector[i];
            }
        }
        
        // calculate average
        double tempAverage;
        for (int i = 0; i < vectorLen; i++){
        	// divide with max not vectors num to avoid everything to 
        	// turn to 0 because of the sparse vectors
        	tempAverage = (double) averageVector[i] / (double) max; 
            averageVector[i] = (tempAverage >= 0.5) ? 1 : 0;
        }
        
        return averageVector;
    }
}
