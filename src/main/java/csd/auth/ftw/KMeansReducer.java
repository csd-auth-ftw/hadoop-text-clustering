package csd.auth.ftw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static csd.auth.ftw.KMeansMapper.getVectorFromLine;
import static csd.auth.ftw.KMeansMapper.getWordFromLine;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
    	Text[] valuesArr = iterableToArray(values);
    	
    	System.out.println("valuesArr:");
    	for (int o=0; o<valuesArr.length; o++)
    		System.out.println(valuesArr[o].toString());
    	
        if (context.getJobName().equals(TextClustering.KMEANS_LAST_JOB_NAME)) { // CHECK NAME FROM CONTEXT
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
        } else if (context.getJobName().equals(TextClustering.KMEANS_JOB_NAME)) {
            int[] average = getAverage(valuesArr);

            context.write(key, IntArrayWritable.arrayToText(average));
        } else {
            System.err.println("Wrong job type.");
            // maybe throw some exception here?
        }
    }

    private Text[] iterableToArray(Iterable<Text> values) {
		ArrayList<String> tmp = new ArrayList<>();
		for (Text value: values)
			tmp.add(value.toString());
		
		System.out.println("iter_size:" + tmp.size());
		
//		return (Text[]) tmp.toArray(); throws error
		
		Text[] arr = new Text[tmp.size()];
		for (int i=0; i<tmp.size(); i++)
			arr[i] = new Text(tmp.get(i));
		
		return arr;
	}

	private int[] getAverage(Text[] values){
        //getting the number of vectors in the Iterable
        int vectorNum = values.length;
        
        System.out.println("vectorNum:" + vectorNum);
        
        //getting the length of the vectors
        System.out.println("values[0]=" + values[0].toString());
        int vectorLen = getVectorFromLine(values[0].toString(), false).length;
        int[] averageVector = new int[vectorLen];
        
        for (Text textVector : values) {
            int[] intVector = getVectorFromLine(textVector.toString(), false);
            for (int i = 0; i < vectorLen; i++) {
                averageVector[i] += intVector[i];
            }
        }
        
        double tempAverage;
        for (int i = 0; i < vectorLen; i++){
            tempAverage = (double) averageVector[i] / (double) vectorNum;
            averageVector[i] = (tempAverage >= 0.5) ? 1 : 0;
        }
        
        return averageVector;
    }

    //small function for finding the number of elements in an Iterable
    public static int getIterableSize(Iterator iterator){
        int i = 0;
        while (iterator.hasNext()) {
            i++;
            iterator.next();
        }
        
        return i;
    }
}
