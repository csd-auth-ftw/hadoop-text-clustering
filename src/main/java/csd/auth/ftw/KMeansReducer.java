package csd.auth.ftw;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import static csd.auth.ftw.IntArrayWritable.arrayToString;
import static csd.auth.ftw.KMeansMapper.getVectorFromLine;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
        if (context.getJobName().equals(TextClustering.KMEANS_LAST_JOB_NAME)) { // CHECK NAME FROM CONTEXT
            // TODO extract words from values and write them
        	//TODO Iterate Values extarct words put them on a String list and write them out.
        	
        	String tempString;
        	String[] words = new String[getIterableSize(values.iterator())];
        	int i =0;
               		  
        		   for(Text temp : values)
        		   {   
        			   tempString = temp.toString();
        			   tempString = getWordFromLine(tempString);
        			   words[i] = tempString;		  
        			 i++;         			  
        		   }
        		   String allWords  = String.join(" ", words);
            	
            context.write(key, new Text(allWords));
            // WRITE THE CENTER ID AND THE LIST OF WORDS.	
        } else if (context.getJobName().equals(TextClustering.KMEANS_JOB_NAME)) {
            // TODO extract vectors and get average
            // find average
            Text average = getAverage(values);
            
            // TODO
//            IntArrayWritable.arrayToString(arr);
            context.write(key, average);
        } else {
            System.err.println("Wrong job type.");
            // maybe throw some exception here?
        }
    }
    
    public static String getWordFromLine(String line) {
        String regx = "([a-zA-Z][a-zA-Z0-9]+) \\[(.*)\\]";
        Pattern pattern = Pattern.compile(regx);
        Matcher matcher = pattern.matcher(line.trim());
        
        if (matcher.find())
            return matcher.group(1);
        
        return null;
    }


    private Text getAverage(Iterable<Text> values){
        //getting the number of vectors in the Iterable
        int vectorNum = getIterableSize(values.iterator());
        //getting the length of the vectors
        int vectorLen = getVectorFromLine(values.iterator().next().toString(), true).length;
        int[] averageVector = new int[vectorLen];
        for (Text textVector : values) {
            int[] intVector = getVectorFromLine(textVector.toString(), true);
            for (int i = 0; i < vectorLen; i++) {
                averageVector[i] += intVector[i];
            }
        }
        float tempAverage;
        for (int i = 0; i < vectorLen; i++){
            tempAverage = (float) averageVector[i]/vectorNum;
            averageVector[i] = (tempAverage >= 0.5) ? 1 : 0;
        }
        return new Text(arrayToString(averageVector));
    }

    //small function for finding the number of elements in an Iterable
    public static int getIterableSize(Iterator iterator){
        int i = 0;
        while(iterator.hasNext()) {
            i++;
            iterator.next();
        }
        return i;
    }
}
