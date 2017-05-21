package csd.auth.ftw;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    public void reduce(IntWritable key, Iterable<javax.xml.soap.Text> values, Context context) throws InterruptedException, IOException {
        if (context.getJobName().equals(TextClustering.KMEANS_LAST_JOB_NAME)) { // CHECK NAME FROM CONTEXT
            // TODO extract words from values and write them
        	//TODO Iterate Values extarct words put them on a String list and write them out.
        	
        	String tempString;
        	String[] words = new String[getIteratableSize(values.iterator())];
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
        } else {
            // TODO extract vectors and get average
            // find average
            IntArrayWritable average = getAverage(values);
            
            // TODO
//            IntArrayWritable.arrayToString(arr);
            context.write(key, average);
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


    private IntArrayWritable getAverage(Iterable<Text> values){
        //getting the number of vectors in the Iterable
        int vectorNum = getIterableSize(values.iterator());
        //getting the length of the vectors
        //Ξ―ΟƒΟ‰Ο‚ Ξ­Ο„ΟƒΞΉ Ο‡Ξ¬Ξ½ΞµΟ„Ξ±ΞΉ Ο„ΞΏ Ο€Ο�Ο�Ο„ΞΏ ΟƒΟ„ΞΏΞΉΟ‡ΞµΞ―ΞΏ, Ξ±Ξ½ Ξ· ΞΊΞ»Ξ®ΟƒΞ· Ο„Ξ·Ο‚ .iterator() Ξ΄ΞµΞ½ Ξ΄Ξ―Ξ½ΞµΞΉ ΞΊΞ±ΞΉΞ½ΞΏΟ�Ο�Ξ³ΞΉΞΏ iterator ΞΊΞ¬ΞΈΞµ Ο†ΞΏΟ�Ξ¬ (same Ξ³ΞΉΞ± Ο„Ξ·Ξ½ getIterableSize())
        int vectorLen = values.iterator().next().get().length;
        int[] averageVector = new int[vectorLen];
        for (IntArrayWritable vector : values) {
            IntWritable[] workingVector = vector.get();
            for (int i = 0; i < vectorLen; i++) {
                averageVector[i] += workingVector[i].get();
            }
        }
        float tempAverage;
        for (int i = 0; i < vectorLen; i++){
            tempAverage = (float) averageVector[i]/vectorNum;
            //Ξ±Ξ½Ξ¬ΞΈΞµΟƒΞ· ΞΌΞ­ΟƒΞ·Ο‚ Ο„ΞΉΞΌΞ®Ο‚ Ξ³ΞΉΞ± ΞΊΞ¬ΞΈΞµ Ξ΄ΞΉΞ¬ΟƒΟ„Ξ±ΟƒΞ· Ο„ΞΏΟ… ΞΌΞ­ΟƒΞΏΟ… vector (1 Ξ±Ξ½ ΞµΞ―Ξ½Ξ±ΞΉ Ο€Ξ¬Ξ½Ο‰ Ξ±Ο€Ο� 0.5, 0 Ξ±Ξ½ ΞµΞ―Ξ½Ξ±ΞΉ ΞΌΞΉΞΊΟ�Ο�Ο„ΞµΟ�ΞΏ)
            averageVector[i] = (tempAverage >= 0.5) ? 1 : 0;
        }
        return IntArrayWritable.createFromArray(averageVector);
    }

    //Ξ‘Ο€Ξ»Ξ® ΞΌΞ­ΞΈΞΏΞ΄ΞΏΟ‚ Ξ³ΞΉΞ± ΞµΟ�Ο�ΞµΟƒΞ· Ο„ΞΏΟ… ΞΌΞµΞ³Ξ­ΞΈΞΏΟ…Ο‚ ΞµΞ½Ο�Ο‚ Iterable
    public static int getIterableSize(Iterator iterator){
        int i = 0;
        while(iterator.hasNext()) {
            i++;
            iterator.next();
        }
        return i;
    }
}
