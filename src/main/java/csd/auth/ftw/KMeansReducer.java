package csd.auth.ftw;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
        if (isLastJob()) { // CHECK NAME FROM CONTEXT
            // TODO extract words from values and write them
            
            context.write(key, average);
        } else {
            // TODO extract vectors and get average
            // find average
            IntArrayWritable average = getAverage(values);
            
            // TODO
//            IntArrayWritable.arrayToString(arr);
            context.write(key, average);
        }
    }

    private IntArrayWritable getAverage(Iterable<Text> values){
        //getting the number of vectors in the Iterable
        int vectorNum = getIterableSize(values.iterator());
        //getting the length of the vectors
        //ίσως έτσι χάνεται το πρώτο στοιχείο, αν η κλήση της .iterator() δεν δίνει καινούργιο iterator κάθε φορά (same για την getIterableSize())
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
            //ανάθεση μέσης τιμής για κάθε διάσταση του μέσου vector (1 αν είναι πάνω από 0.5, 0 αν είναι μικρότερο)
            averageVector[i] = (tempAverage >= 0.5) ? 1 : 0;
        }
        return IntArrayWritable.createFromArray(averageVector);
    }

    //Απλή μέθοδος για εύρεση του μεγέθους ενός Iterable
    public static int getIterableSize(Iterator iterator){
        int i = 0;
        while(iterator.hasNext()) {
            i++;
            iterator.next();
        }
        return i;
    }
}
