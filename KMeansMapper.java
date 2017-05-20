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
        
        
        
        //Number of Centers
        int c = getCurrentCenters(context).length;        
        //Iterrator MATHAFAKAZ
        int i;
        //getCurrentCenters(context)[0] is vector of the 1st center
        
        //Center ID
        int cID= -1;
        
        //Min distance from the cID
        float minDistance = -1.0;
        float tempDistance=-1.0;
        //For all the centers calculate the distance
        for(i=0;i<c;c++)
        {
        	tempDistance = getDistance(list,centers[i]);
        	if (tempDistance < minDistance)
        	{
        		minDistance = distance;
        		cID =i;
        		
        	}
        	
        }
        
        context.write(cID,centers[i]);
        
        

        // TODO find least distance with a center
        
        // TODO write key:min_center value:value as is
        
    }

    private float getDistance(int[] wordVector,int[] centerVector)
    {
    	float distance = 0;
    	float numerator = 0;
    	float temp1=0;
    	float temp2=0;
    	float denominator =1;
    	int i;
    	
    	for(i =0;i<wordVector.length;i++)
    	{
    		numerator = numerator + wordVector[i] * centerVector[i];
    		   		
    	}
    	
    	for(i=0;i<wordVector.length;i++)
    	{
    		temp1 = temp1 + pow(wordVector[i]);
    		temp2 = temp2 + pow(centerVector[i]);
    	}
    	
    	denominator = root(temp1) * root(temp2);
    	
    	distance = 1 - (numerator/denominator);
    	
    	return distance;
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
