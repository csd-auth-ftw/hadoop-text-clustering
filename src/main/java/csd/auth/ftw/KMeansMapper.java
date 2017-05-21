package csd.auth.ftw;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<Text, IntArrayWritable, IntWritable, IntArrayWritable> {
    public static final int K = 3;
    protected static final int RAND_SEED = 2017;
    
    protected int[][] centers = null;
    
    protected void setup(Context context) throws IOException {
        URI[] uris = context.getCacheFiles();
        Path centersPath = null;
        
        // search for centers.txt in cache
        for (URI uri: uris) {
            if (uri.toString().endsWith("centers.txt")) {
                centersPath = new Path(uri);
                break;
            }
        }
        
        // load centers from file if it exists
        if (centersPath != null) {
            // read file
            FileSystem hdfs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(centersPath)));
            String line;
            
            // regex
            String regx = "(\\d+) \\[(.*)\\]";
            Pattern pattern = Pattern.compile(regx);
            
            while ((line = br.readLine()) != null) {
                String centerLine = line.trim();
                
                if (centerLine.length() > 0) {
                    Matcher matcher = pattern.matcher(centerLine);
                    
                    int cID = 0;
                    String vectorStr = "";
                    
                    if (matcher.find()) {
                        cID = Integer.parseInt(matcher.group(1));
                        vectorStr = matcher.group(2);
                    }
                    
                    String[] values = vectorStr.split(",");
                    
                    // init centers
                    if (centers == null)
                        centers = new int[K][values.length];
                    
                    // update centers
                    for (int i=0; i<values.length; i++)
                        centers[cID][i] = Integer.parseInt(values[i].trim());
                }
            }
        }
    }
    
    protected void map(Text key, IntArrayWritable value, Context context) throws IOException, InterruptedException {
        IntWritable[] intWritables = value.get();
        int[] wordVector = new int[intWritables.length];
        for (int i=0; i<wordVector.length; i++)
            wordVector[i] = intWritables[i].get();
        
        int[][] centers = getCenters(wordVector.length);
        int centersNumber = centers.length;
        int minCID = -1;
        double minDistance = -1.0;
        double tempDistance;
        
        // For all the centers calculate the distance
        for(int i=0; i<centersNumber; centersNumber++) {
        	tempDistance = getDistance(wordVector, centers[i]);
        	
        	if (i == 0 || tempDistance < minDistance) {
        		minDistance = tempDistance;
        		minCID = i;
        	}
        }
        
        // write the closest center id with the word vector
        context.write(new IntWritable(minCID), value);
    }

    private double getDistance(int[] wordVector, int[] centerVector) {
        double numerator = 0;
        double temp1 = 0;
        double temp2 = 0;
        double denominator;
    	
    	for(int i=0; i<wordVector.length; i++) {
    		numerator += wordVector[i] * centerVector[i];	
    	}
    	
    	for(int j=0; j<wordVector.length; j++) {
    		temp1 += Math.pow(wordVector[j], 2);
    		temp2 += Math.pow(centerVector[j], 2);
    	}
    	
    	denominator = Math.sqrt(temp1) * Math.sqrt(temp2);
    	
    	return 1 - (numerator/denominator);
    }
    
    private int[][] getCenters(int len) throws IOException {
        // if already set from the file
        if (centers != null)
            return centers;
        
        // else randomize centers
        Random generator = new Random(RAND_SEED);
        int[][] centers = new int[K][len];
        for (int i=0; i<K; i++) {
            for (int j=0; j<len; j++) {
                centers[i][j] = generator.nextInt() % 2;
            }
        }
        
        return centers;
    }
    
}
