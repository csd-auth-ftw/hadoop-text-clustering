package csd.auth.ftw;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
    private static final String CID_VECTOR_REGEX = "(\\d+)\\s+\\[(.*)\\]";
    private static final String WORD_VECTOR_REGEX = "([a-zA-Z][a-zA-Z0-9]+)\\s+\\[(.*)\\]";
    
    private static final int RAND_SEED = 2017;
    
    private int[][] centers = null;
    private boolean hasRandomCenters = false;
    
    /**
     * Extracts the word from a line
     */
    public static String getWordFromLine(String line) {
        String regx = WORD_VECTOR_REGEX;
        Pattern pattern = Pattern.compile(regx);
        Matcher matcher = pattern.matcher(line.trim());
        
        if (matcher.find())
            return matcher.group(1);
        
        return null;
    }
    
    /**
     * Extracts the center id from a line
     */
    public static int getCenterFromLine(String line) {
        String regx = CID_VECTOR_REGEX;
        Pattern pattern = Pattern.compile(regx);
        Matcher matcher = pattern.matcher(line.trim());
        
        if (matcher.find())
            return Integer.parseInt(matcher.group(1));
        
        return -1;
    }
    
    /**
     * Extracts the vector from a line
     */
    public static int[] getVectorFromLine(String line, boolean hasCenter) {
        String regx = WORD_VECTOR_REGEX;
        if (hasCenter)
            regx = CID_VECTOR_REGEX;
        
        Pattern pattern = Pattern.compile(regx);
        Matcher matcher = pattern.matcher(line.trim());
        
        String vectorStr;
        if (matcher.find()) {
            vectorStr = matcher.group(2);
            String[] values = vectorStr.split(IntArrayWritable.SEPARATOR);
            
            int[] vector = new int[values.length];
            
            for (int i=0; i<values.length; i++)
                vector[i] = Integer.parseInt(values[i].trim());
            
            return vector;
        }
        
        return null;
    }
    
    protected void setup(Context context) throws IOException {
        URI[] uris = context.getCacheFiles();
        Path centersPath = null;
        
        // search for centers.txt in cache
        if (uris == null)
        	return;
        
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
            
            // extract data from each line
            while ((line = br.readLine()) != null) {
                String centerLine = line.trim();
                
                if (centerLine.length() > 0) {
                    int cID = getCenterFromLine(centerLine);
                    int[] vector = getVectorFromLine(centerLine, true);
                    
                    // init centers
                    if (centers == null) {
                    	int centersNumber = Integer.parseInt(context.getConfiguration().get(TextClustering.KEY_K_NUMBER));
                    	centers = getRandomCenters(centersNumber, vector.length);
                    }
                    
                    centers[cID] = vector;
                }
            }
        }
    }
    
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int centersNumber = Integer.parseInt(context.getConfiguration().get(TextClustering.KEY_K_NUMBER));
        
        int[] wordVector = getVectorFromLine(line, false);
        
        // ensure centers is set
        setCenters(centersNumber, wordVector.length);
        
//        if (hasRandomCenters) {
//        	int k = Integer.parseInt(context.getConfiguration().get(TextClustering.KEY_K_NUMBER));
//        	Random generator = new Random(RAND_SEED);
//        	int minCID = generator.nextInt(k);
//        	context.write(new IntWritable(minCID), value);
//        	return;
//        }
        
        int minCID = -1;
        double minDistance = Double.MAX_VALUE;
        double tempDistance;
        
        // For all the centers calculate the distance
        for(int i=0; i<centersNumber; i++) {
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
    
    private void setCenters(int centersNumber, int len) throws IOException {
        // if already set from the setup()
        if (centers != null)
            return;
        
        // else randomize centers
        hasRandomCenters = true;
        centers = getRandomCenters(centersNumber, len);
    }
    
    /**
     * Creates random centers using an rng
     * @param len
     * @return
     */
    private int[][] getRandomCenters(int k, int len) {
    	Random generator = new Random(RAND_SEED);
    	int[][] randCenters = new int[k][len];
    	for (int i=0; i<k; i++) {
            for (int j=0; j<len; j++) {
            	randCenters[i][j] = generator.nextInt(2);
            }
        }
    	
    	return randCenters;
    }
    
}
