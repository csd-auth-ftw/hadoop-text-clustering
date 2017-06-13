package csd.auth.ftw;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TextClustering {
    // job names
	public static final String INVERTED_INDEX_JOB_NAME = "INVERTED_INDEX_JOB_NAME";
	public static final String KMEANS_JOB_NAME = "KMEANS_JOB_NAME";
	public static final String KMEANS_LAST_JOB_NAME = "KMEANS_LAST_JOB_NAME";
	
	// conf keys
	public static final String KEY_INPUT_DIR = "key_input_dir";
	public static final String KEY_OUTPUT_DIR = "key_output_dir";
	public static final String KEY_K_NUMBER = "key_k_number";
	
	// default i/o paths
	private static final String DEF_PATH_INVERTED_INDEX_OUT = "def_iiout";
	private static final String DEF_PATH_KMEANS_OUT = "def_kmout";
	
	private static final String JAR_NAME = "text.jar";
	private static final String STOPWORDS_FILEPATH = "cache/stopwords.txt";
	private static final String CENTERS_FILEPATH = "cache/centers.txt";
	
	private Configuration conf;
	private FileSystem hdfs;
	
	public TextClustering(String inputPathStr, String outputPathStr, int k, int n) throws IOException, ClassNotFoundException, InterruptedException {
	    // set i/o paths for jobs
	    Path inputPath = new Path(inputPathStr);
	    Path outputPath = new Path(outputPathStr);
	    Path tmpInvIndOutPath = new Path(DEF_PATH_INVERTED_INDEX_OUT);
        Path tmpKmOutPath = new Path(DEF_PATH_KMEANS_OUT);
	    
	    conf = new Configuration();
	    conf.set(KEY_INPUT_DIR, inputPathStr);
	    conf.set(KEY_OUTPUT_DIR, outputPathStr);
	    conf.set(KEY_K_NUMBER, k + "");
	    
	    hdfs = FileSystem.get(conf);
	    
	    // delete files from previous executions
	    deleteFile(tmpInvIndOutPath, true);
	    deleteFile(outputPath, true);
	    deleteFile(CENTERS_FILEPATH, false);
	    
	    // build inverted index
        executeJob(INVERTED_INDEX_JOB_NAME, inputPath, tmpInvIndOutPath);
        
        // create random centers file
        createRandomCenters(k, tmpInvIndOutPath);
        
        for (int i=0; i<n; i++) {
        	// delete kmeans output
        	deleteFile(tmpKmOutPath, true);
    	    
            if (i < n-1) {
            	executeJob(KMEANS_JOB_NAME, tmpInvIndOutPath, tmpKmOutPath);
            	
            	// delete previous centers.txt
            	deleteFile(CENTERS_FILEPATH, false);
                
                FileUtil.copyMerge(hdfs, tmpKmOutPath, hdfs, new Path(CENTERS_FILEPATH), true, conf, "");
            } else {
            	executeJob(KMEANS_LAST_JOB_NAME, tmpInvIndOutPath, outputPath);
            }
        }
	}
	
	/**
	 * Creates a file containing k random centers based
	 * on the data created from the inverted index job
	 */
	private void createRandomCenters(int k, Path tmpInvIndOutPath) throws FileNotFoundException, IOException {
		FileStatus[] fileStatuses = hdfs.listStatus(tmpInvIndOutPath);
		
		// sort files by size
		Arrays.sort(fileStatuses, new Comparator<FileStatus>() {

			@Override
			public int compare(FileStatus o1, FileStatus o2) {
				return o1.getLen() >= o2.getLen() ? -1: 1;
			}
			
		});
		
		// read from the biggest file
		Path biggestFilePath = fileStatuses[0].getPath();
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(biggestFilePath)));
        String line;
        int[][] centers = null;
        int i = 0;
        
        // extract data from each line
        while ((line = br.readLine()) != null && i < k) {
        	int[] vector = KMeansMapper.getVectorFromLine(line, false);
        	
        	if (centers == null)
        		centers = new int[k][vector.length];
        	
        	if (!containsVector(centers, vector, i))
        		centers[i++] = vector;
        }
        
        // fill the rest with random vectors if needed
        if (i != k) {
        	
        }
        
        // convert centers to file
        StringBuilder sb = new StringBuilder();
        for (int j=0; j<k; j++) {
        	sb.append(j + "\t" + IntArrayWritable.arrayToText(centers[j]).toString() +  "\n");
        }
        
        // write to file
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(CENTERS_FILEPATH), true)));
        bw.write(sb.toString());
        bw.close();
	}
	
	/**
	 * Checks if array contains a certain vector
	 */
	private boolean containsVector(int[][] arr, int[] vector, int len) {
		for (int i=0; i<len; i++)
			if (Arrays.equals(arr[i], vector))
				return true;
		
		return false;
	}
	
	/**
	 * Deletes a file from hdfs
	 */
	private void deleteFile(String pathStr, boolean rec) throws IllegalArgumentException, IOException {
		deleteFile(new Path(pathStr), rec);
	}
	
	/**
	 * Deletes a file from hdfs
	 */
	private void deleteFile(Path path, boolean rec) throws IOException {
		if (hdfs.exists(path))
	    	hdfs.delete(path, rec);
	}
	
	/**
	 * Executes a hadoop job
	 */
	private int executeJob(String name, Path inputPath, Path outputPath) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(TextClustering.class);
        job.setJar(JAR_NAME);
        
        if (name.equals(INVERTED_INDEX_JOB_NAME)) {
            initInvertedIndexJob(job);
        } else {
            initKmeansJob(job);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
	}
	
	/**
	 * Initializes an inverted index job
	 */
	private void initInvertedIndexJob(Job job) {
	    job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntArrayWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        // add cache files
        job.addCacheFile(new Path(STOPWORDS_FILEPATH).toUri());
	}
	
	/**
	 * Initializes a kmeans job
	 */
	private void initKmeansJob(Job job) throws IllegalArgumentException, IOException {
	    job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        // add cache files
        Path centers = new Path(CENTERS_FILEPATH);
        if (hdfs.exists(centers))
            job.addCacheFile(centers.toUri());
    }
    
	/**
	 * The main function of the application
	 */
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        if (args.length != 4) {
			System.out.println("ERROR! Please enter input, output paths, the number of teams (k) and the number of kmeans repeations");
			System.exit(1);
		}
    	
    	// check if third argument is number
    	int k = 3;
    	try {
    	    k = Integer.parseInt(args[2]);
    	} catch(NumberFormatException exc) {
    	    System.out.println("ERROR! The third argument must be a valid number (integer)");
            System.exit(1);
    	}
    	
    	// check if third argument is number
    	int n = 2;
    	try {
    	    n = Integer.parseInt(args[3]);
    	} catch(NumberFormatException exc) {
    	    System.out.println("ERROR! The fourth argument must be a valid number (integer)");
            System.exit(1);
    	}
    	
    	new TextClustering(args[0], args[1], k, n);
    }

}
