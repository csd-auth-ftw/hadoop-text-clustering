package csd.auth.ftw;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
	
	// default i/o paths
	private static final String DEF_PATH_INVERTED_INDEX_OUT = "def_iiout";
	private static final String DEF_PATH_KMEANS_OUT = "def_kmout";
	
	private static final String JAR_NAME = "text.jar";
	private static final String STOPWORDS_FILEPATH = "cache/stopwords.txt";
	private static final String CENTERS_FILEPATH = "cache/centers.txt";
	
	private Configuration conf;
	private FileSystem hdfs;
	
	public TextClustering(String inputPathStr, String outputPathStr, int n) throws IOException, ClassNotFoundException, InterruptedException {
	    // set i/o paths for jobs
	    Path inputPath = new Path(inputPathStr);
	    Path outputPath = new Path(outputPathStr);
	    Path tmpInvIndOutPath = new Path(DEF_PATH_INVERTED_INDEX_OUT);
        Path tmpKmOutPath = new Path(DEF_PATH_KMEANS_OUT);
	    
	    Configuration conf = new Configuration();
	    conf.set(KEY_INPUT_DIR, inputPathStr);
	    conf.set(KEY_OUTPUT_DIR, outputPathStr);
	    
	    hdfs = FileSystem.get(conf);
	    
        executeJob(INVERTED_INDEX_JOB_NAME, inputPath, tmpInvIndOutPath);
        executeJob(KMEANS_JOB_NAME, tmpInvIndOutPath, tmpKmOutPath);
        
        FileUtil.copyMerge(hdfs, tmpKmOutPath, hdfs, new Path(CENTERS_FILEPATH), true, conf, "");
        
        System.out.println("FINISHED");
        
        // TODO merge reduce files, rename to centers.txt
//        for (int i=0; i<n; i++) {
//            if (i < n-1) {
//                
//            } else {
//                // last
//            }
//        }
	}
	
	private int executeJob(String name, Path inputPath, Path outputPath) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
//	    Configuration conf = new Configuration();
//        conf.set(KEY_INPUT_DIR, inputPath);
//        conf.set(KEY_OUTPUT_DIR, outputPath);
        
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(TextClustering.class);
        job.setJar(JAR_NAME);
        
        if (name.equals(INVERTED_INDEX_JOB_NAME)) {
            initInvertedIndexJob(job);
        } else if (name.equals(KMEANS_JOB_NAME)) {
            initKmeansJob(job);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
	}
	
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
	
	private void initKmeansJob(Job job) throws IllegalArgumentException, IOException {
	    job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntArrayWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntArrayWritable.class);
        
        // add cache files
        Path centers = new Path(CENTERS_FILEPATH);
        if (hdfs.exists(centers))
            job.addCacheFile(centers.toUri());
    }
	
	private void initKmeansLastJob(Job job) {
        
    }
    
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        if (args.length != 3) {
			System.out.println("ERROR! Please enter input, output paths and the number of kmeans repeations");
			System.exit(1);
		}
    	
    	// check if third argument is number
    	int n = 1;
    	try {
    	    n = Integer.parseInt(args[2]);
    	} catch(NumberFormatException exc) {
    	    System.out.println("ERROR! The third argument must be a valid number (integer)");
            System.exit(1);
    	}
    	
    	new TextClustering(args[0], args[1], n);
    }

}
