package csd.auth.ftw;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TextClustering {
	public static final String INVERTED_INDEX_JOB_NAME = "INVERTED_INDEX_JOB_NAME";
	public static final String KMEANS_JOB_NAME = "KMEANS_JOB_NAME";
	
	public static final String KEY_INPUT_DIR = "key_input_dir";
	public static final String KEY_OUTPUT_DIR = "key_output_dir";
	
	private static final String JAR_NAME = "text.jar";
	private static final String STOPWORDS_FILEPATH = "cache/stopwords.txt";
	
	private String inputPath;
	private String outputPath;
	private int n;
	
	public TextClustering(String inputPath, String outputPath, int n) throws IOException, ClassNotFoundException, InterruptedException {
	    this.inputPath = inputPath;
	    this.outputPath = outputPath;
	    this.n = n;
	    
	    // TODO
        executeJob(INVERTED_INDEX_JOB_NAME, inputPath, outputPath);
	}
	
	private int executeJob(String name, String inputPath, String outputPath) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
        conf.set(KEY_INPUT_DIR, inputPath);
        conf.set(KEY_OUTPUT_DIR, outputPath);
        
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(TextClustering.class);
        job.setJar(JAR_NAME);
        
        if (name.equals(INVERTED_INDEX_JOB_NAME)) {
            initInvertedIndexJob(job);
        } else if (name.equals(KMEANS_JOB_NAME)) {
            initKmeansJob(job);
        }

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

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
	
	private void initKmeansJob(Job job) {
        
    }
	
	private void initKmeansLastJob(Job job) {
        
    }
    
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        if (args.length != 3) {
			System.out.println("ERROR! Please enter input, output paths and the number of kmeans repeation");
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
