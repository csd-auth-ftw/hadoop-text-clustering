package csd.auth.ftw;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
	
	public TextClustering(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    // TODO
        executeJob(INVERTED_INDEX_JOB_NAME, args[0], args[1]);
	}
	
	private int executeJob(String name, String inputPath, String outputPath) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
        conf.set(KEY_INPUT_DIR, inputPath);
        conf.set(KEY_OUTPUT_DIR, outputPath);
        
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(TextClustering.class);
        job.setJar(JAR_NAME);
        
        if (name.equals(INVERTED_INDEX_JOB_NAME)) {
            job.setMapperClass(MapperClass.class);
            job.setReducerClass(ReducerClass.class);
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntArrayWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            
            // add cache files
            job.addCacheFile(new Path(STOPWORDS_FILEPATH).toUri());
        } else if (name.equals(KMEANS_JOB_NAME)) {
            // TODO
        }

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
	}
    
    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
    	if (args.length != 2) {
			System.out.println("ERROR! Please enter input and output paths");
			System.exit(1);
		}
    	
    	new TextClustering(args);
    }

}
