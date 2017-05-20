package csd.auth.ftw;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
    public static final String PUNCTUATION = "!\"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~´";
    private ArrayList<String> stopWords;
    
    protected void setup(Context context) throws IOException {
    	stopWords = new ArrayList<>();
    	URI[] uris = context.getCacheFiles();
    	Path stopWordsPath = null;
    	
    	for (URI uri: uris) {
    		if (uri.toString().endsWith("stopwords.txt")) {
    			stopWordsPath = new Path(uri);
    			break;
    		}
    	}
    	
    	FileSystem hdfs = FileSystem.get(context.getConfiguration());
    	BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(stopWordsPath)));
    	String line;
    	while ((line = br.readLine()) != null) {
    		String stopWord = line.trim();
    		
    		if (stopWord.length() > 0)
    			stopWords.add(stopWord);
    	}
    }

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();
        
        // remove whitespace
        line = removeWhiteSpace(line);
        
        // remove punctuation
        line = removePunctuation(line);
        
        // keep unique words
        HashSet<String> words = new HashSet<String>(Arrays.asList(line.split(" ")));
        
        // remove stopwords
        words = removeStopwords(words);
       
        // stem each word
        ArrayList<String> stemmedWords = new ArrayList<String>();
        for (String word: words)
            stemmedWords.add(applyStemming(word));
        
        // write result
        Text valueFilename = new Text(getCurrentFilename(context));
        for (String word: stemmedWords) {
            Text keyWord = new Text(word);
            
            context.write(keyWord, valueFilename);
        }
    }
    
    private String removeWhiteSpace(String line) {
        String pattern = "\\s+";
        return line.replaceAll(pattern, " ").trim();
    }

    private String removePunctuation(String str) {
        String pattern = String.join("|\\", PUNCTUATION.split(""));
        return str.replaceAll(pattern, "");
    }
    
    private HashSet<String> removeStopwords(HashSet<String> words) throws IOException {
        HashSet<String> filteredWords = new HashSet<>();
        
        for (String word: words)
            if (!stopWords.contains(word))
                filteredWords.add(word);
        
        return filteredWords;
    }
    
    private String applyStemming(String word) {
        Stemmer stemmer = new Stemmer();
        stemmer.add(word.toCharArray(), word.length());
        stemmer.stem();
        
        return stemmer.toString();
    }
    
    private String getCurrentFilename(Context context) {
        return ((FileSplit) context.getInputSplit()).getPath().toString();
    }
}
