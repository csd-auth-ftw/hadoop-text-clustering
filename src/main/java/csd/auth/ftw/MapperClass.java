package csd.auth.ftw;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapperClass extends Mapper<Text, Text, Text, Text> {
    public static final String PUNCTUATION = "!\"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~´";

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
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
        
        for (String word: stemmedWords) {
            Text keyWord = new Text(word);
            Text valueFilename = new Text(getCurrentFilename(context));
            
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
        List<String> stopwords = Files.readAllLines(Paths.get("C:\\users\\nikos\\stopwords.txt"), StandardCharsets.UTF_8);
        HashSet<String> filteredWords = new HashSet<>();
        
        for (String word: words) {
            if (!stopwords.contains(word))
                filteredWords.add(word);
        }
        
        return filteredWords;
    }
    
    private String applyStemming(String word) {
        Stemmer stemmer = new Stemmer();
        stemmer.add(word.toCharArray(), word.length());
        stemmer.stem();
        
        return stemmer.toString();
    }
    
    private String getCurrentFilename(Context context) {
        return ((FileSplit) context.getInputSplit()).getPath().getName();
    }
}
