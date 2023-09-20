// Import necessary Java libraries and Hadoop classes
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

// Define the WC_Mapper class, which implements the Hadoop Mapper interface
public class WC_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
    // Create an IntWritable object with a constant value of 1
    private final static IntWritable one = new IntWritable(1);

    // Create a Text object to store words
    private Text word = new Text();

    // The map function, which processes each input record
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        // Convert the input Text value to a Java String
        String line = value.toString();

        // Tokenize the input line into words using StringTokenizer
        StringTokenizer tokenizer = new StringTokenizer(line);

        // Iterate through the tokens (words) in the line
        while (tokenizer.hasMoreTokens())
        {
            // Set the Text object 'word' to the current token (word)
            word.set(tokenizer.nextToken());

            // Emit a key-value pair where the key is the word and the value is 1
            output.collect(word, one);
        }
    }
}
