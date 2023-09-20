// Import necessary Java libraries and Hadoop classes
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

// Define the WC_Reducer class, which implements the Hadoop Reducer interface
public class WC_Reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{
    // The reduce function, which processes values for a given key
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int sum = 0; // Initialize a variable to store the sum of values

        // Iterate through the values for the current key
        while (values.hasNext())
        {
            sum += values.next().get(); // Add the value to the sum
        }

        // Emit a key-value pair where the key is the original key and the value is the sum
        output.collect(key, new IntWritable(sum));
    }
}
