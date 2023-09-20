// Import necessary Java libraries and Hadoop classes
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

// Define a Java class named WC_Runner
public class WC_Runner
{
    // Main method that serves as the entry point for the program
    public static void main(String[] args) throws IOException
    {
        // Create a new Hadoop JobConf object, which represents the configuration for the MapReduce job
        JobConf conf = new JobConf(WC_Runner.class);

        // Set a name for the MapReduce job
        conf.setJobName("WordCount_version2");

        // Specify the data types for the output key and value produced by the Mapper and Reducer
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // Set the Mapper class for this job
        conf.setMapperClass(WC_Mapper.class);

        // Specify a Combiner class to be used (it's the same as the Reducer in this case)
        conf.setCombinerClass(WC_Reducer.class);

        // Set the Reducer class for this job
        conf.setReducerClass(WC_Reducer.class);

        // Set the input format class to TextInputFormat (default for processing text data)
        conf.setInputFormat(TextInputFormat.class);

        // Set the output format class to TextOutputFormat (default for writing text data)
        conf.setOutputFormat(TextOutputFormat.class);

        // Specify the input directory for the MapReduce job
        FileInputFormat.setInputPaths(conf, new Path(args[0]));

        // Specify the output directory for the MapReduce job
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Run the MapReduce job using the JobClient
        JobClient.runJob(conf);
    }
}
