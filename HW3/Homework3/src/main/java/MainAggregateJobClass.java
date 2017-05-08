import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by DJ on 2/15/17.
 * This is the main class for creating and executing all jobs
 * It consists of the following Jobs:
 * - Preprocessing/Parsing
 * - PageRank Calculation x10
 * - Top K
 */
public class MainAggregateJobClass {

    //Main Function
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        //Intermediate Output Directory Prefix
        String hdfsOutputName = "result";

        /************** PARSER/PREPROCESSING JOB ***************************/

        //Initialize Configuration & Job Classes
        Configuration confParser = new Configuration();
        //Key Value Separator ":"
        confParser.set("mapred.textoutputformat.separator", ":");
        Job jobParser = Job.getInstance(confParser, "parser job");
        jobParser.setJarByClass(MainAggregateJobClass.class);

        //Mapper Class
        jobParser.setMapperClass(ParserMapperClass.class);
        jobParser.setMapOutputValueClass(AdjacencyListNodeWritableClass.class);

        //Reducer Class
        jobParser.setReducerClass(ParserReducerClass.class);
        jobParser.setOutputKeyClass(Text.class);
        jobParser.setOutputValueClass(AdjacencyListNodeWritableClass.class);

        //Input & Output Paths
        FileInputFormat.addInputPath(jobParser, new Path(args[0]));// here set the input path as the first argument
        FileOutputFormat.setOutputPath(jobParser, new Path(hdfsOutputName+"0"));// final parser output

        jobParser.waitForCompletion(true);

        /************************ PAGERANK JOB ****************************/

        //Delta Value
        Double deltaValue=0.0;

        //Iteration Limit
        int itrLimit = 11;

        for(int i=0;i<=(itrLimit-1);i++) {

            //Initialize Configuration Class
            Configuration confPageRank = new Configuration();
            //Key Value Separator ":"
            confPageRank.set("mapred.textoutputformat.separator", ":");

            //Set IterationCount "i" to Configuration
            confPageRank.setInt("iterationCount",i);

            //Set total pageCount from previous Parser Job -> #Reduce Output Records to Configuration
            confPageRank.setLong("pageCount", jobParser.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS ).getValue());

            //Set Delta Value from previous iteration to Configuration
            confPageRank.setDouble("deltaValue",deltaValue);

            //Initialize Job Class
            Job jobPageRank = Job.getInstance(confPageRank, "pagerank job: "+i);

            //Input & Output Paths
            FileInputFormat.addInputPath(jobPageRank, new Path(hdfsOutputName+i)); // here we set the Parser Job's output as job-2's input
            FileOutputFormat.setOutputPath(jobPageRank, new Path(hdfsOutputName + (i+1))); // final intermediate output

            jobPageRank.setJarByClass(MainAggregateJobClass.class);

            //Mapper Class
            jobPageRank.setMapperClass(PageRankMapperClass.class);
            jobPageRank.setMapOutputKeyClass(Text.class);
            jobPageRank.setMapOutputValueClass(AdjacencyListNodeWritableClass.class);

            //Reducer Class
            jobPageRank.setReducerClass(PageRankReducerClass.class);
            jobPageRank.setOutputKeyClass(Text.class);
            jobPageRank.setOutputValueClass(AdjacencyListNodeWritableClass.class);

            jobPageRank.waitForCompletion(true);

            //Get Accumulated Delta from the current Iteration and update deltaValue to be used in next iteration
            //Shifted Long Bits to Double
            deltaValue = (double)jobPageRank.getCounters().findCounter("deltaValue-Counter","deltaValue").getValue()/Math.pow(10,15);
        }

        /********************TOP K JOB**************************************/

        //Initialize Configuration & Job Classes
        Configuration confTopK = new Configuration();
        Job jobTopK = Job.getInstance(confTopK,"TopK");
        jobTopK.setJarByClass(MainAggregateJobClass.class);

        //Mapper Class
        jobTopK.setMapperClass(TopKMapperClass.class);
        jobTopK.setMapOutputKeyClass(NullWritable.class);
        jobTopK.setMapOutputValueClass(Text.class);

        //Reducer Class (Count == 1)
        jobTopK.setReducerClass(TopKReducerClass.class);
        jobTopK.setNumReduceTasks(1);
        jobTopK.setOutputKeyClass(NullWritable.class);
        jobTopK.setOutputValueClass(Text.class);

        //Input & Output Paths
        FileInputFormat.addInputPath(jobTopK, new Path(hdfsOutputName+itrLimit));// here we set the last PageRank Job's output as TopK Job's input
        FileOutputFormat.setOutputPath(jobTopK, new Path(args[1]));// Final Output

        //Job Exit
        System.exit(jobTopK.waitForCompletion(true) ? 0 : 1);
    }
}
