import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator;

import java.io.File;
import java.io.IOException;

/**
 * Created by DJ on 3/22/17.
 * This is main driver program.
 * It consists of the following jobs -
 * -> Parsing
 * -> Make M and Initial R
 * -> PageRank Computation
 * -> Top K Records & Mapping to PageNames
 */
public class PageRankMatrixAggregateJob {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        /************** JOB AGGREGATOR***************************/

        //Initialize Configuration
        Configuration conf = new Configuration();

        //Parsing Job
        long pageCount = parseInput(conf,args[0],args[1]);

        //Make M & Initial R Job
        NewMakeM(conf,pageCount,args[1]);

        int iter = 1;
        while(iter<11){

            //Delete unwanted #iter Results
            if(iter>2){
                Path p = new Path(args[1]+"-MulResult"+(iter-2));
                p.getFileSystem(conf).delete(p,true);
            }

            //PR Job for #iter
            pageRankCal(conf,iter,pageCount,args[1]);
            iter++;
        }

        //Top K Records Job
        Top100Records(conf,10,args[1]);
    }

    //Parses Input Records to return pages and their outlinks
    public static Long parseInput(Configuration conf,String inputPath,String output) throws IOException, ClassNotFoundException, InterruptedException {
        conf.set("mapred.textoutputformat.separator", ":");

        //Index Counter
        conf.setLong("ID",0);
        Job jobParser = Job.getInstance(conf, "parser job");

        //One Reduce Task for ID Consistency
        jobParser.setNumReduceTasks(1);
        jobParser.setJarByClass(PageRankMatrixAggregateJob.class);

        //Mapper Class
        jobParser.setMapperClass(ParserMapperClass.class);
        jobParser.setMapOutputValueClass(AdjacencyListNodeWritableClass.class);

        //Reducer Class
        jobParser.setReducerClass(ParserReducerClass.class);
        jobParser.setOutputKeyClass(Text.class);
        jobParser.setOutputValueClass(AdjacencyListNodeWritableClass.class);

        //Input & Output Paths
        FileInputFormat.addInputPath(jobParser, new Path(inputPath));// here set the input path as the first argument
        FileOutputFormat.setOutputPath(jobParser, new Path(output+"-pagerank_init"));// final parser output

        jobParser.waitForCompletion(true);

        //Return PageCount
        return jobParser.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
    }

    //Creates the matrices M and Initial R for the Page Rank Calculation.
    public static void NewMakeM(Configuration conf,long pageCount,String output) throws IOException, ClassNotFoundException, InterruptedException{

        //Set PageCount Counter
        conf.setLong("pageCount",pageCount);

        Job job = Job.getInstance(conf, "Making the matrix");
        job.setJarByClass(PageRankMatrixAggregateJob.class);

        //Mapper Class
        job.setMapperClass(MakeNewMMapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //Reducer Class
        job.setReducerClass(MakeNewMReducerClass.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //Input & MultipleOutputs
        FileInputFormat.addInputPath(job, new Path(output+"-pagerank_init"));
        FileOutputFormat.setOutputPath(job, new Path(output+"-new_matrix_out"));
        MultipleOutputs.addNamedOutput(job, "M", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "R", TextOutputFormat.class, NullWritable.class, Text.class);

        job.waitForCompletion(true);
    }

    //Multiplying the row of M with column of R.
    public static void pageRankCal(Configuration conf, int iter,Long pageCount,String output) throws IOException,
            ClassNotFoundException, InterruptedException{
        //Set PageCount Counter
        conf.setLong("PageRankV", pageCount);
        Job job = Job.getInstance(conf,"PageRankCal");
        //If 1st iteration, Load Initial R
        if(iter == 1){
            Path p = new Path(output+"-new_matrix_out/InitialRank");
            FileSystem fs = p.getFileSystem(conf);
            FileStatus[] fileStatus = fs.listStatus(p);
            for (FileStatus status : fileStatus) {
                job.addCacheFile(status.getPath().toUri());	//Adding the file to distributed cache from first iteration.
            }
        }//Load R from last Iteration
        else
        {
            Path p = new Path(output+"-MulResult"+(iter-1));
            FileSystem fs = p.getFileSystem(conf);
            FileStatus[] fileStatus = fs.listStatus(p);
            for (FileStatus status : fileStatus) {
                job.addCacheFile(status.getPath().toUri());	//Adding the file to distributed cache from second iteration onwards.
            }
        }

        //Mapper Class
        job.setJarByClass(PageRankMatrixAggregateJob.class);
        job.setMapperClass(PageRankMapperClass.class);

        //Reducer class
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //Input & Output
        FileInputFormat.setInputPaths(job,new Path(output+"-new_matrix_out/MatrixM"));
        FileOutputFormat.setOutputPath(job, new Path(output+"-MulResult" + iter));

        job.waitForCompletion(true);
    }

    //Picks the top 100 page rank values.
    public static void Top100Records(Configuration conf,int iter,String output) throws IOException, ClassNotFoundException, InterruptedException{
        conf.set("mapred.textoutputformat.separator", ":");
        Job job = Job.getInstance(conf, "Taking the Top 100");
        job.setJarByClass(PageRankMatrixAggregateJob.class);
        //Set # of Reduce Tasks to 1
        job.setNumReduceTasks(1);
        //Load Parsing Output into Cache for PageName Mapping
        Path p = new Path(output+"-pagerank_init");
        FileSystem fs = p.getFileSystem(conf);
        FileStatus[] fileStatus = fs.listStatus(p);
        for (FileStatus status : fileStatus) {
            job.addCacheFile(status.getPath().toUri());	//Adding the file to distributed cache from parsing job.
        }
        //Mapper Class
        job.setMapperClass(TopKMapperClass.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        //Reducer Class
        job.setReducerClass(TopKReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Input & Output
        FileInputFormat.addInputPath(job, new Path(output+"-MulResult" + iter));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
