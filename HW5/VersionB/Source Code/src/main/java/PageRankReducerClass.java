import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by DJ on 3/23/17.
 * This is the PageRank Reducer Class
 * It adds the inlink PR contribution of each inlink for a page
 */
public class PageRankReducerClass extends Reducer<LongWritable,Text,NullWritable,Text>{
    //Declaration
    Long pageCount;

    //Setup Method
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //Get PageCount Counter
        pageCount = context.getConfiguration().getLong("PageRankV", 0);
    }

    //Reduce Method
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //Page Index
        Long pageRow = key.get();

        //Sum
        Double inlinkSum=0.0;

        //Delta
        Double delta = 0.0;

        //IsDangling Flag
        Boolean isDangling = false;

        for(Text t: values){
            String[] vals = t.toString().split(",");

            //Inlink Index
            Long col = Long.parseLong(vals[2]);

            //PR Contribution
            Double contribution = Double.parseDouble(vals[3]);

            //if col index is 0, accumulate delta value & dangling flag
            if(col==0){
                delta = contribution;
                isDangling = Boolean.valueOf(vals[4]);
            }//Accumulate inlink PR
            else{
                inlinkSum += contribution;
            }
        }

        //Add Delta
        inlinkSum += delta/pageCount;

        //Add Alpha Factor
        Double newPR = (0.15/pageCount) + (0.85 * inlinkSum);

        //Emit new PR for the page
        context.write(NullWritable.get(),new Text("R"+","+pageRow+","+ -1 +","+newPR+","+isDangling));
    }
}
