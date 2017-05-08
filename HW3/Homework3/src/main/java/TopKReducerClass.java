import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by DJ on 2/22/17.
 * This is the Top K Reducer Class.
 * It is used to accumulate all local top k values and emit the global top k values with a Null Key
 */
public class TopKReducerClass extends Reducer<NullWritable,Text,NullWritable,Text> {

    //TreeMap to store sorted set of pages based on PageRank
    private TreeMap<Double, Text> repToRecordMap=new TreeMap<>();

    //Reduce Function
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //Iterate over all values
        // i.e # Map Tasks * k since the reducer is set to 1
        for (Text value : values) {
            //Parse Value
            String[] sarr = value.toString().split(":");
            //PageRank
            Double pageRank = Double.parseDouble(sarr[0].trim());
            //PageName
            String pageName = sarr[1].trim();
            //If global map contains pageRank
            if(repToRecordMap.containsKey(pageRank)){
                //Add the page to the list of pages
                Text t = repToRecordMap.get(pageRank);
                String newVal = t.toString()+" "+pageName;
                //Update map for that pageRank
                repToRecordMap.put(pageRank,new Text(newVal));
            }else{
                //Add to the tree map
                repToRecordMap.put(pageRank, new Text(pageName));
            }
            //If treemap exceeds in size above k
            if (repToRecordMap.size() > 100) {
                //Eliminate the smallest value
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }
        //Emit All k entries from the TreeMap with a Null Key
        for (Map.Entry<Double,Text> entry : repToRecordMap.descendingMap().entrySet()) {
            context.write(NullWritable.get(),new Text(entry.getValue()+":"+entry.getKey()));
        }
    }
}
