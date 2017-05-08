import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.*;

/**
 * Created by DJ on 2/22/17.
 * This is the Top K Mapper class
 * It is used to store local top K page ranks and send it to the reducer
 */
public class TopKMapperClass extends Mapper<Object,Text,NullWritable,Text> {

    //TreeMap to store sorted set of pages based on PageRank
    private TreeMap<Double, Text> repToRecordMap;

    //Initialize TreeMap on setup()
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        repToRecordMap = new TreeMap<>();
    }
    //Map Function
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Parse the input string into the map
        String[] sarr = value.toString().split(":");
        //PageName
        String pageName = sarr[0].trim();
        String[] objArr = sarr[1].split(" ");
        //PageRank
        Double pageRank = Double.parseDouble(objArr[0].trim());

        // Get will return null if the key is not there
        if (pageName == null || pageRank == 0.0) {
            // skip this record
            return;
        }
        //If already contains pagerank
        if(repToRecordMap.containsKey(pageRank)){
            //Append Page to the list
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
    //Emit All entries from the TreeMap on cleanup() with a Null Key
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Double,Text> entry: repToRecordMap.entrySet()) {
            context.write(NullWritable.get(),new Text(entry.getKey()+":"+entry.getValue()));
        }
    }
}
