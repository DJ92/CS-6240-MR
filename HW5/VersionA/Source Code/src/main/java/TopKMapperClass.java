import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by DJ on 3/23/17.
 * This is the TOP K Mapper Class
 * Accumulates the local TOP K pages
 */
public class TopKMapperClass extends Mapper<Object, Text, DoubleWritable, Text> {
    TreeMap<Double,Text> prMap;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        prMap= new TreeMap<>();
    }
    public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException{

        String[] inputs = value.toString().split(",");
        double pageRank = Double.parseDouble(inputs[3]);
        Long id = Long.parseLong(inputs[1]);
        //Emit the page rank and text respectively, so that the key comparator can compare the keys.
        if(prMap.containsKey(pageRank)){
            //Append Page to the list
            Text t = prMap.get(pageRank);
            String newVal = t.toString()+" "+id;
            //Update map for that pageRank
            prMap.put(pageRank,new Text(newVal));
        }else{
            //Add to the tree map
            prMap.put(pageRank, new Text(String.valueOf(id)));
        }
        //If treemap exceeds in size above k
        if (prMap.size() > 100) {
            //Eliminate the smallest value
            prMap.remove(prMap.firstKey());
        }

    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Double,Text> entry: prMap.entrySet()) {
            context.write(new DoubleWritable(entry.getKey()),new Text(entry.getValue()));
        }
    }
}
