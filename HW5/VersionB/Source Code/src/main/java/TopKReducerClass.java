import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by DJ on 3/23/17.
 * This is the TOP K Reducer Class
 * Accumulates the Global TOP K Pages
 */
public class TopKReducerClass extends Reducer<DoubleWritable, Text, Text, Text> {
    private TreeMap<Double, Text> repToRecordMap;
    Map<Long,Text> indexMap;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        repToRecordMap=new TreeMap<>();
        indexMap = new HashMap();
        //Add Parsing Job Output from cache to HashMap
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            URI[] localPaths = context.getCacheFiles();
            File f=null;
            BufferedReader br=null;
            for(URI cachedFile: localPaths){
                try{
                    Path p = new Path(cachedFile);
                    br = new BufferedReader(new FileReader(p.getName()));
                    String line;
                    while ((line=br.readLine())!=null) {
                        String[] lines = line.split(":");
                        String pageName = lines[0];
                        String index = lines[1].split(" ")[0];
                        //Add ID's with PageNAme
                        indexMap.put(Long.parseLong(index),new Text(pageName));
                    }
                }
                catch (Exception e){
                    System.out.println(e.toString());
                }finally{
                    br.close();
                }
            }
        }
        else{
            System.out.println("No files found in cache");
        }
    }

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        for(Text t:values){
            String[] sarr = t.toString().split(" ");
            //PageRank
            Double pageRank = key.get();
            //PageName
            String pageName = sarr[0].trim();
            //If global map contains pageRank
            if(repToRecordMap.containsKey(pageRank)){
                //Add the page to the list of pages
                Text pr = new Text(repToRecordMap.get(pageRank));
                String newVal = pr.toString()+" "+pageName;
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

    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Double,Text> entry : repToRecordMap.descendingMap().entrySet()) {
            //Emit Page and its PageRank
            Text pageName = indexMap.get(Long.parseLong(entry.getValue().toString()));
            context.write(pageName, new Text(entry.getKey().toString()));
        }
    }
}
