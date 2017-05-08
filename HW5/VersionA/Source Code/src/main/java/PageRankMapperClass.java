import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by DJ on 3/23/17.
 * This is the PageRank Mapper Class
 * It emits the Rank for each page
 */
public class PageRankMapperClass extends Mapper<Object, Text, NullWritable, Text> {

    //Declaration
    double delta;
    Map<Long,Text> pRMap;
    long pageCount;
    Boolean isDangling=false;

    //Setup Method
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        delta = 0.0;
        pRMap = new HashMap<>();
        //Load R Files from cache into HashMap
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            URI[] localPaths = context.getCacheFiles();
            File f=null;
            BufferedReader br=null;
            for(URI cachedFile: localPaths){
                try{
                    Path p =new Path(cachedFile);
                    br = new BufferedReader(new FileReader(p.getName()));
                    String line;
                    while ((line=br.readLine())!=null) {
                        String[] prVals = line.split(",");
                        pRMap.put(Long.parseLong(prVals[1]), new Text(prVals[3]+","+prVals[4]));
                        //Accumulate Delta for all Dangling Nodes
                        if (Boolean.valueOf(prVals[4])) {
                            delta += Double.parseDouble(prVals[3]);
                        }
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
        //Get PageCount Counter
        pageCount = context.getConfiguration().getLong("PageRankV", 0);
    }

    //Map Function
    public void map(Object _k, Text value, Context context) throws IOException, InterruptedException{
        Map<Long,Double> inlinksMap = new HashMap<>();
        String[] inputs = value.toString().split(":");

        //Page Index
        long row = Long.parseLong(inputs[0]);

        //IsDangling Flag
        isDangling = Boolean.valueOf(pRMap.get(row).toString().split(",")[1]);
        Double newPR = 0.0;

        //If Page has Inlinks
        if(!inputs[1].equals("()")){
            String[] inlinkTuples = inputs[1].split("\t");
            for(String inlink : inlinkTuples){
                //Parse and store inlink index and contribution
                inlink = inlink.substring(1,inlink.length()-1);
                inlinksMap.put(Long.parseLong(inlink.split(",")[0]),Double.parseDouble(inlink.split(",")[1]));
            }
            for(Map.Entry<Long,Double> entry: inlinksMap.entrySet()){
                //Calculate new PR using R from the cache accumulated hash map
                Double cellPR = Double.parseDouble(pRMap.get(entry.getKey()).toString().split(",")[0]);
                newPR += entry.getValue() * cellPR;
            }
        }
        //Add Delta
        newPR += delta/pageCount;
        //Multiply Alpha Factor
        newPR = (0.15/pageCount) + (0.85 * newPR);
        //Emit new PR for the page
        context.write(NullWritable.get(),new Text("R,"+row+","+1+","+newPR+","+isDangling));
    }

    //Cleanup Method
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        delta = 0.0;
        pRMap = new HashMap<>();
        isDangling = false;
    }
}
