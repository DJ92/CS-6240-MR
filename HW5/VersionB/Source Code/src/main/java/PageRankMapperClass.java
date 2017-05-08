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
 * It emits the contribution of each inlink for a page
 */
public class PageRankMapperClass extends Mapper<Object, Text, LongWritable, Text> {

    //Declaration
    static double delta;
    static Map<Long,Text> pRMap;

    //Map Method
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        delta = 0.0;
        pRMap = new HashMap<>();
        //Load R of all pages into cache
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
                        //Accumulate Delta for all dangling nodes
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
    }

    public void map(Object _k, Text value, Context context) throws IOException, InterruptedException{
        String[] inputs = value.toString().split(":");
        //Page Row Index
        long pageRow = Long.parseLong(inputs[0]);
        //IsDangling Flag
        Boolean isDangling = Boolean.valueOf(pRMap.get(pageRow).toString().split(",")[1]);
        //If Page has inlinks
        if(!inputs[1].equals("()")){
            //Inlink (Col) Index
            long inlinkRow = Long.parseLong(inputs[1].split(",")[0]);
            //Contribution
            Double oneByCj = Double.parseDouble(inputs[1].split(",")[1]);
            //R[j] from the HashMap
            Double PR = Double.parseDouble(pRMap.get(inlinkRow).toString().split(",")[0]);
            //New Inlink PR Computation
            Double newPR = oneByCj * PR;
            //Emit PR contribution for every inlink of a page
            context.write(new LongWritable(pageRow),new Text("R,"+pageRow+","+inlinkRow+","+newPR+","+isDangling));
        }
        //Emit delta for every page at Col 0
        context.write(new LongWritable(pageRow),new Text("R,"+pageRow+","+0+","+delta+","+isDangling));
    }
    //Cleanup Method
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        delta = 0.0;
        pRMap = new HashMap<>();
    }
}
