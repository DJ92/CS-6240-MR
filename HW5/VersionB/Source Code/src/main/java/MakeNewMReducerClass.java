import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by DJ on 3/23/17.
 * This is the Make M Reducer Class
 * It emits the page as Node
 * Also, emits single Inlink & Contribution to the page
 */
public class MakeNewMReducerClass extends Reducer<Text, Text, NullWritable, Text> {

    //Declaration
    MultipleOutputs<NullWritable, Text> mos;
    List<Text> AList;
    Text nodeName;
    long pageCount;

    //Setup Method
    public void setup(Context context) {
        AList = new ArrayList<Text>();
        nodeName = new Text();
        mos = new MultipleOutputs(context);
        pageCount = context.getConfiguration().getLong("pageCount", 0);
    }

    //Reduce Method
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Long pageRow = 0l;
        for (Text m : values) {
            String[] vals = m.toString().split(",");
            String nodeType = vals[0];
            //Page Row Index
            Long row = Long.parseLong(vals[1]);
            //Page Col (Inlink) Index
            Long col = Long.parseLong(vals[2]);
            //Contribution
            Double adjSize = Double.parseDouble(vals[3]);
            //IsDangling Flag
            Boolean isDangling = false;
            if (nodeType.equals("N")) {
                isDangling = Boolean.valueOf(vals[4]);
                nodeName = new Text(nodeType+","+row+","+col+","+adjSize+","+isDangling);
                pageRow = row;
                Text m1 = new Text("R" + "," + row + "," + -1 + "," + 1.0 / pageCount + "," + isDangling);
                //Emit Initial R for each page
                mos.write("R",NullWritable.get(), m1, "InitialRank/R");
            } else {
                //Accumulate Inlink & Contribution
                AList.add(new Text(nodeType+","+row+","+col+","+adjSize+","+isDangling));        //Else we add everything in list as these are the outlinks ofa node.
            }
        }
        //If Page has inlinks
        if (AList.size() != 0) {
            for (Text a : AList) {
                String[] vals = a.toString().split(",");
                String nodeType = vals[0];

                //Col (Inlink) Index
                Long row = Long.parseLong(vals[1]);

                //Contribution
                Double adjSize = Double.parseDouble(vals[3]);
                Boolean isDangling = Boolean.valueOf(vals[4]);

                //Emit Cell M[i,j]=1/C[j] where j is an inlink to i and C[j] is the contribution
                mos.write("M",NullWritable.get(), new Text(pageRow + ":" +row+","+ 1.0 / adjSize), "MatrixM/M");
            }
        }
        else{
            //Emit Cell M[i,*]=() where there is no inlink to i
            mos.write("M",NullWritable.get(),new Text(pageRow +":"+"()"),"MatrixM/M");
        }
        AList =new ArrayList<Text>();
        nodeName =new Text();
    }
    //Cleanup Method
    public void cleanup(Context c) throws IOException, InterruptedException {
        mos.close();
    }
}
