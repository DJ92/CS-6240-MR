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
 * Also, emits the Inlinks & Contribution to a page
 */
public class MakeNewMReducerClass extends Reducer<Text, Text, NullWritable, Text> {

    //Declaration
    MultipleOutputs<NullWritable, Text> mos;
    List<Text> AList;
    Text nodeName;
    long pageCount;
    String mInlinks;

    //Setup Method
    public void setup(Context context) {
        //Initialize
        AList = new ArrayList<>();
        nodeName = new Text();
        mos = new MultipleOutputs(context);
        //Get Counter
        pageCount = context.getConfiguration().getLong("pageCount", 0);
        mInlinks = "";
    }

    //Reduce Method
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //For all inlinks
        for (Text m : values) {

            //Parse Inlinks
            String[] vals = m.toString().split(",");

            //NodeType: N(Node) OR A(Inlink)
            String nodeType = vals[0];

            //Page RowIndex
            Long row = Long.parseLong(vals[1]);

            //Page ColIndex
            Long col = Long.parseLong(vals[2]);

            //Contribution
            Double adjSize = Double.parseDouble(vals[3]);

            //IsDangling Flag
            Boolean isDangling = false;

            //If Node
            if (nodeType.equals("N")) {

                //Get flag Value
                isDangling = Boolean.valueOf(vals[4]);

                //Get Node Data
                nodeName = new Text(nodeType+","+row+","+col+","+adjSize+","+isDangling);

                //Emit Initial R value to InitialRank Folder
                mInlinks = row + ":";
                Text m1 = new Text("R" + "," + row + "," + 1 + "," + 1.0 / pageCount + "," + isDangling);
                mos.write("R",NullWritable.get(), m1, "InitialRank/R");
            } else {
                //Accumulate inlinks
                AList.add(new Text(nodeType+","+row+","+col+","+adjSize+","+isDangling));        //Else we add everything in list as these are the outlinks ofa node.
            }
        }
        //For all Inlinks, form a set
        List<String> inlinkSet = new ArrayList<>();
        if (AList.size() != 0) {
            for (Text a : AList) {
                //Parse inlink
                String[] vals = a.toString().split(",");
                //Inlink Page Index
                Long row = Long.parseLong(vals[1]);
                //Contribution
                Double adjSize = Double.parseDouble(vals[3]);
                //Add to list
                inlinkSet.add("(" + row + "," + 1.0 / adjSize + ")");
            }
            //Form Set as String
            for (String t : inlinkSet) {
                mInlinks += t + "\t";
            }
            //Emit Page and Inlinks Set String
            mos.write("M",NullWritable.get(), new Text(mInlinks), "MatrixM/M");
        }else{
            //Emit Page and Empty Inlinks Set String
            mos.write("M",NullWritable.get(), new Text(mInlinks + "()"), "MatrixM/M");
        }
        //Reinitialize
        mInlinks = "";
        AList =new ArrayList<>();
        nodeName =new Text();
    }

    //Cleanup Method
    public void cleanup(Context c) throws IOException, InterruptedException {
        mos.close();
    }
}
