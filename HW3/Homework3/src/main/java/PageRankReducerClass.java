import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by DJ on 2/15/17.
 * This class is the PageRank Reducer Class
 * It receives all pages with corresponding NodeClass Objects -> Either Nodes or inlink Contributions
 */
public class PageRankReducerClass extends Reducer<Text,AdjacencyListNodeWritableClass,Text,AdjacencyListNodeWritableClass> {

    //Reduce Function
    @Override
    protected void reduce(Text key, Iterable<AdjacencyListNodeWritableClass> values, Context context) throws IOException, InterruptedException {

        //Initiaize to Accumulate Sum of all inlink contributions
        Double sumPageRank=0.0;

        //Initialize the hopping factor for random jump or directly arriving probabilities
        Double alpha = 0.15;

        //Get total number of pages from Configuration
        Long pageCount =  context.getConfiguration().getLong("pageCount",0);

        //Get DeltaValue of previous iteration from Configuration
        Double deltaValue = context.getConfiguration().getDouble("deltaValue",0);

        //Initialize NodeClass Object
        AdjacencyListNodeWritableClass adjacencyListNodeWritableClassNew = new AdjacencyListNodeWritableClass();

        //IIterate over NodeClass Objects
        for(AdjacencyListNodeWritableClass adjacencyListNodeWritableClass: values){
            //If Node
            if(adjacencyListNodeWritableClass.getIsNode().get()){
                //Recover and set the graph structure for the Page
                adjacencyListNodeWritableClassNew.setLinkPageNames(adjacencyListNodeWritableClass.getLinkPageNames());
            }else{
                //Else accumulate inlink contributions
                sumPageRank += adjacencyListNodeWritableClass.getPageRank().get();
            }
        }

        //Calculate new PageRank value
        Double newPageRank = (alpha/pageCount) + (1-alpha)*(sumPageRank+deltaValue);

        //Update NodeClass Object with the new PageRank
        adjacencyListNodeWritableClassNew.setPageRank(new DoubleWritable(newPageRank));

        //Emit Page & NodeClass Object
        context.write(key,adjacencyListNodeWritableClassNew);
    }
}
