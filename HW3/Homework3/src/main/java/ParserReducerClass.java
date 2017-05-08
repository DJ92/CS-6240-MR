import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by DJ on 2/17/17.
 * This class is the Parser Reducer Class
 * It receives all pages with corresponding NodeClass Objects
 */
public class ParserReducerClass extends Reducer<Text,AdjacencyListNodeWritableClass,Text,AdjacencyListNodeWritableClass> {

    //Reduce Function
    @Override
    protected void reduce(Text key, Iterable<AdjacencyListNodeWritableClass> values, Context context) throws IOException, InterruptedException {

        //Initalize set for maintaining a coherent unique list
        Set<Text> linkPageNames = new HashSet();

        //Iterate over all NodeClass Objects
        for(AdjacencyListNodeWritableClass adjacencyListNodeWritableClass: values){
            //If NodeClass Object contains set of outlink pages
            if(adjacencyListNodeWritableClass.getLinkPageNames().size() > 0){
                for(Text t: adjacencyListNodeWritableClass.getLinkPageNames()){
                    //Add to global set
                    linkPageNames.add(t);
                }
            }
        }
        //New NodeClass Object
        AdjacencyListNodeWritableClass adjacencyListNodeWritableClass = new AdjacencyListNodeWritableClass();

        //Node Flag is set to True
        adjacencyListNodeWritableClass.setIsNode(new BooleanWritable(true));

        //Set Global LinkPages to the new NodeClass Object
        adjacencyListNodeWritableClass.setLinkPageNames(linkPageNames);

        //Emit Page & NodeClass Object
        context.write(key,adjacencyListNodeWritableClass);
    }
}
