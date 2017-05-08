import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by DJ on 2/17/17.
 * This class is the Parser Reducer Class
 * It receives all pages with corresponding NodeClass Objects
 */
public class ParserReducerClass extends Reducer<Text, AdjacencyListNodeWritableClass,Text, AdjacencyListNodeWritableClass> {
    MultipleOutputs<NullWritable, Text> mos;
    long ID;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        mos = new MultipleOutputs(context);
        ID = context.getConfiguration().getLong("ID", 0);
    }



    //Reduce Function
    @Override
    protected void reduce(Text key, Iterable<AdjacencyListNodeWritableClass> values, Context context) throws IOException, InterruptedException {
        ID = ID + 1;
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
        adjacencyListNodeWritableClass.setIndex(new LongWritable(ID));
        //Node Flag is set to True
        //Set Global LinkPages to the new NodeClass Object
        adjacencyListNodeWritableClass.setLinkPageNames(linkPageNames);
        //mos.write(NullWritable.get(), new Text(ID+ ":" + key), "ID2URL");
        //Emit Page & NodeClass Object
        context.write(key,adjacencyListNodeWritableClass);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mos.close();
    }
}
