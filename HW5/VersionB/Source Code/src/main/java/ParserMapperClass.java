import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Created by DJ on 2/15/17.
 * This class is the Parser Mapper Class
 * It parses every input record and emits the page along with it NodeClass Object
 */
public class ParserMapperClass extends Mapper<Object,Text,Text, AdjacencyListNodeWritableClass>{

    //Map Function
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            //Split to get Page Name
            String pageName = value.toString().split(":")[0];

            //Get the parsed HTML into NodeClass object consisting of pageRank and set of outlinks
            AdjacencyListNodeWritableClass adjacencyListNodeWritableClass = Bz2WikiParser.getLinkPagesWritable(value.toString());

            //If all parsing and sanity conditions were satisfied by the corresponding HTML
            if(adjacencyListNodeWritableClass != null){
                //Emit Page and Node
                context.write(new Text(pageName),adjacencyListNodeWritableClass);
                //If Page consists of outlinks
                if(adjacencyListNodeWritableClass.getLinkPageNames() != null){
                    for(Text t: adjacencyListNodeWritableClass.getLinkPageNames()){
                        //Emit OutLink Page with Default Node
                        //This is done to accumulate sink nodes in the comprehensive list of pages
                        AdjacencyListNodeWritableClass adjacencyListLinkNodeWritableClass = new AdjacencyListNodeWritableClass();
                        context.write(t,adjacencyListLinkNodeWritableClass);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
