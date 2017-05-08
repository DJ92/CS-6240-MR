import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.HashSet;
import java.util.Set;
/**
 * Created by DJ on 2/15/17.
 * This class is the PageRank Mapper Class
 * It parses every input record and emits the page along with it NodeClass Object containing the updated PageRank
 * It also accumulates the deltaValue for all sink nodes encountered to be used in the next iteration
 */
public class PageRankMapperClass extends Mapper<Object, Text, Text, AdjacencyListNodeWritableClass> {

    //Map Function
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            //NodeClass Object
            AdjacencyListNodeWritableClass adjacencyListNodeWritableClass = new AdjacencyListNodeWritableClass();

            //Parsing input record
            String[] sarr = value.toString().split(":");

            //PageName
            String pageName = sarr[0].trim();

            String[] objArr = sarr[1].split(" ");

            Double pageRank = 0.0;
            //If Page has some outlinks
            if (!objArr[1].equals("[]")) {
                //Get the string of set of outlink pages
                objArr[1] = objArr[1].substring(1, objArr[1].length() - 1);
                //If it has more than one pages
                if (objArr[1].contains("~")) {
                    //Split to get list of pages
                    String[] linkArr = objArr[1].split("~");
                    Set<Text> linkPageNames = new HashSet();
                    for (String link : linkArr) {
                        //Add to set
                        linkPageNames.add(new Text(link.trim()));
                    }
                    //Set the list of linkpages accumulated to the NodeClass Object
                    adjacencyListNodeWritableClass.setLinkPageNames(linkPageNames);
                } else {
                    Set<Text> linkPageNames = new HashSet();
                    //Add to set
                    linkPageNames.add(new Text(objArr[1].trim()));
                    //Set the list of linkpages accumulated to the NodeClass Object
                    adjacencyListNodeWritableClass.setLinkPageNames(linkPageNames);
                }
            }

            //IterationCount from the Configuration
            int iterationCount = context.getConfiguration().getInt("iterationCount", 0);

            //PageCount from the Configuration
            Long pageCount = context.getConfiguration().getLong("pageCount", 0);

            //For the 1st Iteration
            if (iterationCount < 1) {

                //PageRank = 1 / Total number of pages
                pageRank = 1.0 / pageCount;

            }else{//For all next consecutive iterations

                //Parse PageRank Value
                pageRank = Double.parseDouble(objArr[0].trim());

            }

            //If the page has no outlinks
            if (adjacencyListNodeWritableClass.getLinkPageNames().size() < 1) {

                //Accumulate pageRank into delta
                context.getCounter("deltaValue-Counter","deltaValue").increment((long) ((pageRank/pageCount)*Math.pow(10,15)));

                //Set pageRank to NodeClass Object
                adjacencyListNodeWritableClass.setPageRank(new DoubleWritable(pageRank));

                //Emit Page & NodeClass Object
                context.write(new Text(pageName), adjacencyListNodeWritableClass);

            } else {// If the page has outlinks

                //Set pageRank to NodeClass Object
                adjacencyListNodeWritableClass.setPageRank(new DoubleWritable(pageRank));

                //Emit Page & NodeClass Object
                context.write(new Text(pageName), adjacencyListNodeWritableClass);

                //PageRank for outgoing links
                pageRank = pageRank/adjacencyListNodeWritableClass.getLinkPageNames().size();

                //Iterate over all outgoing links
                for (Text link : adjacencyListNodeWritableClass.getLinkPageNames()) {

                    //New NodeClass Object
                    AdjacencyListNodeWritableClass adjacencyListNodeWritableClassPR = new AdjacencyListNodeWritableClass();

                    //turn Node Flag to False
                    adjacencyListNodeWritableClassPR.setIsNode(new BooleanWritable(false));

                    //Set new PageRank to NodeClass Object
                    adjacencyListNodeWritableClassPR.setPageRank(new DoubleWritable(pageRank));

                    //Emit Page & NodeClassObject
                    context.write(link, adjacencyListNodeWritableClassPR);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
