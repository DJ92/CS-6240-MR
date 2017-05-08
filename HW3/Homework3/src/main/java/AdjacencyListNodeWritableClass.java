import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by DJ on 2/15/17.
 * This is AdjacencyListNodeClass. It is used to store values for a page
 * It consists of a flag to notify what does the object hold (PageRank or Node Object)
 * Also, the pageRank Value and the set of outlink pages for a particular page.
 */
public class AdjacencyListNodeWritableClass implements Writable {

    //Flag for Node or PageRank Object
    private BooleanWritable isNode;

    //PageRank Value
    private DoubleWritable pageRank;

    //Set of Outlinks
    private Set<Text> linkPageNames;

    //Default Constructor
    public AdjacencyListNodeWritableClass(){
        isNode=new BooleanWritable(true);
        pageRank = new DoubleWritable(0);
        linkPageNames = new HashSet<>();
    }

    //Parametrized Constructor
    public AdjacencyListNodeWritableClass(BooleanWritable isNode,Set<Text> linkPageNames, Double pageRank){
        this.isNode = isNode;
        this.linkPageNames = linkPageNames;
        this.pageRank = new DoubleWritable(pageRank);
    }

    //Getters & Setters
    public DoubleWritable getPageRank() {
        return pageRank;
    }

    public void setPageRank(DoubleWritable pageRank) {
        this.pageRank = pageRank;
    }

    public Set<Text> getLinkPageNames() {
        return linkPageNames;
    }

    public void setLinkPageNames(Set<Text> linkPageNames) {
        this.linkPageNames = linkPageNames;
    }

    public BooleanWritable getIsNode() {return isNode;}

    public void setIsNode(BooleanWritable isNode) {this.isNode = isNode;}


    //Hadoop read & write fields functions
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        isNode.write(dataOutput);
        pageRank.write(dataOutput);
        //For writing a list of values

        // Write out the size of the Set
        dataOutput.writeInt(linkPageNames.size());

        // Write out each Text object
        for(Text t : linkPageNames) {
            t.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        isNode.readFields(dataInput);
        pageRank.readFields(dataInput);
        linkPageNames = new HashSet<>();

        //For reading a list of values

        // Get the number of elements in the set
        int size = dataInput.readInt();

        // Read the correct number of Text objects
        for(int i=0; i<size; i++) {
            Text t = new Text();
            t.readFields(dataInput);
            linkPageNames.add(t);
        }
    }

    // Manipulated to handle data separation using special characters
    // "~" separates outlink pages
    // " " separates the pageRank and the outlinks list
    @Override
    public String toString() {
        String prefix="[",suffix="]";
        String list="";
        int i=0;
        for(Text t: linkPageNames){
            if(i==linkPageNames.size()-1)
                list+=t.toString();
            else if(linkPageNames.size()>1)
                    list+=t.toString()+"~";
            else
                list+=t.toString();
            i++;
        }
        return  pageRank + " " + prefix+list+suffix;
    }
}
