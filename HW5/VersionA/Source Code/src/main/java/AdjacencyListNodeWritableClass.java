import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created by DJ on 2/15/17.
 * This is AdjacencyListNodeClass. It is used to store values for a page
 * It consists of a index for the page
 * Also, the pageRank Value and the set of outlink pages for a particular page.
 */
public class AdjacencyListNodeWritableClass implements Writable {


    //Set of Outlinks
    private Set<Text> linkPageNames;
    //Index
    private LongWritable index;

    //Default Constructor
    public AdjacencyListNodeWritableClass(){
        linkPageNames = new HashSet<>();
        index = new LongWritable(0);

    }


    //Getters & Setters
    public Set<Text> getLinkPageNames() {
        return linkPageNames;
    }

    public void setLinkPageNames(Set<Text> linkPageNames) {
        this.linkPageNames = linkPageNames;
    }

    public LongWritable getIndex() {
        return index;
    }

    public void setIndex(LongWritable index) {
        this.index = index;
    }

    //Hadoop read & write fields functions
    @Override
    public void write(DataOutput dataOutput) throws IOException {

        //Write the index
        index.write(dataOutput);

        // Write out the size of the Set
        dataOutput.writeInt(linkPageNames.size());

        // Write out each Text object
        for(Text t : linkPageNames) {
            t.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //Read index
        index.readFields(dataInput);

        //For reading a list of values
        linkPageNames = new HashSet<>();

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
    // " " separates the index and the outlinks list
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
        return  index.get()+" "+prefix+list+suffix;
    }
}
