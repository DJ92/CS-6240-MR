import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by DJ on 3/23/17.
 * This is the Make M Mapper Class
 * It emits the page as Node
 * Also, emits the outlinks as Inlinks to a page
 */
public class MakeNewMMapperClass extends Mapper<Object, Text, Text, Text> {

    //Map Function
    public void map(Object _k, Text value, Context context) throws IOException, InterruptedException{

        //Record Parsing
        String input = value.toString();
        String[] lines = input.split(":");

        //PageName
        String pageName = lines[0].trim();
        //Index
        Long ID = Long.parseLong(lines[1].split(" ")[0]);

        //Set of Outlinks
        String setOfOutlinks = lines[1].split(" ")[1];

        //Dangling Node
        if(setOfOutlinks.trim().equals("[]")){
            //Emit Page as Dangling Node
            //For Ex: A: [] -> A,N,1,0,0,true
            context.write(new Text(pageName.trim()), new Text("N"+","+ID+","+0+","+0+","+true));
        }else{
            //Emit Page as Node
            //For Ex: A: [B,C,D] -> A,N,1,0,0,false
            context.write(new Text(pageName.trim()), new Text("N"+","+ID+","+0+","+0+","+false));
        }
        //Parse Outllinks
        String[] links = setOfOutlinks.substring(1, setOfOutlinks.length()-1).split("~");
        String lineS = setOfOutlinks.substring(1, setOfOutlinks.length()-1);

        //Iterate through Outlinks
        for(String l:links){
            if(lineS.length() >= 1){
                //Emit Outlink and Contribution
                //For Ex: A:[B,C] -> (B,1,0,3,false)(C,1,0,3,false)
                Text m = new Text("A"+","+ID+","+0+","+links.length+","+false);
                context.write(new Text(l.trim()), m);
            }
        }
    }
}
