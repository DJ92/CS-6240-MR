import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._
/**
 * @author DJ
  * This is the main Spark Driver Class
  * It has all the RDD's and its corresponding transformations
  * and parses each line using the Homework3 parser
 */
object PageRankAggregateJob {
  
  //Main Function
  def main(args : Array[String]) {

    //Handle invalid arguments
    if (args.length < 1) {
      System.err.println("Specify Input Directory")
      System.exit(1)
    }
    //Spark Configuration
    //This needs app name. The Master and other spark-defaults are set in the makefile
    val conf = new SparkConf().setAppName("PageRank")
    
    //Spark Context
    val sc = new SparkContext(conf)
    
    //EmptySet for outlink tuples
    val emptySet = scala.collection.mutable.Set[String]()
    
    //Alpha
    val alpha = 0.15
    
    //Initialize MainGraphRDD
    var inputPageRankGraphRDD: RDD[(String, (Double,scala.collection.mutable.Set[String] ))] = sc.emptyRDD

    //Parse every input record parallelized using the spark context
    //Also, filtered the invalid pageNames and links
    val input = sc.textFile(args(0)).map(line => line.split(":")(0) -> Bz2WikiParser.getLinkPages(line))
      .filter(x => x._2 != null)

    //Conversion from Java Set to Scala Set
    val inputRDD = input.map(x => x._1 -> x._2.asScala).persist

    //Burst the values of the above Graph into a list of nodes and combine with the above graph
    //This adds the sink nodes to the list
    val resultRDD = inputRDD
      .flatMap(_._2.map(link => (link,emptySet)))
      .union(inputRDD).reduceByKey((x,y) => x ++ y)

    //Get total page count from the consolidated RDD
    var pageCount = resultRDD.count

    //Map every graph tuple to Key,Value Pair as (PageName -> (Initial PageRank,Set of Outlinks)) in the Main Graph
    inputPageRankGraphRDD = resultRDD.map(x => x._1 -> ((1.0/pageCount), x._2)).persist

    //10 Iterations for PageRank Convergence
    for(i <- 1 to 10){
      //Delta Value Aggregator
      //We aggregate pagerank of all dangling nodes to form the delta factor for PR computation from the Main Graph
      val deltaValue = inputPageRankGraphRDD.filter(x => x._2._2.isEmpty).aggregate(0.0)(
        (acc,x) => (acc + x._2._1),
        (acc1,acc2) => (acc1+ acc2))

      //RDD for accumulating all inlink contributions from the consolidated set of outlinks
      //We reduce by key to accumulate the constributions
      //Each contribution is the (PR of the page/size of the outlinks set)
      val inlinkContributingRDD = inputPageRankGraphRDD.filter(x => x._2._2.nonEmpty).values.flatMap{
        case(x,y) =>
          y.map(link => link -> x/y.size)
      }.reduceByKey(_+_)

      //RDD for separating the dangling nodes that don't have any outlinks
      val danglingNodeRDD = inputPageRankGraphRDD.subtractByKey(inlinkContributingRDD)

      //TransformedRDD where we initialize the current page Rank of the dangling node to 0 for further computation
      val transformedDanglingRDD = danglingNodeRDD.map(x => x._1 -> (0.0))

      //Union of the RDD with Inlink contributions & Transformed Dangling Node RDD
      val allNewContributionsRDD = inlinkContributingRDD.union(transformedDanglingRDD)

      //Joined RDD for Union of Total Consolidated Contributions & Dangling Nodes with the Main Graph
      //New PR is computed and set to the tuple along with it's outlink set (PR,Set)
      inputPageRankGraphRDD = inputPageRankGraphRDD.join(allNewContributionsRDD)
        .map(x => x._1 -> ((alpha/pageCount) + (1-alpha)*(x._2._2+ deltaValue/pageCount),x._2._1._2)).persist

    }

    //Array for sorting by PageRank field in the descending order from every Tuple in the Main Graph and retrieve top 100
    val topK = inputPageRankGraphRDD.sortBy(_._2._1,false).map(x => x._1 -> x._2._1).take(100)

    //Form an RDD of the above array by using Spark Context and performing coalesce on the RDD pairs
    val topKRDD = sc.parallelize(topK,1).coalesce(1)

    //Save the Top 100 pages output to the output directory
    topKRDD.saveAsTextFile(args(1))
  }
}

