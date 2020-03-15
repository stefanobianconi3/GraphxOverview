import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Analysis {
  def main(args: Array[String]) {
       val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    
    /**
    Load the JSON file called demo.graph.json; 
    path: /home/muletto/Stefano/EDA/exam/demo.graph.json
    */
    val jsonFile = spark.read.json("/home/muletto/Stefano/EDA/exam/demo.graph.json")
    
    //jsonFile.show()
    //jsonFile.printSchema()

    //select and explode all the fields in "vertices" 
    val dfVertices = jsonFile.select(explode(jsonFile("vertices"))).toDF("vertices")
    
    //extract all the column of interest
    val dfetV = dfVertices.select("vertices._id", "vertices.menuLabel", "vertices.uri", "vertices._ctx", "vertices.category", "vertices.type")
    //val dfetV = dfVertices.select("vertices._id", "vertices.menuLabel", "vertices.type", "vertices.uri")

    //create temporary view to be queried
    dfetV.createOrReplaceTempView("queryetV")
    //dfetV.count()
    
    //persist in memory the RDD
    val etlV = spark.sql("SELECT  _id, menuLabel, type, uri  FROM queryetV WHERE _id BETWEEN 0 AND 61500").persist()
    //etlV.show()

    //select and explode all the fields in "edges" 
    val dfEdges = jsonFile.select(explode(jsonFile("edges"))).toDF("edges")

    //extract all the column of interest
    val dfetE = dfEdges.select("edges._id", "edges._label", "edges.uri", "edges._ctx", "edges._outV", "edges._inV")
   
    //create temporary view
    dfetE.createOrReplaceTempView("queryetE")
    //persist in memory
    val etlE = spark.sql("SELECT  _inV, _outV, _label FROM queryetE WHERE _id BETWEEN 593 AND 95950").persist()
    //etlE.show()

    //Show me the 
    val vertexTypes = spark.sql("SELECT DISTINCT type FROM queryetV WHERE _id BETWEEN 0 AND 61500").show()

    //remove the rows with same id
    etlV.dropDuplicates("_id")

    //create variable to be inserted later inside the grap
    val verticesG: RDD[(VertexId, (String, String, String))] = etlV.rdd.map(row => (row.getAs[Long]("_id"), (row.getAs[String]("menuLabel"), row.getAs[String]("type"), row.getAs[String]("type"))))

    val edgesE: RDD[Edge[String]] = etlE.rdd.map(row => Edge(row.getAs[Long]("_inV"), row.getAs[Long]("_outV"), row.getAs[String]("_label")))

    val graph = Graph(verticesG, edgesE)

    //graph.vertices.filter{ case (id, (a, b, c)) => b == "LuxMeter" }.count

    //create triplets from the menuLabel param.
    val facts: RDD[String] = graph.triplets.map(triplet => triplet.srcAttr._1 + " is " + triplet.attr + " of " + triplet.dstAttr._1)
    //print all the triplets
    facts.saveAsTextFile("/home/muletto/Desktop/dir")
    













    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}