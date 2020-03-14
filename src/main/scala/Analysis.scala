import org.apache.spark.sql.SparkSession

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
    val dfetV = dfVertices.select("vertices._id", "vertices._label", "vertices.uri", "vertices._ctx", "vertices.category", "vertices.type")
    
    //create temporary view to be queried
    dfetV.createOrReplaceTempView("queryetV")
    //dfetV.count()
    
    //persist in memory the RDD
    val etlV = spark.sql("SELECT  * FROM queryetV WHERE _id BETWEEN 0 AND 61500").persist()
    //etlV.show()

    //select and explode all the fields in "edges" 
    val dfEdges = jsonFile.select(explode(jsonFile("edges"))).toDF("edges")

    //extract all the column of interest
    val dfetE = dfEdges.select("edges._id", "edges._label", "edges.uri", "edges._ctx", "edges._outV", "edges._inV")
    //create temporary view
    dfetE.createOrReplaceTempView("queryetE")
    //persist in memory
    val etlE = spark.sql("SELECT  * FROM queryetE WHERE _id BETWEEN 593 AND 95950").persist()
    //etlE.show()

    //////GRAPHX
    val sqlcustom4 = spark.sql("SELECT DISTINCT type FROM queryetl WHERE _id BETWEEN 0 AND 61500").show()










    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}