import org.apache.spark.sql.SparkSession

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
**/

object SparkPageRank {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <file> <iter>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("SparkPageRank")
      .getOrCreate()
	
	//Iterations number
    val iters = if (args.length > 1) args(2).toInt else 10

	//Get the links between each element
    val lines = spark.read.textFile(args(0)).rdd
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()

	//Get the URLs
    val linesIndex = spark.read.textFile(args(1)).rdd
    val index = linesIndex.map{ s =>
      val parts = s.split("\\s+")
      (parts(0))
    }.distinct().cache()
    val URLs = index.collect()

	//Assign 1.0 weight to each value
    var ranks = links.mapValues(v => 1.0)

	//Compute the new ranks
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

	//Display the results
    val output = ranks.collect()

    output.foreach{
		tup =>
		val element = URLs(tup._1.toInt)
		println(element + s" has rank:  ${tup._2}.")
	}

    spark.stop()
  }
}
//SparkPageRank.main(Array("example_arcs", "example_index","10"))

