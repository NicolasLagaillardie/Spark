import org.apache.spark.HashPartitioner

object HelloWorld {
	def main(args: Array[String]) {
		val lines = sc.textFile("text.txt")
		val Length = lines.map(s => s.length)
		val lengthCollected = Length.collect()
		println(lengthCollected.deep.mkString("\n"))
		val linesCollected = lines.collect()
		println(linesCollected.deep.mkString("\n"))
		val totalLength = Length.reduce((a, b) => a + b)
		val counts = lines.flatMap(line => line.split("\\s+")).map(word => (word, 1)).reduceByKey(_ + _)
		val countsCollected = counts.collect()
		println(countsCollected.deep.mkString("\n"))
		val lg5 = lines.flatMap(line => line.split(" ")).filter(_.length > 5)
		val lg5Collected = lg5.collect()
		println(lg5Collected.deep.mkString("\n"))
	}
}

HelloWorld.main(null)
