object HelloWorld {
	def main(args: Array[String]) {
		val lines = sc.textFile("text.txt");
		val Length = lines.map(s => s.length)
		Length.collect()
		lines.count()
		val totalLength = Length.reduce((a, b) => a + b)
		val counts = lines.flatMap(line => line.split("\\s+")).map(word => (word, 1)).reduceByKey(_ + _)
		counts.collect()
		val lg5 = lines.flatMap(line => line.split(" ")).filter(_.length > 5)
		lg5.collect()
	}
}
