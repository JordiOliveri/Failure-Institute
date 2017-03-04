import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object FailureInstitute extends App{
	val datos1 = "BaseDatos1.csv"
	val conf = newSparkConf().setAppName("FailureInstitute")
	val sc = newSparkContext(conf)

	val base1 = sc.textFile(datos1)
	val WordCount = base1.flatMap(linea => linea.split(",")).map(palabra => (palabra, 1)).reduceByKey((a, b) => a + b)
	WordCount.collect()
}