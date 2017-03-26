/**
  * Created by wangtuntun on 17-3-5.
  */
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.regression.IsotonicRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import java.io.{File, PrintWriter}
import scala.io.Source
object get_libsvm_data {
  def main(args: Array[String]): Unit = {

    val file=Source.fromFile("/home/wangtuntun/IJCAI/Data/2016_11_1_to_14.txt")
    val writer = new PrintWriter(new File("/home/wangtuntun/IJCAI/Data/lr_libsvm_predict_data.txt"))
    for(line <- file.getLines)
    {
      val line_split=line.split(",")
      writer.print(line_split(0).toString + " ")
      for(i <- 1 to line_split.length-1){
        writer.print(i)
        writer.print(":")
        writer.print(line_split(i).toDouble)//如果不转为Double，会有空格产生，导致数据读取错误
        writer.print(" ")
      }
      writer.print("\n")
    }
    //关闭文件
    file.close
    writer.close()
  }
}
