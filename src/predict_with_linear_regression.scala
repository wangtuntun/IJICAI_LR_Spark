/**
  * Created by wangtuntun on 17-3-7.
  * 利用linear_regression算法进行预测
  * 这个是官网代码，没有预测部分
  */
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
object predict_with_linear_regression {
  def main(args: Array[String]): Unit = {
    //设置环境
    val conf=new SparkConf().setAppName("tianchi").setMaster("local")
    val sc=new SparkContext(conf)
    val sqc=new SQLContext(sc)

    //加载数据
//    val training: DataFrame = sqc.read.format("libsvm").load("/opt/spark/spark-2.0.2-bin-hadoop2.7/data/mllib/sample_linear_regression_data.txt")
//    val training = sqc.read.format("libsvm").load("/opt/spark/spark-2.0.2-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt")
    val training = sqc.read.format("libsvm").load("/home/wangtuntun/lr_libsvm_data.txt")
    //设置模型参数
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)


    // 利用训练集训练模型
    val lrModel = lr.fit(training)
    val predict=lrModel.transform(training)
    val select_result=predict.select("features","label","prediction")
    select_result.foreach(println(_))
    // Print the coefficients and intercept for linear regression
//    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
//    val trainingSummary = lrModel.summary
//    println(s"numIterations: ${trainingSummary.totalIterations}")
//    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
//    trainingSummary.residuals.show()
//    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
//    println(s"r2: ${trainingSummary.r2}")


    sc.stop()
  }
}
