/**
  * Created by wangtuntun on 17-3-8.
  * 利用线性回归模型对数据进行回归预测
  */


import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
object linnear_regression_online {
  def main(args: Array[String]): Unit = {

    //设置环境
    val conf=new SparkConf().setAppName("IJCAI").setMaster("local")
    val sc=new SparkContext(conf)
    val sqc=new SQLContext(sc)

    //准备训练集合
    val raw_data=sc.textFile("/home/wangtuntun/IJCAI/Data/lr_format_data.txt")
    val map_data=raw_data.map{x=>
      val split_list=x.split(",")
      (split_list(0).toDouble,split_list(1).toDouble,split_list(2).toDouble,split_list(3).toDouble,split_list(4).toDouble,split_list(5).toDouble,split_list(6).toDouble,split_list(7).toDouble,split_list(8).toDouble,split_list(9).toDouble,split_list(10).toDouble,split_list(11).toDouble,split_list(12).toDouble)
    }
    val df=sqc.createDataFrame(map_data)
    val data = df.toDF("Shop_id", "City_name", "Location_id", "Per_pay", "Score", "Comment_cnt", "Shop_level", "Cate_1_name","Cate_2_name","Year","Month","Day","Flow")
    val colArray = Array("Shop_id", "City_name", "Location_id", "Per_pay", "Score", "Comment_cnt", "Shop_level", "Cate_1_name","Cate_2_name","Year","Month","Day")
    val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
    val vecDF: DataFrame = assembler.transform(data)

    //准备预测集合
//    val raw_data_predict=sc.textFile("/home/wangtuntun/IJCAI/Data/2016_11_1_to_14.txt")
    val raw_data_predict=sc.textFile("/home/wangtuntun/IJCAI/Data/lr_format_data.txt")
    val map_data_for_predict=raw_data_predict.map{x=>
      val split_list=x.split(",")
      (split_list(0).toDouble,split_list(1).toDouble,split_list(2).toDouble,split_list(3).toDouble,split_list(4).toDouble,split_list(5).toDouble,split_list(6).toDouble,split_list(7).toDouble,split_list(8).toDouble,split_list(9).toDouble,split_list(10).toDouble,split_list(11).toDouble,split_list(12).toDouble)
    }
    val df_for_predict=sqc.createDataFrame(map_data_for_predict)
    val data_for_predict = df_for_predict.toDF("Shop_id", "City_name", "Location_id", "Per_pay", "Score", "Comment_cnt", "Shop_level", "Cate_1_name","Cate_2_name","Year","Month","Day","Flow")
    val colArray_for_predict = Array("Shop_id", "City_name", "Location_id", "Per_pay", "Score", "Comment_cnt", "Shop_level", "Cate_1_name","Cate_2_name","Year","Month","Day")
    val assembler_for_predict = new VectorAssembler().setInputCols(colArray_for_predict).setOutputCol("features")
    val vecDF_for_predict: DataFrame = assembler_for_predict.transform(data_for_predict)

    // 建立模型，预测谋杀率Murder
    // 设置线性回归参数
    val lr1 = new LinearRegression()
//    val lr2 = lr1.setFeaturesCol("features").setLabelCol("Murder").setFitIntercept(true)
    val lr2 = lr1.setFeaturesCol("features").setLabelCol("Flow").setFitIntercept(true)
    // RegParam：正则化
    val lr3 = lr2.setMaxIter(100).setRegParam(0.3).setElasticNetParam(0.8)
    val lr = lr3

    // 将训练集合代入模型进行训练
    val lrModel = lr.fit(vecDF)

    // 输出模型全部参数
    lrModel.extractParamMap()
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // 模型进行评价
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")


    val predictions: DataFrame = lrModel.transform(vecDF_for_predict)
    //    val predictions = lrModel.transform(vecDF)
    println("输出预测结果")
//    val predict_result: DataFrame =predictions.selectExpr("features","Murder", "round(prediction,1) as prediction")
    val predict_result: DataFrame =predictions.select("features","Flow", "prediction")
    predict_result.foreach(println(_))
    sc.stop()
  }
}
