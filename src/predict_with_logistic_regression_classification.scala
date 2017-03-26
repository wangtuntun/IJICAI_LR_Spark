/**
  * Created by wangtuntun on 17-3-7.
  * 将数据代入模型进行预测
  * 本来想用logistic regression做回归的，结果点开的是官网的classification下的代码
  * 而且还是只支持二分类的代码
  */
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
object predict_with_logistic_regression_classification {
  def main(args: Array[String]): Unit = {
    // Prepare training data from a list of (label, features) tuples.
    val conf=new SparkConf().setAppName("tianchi").setMaster("local")
    val sc=new SparkContext(conf)
    val sqc=new SQLContext(sc)
    //Load TrainData
    //Seq类型是有顺序的数据结构
    //training为dataFrame
    val training = sqc.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")//转变为数据框
//    val raw_data=sc.textFile("/home/wangtuntun/IJCAI/Data/lr_format_data.txt")
//    val map_data=raw_data.map{line=>
//      val list_split=line.split(",")
//      val mylable=list_split.last.toDouble //最后一个元素
//      val features=list_split.dropRight(1)//除去右边第一个的所有剩余元素
//      var myarr:Array[Double]=Array()
//      features.foreach{x=>
//        myarr = myarr :+ x.toDouble
//      }
//      val myvector=Vectors.dense(myarr)
//      (mylable,myvector)
//    }
//    val training=sqc.createDataFrame(map_data).toDF("label","features")
    // 创建logistics regression实例
    val lr = new LogisticRegression()
    // Print out the parameters, documentation, and any default values.
    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

    //重新设置模型参数方法
    lr.setMaxIter(10)//最大迭代步数
      .setRegParam(0.01)

    // 根据设定的模型参数与training data拟合训练得到模型
    val model1 = lr.fit(training)
    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
    // we can view the parameters it used during fit().
    // This prints the parameter (name: value) pairs, where names are unique IDs for this
    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

//    val testData = sqlContext.createDataFrame(Seq(
//      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
//      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
//      (1.0, Vectors.dense(0.0, 2.2, -1.5)),
//      (1.0, Vectors.dense(2.0, 2.2, -1.0))
//    )).toDF("label", "features")
//

    model1.transform(training)
      .select("features", "label", "probability", "prediction")//选择数据框的某些列
      .collect()//一般在filter或者足够小的结果的时候，再用collect封装返回一个数组
      .foreach(println(_))

    sc.stop()
  }
}
