/**
  * Created by wangtuntun on 17-3-4.
  * 实现的主要功能是计算出每个商家每天的流量：(shop_id,DS,flow)
  */

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object clean {

  def main(args: Array[String]) {

    //设置环境
    val conf=new SparkConf().setAppName("tianchi").setMaster("local")
    val sc=new SparkContext(conf)
    val sqc=new SQLContext(sc)
    val user_pay_raw=sc.textFile("/home/wangtuntun/IJCAI/Data/user_pay.txt")
    val user_pay_split=user_pay_raw.map(_.split(","))
    val user_transform =user_pay_split.map{ x=>    //数据转换
      val userid=x(0)
      val shop_id=x(1)
      val ts=x(2)
      val ts_split=ts.split(" ")
      val year_month_day=ts_split(0).split("-")
      val year=year_month_day(0)
      val month=year_month_day(1)
      val day=year_month_day(2)
//      (shop_id,userid,year,month,day)
      (shop_id,userid,ts_split(0))
    }

    val df=sqc.createDataFrame(user_transform)  // 生成一个dataframe
    val df_name_colums=df.toDF("shop_id","userid","DS")  //给df的每个列取名字
    df_name_colums.registerTempTable("user_pay_table")     //注册临时表
    val sql="select shop_id ,count(userid),DS from user_pay_table group by shop_id,DS order by shop_id desc,DS"
    val rs =sqc.sql(sql)
    rs.foreach(x=>println(x))
//    user_transform.saveAsTextFile("/home/wangtuntun/test_file4.txt")
    val rs_rdd=rs.map( x => x(0) + ","+ x(2).toString + "," + x(1)  )         //rs转为rdd
    rs_rdd.coalesce(1,true).saveAsTextFile("/home/wangtuntun/ds_flow_raw_data.txt")
    sc.stop();

  }


}