/**
  * Created by wangtuntun on 17-3-4.
  * 实现的主要功能是计算出每个商家每天的流量：(shop_id,days_int,attr1,attr2....,flow)
  */

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object clean2 {

  def main(args: Array[String]) {

    //设置环境
    val conf=new SparkConf().setAppName("tianchi").setMaster("local")
    val sc=new SparkContext(conf)
    val sqc=new SQLContext(sc)

    //生成注册表 ds_flow_table
    val ds_flow_raw=sc.textFile("/home/wangtuntun/IJCAI/Data/ds_flow_raw_data.txt")
    val ds_flow_split=ds_flow_raw.map{line=>
      val split1=line.split("\\(")
      val split2=split1(1).split("\\)")
      val line_split=split2(0).split(",")
//      val line_split=line.split("(")(1).split(")")(0).split(",")
      ( line_split(0),line_split(1),line_split(2) )
    }
    val ds_flow_df=sqc.createDataFrame(ds_flow_split)  // 生成一个dataframe
    val ds_flow_df_name_columns=ds_flow_df.toDF("ds_flow_shop_id","DS","flow")  //给df的每个列取名字
    ds_flow_df_name_columns.registerTempTable("ds_flow_table")     //注册临时表

    // 生成注册表 shop_info
    val shop_info_raw=sc.textFile("/home/wangtuntun/IJCAI/Data/shop_info.txt")
    val shop_info_split=shop_info_raw.map{x=>
      var split_list=x.split(",")
      for(i <- 0 until split_list.length){
        if (split_list(i) == ""){
          split_list(i)="0"
        }
      }
      (split_list(0),split_list(1),split_list(2),split_list(3),split_list(4),split_list(5),split_list(6),split_list(7),split_list(8))
    }
//    shop_info_split.saveAsTextFile("/home/wangtuntun/shop_info_split1")//结果没问题
    val shop_info_df=sqc.createDataFrame(shop_info_split)
    val shop_info_df_name_columns=shop_info_df.toDF("shop_info_shop_id","city_name","location_id","per_pay","score","comment_cnt","shop_level","cate_1_name","cate_2_name")
    shop_info_df_name_columns.registerTempTable("shop_info_table")

    //开始执行sql语句
    val sql="select shop_info_shop_id,city_name,location_id,per_pay,score,comment_cnt,shop_level,cate_1_name,cate_2_name,DS,flow from ds_flow_table,shop_info_table where shop_info_shop_id = ds_flow_shop_id"
//    val sql="select shop_info_shop_id,city_name,location_id from shop_info_table "
//    val sql="select ds_flow_shop_id,DS,flow from ds_flow_table"
    val rs=sqc.sql(sql)
    rs.foreach(x=>println(x(0)))
    val rs_rdd=rs.map( x=>( x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10) ) )         //rs转为rdd
    rs_rdd.coalesce(1,true).saveAsTextFile("/home/wangtuntun/IJCAI/Data/ml_flow_raw_data_file.txt")
    sc.stop();

  }


}