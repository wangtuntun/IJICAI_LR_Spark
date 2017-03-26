import java.io.{File, PrintWriter}

import scala.io.Source

/**
  * Created by wangtuntun on 17-3-6.
  * 该文件的主要目的是将文件ml_flow转换为LR需要的格式LR_format_data.txt
  */

object get_linear_regression_predict_data {
  def main(args: Array[String]): Unit = {
    val file=Source.fromFile("/home/wangtuntun//IJCAI/Data/ml_flow_raw_data_file.txt/part-00000")
    //city_list保存所有city_name
    var city_list: List[String] = List()
    //cate_list保存所有商家的分类
    var cate_list: List[String] = List()
    //info_list保存源文件的内容
    var info_list: List[String] = List()
    //遍历文件
    for(line <- file.getLines)
    {
      val split1=line.split("\\(")
      val split2=split1(1).split("\\)")
      val split3=split2(0).split(",")
      info_list = split3.dropRight(2).mkString(",") +: info_list
      city_list = split3(1) +: city_list
      cate_list = split3(7) +: cate_list
      cate_list = split3(8) +: cate_list
    }

    //关闭文件
    file.close

    //去掉list中重复的元素
    val info_list_distinct=info_list.distinct
    val city_list_distinct=city_list.distinct
    val cate_list_distinct=cate_list.distinct

    //文件的写入
    val writer = new PrintWriter(new File("/home/wangtuntun/IJCAI/Data/2016_11_1_to_14.txt"))
    //    for(i <- 1 to info_list.length) {
    info_list_distinct.foreach{x=>
      val split_list = x.split(",")
      var list_temp: List[String] = List()
      list_temp = split_list(0)  +: list_temp//shop_id
      list_temp = city_list_distinct.indexOf(split_list(1)).toString +: list_temp//city_name
      list_temp = split_list(2) +: list_temp
      list_temp = split_list(3) +: list_temp
      list_temp = split_list(4) +: list_temp
      list_temp = split_list(5) +: list_temp
      list_temp = split_list(6) +: list_temp
      list_temp = cate_list_distinct.indexOf(split_list(7)).toString +: list_temp//cate1
      list_temp = cate_list_distinct.indexOf(split_list(8)).toString +: list_temp//cate2
//      val ds_split=split_list(9).split("-")
//      list_temp = ds_split(0) +: list_temp//year
//      list_temp = ds_split(1) +: list_temp//month
//      list_temp = ds_split(2) +: list_temp//day
//      list_temp = split_list(10) +: list_temp//flow

      list_temp=list_temp.reverse
      val str_temp=list_temp.mkString(",")
//      writer.println(list_temp.mkString(","))
      for (day <- 1 to 14){
        writer.print(str_temp)//shop_info
        writer.print(",")
        writer.print("2016,11,")//year,month
        writer.print(day)//day
        writer.println(",0")//flow
      }
    }
    writer.close()
    //    info_list.foreach(println(_))
  }
}
