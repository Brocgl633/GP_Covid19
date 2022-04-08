package cao.bg.process

import cao.bg.bean.{CovidBean, StatisticsDataBean}
import cao.bg.util.BaseJdbcSink
import com.alibaba.fastjson.JSON
import com.mysql.cj.result
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.immutable.StringOps
import scala.collection.mutable

/**
 * @Author : CGL
 * @Date : 2022 2022/1/5 20:40
 * @Desc : 全国各省市疫情数据实时处理统计分析
 */
object Covid19_Data_Process {
  def main(args: Array[String]): Unit = {
    // 1.创建StructuredStreaming执行环境
    // StructuredStreaming支持使用SQL来处理实时流数据,数据抽象和SparkSQL一样,也是DataFrame和DataSet
    // 所以这里创建StructuredStreaming执行环境就直接创建SparkSession即可
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Covid19_Data_Process").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    // 导入隐式转换方便后续使用
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import scala.collection.JavaConversions._

    // 2.链接Kafka
    // 从kafka接收消息
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ha150:9092, ha160:9092, ha170:9092")
      .option("subscribe", "covid19")
      .load()
    // 取出消息中的value
    // as[String]：将DF转化成String
    val jsonStrDS: Dataset[String] = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]
    /*jsonStrDS.writeStream
      .format(source = "console")   // 往控制台输出
      .outputMode(outputMode = "append")    // 输出模式，默认是append，表示新增行
      .trigger(Trigger.ProcessingTime(0))   // 出发间隔，0表示尽可能快地执行
      .option("truncate",false)   // 表示如果列名过长不进行截断
      .start()
      .awaitTermination()
    */

    //+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    //|value                                                                                                                                                                                                   |
    //+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    //|{"cityName":"西安","confirmedCount":2123,"curedCount":156,"currentConfirmedCount":1964,"datetime":"2022-01-09","deadCount":3,"locationId":610100,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}|
    //|{"cityName":"境外输入","confirmedCount":483,"curedCount":468,"currentConfirmedCount":15,"datetime":"2022-01-09","deadCount":0,"locationId":0,"pid":610000,"provinceShortName":"陕西","suspectedCount":4}      |
    //|{"cityName":"咸阳","confirmedCount":32,"curedCount":17,"currentConfirmedCount":15,"datetime":"2022-01-09","deadCount":0,"locationId":610400,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}     |
    //|{"cityName":"延安","confirmedCount":21,"curedCount":8,"currentConfirmedCount":13,"datetime":"2022-01-09","deadCount":0,"locationId":610600,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}      |
    //|{"cityName":"渭南","confirmedCount":18,"curedCount":17,"currentConfirmedCount":1,"datetime":"2022-01-09","deadCount":0,"locationId":610500,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}      |
    //|{"cityName":"安康","confirmedCount":26,"curedCount":26,"currentConfirmedCount":0,"datetime":"2022-01-09","deadCount":0,"locationId":610900,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}      |
    //|{"cityName":"汉中","confirmedCount":26,"curedCount":26,"currentConfirmedCount":0,"datetime":"2022-01-09","deadCount":0,"locationId":610700,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}      |
    //|{"cityName":"宝鸡","confirmedCount":13,"curedCount":13,"currentConfirmedCount":0,"datetime":"2022-01-09","deadCount":0,"locationId":610300,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}      |
    //|{"cityName":"铜川","confirmedCount":8,"curedCount":8,"currentConfirmedCount":0,"datetime":"2022-01-09","deadCount":0,"locationId":610200,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}        |
    //|{"cityName":"商洛","confirmedCount":7,"curedCount":7,"currentConfirmedCount":0,"datetime":"2022-01-09","deadCount":0,"locationId":611000,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}        |
    //|{"cityName":"榆林","confirmedCount":3,"curedCount":3,"currentConfirmedCount":0,"datetime":"2022-01-09","deadCount":0,"locationId":610800,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}        |
    //|{"cityName":"韩城","confirmedCount":1,"curedCount":1,"currentConfirmedCount":0,"datetime":"2022-01-09","deadCount":0,"locationId":610581,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}        |
    //|{"cityName":"杨凌","confirmedCount":1,"curedCount":1,"currentConfirmedCount":0,"datetime":"2022-01-09","deadCount":0,"locationId":0,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}             |
    //|{"cityName":"待明确地区","confirmedCount":0,"curedCount":245,"currentConfirmedCount":-245,"datetime":"2022-01-09","deadCount":0,"locationId":0,"pid":610000,"provinceShortName":"陕西","suspectedCount":0}     |
    //+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

    // 3.处理数据
    // 将jsonStr转为样例类
    val covidBeanDS: Dataset[CovidBean] = jsonStrDS.map(jsonStr => {
      // 注意：Scala中获取class对象使用classOf[类名]
      //      Java中使用类名.class/Class.forName(全类路径)/对象.getClass()
      JSON.parseObject(jsonStr, classOf[CovidBean])
    })

    // 分离出省份数据(省份数据有statisticsData)
    val provinceDS: Dataset[CovidBean] = covidBeanDS.filter(_.statisticsData != null)

    // 分离出城市数据(城市数据没有statisticsData)
    val cityDS: Dataset[CovidBean] = covidBeanDS.filter(_.statisticsData == null)

    // 分离出各省份每一天的统计数据(json格式)
    // flatMap：返回多行
    val statisticsDataDS: Dataset[StatisticsDataBean] = provinceDS.flatMap(p => {
      val jsonStr: StringOps = p.statisticsData //获取到的是该省份每一天的统计数据组成的jsonStr数组
       // 将jsonStr转化成数组，而数组里面所存的是后面classOf这个类的格式
      val list: mutable.Buffer[StatisticsDataBean] = JSON.parseArray(jsonStr, classOf[StatisticsDataBean])
      list.map(s => {
        s.provinceShortName = p.provinceShortName
        s.locationId = p.locationId
        s   // s返回
      })
    })

    /*
    statisticsDataDS.writeStream
      .format("console")//输出目的地
      .outputMode("append")//输出模式,默认就是append表示显示新增行
      .trigger(Trigger.ProcessingTime(0))//触发间隔,0表示尽可能快的执行
      .option("truncate",false)//表示如果列名过长不进行截断
      .start()
      .awaitTermination()
     */

    //+--------+-----------------+----------+--------------+---------------------+--------------------+-------------+----------+---------+------------------+--------------+---------+--------+
    //|dateId  |provinceShortName|locationId|confirmedCount|currentConfirmedCount|currentConfirmedIncr|confirmedIncr|curedCount|curedIncr|suspectedCountIncr|suspectedCount|deadCount|deadIncr|
    //+--------+-----------------+----------+--------------+---------------------+--------------------+-------------+----------+---------+------------------+--------------+---------+--------+
    //|20200123|陕西               |610000    |3             |3                    |3                   |3            |0         |0        |0                 |0             |0        |0       |
    //|20200124|陕西               |610000    |5             |5                    |2                   |2            |0         |0        |0                 |0             |0        |0       |
    //|20200125|陕西               |610000    |22            |22                   |17                  |17           |0         |0        |0                 |0             |0        |0       |
    //|20200126|陕西               |610000    |35            |35                   |13                  |13           |0         |0        |0                 |0             |0        |0       |

    // 4.统计分析
    // 4.1.全国疫情汇总信息:现有确诊,累计确诊,现有疑似,累计治愈,累计死亡--注意:按照日期分组统计
    val result1: DataFrame = provinceDS.groupBy('datetime)
      .agg(
        sum('currentConfirmedCount) as "currentConfirmedCount",   // 现有确诊
        sum('confirmedCount) as "confirmedCount",                 // 累计确诊
        sum('suspectedCount) as "suspectedCount",                 // 疑似确诊
        sum('curedCount) as "curedCount",                         // 累计治愈
        sum('deadCount) as "deadCount"                            // 累计死亡
      )

    // 4.2.全国各省份累计确诊数地图--注意:按照日期-省份分组
    /*cityDS.groupBy('datetime,'provinceShortName)
      .agg(sum('confirmedCount) as "confirmedCount")*/
    val result2: DataFrame = provinceDS.select(
      'datetime, 'locationId, 'provinceShortName, 'currentConfirmedCount,
      'confirmedCount, 'suspectedCount, 'curedCount, 'deadCount)

    // 4.3.全国疫情趋势--注意:按照日期分组聚合
    val result3: DataFrame = statisticsDataDS.groupBy('dateId)
      .agg(
        sum('confirmedIncr) as "confirmedIncr", //新增确诊
        sum('confirmedCount) as "confirmedCount", //累计确诊
        sum('suspectedCount) as "suspectedCount", //累计疑似
        sum('curedCount) as "curedCount", //累计治愈
        sum('deadCount) as "deadCount" //累计死亡
      )

    // 4.4.境外输入排行--注意:按照日期-城市分组聚合
    val result4: Dataset[Row] = cityDS.filter(_.cityName.contains("境外输入"))    // cityName中包含了“境外输入”
      .groupBy('datetime, 'provinceShortName, 'pid)
      .agg(sum('confirmedCount) as "confirmedCount")
      .sort('confirmedCount.desc)     // 降序

    // 4.5.统计湖北省的累计确诊地图
    val result5: DataFrame = cityDS.filter(_.provinceShortName.equals("湖北")).select(
      'datetime, 'locationId, 'provinceShortName, 'cityName,
      'currentConfirmedCount, 'confirmedCount, 'suspectedCount,
      'curedCount, 'deadCount
    )


    //5.结果输出--先输出到控制台观察,最终输出到MySQL
    //----------------------------------------------------------------------------------------
    //                                      Result1
    //----------------------------------------------------------------------------------------
    /*
    result1.writeStream
      .format("console")
      //输出模式:
      //1.append:默认的,表示只输出新增的数据,只支持简单的查询,不支持聚合
      //2.complete:表示完整模式,所有数据都会输出,必须包含聚合操作
      //3.update:表示更新模式,只输出有变化的数据,不支持排序
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()
      .awaitTermination()
     */

    // result1的建表语句：
    //CREATE TABLE `covid19_1` (
    //  `datetime` varchar(20) NOT NULL DEFAULT '',
    //  `currentConfirmedCount` bigint(20) DEFAULT '0',
    //  `confirmedCount` bigint(20) DEFAULT '0',
    //  `suspectedCount` bigint(20) DEFAULT '0',
    //  `curedCount` bigint(20) DEFAULT '0',
    //  `deadCount` bigint(20) DEFAULT '0',
    //  PRIMARY KEY (`datetime`)
    //) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    result1.writeStream
      .foreach(new BaseJdbcSink("replace into covid19_1 (datetime,currentConfirmedCount,confirmedCount,suspectedCount,curedCount,deadCount) values(?,?,?,?,?,?)") {
        override def realProcess(sql: String, row: Row): Unit = {
          //取出row中的数据
          val datetime: String = row.getAs[String]("datetime")
          val currentConfirmedCount: Long = row.getAs[Long]("currentConfirmedCount")
          val confirmedCount: Long = row.getAs[Long]("confirmedCount")
          val suspectedCount: Long = row.getAs[Long]("suspectedCount")
          val curedCount: Long = row.getAs[Long]("curedCount")
          val deadCount: Long = row.getAs[Long]("deadCount")
          //获取预编译语句对象
          ps = conn.prepareStatement(sql)
          //给sql设置参数值
          ps.setString(1, datetime)
          ps.setLong(2, currentConfirmedCount)
          ps.setLong(3, confirmedCount)
          ps.setLong(4, suspectedCount)
          ps.setLong(5, curedCount)
          ps.setLong(6, deadCount)
          ps.executeUpdate()
          }
        })
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()


    //----------------------------------------------------------------------------------------
    //                                      Result2
    //----------------------------------------------------------------------------------------

    /*
    result2.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()
      .awaitTermination()
    */

    // result2的建表语句：
    //CREATE TABLE `covid19_2` (
    //  `datetime` varchar(20) NOT NULL DEFAULT '',
    //  `locationId` int(11) NOT NULL DEFAULT '0',
    //  `provinceShortName` varchar(20) DEFAULT '',
    //  `currentConfirmedCount` int(11) DEFAULT '0',
    //  `confirmedCount` int(11) DEFAULT '0',
    //  `suspectedCount` int(11) DEFAULT '0',
    //  `curedCount` int(11) DEFAULT '0',
    //  `deadCount` int(11) DEFAULT '0',
    //  PRIMARY KEY (`datetime`,`locationId`)
    //) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    result2.writeStream
      .foreach(new BaseJdbcSink("replace into covid19_2 (datetime,locationId,provinceShortName,currentConfirmedCount,confirmedCount,suspectedCount,curedCount,deadCount) values(?,?,?,?,?,?,?,?)") {
        override def realProcess(sql: String, row: Row): Unit = {
          val datetime: String = row.getAs[String]("datetime")
          val locationId: Int = row.getAs[Int]("locationId")
          val provinceShortName: String = row.getAs[String]("provinceShortName")
          val currentConfirmedCount: Int = row.getAs[Int]("currentConfirmedCount")
          val confirmedCount: Int = row.getAs[Int]("confirmedCount")
          val suspectedCount: Int = row.getAs[Int]("suspectedCount")
          val curedCount: Int = row.getAs[Int]("curedCount")
          val deadCount: Int = row.getAs[Int]("deadCount")
          ps = conn.prepareStatement(sql)
          ps.setString(1, datetime)
          ps.setInt(2, locationId)
          ps.setString(3, provinceShortName)
          ps.setInt(4, currentConfirmedCount)
          ps.setInt(5, confirmedCount)
          ps.setInt(6, suspectedCount)
          ps.setInt(7, curedCount)
          ps.setInt(8, deadCount)
          ps.executeUpdate()
        }
      })
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()


    //----------------------------------------------------------------------------------------
    //                                      Result3
    //----------------------------------------------------------------------------------------

    /*
    result3.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()
      .awaitTermination()
    */

    // result3的建表语句：
    //CREATE TABLE `covid19_3` (
    //  `dateId` varchar(20) NOT NULL DEFAULT '',
    //  `confirmedIncr` bigint(20) DEFAULT '0',
    //  `confirmedCount` bigint(20) DEFAULT '0',
    //  `suspectedCount` bigint(20) DEFAULT '0',
    //  `curedCount` bigint(20) DEFAULT '0',
    //  `deadCount` bigint(20) DEFAULT '0',
    //  PRIMARY KEY (`dateId`)
    //) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    result3.writeStream
      .foreach(new BaseJdbcSink("replace into covid19_3 (dateId,confirmedIncr,confirmedCount,suspectedCount,curedCount,deadCount) values(?,?,?,?,?,?)") {
        override def realProcess(str: String, row: Row): Unit = {
          //取出row中的数据
          val dateId: String = row.getAs[String]("dateId")
          val confirmedIncr: Long = row.getAs[Long]("confirmedIncr")
          val confirmedCount: Long = row.getAs[Long]("confirmedCount")
          val suspectedCount: Long = row.getAs[Long]("suspectedCount")
          val curedCount: Long = row.getAs[Long]("curedCount")
          val deadCount: Long = row.getAs[Long]("deadCount")
          //获取预编译语句对象
          ps = conn.prepareStatement(str)
          // 给sql设置参数值
          ps.setString(1, dateId)
          ps.setLong(2, confirmedIncr)
          ps.setLong(3, confirmedCount)
          ps.setLong(4, suspectedCount)
          ps.setLong(5, curedCount)
          ps.setLong(6, deadCount)
          ps.executeUpdate()
        }
      })
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()


    //----------------------------------------------------------------------------------------
    //                                      Result4
    //----------------------------------------------------------------------------------------

    /*
    result4.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()
      .awaitTermination()
    */

    // result4的建表语句：
    //CREATE TABLE `covid19_4` (
    //  `datetime` varchar(20) NOT NULL DEFAULT '',
    //  `provinceShortName` varchar(20) NOT NULL DEFAULT '',
    //  `pid` int(11) DEFAULT '0',
    //  `confirmedCount` bigint(20) DEFAULT '0',
    //  PRIMARY KEY (`datetime`,`provinceShortName`)
    //) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    result4.writeStream
      .foreach(new BaseJdbcSink("replace into covid19_4 (datetime,provinceShortName,pid,confirmedCount) values(?,?,?,?)") {
        override def realProcess(sql: String, row: Row): Unit = {
          //取出row中的数据
          val datetime: String = row.getAs[String]("datetime")
          val provinceShortName: String = row.getAs[String]("provinceShortName")
          val pid: Int = row.getAs[Int]("pid")
          val confirmedCount: Long = row.getAs[Long]("confirmedCount")
          //获取预编译语句对象
          ps = conn.prepareStatement(sql)
          //给sql设置参数值
          ps.setString(1, datetime)
          ps.setString(2, provinceShortName)
          ps.setInt(3, pid)
          ps.setLong(4, confirmedCount)
          ps.executeUpdate()
        }
      })
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()


    //----------------------------------------------------------------------------------------
    //                                      Result5
    //----------------------------------------------------------------------------------------

    /*
    result5.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()
      .awaitTermination()
    */

    // result5的建表语句：
    //CREATE TABLE `covid19_5` (
    //  `datetime` varchar(20) NOT NULL DEFAULT '',
    //  `locationId` int(11) NOT NULL DEFAULT '0',
    //  `provinceShortName` varchar(20) DEFAULT '',
    //  `cityName` varchar(20) DEFAULT '',
    //  `currentConfirmedCount` int(11) DEFAULT '0',
    //  `confirmedCount` int(11) DEFAULT '0',
    //  `suspectedCount` int(11) DEFAULT '0',
    //  `curedCount` int(11) DEFAULT '0',
    //  `deadCount` int(11) DEFAULT '0',
    //  PRIMARY KEY (`datetime`,`locationId`)
    //) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    result5.writeStream
      .foreach(new BaseJdbcSink("replace into covid19_5 (datetime,locationId,provinceShortName,cityName,currentConfirmedCount,confirmedCount,suspectedCount,curedCount,deadCount) values(?,?,?,?,?,?,?,?,?)") {
        override def realProcess(sql: String, row: Row): Unit = {
          val datetime: String = row.getAs[String]("datetime")
          val locationId: Int = row.getAs[Int]("locationId")
          val provinceShortName: String = row.getAs[String]("provinceShortName")
          val cityName: String = row.getAs[String]("cityName")
          val currentConfirmedCount: Int = row.getAs[Int]("currentConfirmedCount")
          val confirmedCount: Int = row.getAs[Int]("confirmedCount")
          val suspectedCount: Int = row.getAs[Int]("suspectedCount")
          val curedCount: Int = row.getAs[Int]("curedCount")
          val deadCount: Int = row.getAs[Int]("deadCount")
          ps = conn.prepareStatement(sql)
          ps.setString(1, datetime)
          ps.setInt(2, locationId)
          ps.setString(3, provinceShortName)
          ps.setString(4, cityName)
          ps.setInt(5, currentConfirmedCount)
          ps.setInt(6, confirmedCount)
          ps.setInt(7, suspectedCount)
          ps.setInt(8, curedCount)
          ps.setInt(9, deadCount)
          ps.executeUpdate()
        }
      })
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()
      .awaitTermination()

  }
}









