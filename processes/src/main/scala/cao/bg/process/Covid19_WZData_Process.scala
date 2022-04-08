package cao.bg.process

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import cao.bg.util.OffsetUtils

import scala.collection.mutable
import com.alibaba.fastjson.{JSON, JSONObject}

/**
 * @Author : CGL
 * @Date : 2022 2022/1/5 20:43
 * @Desc : 疫情物资数据的实时处理与分析
 */
object Covid19_WZData_Process {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "G:\\Environment\\hadoop\\hadoop-winutilsmaster\\winutils-master\\hadoop-2.8.1")

    //1.准备SparkStreaming的开发环境
    val conf: SparkConf = new SparkConf().setAppName("Covid19_WZData_Process").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    // 设置日志级别
    sc.setLogLevel("WARN")
    // 每隔5s，就将实时到来的数据划分成一个一个的小批次
    // 底层实际上是微批处理，将源源不断的数据按一定的时间间隔，做小批次的划分
    val ssc: StreamingContext = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("./sscckp")

    //补充知识点:SparkStreaming整合Kafka的两种方式

    //1.Receiver模式
    //KafkaUtils.creatDStream--API创建
    //会有一个Receiver作为常驻Task运行在Executor进程中,一直等待数据的到来
    //一个Receiver效率会比较低,那么可以使用多个Receiver,但是多个Receiver中的数据又需要手动进行Union(合并)很麻烦
    //且其中某个Receiver挂了,会导致数据丢失,需要开启WAL预写日志来保证数据安全,但是效率又低了
    //Receiver模式使用Zookeeper来连接Kafka(Kafka的新版本中已经不推荐使用该方式了)
    //Receiver模式使用的是Kafka的高阶API(高度封装的),offset由Receiver提交到ZK中(Kafka的新版本中offset默认存储在默认主题
    //__consumer__offset中的,不推荐存入到ZK中了),容易和Spark维护在Checkpoint中的offset不一致
    //所以不管从何种角度去说Receiver模式都已经不再适合现如今的Kafka版本了,面试的时候要说出以上的因为!

    //2.Direct模式
    //KafkaUtils.createDirectStream--API创建
    //Direct模式是直接连接到Kafka的各个分区,并拉取数据,提高了数据读取的并发能力
    //Direct模式使用的是Kafka低阶API(底层API),可以自己维护偏移量到任何地方
    //(默认是由Spark提交到默认主题/Checkpoint)
    //Direct模式+手动操作可以保证数据的Exactly-Once精准一次(数据仅会被处理一次)

    //补充知识点:SparkStreaming整合Kafka的两个版本的API
    //Spark-streaming-kafka-0-8
    //支持Receiver模式和Direct模式,但是不支持offset维护API,不支持动态分区订阅..

    //Spark-streaming-kafka-0-10
    //支持Direct,不支持Receiver模式,支持offset维护API,支持动态分区订阅..
    //结论:使用Spark-streaming-kafka-0-10版本即可


    //2.准备kafka的连接参数
    val kafkaParams: Map[String, Object] = Map[String, Object] (
      elems = "bootstrap.servers" -> "ha150:9092,ha160:9092,ha170:9092",  // kafka集群地址
      "group.id" -> "SparkKafka", //自定义名字
      //latest表示如果记录了偏移量,则从记录的位置开始消费;如果没有记录则从最新/最后的位置开始消费
      //earliest表示如果记录了偏移量则从记录的位置开始消费,如果没有记录则从最开始/最早的位置开始消费
      //none表示如果记录了偏移量则从记录的位置开始消费,如果没有记录则报错
      "auto.offset.reset" -> "latest", //偏移量重置位置
      "enable.auto.commit" -> (false: java.lang.Boolean), //是否自动提交偏移量
      //scala序列化就是指把scala对象转换为字节序列的过程:
      //     序列化最重要的作用:在传递和保存对象时,保证对象的完整性和可传递性.
      //     对象转换为有序字节流,以便在网络上传输或者保存在本地文件中
      //反序列化就是指把字节序列恢复为scala对象的过程:
      //     反序列化的最重要的作用:根据字节流中保存的对象状态及描述信息,通过反序列化重建对象
      "key.deserializer" -> classOf[StringDeserializer],  //序列化
      "value.deserializer" -> classOf[StringDeserializer]
    )

    val topics: Array[String] = Array("covid19_wz")

    // 从MySQL中查询出offsets:Map[TopicPartition, Long]
    val offsetsMap: mutable.Map[TopicPartition, Long] = OffsetUtils.getOffsetsMap("SparkKafka","covid19_wz")
    val kafkaDS: InputDStream[ConsumerRecord[String,String]] = if(offsetsMap.size > 0) {
      println("MySQL记录了offset信息，从offset处开始消费")
      //3.连接kafka
      KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsetsMap)
      )
    } else {
      println("MySQL没有记录了offset信息，从latest处开始消费")
      //3.连接kafka
      KafkaUtils.createDirectStream[String,String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    }


    /* 测试使用
    //3.测试连接kafka
    val kafkaDS: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String,String](
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )
    */


    //4.实时处理数据
    //    val valueDS: DStream[String] = kafkaDS.map(_.value())  //_表示从Kafka中消费出来的每一条消息
    //    valueDS.print()
    //    (name=医用外科口罩/个, from=需求, count=273)
    //    (name=N95口罩/个, from=消耗, count=582)
    //    (name=电子体温计/个, from=下拨, count=416)
    //    (name=电子体温计/个, from=采购, count=353)
    //    (name=医用防护服/套, from=采购, count=10)
    //    (name=医用外科口罩/个, from=采购, count=513)
    //    (name=医用外科口罩/个, from=消耗, count=809)
    //    (name=医用防护服/套, from=消耗, count=851)
    //    (name=医用外科口罩/个, from=下拨, count=747)
    //    (name=一次性手套/副, from=下拨, count=917)
    // 从kafka中消费的数据为如上格式的jsonStr，需要解析为json对象(或者是样例类)
    // 目标是:将数据转化为如下格式:
    //      名称,     采购, 下拨,   捐赠,  消耗,   需求,   库存
    //    N95口罩/个, 1000, 1000,  500,   -1000, -1000,  500
    //    护目镜/副,   500,  300,  100,   -400,  -100,   400

    // 为了达成目标结果格式,我们需要对每一条数据进行处理,得出如下格式:
    // (name,(采购,下拨,捐赠,消耗,需求,库存))

    // 如:接收到一条数据为:
    // (name=医用防护服/套, from=采购, count=10)
    // 应记为:
    // (医用防护服/套,(采购10,下拨0,捐赠0,消耗0,需求0,库存10)

    // (name=医用防护服/套, from=消耗, count=851)
    // 应记为:
    // (医用防护服/套,(采购0,下拨0,捐赠0,消耗-851,需求0,库存-851))

    // 最后聚合结果:
    // (医用防护服/套,(采购10,下拨0,捐赠0,消耗-851,需求0,库存-841))

    // 4.1 将接收到的数据转换为需要的元组格式:(name,(采购,下拨,捐赠,消耗,需求,库存))
    val tupleDS: DStream[(String,(Int, Int, Int, Int, Int, Int))] = kafkaDS.map(record => {
      val jsonStr: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonStr)
      val name: String = jsonObj.getString("name")
      val from: String = jsonObj.getString("from")
      val count: Int = jsonObj.getInteger("count")
      // 根据物资来源不同，将count记在不同的位置，最终形成统一的格式
      from match {
        case "采购" => (name, (count, 0, 0, 0, 0, count))
        case "下拨" => (name, (0, count, 0, 0, 0, count))
        case "捐赠" => (name, (0, 0, count, 0, 0, count))
        case "消耗" => (name, (0, 0, 0, -count, 0, -count))
        case "需求" => (name, (0, 0, 0, 0, -count, -count))
      }
    })
    //tupleDS.print()
    // 输出结果如下:
    //    (84消毒液/瓶,(0,177,0,0,0,177))
    //    (N95口罩/个,(0,964,0,0,0,964))
    //    (N95口罩/个,(0,0,0,0,-329,-329))
    //    (医用外科口罩/个,(0,0,0,0,-662,-662))
    //    (电子体温计/个,(621,0,0,0,0,621))
    //    (护目镜/副,(0,435,0,0,0,435))
    //    (N95口罩/个,(0,0,0,-241,0,-241))
    //    (电子体温计/个,(164,0,0,0,0,164))
    //    (N95口罩/个,(0,868,0,0,0,868))
    //    (一次性手套/副,(209,0,0,0,0,209))

    // 4.2 将上述格式的数据按照key进行聚合(有状态的计算)--使用updateStateBykey
    // 定义一个函数,用来将当前批次的数据和历史数据进行聚合
    val updateFunc = (currentValues: Seq[(Int, Int, Int, Int, Int, Int)], historyValues: Option[(Int, Int, Int, Int, Int, Int)]) => {
      // 4.2.1 定义变量用来接收当前批次数据(采购,下拨,捐赠,消耗,需求,库存)
      var current_cg: Int = 0
      var current_xb: Int = 0
      var current_jz: Int = 0
      var current_xh: Int = 0
      var current_xq: Int = 0
      var current_kc: Int = 0
      if (currentValues.size > 0) {
        // 4.2.2 取出当前批次数据
        for (currentValue <- currentValues) {
          current_cg += currentValue._1
          current_xb += currentValue._2
          current_jz += currentValue._3
          current_xh += currentValue._4
          current_xq += currentValue._5
          current_kc += currentValue._6
        }
        // 4.2.3 取出历史数据
        // getOrElse: 没有取到数据，则为全0；如果取到数据，则将第(_n)个选项数据赋给前面的变量
        val history_cg: Int = historyValues.getOrElse((0, 0, 0, 0, 0, 0))._1
        val history_xb: Int = historyValues.getOrElse((0, 0, 0, 0, 0, 0))._2
        val history_jz: Int = historyValues.getOrElse((0, 0, 0, 0, 0, 0))._3
        val history_xh: Int = historyValues.getOrElse((0, 0, 0, 0, 0, 0))._4
        val history_xq: Int = historyValues.getOrElse((0, 0, 0, 0, 0, 0))._5
        val history_kc: Int = historyValues.getOrElse((0, 0, 0, 0, 0, 0))._6
        // 4.2.4 将当前批次数据和历史数据进行聚合
        val result_cg: Int = current_cg + history_cg
        val result_xb: Int = current_xb + history_xb
        val result_jz: Int = current_jz + history_jz
        val result_xh: Int = current_xh + history_xh
        val result_xq: Int = current_xq + history_xq
        val result_kc: Int = current_kc + history_kc
        //4.2.5 将聚合结果进行返回
        Some((
          result_cg,
          result_xb,
          result_jz,
          result_xh,
          result_xq,
          result_kc
        ))
      } else {    // 当前没有数据，则直接返回历史数据
        historyValues
      }
    }
    val resultDS: DStream[(String,(Int, Int, Int, Int, Int, Int))] = tupleDS.updateStateByKey(updateFunc)
    //resultDS.print()
    //(一次性手套/副,(52,699,936,-771,-723,193))
    //(电子体温计/个,(1203,1567,1424,-1372,-1421,1401))
    //(84消毒液/瓶,(2197,1915,1238,-2248,-152,2950))
    //(N95口罩/个,(1895,1394,1315,-3537,-1694,-627))
    //(医用外科口罩/个,(1840,2586,2609,-2192,-833,4010))
    //(医用防护服/套,(1496,610,306,-2347,-1583,-1518))
    //(护目镜/副,(1941,4709,1272,-2595,-554,4773))


    // 5. 将处理分析的结果存入到MySQL中
    /*
            CREATE TABLE `covid19_wz` (
              `name_GP` varchar(12) NOT NULL DEFAULT '',
              `cg_GP` int(11) DEFAULT '0',
              `xb_GP` int(11) DEFAULT '0',
              `jz_GP` int(11) DEFAULT '0',
              `xh_GP` int(11) DEFAULT '0',
              `xq_GP` int(11) DEFAULT '0',
              `kc_GP` int(11) DEFAULT '0',
              PRIMARY KEY (`name_GP`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
     */
    resultDS.foreachRDD(rdd => {
      rdd.foreachPartition(lines => {
        //1.开启连接
        val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8&serverTimezone=GMT%2B8", "root", "123456")
        //2.编写sql并获取ps
        val sql: String = "replace into covid19_wz(name_GP,cg_GP,xb_GP,jz_GP,xh_GP,xq_GP,kc_GP) values(?,?,?,?,?,?,?)"
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //3.设置参数并执行
        for (line <- lines) {
          ps.setString(1, line._1)    // 元组中的第一个元素
          ps.setInt(2, line._2._1)    // 元组中的第二个元素中的第一个元素
          ps.setInt(3, line._2._2)
          ps.setInt(4, line._2._3)
          ps.setInt(5, line._2._4)
          ps.setInt(6, line._2._5)
          ps.setInt(7, line._2._6)
          ps.executeUpdate()
        }
        //4.关闭资源
        ps.close()
        conn.close()
      })
    })



    // 6.手动提交偏移量
    // 实时处理数据，并且手动提交偏移量
    // 我们要手动提交偏移量，那么就意味着，消费了一批数据就应该提交一次偏移量
    // 在SparkStreaming中数据抽象为DStream，DStream的底层其实也就是RDD，也就是每一批次的数据
    // 所以接下来我们应该对DStream中的RDD进行处理
    kafkaDS.foreachRDD(rdd=> {
      if(rdd.count() > 0) {   // 如果该rdd中有数据则处理
        //rdd.foreach(record => println("从kafka中消费到的每一条消息" + record))
        // 从kafka中消费到的每一条消息ConsumerRecord(topic = covid19_wz, partition = 2, offset = 310, CreateTime = 1641525440079, checksum = 3571402895, serialized key size = -1, serialized value size = 3, key = null, value = 123)
        // 获取偏移量
        // 使用Spark-Streaming-kafka-0-10中封装好的API来存放偏移量并提交
        // Spark内部维护Kafka偏移量信息是存储在HasOffsetRanges类的offsetRanges中
        val offsets: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges    // 获取所有的偏移量
        /*        for (o <- offsets) {

                  // s是字符串拼接命名
                  println(s"topics=${o.topic},partition=${o.partition},fromOffset=${o.fromOffset},until=${o.untilOffset}");
        //          topics=covid19_wz,partition=0,fromOffset=287,until=288
        //          topics=covid19_wz,partition=1,fromOffset=305,until=305
        //          topics=covid19_wz,partition=2,fromOffset=312,until=314

        */
        // 手动提交偏移量到kafka的默认主题:_consumer_offsets中，如果开启了Checkpoint还会提交到Checkpoint
        //kafkaDS.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
        // 将偏移量提交到MySQL
        OffsetUtils.saveOffsets("SparkKafka",offsets)
      }
    })


    //7.开启SparkStreaming任务并等待结束
    ssc.start()
    ssc.awaitTermination()

  }
}
