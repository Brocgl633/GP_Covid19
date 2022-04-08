package cao.bg.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

/**
 * @Author : CGL
 * @Date : 2022 2022/1/8 10:13
 * @Desc : 用来手动维护偏移量到MySQL中的工具类
 */



object OffsetUtils {
  /**
   *
   * @param groupId : 消费者组名称
   * @param topic : 主题
   * @return : 偏移量信息封装成的Map
   */
  def getOffsetsMap(groupId: String, topic: String): mutable.Map[TopicPartition, Long] = {
    //1.获取连接
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8&serverTimezone=GMT%2B8","root","123456")
    //2.编写sql
    val sql:String = "select partitionGP,offsetGP from t_offset where groupidGP = ? and topicGP = ?"
    //3.获取ps
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //4.设置参数并执行
    ps.setString(1,groupId)
    ps.setString(2,topic)
    val rs: ResultSet = ps.executeQuery()
    //5.获取返回值并封装成Map
    val offsetsMap: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    while(rs.next()){
      val partition: Int = rs.getInt("partitionGP")
      val offset: Int = rs.getInt("offsetGP")
      // TopicPartition(topic,partition): 键
      // offset: 值
      offsetsMap += new TopicPartition(topic,partition) -> offset
    }

    //6.关闭资源
    rs.close()
    ps.close()
    conn.close()
    //7.返回Map
    offsetsMap
  }


  /**
   * 将消费者组的偏移量信息存入到MySQL
   *
   * @param groupId : 消费者组名称
   * @param offsets : 偏移量信息
   * 建表语句：
   *    CREATE TABLE `t_offset` (
   *    `topicGP` varchar(255) NOT NULL,
   *    `partitionGP` int(11) NOT NULL,
   *    `groupidGP` varchar(255) NOT NULL,
   *    `offsetGP` bigint(20) DEFAULT NULL,   // 紧接着该偏移量进行消费,不应用insert into,要用replace into
   *    PRIMARY KEY (`topicGP`,`partitionGP`,`groupidGP`)
   *    )ENGINE=InnoDB DEFAULT CHARSET=utf8;
   */
  def saveOffsets(groupId: String, offsets: Array[OffsetRange]) = {
    //1.加载驱动并获取连接
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8&serverTimezone=GMT%2B8","root","123456")
    //2.编写sql
    val sql:String = "replace into t_offset (topicGP,partitionGP,groupidGP,offsetGP) values(?,?,?,?)"
    //3.创建预编译语句对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    //4.设置参数并执行
    for (o<-offsets){
      ps.setString(1,o.topic)
      ps.setInt(2,o.partition)
      ps.setString(3,groupId)
      ps.setLong(4,o.untilOffset)     // 消费在此处，下次从此处开始
      ps.executeUpdate()
    }
    //5.关闭资源
    ps.close()
    conn.close()
  }



}
