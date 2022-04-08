package cao.bg.util

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row}

/**
 * @Author : CGL
 * @Date : 2022/1/10 12:37  
 * @Desc : 连接数据库
 */
abstract class BaseJdbcSink(sql: String) extends ForeachWriter[Row]{
  def realProcess(str: String, row: Row)

  var conn: Connection = _
  var ps: PreparedStatement = _

  // 开启连接
  override def open(partitionId: Long, version: Long): Boolean = {
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8&serverTimezone=GMT%2B8","root","123456")
    true      // 创建连接成功
  }

  // 处理数据
  override def process(value: Row): Unit = {
    realProcess(sql, value)
  }

  // 关闭连接
  override def close(errorOrNull: Throwable): Unit = {
    if (conn != null) {
      conn.close()
    }
    if (ps != null) {
      ps.close()
    }
  }

}
