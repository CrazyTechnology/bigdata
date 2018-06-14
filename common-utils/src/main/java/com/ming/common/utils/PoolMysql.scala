package com.ming.common.utils

import java.sql.{DriverManager, PreparedStatement, ResultSet}

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

trait QueryCallback{
  def process(rs:ResultSet)
}
class MySqlProxy(url:String,username:String,password:String){

  private val mysqlClient = DriverManager.getConnection(url,username,password)

  /**
    * 增删改
    * @return
    */

  def executeUpdate(sql:String,params:Array[Any]):Int={
    var returnCode = 0
    var pstmt:PreparedStatement = null
    try{
      mysqlClient.setAutoCommit(false)
      pstmt = mysqlClient.prepareStatement(sql)

      if(params != null && params.length > 0){
        for(i<-params.indices){
          pstmt.setObject(i+1,params(i))
        }
      }
      returnCode = pstmt.executeUpdate()
      mysqlClient.commit()
    }catch{
      case e:Exception => e.printStackTrace()
    }
    returnCode
  }
  def delete(sql:String): Unit ={
    var pstmt:PreparedStatement = null
    pstmt = mysqlClient.prepareStatement(sql)
    pstmt.execute()
  }

  /**
    * 查询
    */
  def executeQuery(sql:String,params:Array[Any],queryCallback: QueryCallback): Unit ={
    var  pstmt :PreparedStatement=null
    var rs:ResultSet = null
    try{
      pstmt = mysqlClient.prepareStatement(sql)
      if(params!=null && params.length>0){
        for(i<-params.indices){
          pstmt.setObject(i+1,params(i))
        }
      }
      rs = pstmt.executeQuery()
      queryCallback.process(rs)
    }catch {
      case e:Exception=>e.printStackTrace()
    }
  }

  /**
    * 批量执行增删改
    */
  def executeBatch(sql:String,paramList:Array[Array[Any]]):Array[Int]={
    var returnCode:Array[Int] = null
    var pstmt:PreparedStatement = null

    try{
      mysqlClient.setAutoCommit(false)
      pstmt=mysqlClient.prepareStatement(sql)
      if(paramList !=null && paramList.length>0){
        for(params <- paramList){
          for(i <- params.indices){
            pstmt.setObject(i+1,params(i))
          }
          pstmt.addBatch()
        }
      }
      returnCode = pstmt.executeBatch()
      pstmt.clearBatch()
      mysqlClient.commit()
    }catch {
      case e:Exception =>e.printStackTrace()
    }
    returnCode
  }
}
class MySqlProxyFactory(url:String, username:String, password:String) extends BasePooledObjectFactory[MySqlProxy]{
  override def create(): MySqlProxy = new MySqlProxy(url,username,password)

  override def wrap(t: MySqlProxy): PooledObject[MySqlProxy] = new DefaultPooledObject[MySqlProxy](t)
}

object PoolMysql{

  Class.forName("com.mysql.jdbc.Driver")

  private var pool:GenericObjectPool[MySqlProxy] = null

  def apply(url: String,username:String,password:String): GenericObjectPool[MySqlProxy] = {
    val mySqlProxyFactory=new MySqlProxyFactory(url,username,password)
    this.pool = new GenericObjectPool[MySqlProxy](mySqlProxyFactory)
    this.pool
  }


  def main(args: Array[String]): Unit = {

  }

}