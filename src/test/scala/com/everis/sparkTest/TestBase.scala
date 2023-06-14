package com.everis.sparkTest
import com.holdenkarau.spark.testing._
import org.apache.spark._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}


trait TestBase extends BeforeAndAfterAll with SparkContextProvider {
    self: Suite =>

    @transient var _sparkSql: SparkSession = _
    @transient private var _sc: SparkContext = _
    @transient private var _sqlContext: SQLContext = _

    val loader = getClass.getClassLoader

    override def sc: SparkContext = _sc

    def conf: SparkConf

    def sparkSql: SparkSession = _sparkSql


    override def beforeAll() {
      _sparkSql = SparkSession.builder().config(conf).getOrCreate()
      super.beforeAll()
    }

    override def afterAll() {
      try {
        _sparkSql.close()
        _sparkSql = null
      } finally {
        super.afterAll()
      }
    }

  }
