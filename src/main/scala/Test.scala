import org.apache.avro.Schema
import org.apache.log4j.Level
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.Window
import org.apache.spark.sql.functions.{array, ceil, coalesce, col, current_timestamp, desc, element_at, explode, lit, rand, row_number, udf, when}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

import scala.util.Random
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.io.File
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try


object Test {


  def main(args: Array[String]): Unit = {
    val spark=Util.spark
    Util.LogLevel(Level.OFF)

    val logger: Logger = LoggerFactory.getLogger(getClass)
    logger.warn(s"This is the start....")
    def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

//    val x1=Seq("ABC","DEF","RED","CAT")
//    val x2=Seq("def","cat")
//    val x3=x1.filter(x => !x2.contains(x.toLowerCase()))
//    x3.foreach(println)
    if(true) {
      val modified_timestamp=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now().minusDays(3))
      val headerList=Seq("REFRESH","UPDATE","INSERT","DELETE")
      val headerList2=Array("REFRESH","UPDATE","INSERT","DELETE")
      val csvDF = spark.read.format("csv").option("header", true).load("C:\\Users\\15148\\IdeaProjects\\executeEngine\\src\\main\\resources\\CSV/Sales_Records.csv")
      val csvmodDF=csvDF.limit(10).withColumn("modified_timestamp",lit(modified_timestamp)).withColumn("headerOperation",element_at(array(headerList2.map(lit(_)):_*),lit(ceil(rand()*headerList2.size)).cast("int")))
    //  csvmodDF.show(10)
      // val csvModDF = csvDF.withColumn("modified_timestamp",lit(2022))
  val incDF = spark.read.format("csv").option("header", true).load("C:\\Users\\15148\\IdeaProjects\\executeEngine\\src\\main\\resources\\CSV/SalesRecords2.csv")
      val incmodified_timestamp=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now())
      val incmodDF=incDF.limit(10).withColumn("modified_timestamp",lit(incmodified_timestamp)).withColumn("headerOperation",element_at(array(headerList2.map(lit(_)):_*),lit(ceil(rand()*headerList2.size)).cast("int")))
      // val colCount = csvDF.columns.length
 // println(colCount)
      import spark.implicits._
      import org.apache.spark.sql.functions._

      //      val red=Seq((11,"tr"),(22,"yu")).toDF("a","b")
//      red.show()
      val schema=incmodDF.schema
     // schema.foreach(println())
     // val finList=Seq((null,"Region"),(null,"Country"),(null,"Item Type"),(null,"Sales Channel"),(null,"Order Priority"),(null,"Order Date"),(null,"Order ID"), (null,"Ship Date"), (null,"Units Sold"),(null,"Unit Price"),(null,"Unit Cost"),(null,"Total Revenue"),(null,"Total Cost"),(null,"Total Profit"),(null,"modified_timestamp"), ("UPDATE","headerOperation"))
      val unkList=Seq((null,null,null,null,null,null,null,null,100,null,null,null,null,null,null,"UPDATE"),(null,null,null,"TEST1",null,null,null,null,200,null,null,null,null,null,null,"UPDATE"),("HERO",null,null,"TEST3",null,null,null,null,300,null,null,null,null,null,null,"UPDATE")).toDF("Region","Country","Item Type","Sales Channel","Order Priority","Order Date","Order ID","Ship Date","Units Sold","Unit Price","Unit Cost","Total Revenue","Total Cost","Total Profit","modified_timestamp","headerOperation")
      val unkbaseList=Seq(("Nishanth","India","Engineer","online","M","4/10/2010",null,"123456",100,444.4,555.5,666.6,987456.12,456123.78,"2022-04-04 13:52:18","INSERT"),("Earth","Solar System","Universe","offline","L","5/12/2010",null,"741852",200,111.1,333.3,777.7,10083.12,48523.78,"2022-03-02 13:52:14","INSERT"),("ABC","ert","tipper","online","H","6/06/2030",null,"95123",300,123.4,456.5,789.6,785412.12,852147.78,"2022-05-01 15:22:48","INSERT")).toDF("Region","Country","Item Type","Sales Channel","Order Priority","Order Date","Order ID","Ship Date","Units Sold","Unit Price","Unit Cost","Total Revenue","Total Cost","Total Profit","modified_timestamp","headerOperation")

      //   val testDF=finList.toDF()
     // unkList.show()
     val finDF=incmodDF.unionByName(unkList)
     // finDF.show(15)
  val finbaseDF=csvmodDF.unionByName(unkbaseList)
      finbaseDF.show()
      val colList=finDF.columns

      val filterDF=finDF.filter(finDF("headerOperation")==="UPDATE")
      filterDF.show()

      val runkeyDF=filterDF.select("Units Sold").distinct().map(f=>f.getString(0)).collect.toList

      runkeyDF.foreach(println)
      val insertValueDF=finbaseDF.filter(col("Units Sold").isin(runkeyDF:_*))

     // insertValueDF.show()
      val prefixBase2 = "_o"
      val renamedbaseColumns2 = colList.map(c=> insertValueDF(s"$c").as(s"$c$prefixBase2"))
//      renamedbaseColumns2.foreach(println)
      val insertValueRenameDF = insertValueDF.select(renamedbaseColumns2: _*)
    //  insertValueRenameDF.show()

      val joinDF=filterDF.as('a).join(insertValueRenameDF.as('b),(filterDF("Units Sold")<=>insertValueRenameDF("Units Sold_o")))

      joinDF.show()
      def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess


      val foldDF: DataFrame = colList.foldLeft(joinDF) { (acc:DataFrame, colName:String) =>
        acc.withColumn(colName+"_j", hasColumn(filterDF,colName) match {
          case true=> coalesce(col(colName), col(colName+"_o"))
          case false=>col(colName)
        }).drop(colName+"_o").drop(colName).withColumnRenamed(colName+"_j",colName)
      }
println("Final FOLDLEFT: ")
      foldDF.show()
println("incremental data before merge: ")
      finDF.show()
val modifiedRunKeyList=foldDF.select("Units Sold").map(f=>f.getString(0)).collect.toSeq
println("Modied UPDATE LIST: ")
modifiedRunKeyList.foreach(println)
// val finWithooutModUpdateDF=finDF.filter(x => !modifiedRunKeyList.contains(x))
      val tempDF=finDF.filter(col("Units sold").isin(modifiedRunKeyList:_*))
     // tempDF.show()
val finWithooutModUpdateDF=finDF.except(tempDF)
     //
      // finWithooutModUpdateDF.show()
      println("incremental data AFTER merge: ")
val finalDF=finWithooutModUpdateDF.unionByName(foldDF)
      finalDF.show()
      /*
      val allDF=csvmodDF.unionByName(incmodDF)
            allDF.show(20)
           // allDF.orderBy(desc("modified_timestamp")).show(10)
            val uniqueKeyList=Seq("Order ID","Item Type","Country")
          val allrowDF=allDF.withColumn("headerType",
               when(col("headerOperation") === "REFRESH",lit(1).cast(IntegerType))
              .when(col("headerOperation") === "INSERT",lit(2).cast(IntegerType))
              .when(col("headerOperation") === "UPDATE",lit(3).cast(IntegerType))
              .when(col("headerOperation") === "DELETE",lit(4).cast(IntegerType))
              .otherwise(lit(0).cast(IntegerType)))

            allrowDF.show()
            import org.apache.spark.sql.expressions.Window

      val windowFunction=Window.partitionBy(uniqueKeyList.head,uniqueKeyList.tail:_*).orderBy(
        desc("headerType"),
          col("Region").desc_nulls_last,
        desc("modified_timestamp")
      )
            val matDF=allrowDF.withColumn("row_number_helper",row_number.over(windowFunction))
              .where(s"row_number_helper == 1")
              .filter(s"headerOperation != 'DELETE'")
              .drop("headerOperation")
              .drop("row_number_helper")

            matDF.show()
      */



/*

  incDF.show()

  val uniqueKeyList = Seq("c_1", "c_2")
  val joinExprs = uniqueKeyList.zip(uniqueKeyList).map { case (x1, x2) => csvDF(x1) === incDF(x2 + "_i") }.reduce(_ && _)
  println(joinExprs)
  val colList = csvDF.columns

  val unionDF = csvDF.join(incDF, joinExprs, "outer")
  unionDF.show(3)

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  /* Stackoverflow Error
    val foldDF: DataFrame = colList.foldLeft(unionDF) { (acc:DataFrame, colName:String) =>
      acc.withColumn(colName+"_j", coalesce(col(colName+"_i"), col(colName)))
        .drop(colName+"_i").drop(colName).withColumnRenamed(colName+"_j",colName)
    }*/
  // Original Stackoverflow Error
    /*
      val foldDF: DataFrame = colList.foldLeft(unionDF) { (acc:DataFrame, colName:String) =>
      acc.withColumn(colName+"_j", hasColumn(unionDF,colName+"_i") match {
        case true=> coalesce(col(colName+"_i"), col(colName))
        case false=>col(colName)
    }).drop(colName+"_i").drop(colName).withColumnRenamed(colName+"_j",colName)
    }
    */

  //Working Model
//  val foldDF: DataFrame = colList.foldLeft(unionDF) { (acc: DataFrame, colName: String) =>
//    acc.withColumn(colName,  hasColumn(unionDF,colName+"_i") match {
//      case true=> coalesce(col(colName+"_i"), col(colName))
//      case false=>col(colName)
//    }).drop(colName + "_i")}
//  // baseDF.show(3)
//  foldDF.show(3)

      //Working Model 2
      val fDF3: DataFrame = colList.foldLeft(unionDF) { (acc:DataFrame, colName:String) =>
        acc.withColumn(colName+"_j", hasColumn(unionDF,colName+"_i") match {
          case true=> coalesce(col(colName+"_i"), col(colName))
          case false=>col(colName)
        }).drop(colName+"_i")
      }
      val f2D=fDF3.drop(colList:_*)
     // val prefixBase2 = "_j"
    //  val renamedbaseColumns2 = colList.map(c=> f2D(s"$c$prefixBase2").as(s"$c"))
      val fDF4 = f2D.toDF(colList:_*)
      fDF4.show()
      //foldDF.except(fDF4).show()

      //Working Model 3
      val fDF: DataFrame = colList.foldLeft(unionDF) { (acc:DataFrame, colName:String) =>
        acc.withColumn(colName, hasColumn(unionDF,colName+"_i") match {
          case true=> coalesce(col(colName+"_i"), col(colName))
          case false=>col(colName)
        }).drop(colName+"_i")
      }
println("comparision: ")
      fDF4.except(fDF)

      */
}
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._




    if(false){
    //METHOD 1 directly read avro files
    val baseDF=spark.read.format("com.databricks.spark.avro").load("C:\\Users\\15148\\IdeaProjects\\executeEngine\\src\\main\\resources\\AVRO/userdata1.avro")
    //Thread.sleep(2000)
    val incDF=spark.read.format("com.databricks.spark.avro").load("C:\\Users\\15148\\IdeaProjects\\executeEngine\\src\\main\\resources\\AVRO/userdata2.avro")

    //baseDF.show(1)
//    val dupbaseDF=baseDF.withColumn("rowNum", when(col("salary")>50000,23)
//      . when(col("salary")<50000,50)
//      .otherwise(56))
    import org.apache.spark.sql.expressions.Window
 //   import scala.collection.mutable._
   // import scala.io.Source

   // val base2DF= baseDF.select(baseDF.col("c").alias("c_i"))

    val prefixBase = "_j"
   // val renamedbaseColumns = baseDF.columns.filterNot(_ == "timestamp").map(c=> baseDF(c).as(s"$c$prefixBase"))
    val renamedbaseColumns = baseDF.columns.map(c=> baseDF(c).as(s"$c$prefixBase"))
    val baseAllDF = baseDF.select(renamedbaseColumns: _*)
 //  val baseAllDF=baseRenamedDf.withColumnRenamed("timestamp_j","timestamp")

    val prefix = "_i"
    val renamedColumns = incDF.columns.map(c=> incDF(c).as(s"$c$prefix"))
    val incrementalDF = incDF.select(renamedColumns: _*)
      val basedupDF=baseDF.withColumn("modified_timestamp",lit(2022)).drop("comments")
      val incrDF=incrementalDF.drop("comments_i")
  //  val unionDF = baseAllDF.union(incrementalDF)
//
  // baseAllDF.show(3)
   // incrementalDF.show(3)
//    println(baseAllDF.count())
//    println(incrementalDF.count())
//    println(unionDF.count())
 // val newDF= unionDF.select(when(unionDF("timestamp")==date_sub(current_timestamp(),1))
val colList = baseDF.columns
    //colList.foreach(println)
    val uniqueKeyList=Seq("id")
//uniqueKeyList.foreach(println)
//    val s1=uniqueKeyList.zip(uniqueKeyList)
//    s1.foreach(println)
//    val s2=uniqueKeyList.zip(uniqueKeyList).map{case(x1,x2)=>baseAllDF(x1+"_j")===incrementalDF(x2+"_i")}
//    s2.foreach(println)
    val joinExprs=uniqueKeyList.zip(uniqueKeyList).map{case(x1,x2)=>baseDF(x1)===incrementalDF(x2+"_i")}.reduce(_&&_)
   // println(joinExprs)
val colList2=basedupDF.columns
  val unionDF = baseDF.join(incrementalDF,joinExprs,"outer")
      val union2DF = basedupDF.join(incrDF,joinExprs,"outer")

      //val nullDF:DataFrame=baseDF.withColumn("litCol",lit(null))
  //val orgDF=baseDF.withColumn("timestamp",current_timestamp())
  //unionDF.show(3)

//  val foldDF: DataFrame = colList.foldLeft(unionDF) { (acc:DataFrame, colName:String) =>
//    acc.withColumn(colName, coalesce(col(colName+"_i"), col(colName+"_j")))
//      .drop(colName+"_i")
//      .drop(colName+"_j")
//  }
val foldDF: DataFrame = colList2.foldLeft(union2DF) { (acc:DataFrame, colName:String) =>
  acc.withColumn(colName+"_j", hasColumn(union2DF,colName+"_i") match {
    case true=> coalesce(col(colName+"_i"), col(colName))
    case false=>col(colName)
  }).drop(colName).drop(colName+"_i").withColumnRenamed(colName+"_j",colName)
}

      val fDF3: DataFrame = colList2.foldLeft(union2DF) { (acc:DataFrame, colName:String) =>
        acc.withColumn(colName+"_j", hasColumn(union2DF,colName+"_i") match {
          case true=> coalesce(col(colName+"_i"), col(colName))
          case false=>col(colName)
        }).drop(colName).drop(colName+"_i")
      }
      val prefixBase2 = "_j"
      val renamedbaseColumns2 = foldDF.columns.map(c=> fDF3(s"$c$prefixBase2").as(s"$c"))
      val fDF4 = fDF3.select(renamedbaseColumns2: _*)

 // baseDF.show(3)
  foldDF.show(3,false)
//  logger.warn("Count of baseDF is :"+baseDF.count())
//  logger.warn("Count of IncrementalDF is :"+incrementalDF.count())
//  logger.warn("Count of joinDF is :"+unionDF.count())
//  logger.warn("Count of foldleftDf is :"+foldDF.count())




//      val foldDF2: DataFrame = colList2.foldLeft(union2DF) { (acc: DataFrame, colName: String) =>
//        acc.withColumn(colName,  hasColumn(union2DF,colName+"_i") match {
//          case true=> coalesce(col(colName+"_i"), col(colName))
//          case false=>col(colName)
//        }).drop(colName + "_i")}
//
//      logger.warn("Count of foldleftDf2 is :"+foldDF2.count())
//      foldDF2.show(1000,false)

      val diffDF=foldDF.except(fDF4)
      diffDF.show(5,false)
      println("count of Diff Dataframe is :"+diffDF.count())

     // val diffDf=foldDF.union(foldDF2).subtract(foldDF.intersect(foldDF2))

      // val df3=foldDF.unionAll(foldDF2).except(foldDF.intersect(foldDF2))
      //df3.show()


      //  val groupDF= baseDF.groupBy("gender")
  // val windowSpec = Window.partitionBy("gender")

}
    //val rankDF=baseDF.
    //baseDF.show(3)
    //groupDF
    //dupbaseDF.show(3)
    //val parallel = spark.sparkContext.parallelize(1 to 9)
    //val mapDF=parallel.map( x => List(x.next).iterator).collect

 //   val mapPartDF=parallel.mapPartitions( x => List(x.next).iterator).collect
   // mapPartDF.foreach(println)
   // parallel.take(2).foreach(println)
    //val groupbyDF=dupbaseDF.withColumn("GenderEQ").groupBy("gender")

   // groupbyDF.)show(10)
   // parquetDF.show(1)
    //to save dataframe in AVRO format
    //df.write.format("avro").save("person.avro")

   // val explodeDF = baseDF.select(explode($"email"))
   //explodeDF.show(3)
    //baseDF.show(1)
    //baseDF.printSchema()
  // print(baseDF.count())  current count of records- 4998

    //METHOD 2 provide schema file and parse data  from avro files using this schema.
  //  val schemaAvro = new Schema.Parser().parse(new File("C:\\Users\\15148\\IdeaProjects\\executeEngine\\src\\main\\resources\\AVRO\\schema/schema.avsc"))

 //   val baseSchemaDF = spark.read
//      .format("com.databricks.spark.avro")
//      .option("avroSchema", schemaAvro.toString)
//      .load("C:\\Users\\15148\\IdeaProjects\\executeEngine\\src\\main\\resources\\AVRO\\*")

   // baseSchemaDF.show(1)
    //baseSchemaDF.printSchema()
   // println(baseSchemaDF.schema)
    //val schemaFields=baseSchemaDF.schema.fields
    //val columnsList=baseSchemaDF.columns.toSeq
    //columnsList.foreach(println)
    //schemaFields.foreach(println)





  }
}
