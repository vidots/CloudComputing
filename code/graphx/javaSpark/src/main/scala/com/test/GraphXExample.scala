package com.test

import com.mongodb.casbah.Imports.{MongoClient, MongoDBObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source

object GraphXExample {
  def toGexf[VD, ED](g: Graph[VD, ED]): String = {
    val string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      " <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      " <nodes>\n" +
      g.vertices.map(v => " <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      " </nodes>\n" +
      " <edges>\n" +
      g.edges.map(e => " <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      " </edges>\n" +
      " </graph>\n" +
      "</gexf>"
    return string
  }

  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
    if (a._2 > b._2) a else b
  }

  def find(a: Long, b: Array[(Long, String)]): String = {
    for ((x, y) <- b) {
      if (a == x) {
        return y
      }
    }
    return ""
  }

  def findArr(a: Iterable[VertexId], b: Array[(Long, String)]): Array[String] = {
    val arr = Array[String]()
    for (i <- a) {
      for ((x, y) <- b) {
        if (i == x) {
          print("\"" + y + "\"" + ",")
          y +: arr
        }
      }
    }
    return arr
  }

  def main(args: Array[String]) {
    val mongoClient = MongoClient("localhost", 27017)
    val db = mongoClient("graphx")
    val collcoopNeighbour = db("coopNeighbour")
    val collneighbour = db("neighbour")
    val collpageRank = db("pageRank")
    val colltriangle = db("triangle")
    val colljump = db("jump")
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    //设置运行环境
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    val nameFile = "/Users/tyf/Downloads/name.txt"
    val coopFile = "/Users/tyf/Downloads/coop.txt"

    //读取id 对应 昵称 的 Array
    var a = Array[(Long, String)]()
    val sourse = Source.fromFile(nameFile, "UTF-8")
    val lineIterator = sourse.getLines()
    for (line <- lineIterator) {
      val word = line.split(" ")
      a = (word(0).toLong, word(1)) +: a
    }

    //读入数据文件
    val articles: RDD[String] = sc.textFile(nameFile)

    //装载顶点
    val vertices = articles.map { line =>
      val fields = line.split(' ')
      (fields(0).toLong, fields(1))
    }

    //装载边
    var b = Array[Edge[Int]]()
    val sourse2 = Source.fromFile(coopFile, "UTF-8")
    val lineIterator2 = sourse2.getLines()
    for (line <- lineIterator2) {
      val word = line.split(" ")
      b = Edge(word(0).toLong, word(1).toLong, word(2).toInt) +: b
      b = Edge(word(1).toLong, word(0).toLong, word(2).toInt) +: b
    }
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(b)

    //构造图
    val graph = Graph(vertices, edgeRDD, "").persist()

    println(toGexf(graph))

    //最大的出度 跟算邻居差不多
    //println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))

    //计算每个人合作总次数
    val rdd: RDD[(String, Int)] = graph.triplets.map(triplet => (triplet.srcAttr, triplet.attr))
    val gg = rdd.reduceByKey((a, b) => a + b).sortBy(_._1).collect()
    //gg.foreach(println)  974个人

    //计算每个人的邻居
    val neighbourAll = graph.aggregateMessages[Int](triple => triple.sendToDst(1), (o1, o2) => o1 + o2).sortBy(_._2, false)
    val neighbourRddAll = neighbourAll.map(s => (find(s._1, a), s._2)).sortBy(_._1).collect()
    // neighbourRddAll.foreach(println)

    //构造前端散点图所需数据格式
    var coopAndNeighbour = Array[(Int, Int)]()
    for (a <- 0 to 973) {
      coopAndNeighbour = (neighbourRddAll(a)._2, gg(a)._2) +: coopAndNeighbour
    }
    var cnstr = ""
    for (x <- coopAndNeighbour) {
      cnstr += "["
      cnstr += x._1
      cnstr += ","
      cnstr += x._2
      cnstr += "],"
      val a = MongoDBObject("nei" -> x._1, "coop" -> x._2) //保存到mongodb
      collcoopNeighbour.insert(a)
    }
    cnstr = "[" + cnstr.substring(0, cnstr.length - 1) + "]"
    println("散点：" + cnstr)

    //计算合作次数排序
    val rdd2: RDD[(String, Int)] = graph.triplets.map(triplet => (triplet.srcAttr, triplet.attr))
    val gg2 = rdd2.reduceByKey((a, b) => a + b).sortBy(_._2, false).collect()
    gg2.take(20).foreach(println)
    var coopstr = ""
    for (x <- gg2.take(20)) {
      coopstr += "[\""
      coopstr += x._1
      coopstr += "\","
      coopstr += x._2
      coopstr += "],"
      val a = MongoDBObject(x._1 -> x._2) //保存到mongodb
      collcoopNeighbour.insert(a)
    }
    coopstr = "[" + coopstr.substring(0, coopstr.length - 1) + "]"
    println("合作视频数最高的20个：" + coopstr)

    //neighbour计算
    val neighbour = graph.aggregateMessages[Int](triple => triple.sendToDst(1), (o1, o2) => o1 + o2).sortBy(_._2, false)
    val neighbourRdd = neighbour.map(s => (find(s._1, a), s._2))
    //neighbourRdd.take(20).foreach(println)
    var str = ""
    for (x <- neighbourRdd.take(20)) {
      str += "[\""
      str += x._1
      str += "\","
      str += x._2
      str += "],"
      val a = MongoDBObject(x._1 -> x._2) //保存到mongodb
      collneighbour.insert(a)
    }
    str = "[" + str.substring(0, str.length - 1) + "]"
    println("邻居最多的20个：" + str)


    //pageRank
    val prGraph = graph.pageRank(0.001).cache()
    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }
    val k = titleAndPrGraph.vertices.top(20) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    } //.foreach(t => println(t._2._2 + ": " + t._2._1))
    var str1 = ""
    for (x <- k) {
      str1 += "[\""
      str1 += x._2._2
      str1 += "\","
      str1 += x._2._1
      str1 += "],"
      val a = MongoDBObject(x._2._2 -> x._2._1) //保存到mongodb
      collpageRank.insert(a)
    }
    str1 = "[" + str1.substring(0, str1.length - 1) + "]"
    println("pageRank最高的20个：" + str1)

    //三角环统计
    val triangle = graph.triangleCount().vertices.sortBy(_._2, false)
    val triangleRdd = triangle.map(s => (find(s._1, a), s._2))
    //triangleRdd.take(20).foreach(println)
    var str2 = ""
    for (x <- triangleRdd.take(20)) {
      str2 += "[\""
      str2 += x._1
      str2 += "\","
      str2 += x._2
      str2 += "],"
      val a = MongoDBObject(x._1 -> x._2) //保存到mongodb
      colltriangle.insert(a)
    }
    str2 = "[" + str2.substring(0, str2.length - 1) + "]"
    println("三角环最多的20个：" + str)

    //二跳邻居统计
    //pregel实现
    type VMap = Map[VertexId, Int] //定义每个节点存放的数据类型，为若干个（节点编号，一个整数）构成的map，当然发送的消息也得遵守这个类型

    /**
     * 节点数据的更新 就是集合的union
     */
    def vprog(vid: VertexId, vdata: VMap, message: VMap) //每轮迭代后都会用此函数来更新节点的数据（利用消息更新本身），vdata为本身数据，message为消息数据
    : Map[VertexId, Int] = addMaps(vdata, message)

    /**
     * 节点数据的更新 就是集合的union
     */
    def sendMsg(e: EdgeTriplet[VMap, _]) = {
      //取两个集合的差集  然后将生命值减1
      val srcMap = (e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k -> (e.dstAttr(k) - 1) }.toMap
      val dstMap = (e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k -> (e.srcAttr(k) - 1) }.toMap
      if (srcMap.size == 0 && dstMap.size == 0)
        Iterator.empty
      else
        Iterator((e.dstId, dstMap), (e.srcId, srcMap)) //发送消息的内容
    }

    /**
     * 消息的合并
     */
    def addMaps(spmap1: VMap, spmap2: VMap): VMap =
      (spmap1.keySet ++ spmap2.keySet).map { //合并两个map，求并集
        k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue)) //对于交集的点的处理，取spmap1和spmap2中最小的值
      }.toMap

    val two = 2 //这里是二跳邻居 所以只需要定义为2即可
    val newG = graph.mapVertices((vid, _) => Map[VertexId, Int](vid -> two)) //每个节点存储的数据由一个Map组成，开始的时候只存储了 （该节点编号，2）这一个键值对
      .pregel(Map[VertexId, Int](), two, EdgeDirection.Out)(vprog, sendMsg, addMaps)
    //pregel参数
    //第一个参数 Map[VertexId, Int]() ，是初始消息，面向所有节点，使用一次vprog来更新节点的值，由于Map[VertexId, Int]()是一个空map类型，所以相当于初始消息什么都没做
    //第二个参数 two，是迭代次数，此时two=2，代表迭代两次（进行两轮的active节点发送消息），第一轮所有节点都是active节点，第二轮收到消息的节点才是active节点。
    //第三个参数 EdgeDirection.Out，是消息发送方向，out代表源节点-》目标节点 这个方向    //pregel 函数参数    //第一个函数 vprog，是用户更新节点数据的程序，此时vprog又调用了addMaps
    //第二个函数 sendMsg，是发送消息的函数，此时用目标节点的map与源节点的map做差，将差集的数据减一；然后同样用源节点的map与目标节点的map做差，同样差集的数据减一
    //第一轮迭代，由于所有节点都只存着自己和2这个键值对，所以对于两个用户之间存在变关系的一对点，都会收到对方的一条消息，内容是（本节点，1）和（对方节点，1）这两个键值对
    //第二轮迭代，收到消息的节点会再一次的沿着边发送消息，此时消息的内容变成了（自己的朋友，0）    //第三个函数 addMaps, 是合并消息，将map合并（相当于求个并集），不过如果有交集（key相同），那么，交集中的key取值（value）为最小的值。

    //过滤得到二跳邻居 就是value=0 的顶点
    val twoJumpFirends = newG.vertices
      .mapValues(_.filter(_._2 == 0).keys) //由于在第二轮迭代，源节点会将自己的邻居（非目标节点）推荐给目标节点——各个邻居就是目标节点的二跳邻居，并将邻居对应的值减为0，
    //twoJumpFirends.collect().foreach(println(_))
    //twoJumpFirends.filter(x => x._2 != Set() && (x._1 == 546195 || x._1 == 12807175)).map(x => (find(x._1, a), findArr(x._2, a))).foreach(println(_))

    val potato = twoJumpFirends.filter(x => x._1 == 546195).map(x => (find(x._1, a), findArr(x._2, a))).foreach(print(_)) //老番茄二跳邻居
    println()
    val old = twoJumpFirends.filter(x => x._1 == 12807175).map(x => (find(x._1, a), findArr(x._2, a))).foreach(print(_))  //法老二跳邻居


  }

}