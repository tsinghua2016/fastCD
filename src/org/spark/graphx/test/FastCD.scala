package org.spark.graphx

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.util.Date
class NodeData(val nodeid:Long,var communityID:Long) extends Serializable {

  var li:Long=0
  var q:Double=0.0
  var update:Boolean=false
  var si:Long=0
 override def  toString:String=
   "nodeid =>"+nodeid+" li=>"+li +" q => "+q+" communityID "+communityID+" update "+update +" si => "+si

}
object FastCD {
var debug=false
  //节点数据更新
 def updateNodeData(graph: Graph[NodeData, Long]):RDD[(VertexId, NodeData)]={
   var temp=graph.aggregateMessages[(Long,Long,Long,Long,NodeData)](//边  社区id  li si节点
            triplet =>
             // if(triplet.srcAttr.communityID!=triplet.dstAttr.communityID){ 
              {   triplet.sendToDst(triplet.attr,triplet.srcAttr.communityID,0,0,triplet.dstAttr)
                triplet.sendToSrc(triplet.attr,triplet.dstAttr.communityID,0,0,triplet.srcAttr)
                
          /*    }
              else if(triplet.srcId==triplet.dstId)
              {
                triplet.sendToSrc(triplet.attr,triplet.dstAttr.communityID,0,triplet.srcAttr)
              }*/
               } ,
           
         (a,b)=>
           {
             var li=b._3+a._3
               li+=a._1
               li+=b._1
              var si=0L
              if(a._2!=a._5.communityID)
              {
                si+=a._1
              }
              if(b._2!=b._5.communityID)
              {
                si+=b._1
              }
             (0L,-1L,li,si,a._5)
           } 
        ).map{x=> 
         var node=new NodeData(x._2._5.nodeid,x._2._5.communityID)
         var li=x._2._3
         var si=x._2._4
         if(li==0 )li=x._2._1
         if( si==0 && x._2._2!=x._2._5.communityID) si=x._2._1
         
          node.li=li
          node.si=si
         
         Tuple2(x._1,node)
        }
   if(debug)
   {
    var tt=temp.collect()
     var sum=0L
     for(i <-0 until tt.length)
       {
         println(tt(i)._2)
         sum+=tt(i)._2.li
       }
     println("sum=>"+sum)
   }
     temp
 } 
 
 def distributeCommunity(graph: Graph[NodeData, Long],w:Broadcast[Long]):Graph[NodeData,Long]={
   //分配新社区
  
      var old=graph.vertices
       var newvertexRDD:RDD[(VertexId, NodeData)]= graph.aggregateMessages[(Long,NodeData,NodeData,Double)](
          
           triplet=> if(triplet.srcAttr.communityID!=triplet.dstAttr.communityID){
             triplet.sendToDst(triplet.attr,triplet.srcAttr,triplet.dstAttr,0)
            triplet.sendToSrc(triplet.attr,triplet.dstAttr,triplet.srcAttr,0)
            },
          (a,b)=>
            {
             var avalue=0.0
             var bvalue=0.0
            if(a._1!=0) avalue=(w.value*a._1-a._2.si*a._3.li)/(2.0*w.value*w.value)
             if(b._1!=0) bvalue=(w.value*b._1-b._2.si*b._3.li)/(2.0*w.value*w.value)
             if(avalue<=0 && bvalue<=0) (0,null,null,0)
             else if(avalue>bvalue)(a._1,a._2,a._3,avalue)
             else (b._1,b._2,b._3,bvalue)
            }
        ).filter(x=>x._2._1!=0).map{
        data=>
          {  
            var node=new NodeData(data._1,data._2._2.communityID)
            node.li=data._2._3.li
            node.q=data._2._4
           //node.sci=data._2._3.sci
            node.update=true
           Tuple2(data._1,node)
          }
        }

     newvertexRDD=old.union(newvertexRDD).reduceByKey{
         (x,y)=>if(x.update) {
           x.update=false
           x
         }
         else
          {
           y.update=false
           y
          }
         
       }
       var tgraph= Graph(newvertexRDD,graph.edges).cache() 
       var pregraph=tgraph


       var subRdd:RDD[(VertexId, NodeData)]=tgraph.aggregateMessages[(NodeData)](
           tri=>{
             var node:NodeData= null
             
             if(tri.dstAttr.communityID==tri.srcAttr.nodeid && tri.srcAttr.communityID!=tri.srcAttr.nodeid)
             {
                node=tri.dstAttr
                node.communityID=tri.srcAttr.communityID
                node.update=true
                tri.sendToDst(node)
             }
             else if(tri.srcAttr.communityID==tri.dstAttr.nodeid && tri.dstAttr.communityID!=tri.dstAttr.nodeid)
             {
                node=tri.srcAttr
                node.communityID=tri.dstAttr.communityID
                node.update=true
                tri.sendToSrc(node)
             }
           
           },
           (a,b)=>b
       
       )
     
      newvertexRDD=newvertexRDD.union(subRdd).reduceByKey{
         (x,y)=>if(x.update) {
           x.update=false
           x
         }
         else
          {
           y.update=false
           y
          }
         
       }
       if(debug)
       {
         println("##### distribute result #####")
         var s=newvertexRDD.collect()
         var si=s.iterator
         while(si.hasNext)
           println(si.next)
         println("#####distribute result end#####")           
       }
       tgraph= Graph(newvertexRDD,tgraph.edges).cache() 
       pregraph.unpersist(false)
       pregraph=tgraph
       //更新图数据
       var edges=EdgesReduce(tgraph)
       
       tgraph= Graph(newvertexRDD,edges).cache() 
       pregraph.unpersist(false)
       pregraph=tgraph
       
       newvertexRDD=updateNodeData(tgraph)
       

       pregraph.unpersist(false)
       Graph(newvertexRDD,edges)
 }
def EdgesReduce(graph: Graph[NodeData, Long]):RDD[Edge[Long]]={

            var e=graph.triplets.map{f=>
               Edge(f.srcAttr.communityID,f.dstAttr.communityID,(f.attr))
             }.flatMap { 
               e=>
                 {
                   var key=""
                   if(e.srcId>e.dstId) key=e.srcId+"-"+e.dstId
                   else key=e.dstId+"-"+e.srcId
                   List((key,e.attr))
                 }
             }.reduceByKey(_ + _).map
             {
               e=>
                 {
                   var src=e._1.split("-")
                   Edge(src(0).toLong,src(1).toLong,e._2)
                 }
             }
             if(debug)
             {
               println("\n#### EdgesReduce result######\n")
               var i=e.collect().iterator
               var isum=0L
               while(i.hasNext)
               {
                 var t=i.next()
                 println(t)
                 isum+=t.attr
               }
               println("sum =>"+isum)
               println("\n#####EdgesReduce result end#####\n")
               }
             e
             
} 
 
 def main(args:Array[String])
  {
   
        if(args.length<3){
          println("Param:<edgesFile> <verticeFile> <numPartitions> [debug]")
          System.exit(0)
        }
        if(args.length==4)
          debug=args(3).toBoolean
        var run=true
        val starttime=new Date().getTime
        val edgesFile=args(0)
        val verticeFile=args(1)
        val numPartitions=args(2).toInt
        val sf = new SparkConf().setAppName("Spark Graphx FastCD")
        val sc = new SparkContext(sf)
         var edges:RDD[Edge[(Long)]]=sc.textFile(edgesFile, numPartitions).map{
          line=>
              var record=line.split('\t')
              if(record.length!=2) record=line.split(' ')
              Edge(record(0).toLong,record(1).toLong,(1L))
          }
        val w = sc.broadcast(edges.reduce((x1,x2)=>new Edge(x1.srcId,x2.dstId,x1.attr+x2.attr)).attr)
      
      if(debug )println("W => "+w.value)
 

       var vertices:RDD[(VertexId, NodeData)]= sc.textFile(verticeFile, numPartitions).map(line => {  
                var lineLong=line.toLong
                var node=new NodeData(lineLong,lineLong)
                Tuple2(lineLong,node)
            })
        
   /*     var graph:Graph[NodeData,Long]= _
        var pregraph:Graph[NodeData,Long]= _*/

         var graph= Graph(vertices,edges).cache() 
          //初始化节点数据

        var pregraph=graph
        var newnum:Long=0L
    
        println("verteices => "+vertices.count()+" ; edges => "+edges.count())
        var newvertexRDD=updateNodeData(graph)
        graph= Graph(newvertexRDD,edges).cache() 
        pregraph.unpersist(blocking=false)
        pregraph=graph
        //var graphb=sc.broadcast(graph)
 
      do{
       
       if(debug)
       {
         println("\n$$$$$$$$ Community message$$$$$$$$$\n")
         var itri=graph.triplets.collect().iterator
         while(itri.hasNext)
            println(itri.next())
         println("\n$$$$$$$$Community message end $$$$$$$$$\n")
       }
      
      //分配新社区
        graph= distributeCommunity(graph,w)
        graph.cache()
        pregraph.unpersist(false)
        pregraph.edges.unpersist(false)
        newnum=graph.vertices.filter(x=>x._2.q>0).count()
       if(debug) println("q temp =>"+newnum)

          if(newnum==0) run=false 
       
          
          
     }while(run)
     val endtime=new Date().getTime
      val ss=(endtime-starttime)/(1000)
      println("Runtime "+ss+"s")
     
     val Qs=graph.aggregateMessages[(NodeData)](
       {tri=>
         if(tri.srcAttr.communityID==tri.dstAttr.communityID ) 
         if(tri.srcAttr.nodeid==tri.srcAttr.communityID)tri.sendToSrc(tri.srcAttr) 
         else if(tri.dstAttr.nodeid==tri.dstAttr.communityID)
           tri.sendToDst(tri.dstAttr) 
       },
         (a,b)=> 
           {
            a
           }
     ).map{
       x=>
         {
           val Sc=x._2.si
           val Ic=x._2.li
           val ID=x._2.communityID
           val q=(Ic-Sc*Sc*1.0/(2*w.value))/(2*w.value)
           Tuple2(ID,q)
         }    
     }.collect()
     val iter=Qs.iterator
     var sum=0.0
     var number=0L
     println("++++++ Q +++++++")
     while(iter.hasNext)
     {
       var line=iter.next()
       if(debug)println(line)
       sum+=line._2
       number+=1
     }
     println("Q => "+sum)
     println("++++++++++++++++")
     
     println("The number of community is "+number)  


  }
}
