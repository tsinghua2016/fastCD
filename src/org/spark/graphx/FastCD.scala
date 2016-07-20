package org.spark.graphx

import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
object FastCD {
 
 // update the information of nodes
 def updateNodeData(graph: Graph[NodeData, Long]):RDD[(VertexId, NodeData)]={
   graph.aggregateMessages[(Long,Long,Long,Long,NodeData)](
       //edges attribute,  the communityId of sender, Ic ,Sc,the information of receiver
            triplet =>
              {   triplet.sendToDst(triplet.attr,triplet.srcAttr.communityID,0,0,triplet.dstAttr)
                triplet.sendToSrc(triplet.attr,triplet.dstAttr.communityID,0,0,triplet.srcAttr)
               } ,
           
         (a,b)=> // reduce message if receiver receive more than one message
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
        ).map{// create new vertice
     x=>  
         var node=new NodeData(x._2._5.nodeid,x._2._5.communityID)
         var li=x._2._3
         var si=x._2._4
         if(li==0 )li=x._2._1
         if( si==0 && x._2._2!=x._2._5.communityID) si=x._2._1
         
          node.communityDegreeSum=li
          node.neighCommunityDegreeSum=si
         Tuple2(x._1,node)
        }
 } 
 
 def distributeCommunity(graph: Graph[NodeData, Long],w:Broadcast[Long]):Graph[NodeData,Long]={
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
            if(a._1!=0) avalue=(w.value*a._1-a._2.neighCommunityDegreeSum*a._3.communityDegreeSum)/(2.0*w.value*w.value)
             if(b._1!=0) bvalue=(w.value*b._1-b._2.neighCommunityDegreeSum*b._3.communityDegreeSum)/(2.0*w.value*w.value)
             if(avalue<=0 && bvalue<=0) (0,null,null,0)
             else if(avalue>bvalue)(a._1,a._2,a._3,avalue)
             else (b._1,b._2,b._3,bvalue)
            }
        ).filter(x=>x._2._1!=0).map{
        data=>
          {  
            var node=new NodeData(data._1,data._2._2.communityID)
            node.communityDegreeSum=data._2._3.communityDegreeSum
            node.Q=data._2._4
           //node.sci=data._2._3.sci
            node.isUpdate=true
           Tuple2(data._1,node)
          }
        }

     newvertexRDD=old.union(newvertexRDD).reduceByKey{
         (x,y)=>if(x.isUpdate) {
           x.isUpdate=false
           x
         }
         else
          {
           y.isUpdate=false
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
                node.isUpdate=true
                tri.sendToDst(node)
             }
             else if(tri.srcAttr.communityID==tri.dstAttr.nodeid && tri.dstAttr.communityID!=tri.dstAttr.nodeid)
             {
                node=tri.srcAttr
                node.communityID=tri.dstAttr.communityID
                node.isUpdate=true
                tri.sendToSrc(node)
             }
           
           },
           (a,b)=>b
       
       )
     
      newvertexRDD=newvertexRDD.union(subRdd).reduceByKey{
         (x,y)=>if(x.isUpdate) {
           x.isUpdate=false
           x
         }
         else
          {
           y.isUpdate=false
           y
          }
         
       }

       tgraph= Graph(newvertexRDD,tgraph.edges).cache() 
       pregraph.unpersist(false)
       pregraph=tgraph
       var edges=EdgesReduce(tgraph)
       tgraph= Graph(newvertexRDD,edges).cache() 
       pregraph.unpersist(false)
       pregraph=tgraph
       
       newvertexRDD=updateNodeData(tgraph)
       

       pregraph.unpersist(false)
       Graph(newvertexRDD,edges)
 }
 // update the information of edges
def EdgesReduce(graph: Graph[NodeData, Long]):RDD[Edge[Long]]={

            graph.triplets.map{f=>
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
} 

// execute fastCD Algorithm 
 def run(sc:SparkContext,edgesFile:String, numPartitions:Int,verticeFile:String)=
  {
        var run=true
         var edges:RDD[Edge[(Long)]]=sc.textFile(edgesFile, numPartitions).map{
          line=>
              var record=line.split('\t')
              if(record.length!=2) record=line.split(' ')
              Edge(record(0).toLong,record(1).toLong,(1L))
          }
       //count w.value
       val w = sc.broadcast(edges.reduce((x1,x2)=>new Edge(x1.srcId,x2.dstId,x1.attr+x2.attr)).attr)
       var vertices:RDD[(VertexId, NodeData)]= sc.textFile(verticeFile, numPartitions).map(line => {  
                var lineLong=line.toLong
                var node=new NodeData(lineLong,lineLong)
                Tuple2(lineLong,node)
            })
        var graph= Graph(vertices,edges).cache() 
         // the pregraph is used for release the cache of graph
        var pregraph=graph 
        // count the number of vertices if the property named Q in NodeData is more than 0
        var newnum:Long=0L 
        var newvertexRDD=updateNodeData(graph)
        graph= Graph(newvertexRDD,edges).cache() 
        pregraph.unpersist(blocking=false)
        pregraph=graph
      do{
        graph= distributeCommunity(graph,w)
        graph.cache()
        pregraph.unpersist(false)
        pregraph.edges.unpersist(false)
        newnum=graph.vertices.filter(x=>x._2.Q>0).count()
        if(newnum==0) run=false 
     }while(run)
       
     graph

  }
 def getQAndCommunityNumbers(sc:SparkContext,graph: Graph[NodeData, Long])={
 val w = sc.broadcast(graph.edges.reduce((x1,x2)=>new Edge(x1.srcId,x2.dstId,x1.attr+x2.attr)).attr)
  val Qs=graph.aggregateMessages[(NodeData)](
       {tri=>
         if(tri.srcAttr.communityID==tri.dstAttr.communityID ) 
         {
         if(tri.srcAttr.nodeid==tri.srcAttr.communityID)tri.sendToSrc(tri.srcAttr) 
         else if(tri.dstAttr.nodeid==tri.dstAttr.communityID)
           tri.sendToDst(tri.dstAttr) 
         }
       },
       //  find the nodes that can represent different communities.If the number of message is more than one ,a must equal b
         (a,b)=> 
           {
            a
           }
     ).map{
       x=>
         {
           val Sc=x._2.neighCommunityDegreeSum
           val Ic=x._2.communityDegreeSum
           val ID=x._2.communityID
           val q=(Ic-Sc*Sc*1.0/(2*w.value))/(2*w.value)
           Tuple2(ID,q)
         }    
}.collect()
     val iter=Qs.iterator
     var sum=0.0
     var number=0L
     while(iter.hasNext)
     {
       var line=iter.next()
       sum+=line._2
       number+=1
     }
     Tuple2(sum,number)
 }
}