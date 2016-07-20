package org.spark.graphx

class NodeData(val nodeid:Long,var communityID:Long) extends Serializable {

  var communityDegreeSum:Long=0
  var Q:Double=0.0
  var isUpdate:Boolean=false
  var neighCommunityDegreeSum:Long=0
 override def  toString:String=
   "nodeid =>"+nodeid+" communityDegreeSum=>"+communityDegreeSum +" q => "+Q+" communityID "+communityID+" update "+isUpdate +" neighCommunityDegreeSum => "+neighCommunityDegreeSum

}