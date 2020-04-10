
case class DroneMessage(id_Drone: Integer, date: String, time : String, latitude: Double, longitude: Double, image: String, id_image: String, violationCode: Integer, id_plate: String){
  
  def toArray() : Array[String] = {
      if(violationCode == null){
          return Array(id_Drone.toString, date, time, latitude.toString,longitude.toString)
      }
    return Array(id_Drone.toString, date, time, latitude.toString,longitude.toString, id_image, violationCode.toString,id_plate)
  }
}

