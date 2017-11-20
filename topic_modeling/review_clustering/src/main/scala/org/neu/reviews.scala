package org.neu

import java.io.Serializable

class reviews(row: String) extends Serializable {

  val columns = row.split(",") //All the columns from the row passed in the constructor

  //Attributes of the song required to perform the queries
  val stars: Float = try {
    columns(0).toFloat
  }
  catch {
    case e: Throwable => -1
  }

  val review_id: String = columns(1)
  val user_id: String = columns(2)
  val business_id: String = columns(3)
  val review:String = columns(4).replaceAll("\"","")

}
