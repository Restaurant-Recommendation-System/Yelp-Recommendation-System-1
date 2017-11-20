package org.neu

import java.io.Serializable

class business(row: String) extends Serializable {

  val columns = row.split(",") //All the columns from the row passed in the constructor

  //Attributes of the song required to perform the queries
  val stars: Float = try {
    columns(0).toFloat
  }
  catch {
    case e: Throwable => -1
  }
  val business_id: String = columns(1)
  val name: String = columns(2)
  val city: String = columns(3)

}
