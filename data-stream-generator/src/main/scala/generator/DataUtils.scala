package generator

object DataUtils extends Serializable {

  def splitLine(line: String): (String, String, String) = {
    val splitLine = line.split(" ", 3)
    (splitLine(0), splitLine(1), splitLine(2))
  }
}