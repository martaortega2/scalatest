package com.everis.sparkTest
import org.apache.commons.cli._

class ConfigArgs  extends  Serializable {

  // Valores de las opciones
  // Por ejemplo ("h", "help") corresponde a -h y --help

  private val OPT_HELP = ("h", "help")
  lazy private val OPT_FILE = ("f", "file")
  lazy private val OPT_SCH = ("s", "schema")

  // valores por defecto

  private var help = false
  private var file: String = null
  private var schema:List[String] = null

  // se inicializa el objeto que contendrá todos los argumentos de entrada
  lazy private val options = new Options()

  options.addOption(OPT_HELP._1, OPT_HELP._2, false, "Mostrar la ayuda")
  options.addOption(OPT_FILE._1, OPT_FILE._2, true, "Ruta del fichero de empleados")
  options.addOption(OPT_SCH._1, OPT_SCH._2, true, "Lista de campos del fichero de empleados")


  /**
   * Parse: función que recoge todos los argumentos de entrada
   * @param args: array con los argumentos de entrada de tipo string
   */
  def parse(args: Array[String]) = {

    val parser = new BasicParser()
    val cmd = parser.parse(options, args)

    if (cmd.hasOption(OPT_HELP._1) && args.length == 1) help = true
    // se configura la recepción tanto de la opción -n como --name
    if (cmd.hasOption(OPT_FILE._1)) file = cmd.getOptionValue(OPT_FILE._1)
    if (cmd.hasOption(OPT_FILE._2)) file = cmd.getOptionValue(OPT_FILE._2)
    if (cmd.hasOption(OPT_SCH._1)) schema = cmd.getOptionValue(OPT_SCH._1).split(",").toList
    if (cmd.hasOption(OPT_SCH._2)) schema = cmd.getOptionValue(OPT_SCH._2).split(",").toList


    // Lanzar excepciones si no aparece algun parámetro obligatorio
    if (!help && file == null) throw new ParseException("El parametro '--" + OPT_FILE._2 + "' es obligatorio")
    if (!help && schema == null) throw new ParseException("El parametro '--" + OPT_SCH._2 + "' es obligatorio")

  }

  def mostrarAyuda = help

  def getFile = file

  def getSchema = schema

  // Ayuda por pantalla
  def printHelp() {
    val formatter = new HelpFormatter()
    formatter.setWidth(160)
    formatter.setDescPadding(5)

    val usage = "Parametros de lanzamiento del proceso:\n"
    formatter.printHelp(usage, options)
  }

}
