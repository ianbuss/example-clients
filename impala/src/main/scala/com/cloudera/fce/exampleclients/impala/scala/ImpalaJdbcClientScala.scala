package com.cloudera.fce.exampleclients.impala.scala

import java.io.{BufferedOutputStream, FileOutputStream, PrintWriter}
import java.sql.{Connection, DriverManager, ResultSet}
import java.util.UUID
import com.cloudera.fce.exampleclients.impala.java.SimbaImpalaDriver
import org.apache.hadoop.security.UserGroupInformation

trait ImpalaDriver {
  def name: String

  def constructJdbcUrl(host: String, port: Int): String

  def constructJdbcUrl(host: String, port: Int, princ: String, realm: String): String

  def loginViaJaas(configFile: String) = {
    System.setProperty("java.security.auth.login.config", configFile)
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
  }

  def loginViaKeytab(princ: String, keytab: String)
}

class SimbaImpalaDriver extends ImpalaDriver {
  override def name: String = "com.cloudera.hive.jdbc41.HS2Driver"

  override def constructJdbcUrl(host: String, port: Int) = s"jdbc:hive2://$host:$port"

  override def constructJdbcUrl(host: String, port: Int, princ: String, realm: String) = {
    constructJdbcUrl(host, port) + s";AuthMech=1;KrbRealm=${realm.toUpperCase};KrbHostFQDN=$host;KrbServiceName=$princ"
  }

  override def loginViaKeytab(princ: String, keytab: String) = {
    val jaasFile = s"/tmp/${UUID.randomUUID}";
    val out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(jaasFile)))
    out.write("Client {\n")
    out.write(" com.sun.security.auth.module.Krb5LoginModule required\n")
    out.write(" useKeyTab=true\n")
    out.write(" keyTab=" + keytab + "\n")
    out.write(" doNotPrompt=true\n")
    out.write(" principal=" + princ + ";\n")
    out.write("};\n")
    out.close
    loginViaJaas(jaasFile)
  }
}

class ImpalaJdbcClientScala(opts: Map[String, Any]) {

  def openConnection(driver: ImpalaDriver): Connection = {
    val secure = opts.getOrElse("secure", false).asInstanceOf[Boolean]
    val host = opts("host").asInstanceOf[String]
    val port = opts.getOrElse("port", 10000).asInstanceOf[Int]
    val serverprinc = opts.getOrElse("serverprinc", "").asInstanceOf[String]
    val realm = opts.getOrElse("realm", "").asInstanceOf[String]
    val userprinc = opts.getOrElse("userprinc", "").asInstanceOf[String]
    val keytab = opts.getOrElse("keytab", "").asInstanceOf[String]
    val jaas = opts.getOrElse("jaas", "").asInstanceOf[String]
    val url = if (secure)
      driver.constructJdbcUrl(host, port, serverprinc, realm)
    else
      driver.constructJdbcUrl(host, port)
    if (secure)
      if (jaas.isEmpty) driver.loginViaKeytab(userprinc, keytab)
      else driver.loginViaJaas(jaas)

    DriverManager.getConnection(url)
  }

  def loadDriver(): ImpalaDriver = {
    val driverName = opts.getOrElse("driver", "APACHE").toString.toUpperCase
    val driver = new SimbaImpalaDriver
    Class.forName(driver.name)
    driver
  }

  def runQuery(conn: Connection): Unit = {
    def printResults(res: ResultSet): Unit = {
      val meta = res.getMetaData
      val types = for (i <- 1 until meta.getColumnCount) yield meta.getColumnType(i)
      while (res.next()) {
        val cols = for (i <- 1 until meta.getColumnCount) yield res.getObject(i).toString
        println(cols.mkString("\t"))
      }
    }

    val query = opts("query").asInstanceOf[String]
    val stmt = conn.createStatement()
    try {
      if (stmt.execute(query)) {
        printResults(stmt.getResultSet)
      } else {
        println("Error: Query failed")
        println(s"Warnings: ${stmt.getWarnings}")
      }
    } finally {
      stmt.close()
    }
  }

  def run: Unit = {
    val driver = loadDriver()
    val conn = Option(openConnection(driver))
    try {
      conn.foreach(runQuery)
    } finally {
      conn.foreach(_.close)
    }
  }

}

object ImpalaJdbcClientScala {
  val DEFAULT_HS2_PRINCIPAL = "impala";
  val DEFAULT_HS2_PORT = 21050;

  def usage(code: Int, msg: String = "") = {
    val usg =
      s"Usage: ${ImpalaJdbcClientScala.getClass.getName} -h HOST -q QUERY " +
        s"[-p PORT] [-s SERVER_PRINC] [-k] [-t KEYTAB] [-u USER_PRINC] [-d {APACHE|SIMBA}] [-j JAAS_FILE] [-S]"
    if (!msg.isEmpty) println(msg)
    println(usg)
    sys.exit(code)
  }

  def validateOpts(opts: Map[String, Any]) = {
    if (!opts.contains("host")) {
      println("No server hostname supplied [-h]")
      usage(-1)
    }
    if (!opts.contains("query")) {
      println("No query supplied [-q")
      usage(-1)
    }
  }

  def main(args: Array[String]) = {
    if (args.length < 2) usage(-1)

    def parseOption(opts: Map[String, Any], args: List[String]): Map[String, Any] = args match {
      case "-h" :: optarg :: tail => parseOption(opts ++ Map("host" -> optarg), tail)
      case "-q" :: optarg :: tail => parseOption(opts ++ Map("query" -> optarg), tail)
      case "-p" :: optarg :: tail => parseOption(opts ++ Map("port" -> optarg.toInt), tail)
      case "-s" :: optarg :: tail => parseOption(opts ++ Map("serverprinc" -> optarg), tail)
      case "-t" :: optarg :: tail => parseOption(opts ++ Map("keytab" -> optarg), tail)
      case "-u" :: optarg :: tail => parseOption(opts ++ Map("userprinc" -> optarg), tail)
      case "-d" :: optarg :: tail => parseOption(opts ++ Map("driver" -> optarg), tail)
      case "-j" :: optarg :: tail => parseOption(opts ++ Map("jaas" -> optarg), tail)
      case "-r" :: optarg :: tail => parseOption(opts ++ Map("realm" -> optarg), tail)
      case "-k" :: tail => parseOption(opts ++ Map("secure" -> true), tail)
      case "-S" :: tail => parseOption(opts ++ Map("ssl" -> true), tail)
      case Nil => opts
    }

    val opts = parseOption(Map[String, Any](), args.toList)
    validateOpts(opts)

    val client = new ImpalaJdbcClientScala(opts)
    client.run
  }
}
