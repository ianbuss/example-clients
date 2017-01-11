package com.cloudera.fce.exampleclients.hive.java;

import com.cloudera.fce.exampleclients.common.JdbcClient;
import com.cloudera.fce.exampleclients.common.JdbcDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HiveJdbcClient extends JdbcClient {
  private static final Logger LOG = LoggerFactory.getLogger(HiveJdbcClient.class);
  private static final int DEFAULT_HS2_PORT = 10000;
  private static final String DEFAULT_HS2_PRINCIPAL = "hive";

  private Properties properties;

  public HiveJdbcClient(Properties properties) {
    this.properties = properties;
  }

  public JdbcDriver loadDriver() throws ClassNotFoundException {
    String driverName = properties.getProperty("driver");
    LOG.debug("driverName: %s", driverName.toUpperCase());
    JdbcDriver driver = new ApacheHiveDriver();
    switch(driverName.toUpperCase()) {
      case "APACHE": break;
      case "SIMBA": driver = new SimbaHiveDriver(); break;
      default: exitWithUsage("Unrecognised driver: " + driverName, -1);
    }
    Class.forName(driver.getName());
    return driver;
  }

  public void run() throws Exception {
    JdbcDriver driver = loadDriver();
    String url = driver.constructJdbcUrl(
      properties.getProperty("host"),
      Integer.parseInt(properties.getProperty("port")),
      properties.getProperty("serverprinc", null),
      properties.getProperty("realm",  null),
      properties.getProperty("username",  null),
      properties.getProperty("password",  null),
      properties.getProperty("ssltruststore",  null),
      properties.getProperty("ssltruststorepassword",  null));
    LOG.debug("URL: %s", url);

    // Set up security
    boolean secure = Boolean.parseBoolean(properties.getProperty("secure"));
    if (secure) {
      // Use JAAS config if specified
      if (null != properties.getProperty("jaas")) {
        driver.loginViaJaas(properties.getProperty("jaas"));
      }
      // Otherwise use supplied user principal and keytab
      else {
        String userPrincipal = properties.getProperty("userprinc");
        String keyTab = properties.getProperty("keytab");
        if (userPrincipal == null || keyTab == null) {
          exitWithUsage("Must supply both user principal and keytab if not using JAAS config", -1);
        } else {
          driver.loginViaKeytab(userPrincipal, keyTab);
        }
      }
    }

    try {
      openConnection(url);
      runQuery(properties.getProperty("query"), properties.getProperty("db"));
    } finally {
      closeConnection();
      driver.cleanup();
    }
  }

  private static String getNextArg(String[] args, String opt, int next) {
    if (next >= args.length) {
      throw new IllegalArgumentException("No value for option: " + opt);
    } else {
      return args[next].trim();
    }
  }

  private static void exitWithUsage(String msg, int exit) {
    System.err.println(msg);
    StringBuilder sb = new StringBuilder();
    sb.append("Usage:" + HiveJdbcClient.class.getName() + "\n");
    sb.append("\t-h HOST\n");
    sb.append("\t-q QUERY\n");
    sb.append("\t[-d {APACHE|SIMBA}]\n");
    sb.append("\t[-db DATABASE] [-p PORT]\n");
    sb.append("\t[-s SERVER_PRINC] [-k] [-t KEYTAB] [-u USER_PRINC] [-r REALM]\n");
    sb.append("\t[-j JAAS_FILE]\n");
    sb.append("\t[-St SSL_TRUSTSTORE] [-Sp SSL_TRUSTSTORE_PASS]\n");
    sb.append("\t[-debug]\n");
    System.err.printf(sb.toString());
    System.exit(exit);
  }

  private static Properties getProperties() {
    Properties properties = new Properties();
    properties.setProperty("serverprinc", DEFAULT_HS2_PRINCIPAL);
    properties.setProperty("port", Integer.toString(DEFAULT_HS2_PORT));
    properties.setProperty("secure", "false");
    properties.setProperty("driver", "APACHE");
    return properties;
  }

  public static void main(String[] args) throws Exception {
    Properties properties = getProperties();

    for (int i = 0; i < args.length; ++i) {
      String arg = args[i];
      switch (arg) {
        case "-h":
          properties.setProperty("host", getNextArg(args, "-h", ++i)); break;
        case "-s":
          properties.setProperty("serverprinc", getNextArg(args, "-s", ++i)); break;
        case "-p":
          properties.setProperty("port", getNextArg(args, "-p", ++i)); break;
        case "-k":
          properties.setProperty("secure", "true"); break;
        case "-t":
          properties.setProperty("keytab", getNextArg(args, "-t", ++i)); break;
        case "-u":
          properties.setProperty("userprinc", getNextArg(args, "-u", ++i)); break;
        case "-d":
          properties.setProperty("driver", getNextArg(args, "-d", ++i)); break;
        case "-j":
          properties.setProperty("jaas", getNextArg(args, "-j", ++i)); break;
        case "-q":
          properties.setProperty("query", getNextArg(args, "-q", ++i)); break;
        case "-r":
          properties.setProperty("realm", getNextArg(args, "-r", ++i)); break;
        case "-St":
          properties.setProperty("ssltruststore", getNextArg(args, "-St", ++i)); break;
        case "-Sp":
          properties.setProperty("ssltruststorepassword", getNextArg(args, "-Sp", ++i)); break;
        case "-db":
          properties.setProperty("db", getNextArg(args, "-db", ++i)); break;
        case "-debug":
          properties.setProperty("debug", "true"); break;
        default:
          exitWithUsage("Unrecognised option: " + arg, -1);
      }
    }

    if (properties.getProperty("query") == null) {
      exitWithUsage("No query supplied", -1);
    }

    if (properties.getProperty("host") == null) {
      exitWithUsage("Server hostname cannot be null", -1);
    }

    configureLogger(Boolean.parseBoolean(properties.getProperty("debug")));

    HiveJdbcClient client = new HiveJdbcClient(properties);
    client.run();
  }
}
