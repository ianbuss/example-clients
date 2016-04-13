package com.cloudera.fce.example.impala.java;

import com.cloudera.fce.exampleclients.common.JdbcClient;
import com.cloudera.fce.exampleclients.common.JdbcDriver;

import java.util.Properties;

public class ImpalaJdbcClient extends JdbcClient {

  private static final int DEFAULT_HS2_PORT = 21050;
  private static final String DEFAULT_HS2_PRINCIPAL = "impala";

  private Properties properties;

  public ImpalaJdbcClient(Properties properties) {
    this.properties = properties;
  }

  public JdbcDriver loadDriver() throws ClassNotFoundException {
    JdbcDriver driver = new SimbaImpalaDriver();
    Class.forName(driver.getName());
    return driver;
  }

  public void run() throws Exception {
    JdbcDriver driver = loadDriver();
    String url;
    boolean secure = Boolean.parseBoolean(properties.getProperty("secure"));
    if (secure) {
      url = driver.constructJdbcUrl(
        properties.getProperty("host"),
        Integer.parseInt(properties.getProperty("port")),
        properties.getProperty("serverprinc"),
        properties.getProperty("realm"));
    } else {
      url = driver.constructJdbcUrl(
        properties.getProperty("host"),
        Integer.parseInt(properties.getProperty("port")));
    }

    // Set up security
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
      runQuery(properties.getProperty("query"));
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
    System.err.printf(
      "Usage: %s -h HOST -q QUERY [-p PORT] [-s SERVER_PRINC] [-k] [-t KEYTAB] [-u USER_PRINC] [-j JAAS_FILE] [-S]\n",
      ImpalaJdbcClient.class.getName());
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
      if (arg.equals("-h")) {
        properties.setProperty("host", getNextArg(args, "-h", ++i));
      } else if (arg.equals("-s")) {
        properties.setProperty("serverprinc", getNextArg(args, "-s", ++i));
      } else if (arg.equals("-p")) {
        properties.setProperty("port", getNextArg(args, "-p", ++i));
      } else if (arg.equals("-k")) {
        properties.setProperty("secure", "true");
      } else if (arg.equals("-t")) {
        properties.setProperty("keytab", getNextArg(args, "-t", ++i));
      } else if (arg.equals("-u")) {
        properties.setProperty("userprinc", getNextArg(args, "-u", ++i));
      } else if (arg.equals("-j")) {
        properties.setProperty("jaas", getNextArg(args, "-j", ++i));
      } else if (arg.equals("-q")) {
        properties.setProperty("query", getNextArg(args, "-q", ++i));
      } else if (arg.equals("-r")) {
        properties.setProperty("realm", getNextArg(args, "-r", ++i));
      } else if (arg.equals("-S")) {
        properties.setProperty("ssl", "true");
      }
    }

    if (properties.getProperty("query") == null) {
      exitWithUsage("No query supplied", -1);
    }

    if (properties.getProperty("host") == null) {
      exitWithUsage("Server hostname cannot be null", -1);
    }

    ImpalaJdbcClient client = new ImpalaJdbcClient(properties);
    client.run();
  }

}
