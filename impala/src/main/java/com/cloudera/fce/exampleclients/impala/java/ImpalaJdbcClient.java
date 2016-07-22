package com.cloudera.fce.exampleclients.impala.java;

import com.cloudera.fce.exampleclients.common.JdbcClient;
import com.cloudera.fce.exampleclients.common.JdbcDriver;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

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
    String url = driver.constructJdbcUrl(
      properties.getProperty("host"),
      Integer.parseInt(properties.getProperty("port")),
      properties.getProperty("serverprinc", null),
      properties.getProperty("realm", null),
      properties.getProperty("username", null),
      properties.getProperty("password", null),
      properties.getProperty("ssltruststore", null),
      properties.getProperty("ssltruststorepassword", null));

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
    sb.append("Usage:" + ImpalaJdbcClient.class.getName() + "\n");
    sb.append("\t-h HOST\n");
    sb.append("\t-q QUERY\n");
    sb.append("\t[-d DATABASE] [-p PORT]\n");
    sb.append("\t[-s SERVER_PRINC] [-k] [-t KEYTAB] [-u USER_PRINC] [-r REALM]\n");
    sb.append("\t[-j JAAS_FILE]\n");
    sb.append("\t[-user LDAP_UID] [-pass LDAP_PASSWD]\n");
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
    properties.setProperty("db", "default");
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
      } else if (arg.equals("-St")) {
        properties.setProperty("ssltruststore", getNextArg(args, "-St", ++i));
      } else if (arg.equals("-Sp")) {
        properties.setProperty("ssltruststorepassword", getNextArg(args, "-Sp", ++i));
      } else if (arg.equals("-d")) {
        properties.setProperty("db", getNextArg(args, "-d", ++i));
      } else if (arg.equals("-user")) {
        properties.setProperty("username", getNextArg(args, "-user", ++i));
      } else if (arg.equals("-pass")) {
        properties.setProperty("password", getNextArg(args, "-pass", ++i));
      } else if (arg.equals("-debug")) {
        properties.setProperty("debug", "true");
      }
    }

    if (properties.getProperty("query") == null) {
      exitWithUsage("No query supplied", -1);
    }

    if (properties.getProperty("host") == null) {
      exitWithUsage("Server hostname cannot be null", -1);
    }

    ConsoleAppender console = new ConsoleAppender();
    console.setLayout(new PatternLayout("%d [%p] %m%n"));
    if (Boolean.parseBoolean(properties.getProperty("debug"))) {
      console.setThreshold(Level.DEBUG);
    } else {
      console.setThreshold(Level.INFO);
    }
    console.activateOptions();
    Logger.getRootLogger().addAppender(console);

    ImpalaJdbcClient client = new ImpalaJdbcClient(properties);
    client.run();
  }

}
