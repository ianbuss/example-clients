package com.cloudera.fce.exampleclients.common;

import java.io.IOException;

public interface JdbcDriver {

  /**
   * Get the fully qualified class name of the driver so it can be loaded via reflection
   *
   * @return
   */
  String getName();

  /**
   * Create the JDBC connection URL for this driver
   * @return
   */
  String constructJdbcUrl();

  /**
   * Specify the host and port of the server
   * @param host                  FQDN of the server host
   * @param port                  port on which to contact the server
   * @return
   */
  JdbcDriver withHostPort(String host, int port);

  /**
   * Specify the database to connect to. Defaults to "default".
   * @param database
   * @return
   */
  JdbcDriver withDatabase(String database);

  /**
   * Specify the Kerberos connection parameters
   * @param serverPrincipal       server Kerberos short name, e.g. "impala" or "hive"
   * @param kerberosRealm         Kerberos realm of the server
   * @return
   */
  JdbcDriver withKerberos(String serverPrincipal, String kerberosRealm);

  /**
   * Specify LDAP connection parameters
   * @param user
   * @param pass
   * @return
   */
  JdbcDriver withLDAP(String user, String pass);

  /**
   * Specify SSL connection parameters
   * @param trustStore
   * @param trustStorePassword
   * @return
   */
  JdbcDriver withSSL(String trustStore, String trustStorePassword);

  /**
   * Define the delegation user (impersonation)
   * @param doAsUser
   * @return
   */
  JdbcDriver withImpersonation(String doAsUser);

  /**
   * Configure the session to use a specified JAAS configuration file
   *
   * @param configFile file location of the JAAS configuration file
   */
  void loginViaJaas(String configFile);

  /**
   * Use the supplied keytab and user.  The keytab can be created using the command line tool <code>ktutil</code>,
   * ensuring all the required encyption types are present.  For example:
   * <pre>
   *   {@code
   *   $ ktutil
   *   ktutil: addent -password -p alice@DEV -k 1 -e aes256-cts
   *   Password for alice@DEV:
   *   ktutil: addent -password -p alice@DEV -k 1 -e aes128-cts
   *   Password for alice@DEV:
   *   ktutil: addent -password -p alice@DEV -k 1 -e rc4-hmac
   *   Password for alice@DEV:
   *   ktutil: wkt alice.keytab
   *   ktutil: q
   *   }
   * </pre>
   *
   * @param userPrinc Kerberos principal of the connecting user. If using the default Kerberos realm, the short name
   *                  should suffice. E.g. "ian" or "ian@REALM".
   * @param keyTab    file location of the user's keytab. If authenticating against AD ensure it has entries with
   *                  the appropriate configured encryption types.
   * @throws IOException
   */
  void loginViaKeytab(String userPrinc, String keyTab) throws IOException;

  /**
   * Release any resources used when logging in via Kerberos (e.g. temporary config files)
   */
  void cleanup();

}
