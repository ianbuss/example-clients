package com.cloudera.fce.exampleclients.common;

import java.io.IOException;

public interface JdbcDriver {

  /**
   * Get the fully qualified class name of the driver so it can be loaded via reflection
   * @return
   */
  String getName();

  /**
   * Create the JDBC connection URL for this driver
   * @param host  FQDN of the server host
   * @param port  port on which to contact the server
   * @param serverPrincipal  server Kerberos short name, e.g. "impala" or "hive"
   * @param kerberosRealm  Kerberos realm of the server
   * @param sslTrustStore  file location of the trust store containing the CA or server public cert (in JKS format)
   * @param sslTrustStorePassword  password for the truststore
   * @return
   */
  String constructJdbcUrl(String host, int port, String serverPrincipal, String kerberosRealm,
                          String sslTrustStore, String sslTrustStorePassword);

  /**
   * Configure the session to use a specified JAAS configuration file
   * @param configFile  file location of the JAAS configuration file
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
   * @param userPrinc  Kerberos principal of the connecting user. If using the default Kerberos realm, the short name
   *                  should suffice. E.g. "ian" or "ian@REALM".
   * @param keyTab  file location of the user's keytab. If authenticating against AD ensure it has entries with
   *               the appropriate configured encryption types.
   * @throws IOException
   */
  void loginViaKeytab(String userPrinc, String keyTab) throws IOException;

  /**
   * Release any resources used when logging in via Kerberos (e.g. temporary config files)
   */
  void cleanup();

}
