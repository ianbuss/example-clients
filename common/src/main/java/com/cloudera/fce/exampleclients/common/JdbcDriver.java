package com.cloudera.fce.exampleclients.common;

import java.io.IOException;

public interface JdbcDriver {

  String getName();

  String constructJdbcUrl(String host, int port);

  String constructJdbcUrl(String host, int port, String serverPrincipal, String kerberosRealm);

  void loginViaJaas(String configFile);

  void loginViaKeytab(String userPrinc, String keyTab) throws IOException;

  void cleanup();

}
