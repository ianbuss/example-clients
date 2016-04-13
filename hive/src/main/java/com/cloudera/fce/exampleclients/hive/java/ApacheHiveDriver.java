package com.cloudera.fce.exampleclients.hive.java;

import com.cloudera.fce.exampleclients.common.JdbcDriver;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class ApacheHiveDriver implements JdbcDriver {
  @Override
  public String getName() {
    return "org.apache.hive.jdbc.HiveDriver";
  }

  @Override
  public String constructJdbcUrl(String host, int port) {
    return String.format("jdbc:hive2://%s:%d/default", host, port);
  }

  @Override
  public String constructJdbcUrl(String host, int port, String serverPrincipal,
                                 String kerberosRealm) {
    return constructJdbcUrl(host, port) + String.format(";principal=%s/%s@%s",
      serverPrincipal, host, kerberosRealm);
  }

  @Override
  public void loginViaJaas(String configFile) {
    System.setProperty("java.security.auth.login.config", configFile);
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
  }

  @Override
  public void loginViaKeytab(String userPrinc, String keyTab) throws IOException {
    UserGroupInformation.loginUserFromKeytab(userPrinc, keyTab);
  }

  @Override
  public void cleanup() {

  }
}
