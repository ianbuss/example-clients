package com.cloudera.fce.exampleclients.impala.java;

import com.cloudera.fce.exampleclients.common.JdbcDriver;

import java.io.*;
import java.util.UUID;

public class SimbaImpalaDriver implements JdbcDriver {

  private String jaasFile = null;

  @Override
  public String getName() {
    return "com.cloudera.impala.jdbc41.Driver";
  }

  @Override
  public String constructJdbcUrl(String host, int port) {
    return String.format("jdbc:impala://%s:%d;AuthMech=0", host, port);
  }

  @Override
  public String constructJdbcUrl(String host, int port, String princ, String realm) {
    return String.format("jdbc:impala://%s:%d", host, port) +
      String.format(";AuthMech=1;KrbRealm=%s;KrbHostFQDN=%s;KrbServiceName=%s",
        realm, host, princ);
  }

  @Override
  public void loginViaJaas(String configFile) {
    System.setProperty("java.security.auth.login.config", configFile);
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
  }

  @Override
  public void loginViaKeytab(String userPrinc, String keyTab) throws IOException {
    jaasFile = "/tmp/" + UUID.randomUUID().toString();
    System.out.println("Writing JAAS config to: " + jaasFile);
    PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(jaasFile)));
    out.write("Client {\n");
    out.write(" com.sun.security.auth.module.Krb5LoginModule required\n");
    out.write(" useKeyTab=true\n");
    out.write(" keyTab=" + keyTab + "\n");
    out.write(" doNotPrompt=true\n");
    out.write(" principal=" + userPrinc + ";\n");
    out.write("};\n");
    out.close();
    loginViaJaas(jaasFile);
  }

  @Override
  public void cleanup() {
    if (null != jaasFile) {
      new File(jaasFile).delete();
    }
  }
}
