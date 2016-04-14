package com.cloudera.fce.exampleclients.solr.java;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class SolrClient {

  private CloudSolrServer solrServer;
  private String zkEnsemble;
  private String user;
  private String keytab;
  private String collection;
  private String query;

  public SolrClient(String zkEnsemble, String collection, String query, String user, String keytab) {
    this.zkEnsemble = zkEnsemble;
    this.user = user;
    this.keytab = keytab;
    this.query = query;
  }

  private static void usageAndExit(int retCode) {
    System.err.printf("Usage: %s -s COLLECTION -z ZOOKEEPERS -q QUERY [-k] [-t KEYTAB]\n", SolrClient.class.getSimpleName());
    System.exit(retCode);
  }

  private static void exitWithMessage(String msg, int retCode) {
    System.err.println(msg);
    System.exit(retCode);
  }

  public void initialise() {
    if (collection == null || collection.isEmpty()) exitWithMessage("No Solr collection specified", -1);
    if (zkEnsemble == null || zkEnsemble.isEmpty()) exitWithMessage("No ZooKeeper ensemble specified", -1);

    solrServer = new CloudSolrServer(zkEnsemble);
    solrServer.setDefaultCollection(collection);
  }

  public void doQuery() throws SolrServerException {
    SolrQuery sQuery = new SolrQuery();
    sQuery.setQuery(query);
    QueryResponse response = solrServer.query(sQuery);
    SolrDocumentList list = response.getResults();
    for (SolrDocument doc : list) {
      System.out.println(doc.toString());
    }
  }


}
