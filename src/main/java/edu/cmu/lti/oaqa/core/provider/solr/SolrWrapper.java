/*
 *  Copyright 2012 Carnegie Mellon University
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.cmu.lti.oaqa.core.provider.solr;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.core.CoreContainer;
public final class SolrWrapper implements Closeable {

  private static final String LOCALHOST = "localhost";

  private final SolrServer server;

  boolean embedded;

  public SolrWrapper(String serverUrl, Integer serverPort, Boolean embedded, String core)
          throws Exception {
    if (embedded != null && embedded.booleanValue()) {
      this.server = createEmbeddedSolrServer(core);
      this.embedded = true;
    } else {
      if (canRunInLocalMode(serverUrl)) {
        System.err.printf("Running Solr retrieval on local mode\n");
        this.server = createSolrServer(LOCALHOST);
      } else {
        System.err.printf("Running Solr retrieval on remote mode\n");
        this.server = createSolrServer(serverUrl);
      }
    }
  }

  private boolean canRunInLocalMode(String serverUrl) {
    try {
      URL url = new URL(serverUrl);
      int port = url.getPort();
      Socket socket = new Socket("localhost", port);
      socket.close();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  private SolrServer createSolrServer(String url) throws Exception {
    SolrServer server = new HttpSolrServer(url);
    server.ping();
    return server;
  }

  private SolrServer createEmbeddedSolrServer(String core) throws Exception {
    System.setProperty("solr.solr.home", core);
    CoreContainer.Initializer initializer = new CoreContainer.Initializer();
    CoreContainer coreContainer = initializer.initialize();
    return new EmbeddedSolrServer(coreContainer, "");
  }
  
  public SolrDocumentList runQuery(String q, int results, String... fields) throws SolrServerException {
    SolrQuery query = new SolrQuery();
    query.setQuery(escapeQuery(q));
    query.setRows(results);
    if (fields.length == 0) {
      query.setFields("*", "score");
    } else {
      List<String> newFields = Arrays.asList(fields);
      newFields.add("score");
      query.setFields(newFields.toArray(new String[0]));
    }
    return runQuery(query, results);
  }

  public SolrDocumentList runQuery(SolrQuery query, int results) throws SolrServerException {
    QueryResponse rsp = server.query(query);
    return rsp.getResults();
  }

	public String getDocText(String id) throws SolrServerException {
		String q = "id:" + id;
		SolrQuery query = new SolrQuery();
		query.setQuery(q);
		query.setFields("text");
		QueryResponse rsp = server.query(query);
		String docText = "";
		if (rsp.getResults().getNumFound() > 0) {
			docText = (String) rsp.getResults().get(0).getFieldValue("text");
		}
		return docText;
	}

  public String escapeQuery(String term) {
    term = term.replace('?', ' ');
    term = term.replace('[', ' ');
    term = term.replace(']', ' ');
    term = term.replace('/', ' ');
    term = term.replaceAll("\'", "");
    return term;
  }

  public void close() {
    if (embedded) {
      ((EmbeddedSolrServer) server).shutdown();
    }
  }
}
