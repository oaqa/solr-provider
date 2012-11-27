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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.DirectXmlRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
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
      /*if (canRunInLocalMode(serverUrl)) {
        System.err.printf("Running Solr retrieval on local mode\n");
        this.server = createSolrServer(LOCALHOST);
      } else {*/
        System.err.printf("Running Solr retrieval on remote mode\n");
        this.server = createSolrServer(serverUrl);
      //}
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

  public SolrDocumentList runQuery(String q, int results) throws SolrServerException {
    SolrQuery query = new SolrQuery();
    query.setQuery(escapeQuery(q));
    query.setRows(results);
    query.setFields("*", "score");
    QueryResponse rsp = server.query(query,METHOD.POST);
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
			ArrayList<String>results=(ArrayList)rsp.getResults().get(0).getFieldValues("text");
			docText =  results.get(0);
		}		
		return docText;
	}

  private String escapeQuery(String term) {
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
  
  public static void main(String args[]){
	  try{
		  SolrWrapper wrapper=new SolrWrapper("http://peace.isri.cs.cmu.edu:9080/solr/genomics-simple/",9080,null,null);
		  String text=wrapper.getDocText("15342797");
		  System.out.println(text);
		  
	  }catch(Exception e){
		  e.printStackTrace();
	  }
  }
  
	public void indexDocument(String docXML) {

		String xml = "<add>" + docXML + "</add>";
		// System.out.println(xml);
		DirectXmlRequest xmlreq = new DirectXmlRequest("/update", xml);
		try {
			server.request(xmlreq);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public SolrServer getServer() {
		return server;
	}


	public SolrInputDocument makeSolrDocument(HashMap<String, Object> hshMap)
			throws Exception {

		SolrInputDocument doc = new SolrInputDocument();

		Iterator<String> keys = hshMap.keySet().iterator();
		while (keys.hasNext()) {
			String key = keys.next();
			Object value = hshMap.get(key);

			SolrInputField field = new SolrInputField(key);
			try {
				doc.addField(field.getName(), value, 1.0f);

			} catch (Exception e) {
				e.printStackTrace();

			}

		}

		return doc;

	}

}
