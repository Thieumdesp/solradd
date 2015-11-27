package net.auchan.bird.solr.exporter;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;

/**
 * Created by madespat on 26/11/2015.
 */
public class SimpleSolrExporter {
    public static void main(String[] args) {
        SolrServer server = new HttpSolrServer("http://localhost:18080/solr/master_thieum");


        try {
            server.deleteByQuery("*");
            server.commit();

            for(int i=0;i<1000;++i) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField("cat_string", "book");
                doc.addField("id", "book-" + i);
                doc.addField("name_string", "The Legend of the Hobbit part " + i);

                for (int j = 0; j < 4 ; j++) {
                    SolrInputDocument childDocument = new SolrInputDocument();
                    childDocument.addField("id", "book-" + 1 + "-" + j);
                    childDocument.addField("buyer_string", "me" + j);
                    doc.addChildDocument(childDocument);
                }

                server.add(doc);
                if(i%100==0) server.commit();  // periodically flush
            }
            server.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }
}
