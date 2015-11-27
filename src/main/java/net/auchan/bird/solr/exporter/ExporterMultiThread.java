package net.auchan.bird.solr.exporter;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by madespat on 26/11/2015.
 */
public class ExporterMultiThread {

    public static final int NB_THREAD = 8;
    public static final int NB_LINE = 10000;

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(NB_THREAD);
        SolrServer solrServer = new HttpSolrServer("http://localhost:18080/solr/master_thieum");

        try {
            solrServer.deleteByQuery("*");
            solrServer.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < NB_THREAD; i++) {
            Runnable worker = new SolrExporterRunnable(solrServer, i * NB_LINE, NB_LINE);
            executorService.execute(worker);

        }
        executorService.shutdown();
        // Wait until all threads are finish
        while (!executorService.isTerminated()) {

        }
        System.out.println("\nFinished all threads");
    }

    public static class SolrExporterRunnable implements Runnable {

        SolrServer solrServer;
        int start;
        int nblines;

        public SolrExporterRunnable(SolrServer solrServer, int start, int nblines) {
            this.solrServer = solrServer;
            this.start = start;
            this.nblines = nblines;
        }

        public void run() {
            //solrServer = new HttpSolrServer("http://localhost:18080/solr/master_thieum");
            System.out.println("launching " + Thread.currentThread().toString());
            try {
                for(int i=start;i< start + nblines;++i) {
                    SolrInputDocument doc = new SolrInputDocument();
                    doc.addField("cat_string", "book");
                    doc.addField("id", "book-" + i);
                    doc.addField("name_string", "The Legend of the Hobbit part " + i);

                    for (int j = 0; j < 4 ; j++) {
                        SolrInputDocument childDocument = new SolrInputDocument();
                        childDocument.addField("id", "book-" + i + "-" + j);
                        childDocument.addField("buyer_string", "me" + j);
                        doc.addChildDocument(childDocument);
                    }

                    solrServer.add(doc);
                    //if(i%100==0) solrServer.commit();  // periodically flush
                }
                solrServer.commit();
            } catch (SolrServerException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
