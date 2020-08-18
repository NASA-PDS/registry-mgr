package gov.nasa.pds.registry.mgr.cmd;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.Constants;
import gov.nasa.pds.registry.mgr.util.CloseUtils;
import gov.nasa.pds.registry.mgr.util.es.EsClientBuilder;
import gov.nasa.pds.registry.mgr.util.es.EsDocWriter;
import gov.nasa.pds.registry.mgr.util.es.EsRequestBuilder;


public class ExportDataCmd implements CliCommand
{
    private static final int BATCH_SIZE = 100;
    private String filterMessage;
    
    
    public ExportDataCmd()
    {
    }

    
    @Override
    public void run(CommandLine cmdLine) throws Exception
    {
        if(cmdLine.hasOption("help"))
        {
            printHelp();
            return;
        }
        
        // File path
        String filePath = cmdLine.getOptionValue("file");
        if(filePath == null) 
        {
            throw new Exception("Missing required parameter '-file'");
        }
        
        String esUrl = cmdLine.getOptionValue("es", "http://localhost:9200");
        String indexName = cmdLine.getOptionValue("index", Constants.DEFAULT_REGISTRY_INDEX);

        String query = buildEsQuery(cmdLine);
        if(query == null)
        {
            throw new Exception("One of the following options is required: -lidvid, -packageId, -all");
        }

        System.out.println("Elasticsearch URL: " + esUrl);
        System.out.println("            Index: " + indexName);
        System.out.println(filterMessage);
        System.out.println();
        
        EsDocWriter writer = null; 
        RestClient client = null;
        
        try
        {
            writer = new EsDocWriter(new File(filePath));
            
            client = EsClientBuilder.createClient(esUrl);

/*            
            SolrQuery solrQuery = new SolrQuery(query);

            // Sort is required by Solr cursor
            solrQuery.setSort(SortClause.asc("lidvid"));
            solrQuery.setRows(BATCH_SIZE);

            int numDocs = 0;
            
            SolrCursor cursor = new SolrCursor(client, collectionName, solrQuery);
            while(cursor.next())
            {
                SolrDocumentList docs = cursor.getResults();
                writer.write(docs);
                
                numDocs += docs.size();
                if(docs.size() != 0)
                {
                    System.out.println("Exported " + numDocs + " document(s)");
                }
            }
*/            
            System.out.println("Done");
        }
        finally
        {
            CloseUtils.close(client);
            CloseUtils.close(writer);
        }
    }

    
    private String buildEsQuery(CommandLine cmdLine) throws Exception
    {
        EsRequestBuilder bld = new EsRequestBuilder();
        
        String id = cmdLine.getOptionValue("lidvid");
        if(id != null)
        {
            filterMessage = "           LIDVID: " + id;
            return bld.createFilterQuery("lidvid", id);
        }
        
        id = cmdLine.getOptionValue("packageId");
        if(id != null)
        {
            filterMessage = "       Package ID: " + id;            
            return bld.createFilterQuery("_package_id", id);
        }

        if(cmdLine.hasOption("all"))
        {
            filterMessage = "Export all documents ";
            return bld.createMatchAllQuery();
        }

        return null;
    }
    
    
    public void printHelp()
    {
        System.out.println("Usage: registry-manager export-data <options>");

        System.out.println();
        System.out.println("Export data from registry index");
        System.out.println();
        System.out.println("Required parameters:");
        System.out.println("  -file <path>        Output file path");        
        System.out.println("  -lidvid <id>        Export data by lidvid");
        System.out.println("  -packageId <id>     Export data by package id");
        System.out.println("  -all                Export all data");
        System.out.println("Optional parameters:");
        System.out.println("  -es <url>       Elasticsearch URL. Default is http://localhost:9200");
        System.out.println("  -index <name>   Elasticsearch index name. Default is 'registry'");
        System.out.println();
    }

}
