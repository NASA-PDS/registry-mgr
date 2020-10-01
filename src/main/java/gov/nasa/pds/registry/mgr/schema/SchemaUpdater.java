package gov.nasa.pds.registry.mgr.schema;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.dao.SchemaDAO;


/**
 * Updates Elasticsearch schema by calling Elasticsearch API  
 * @author karpenko
 */
public class SchemaUpdater
{
    private RestClient client;
    private String indexName;
    
    private Set<String> esFieldNames;
    
    private UpdateSchemaBatch batch;
    private int totalCount;
    private int lastBatchCount;
    private int batchSize = 100;
    
    /**
     * Constructor 
     * @param cfg Registry manager configuration
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name
     * @throws Exception
     */
    public SchemaUpdater(RestClient client, String indexName) throws Exception
    {
        this.client = client;
        this.indexName = indexName;
        
        // Get a list of existing field names from Solr
        this.esFieldNames = SchemaDAO.getFieldNames(client, indexName);
    }


    /**
     * Add fields from data dictionary to Elasticsearch schema. Ignore existing fields.
     * @param dd
     * @throws Exception
     */
    public void updateSchema(List<String> newFields) throws Exception
    {
        lastBatchCount = 0;
        totalCount = 0;
        batch = new UpdateSchemaBatch();

        for(String newField: newFields)
        {
            addField(newField);
        }

        finish();
    }
    
    
    private void addField(String name) throws Exception
    {
        if(esFieldNames.contains(name)) return;
        
        // Add field request to the batch
        batch.addField(name, "");
        totalCount++;

        // Commit if reached batch/commit size
        if(totalCount % batchSize == 0)
        {
            System.out.println("Adding fields " + (lastBatchCount+1) + "-" + totalCount);
            SchemaDAO.updateMappings(client, indexName, batch.closeAndGetJson());
            lastBatchCount = totalCount;
            batch = new UpdateSchemaBatch();
        }
    }
    
    
    private void finish() throws Exception
    {
        if(batch.isEmpty()) return;
        
        System.out.println("Adding fields " + (lastBatchCount+1) + "-" + totalCount);
        SchemaDAO.updateMappings(client, indexName, batch.closeAndGetJson());
        lastBatchCount = totalCount;
    }
    
    
}
