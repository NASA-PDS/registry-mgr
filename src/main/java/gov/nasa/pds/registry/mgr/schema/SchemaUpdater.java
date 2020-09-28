package gov.nasa.pds.registry.mgr.schema;

import java.util.Map;
import java.util.Set;
import org.elasticsearch.client.RestClient;
import gov.nasa.pds.registry.mgr.util.es.EsSchemaUtils;


/**
 * Updates Elasticsearch schema by calling Elasticsearch API  
 * @author karpenko
 */
public class SchemaUpdater
{
    private RestClient client;
    private String indexName;
    
    private Set<String> existingFieldNames;
    
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
        this.existingFieldNames = EsSchemaUtils.getFieldNames(client, indexName);
    }


    /**
     * Add fields from data dictionary to Elasticsearch schema. Ignore existing fields.
     * @param dd
     * @throws Exception
     */
    public void updateSchema(Map<String, String> ddFields) throws Exception
    {
        lastBatchCount = 0;
        totalCount = 0;
        batch = new UpdateSchemaBatch();

        for(Map.Entry<String, String> item: ddFields.entrySet())
        {
            addField(item.getKey(), item.getValue());
        }

        finish();
    }
    
    
    private void addField(String name, String type) throws Exception
    {
        if(existingFieldNames.contains(name)) return;
        existingFieldNames.add(name);
        
        // Add field request to the batch
        batch.addField(name, type);
        totalCount++;

        // Commit if reached batch/commit size
        if(totalCount % batchSize == 0)
        {
            System.out.println("Adding fields " + (lastBatchCount+1) + "-" + totalCount);
            EsSchemaUtils.updateMappings(client, indexName, batch.closeAndGetJson());
            lastBatchCount = totalCount;
            batch = new UpdateSchemaBatch();
        }
    }
    
    
    private void finish() throws Exception
    {
        if(batch.isEmpty()) return;
        
        System.out.println("Adding fields " + (lastBatchCount+1) + "-" + totalCount);
        EsSchemaUtils.updateMappings(client, indexName, batch.closeAndGetJson());
        lastBatchCount = totalCount;
    }
    
    
}
