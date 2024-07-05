package gov.nasa.pds.registry.mgr.dao;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import gov.nasa.pds.registry.common.EstablishConnectionFactory;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.es.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.common.es.dao.schema.SchemaDao;


/**
 * A singleton object to query Elasticsearch.
 *  
 * @author karpenko
 */
public class RegistryManager
{
    private static RegistryManager instance = null;
    
    private RestClient client;
    private SchemaDao schemaDao;
    private DataDictionaryDao dataDictionaryDao;
    private RegistryDao registryDao;
    
    
    /**
     * Private constructor. Use getInstance() instead.
     * @param cfg Registry (Elasticsearch) configuration parameters.
     * @throws Exception Generic exception
     */
    private RegistryManager(String connURL, String authFile, String index) throws Exception
    {
        if(connURL == null || connURL.isEmpty()) throw new IllegalArgumentException("Missing Registry URL");
        
        client = EstablishConnectionFactory.from(connURL, authFile).createRestClient();
        
        String indexName = index;
        if(indexName == null || indexName.isEmpty()) 
        {
            indexName = "registry";
        }

        Logger log = LogManager.getLogger(this.getClass());
        log.info("Registry URL: " + connURL);
        log.info("Registry index: " + indexName);
        
        schemaDao = new SchemaDao(client, indexName);
        dataDictionaryDao = new DataDictionaryDao(client, indexName);
        registryDao = new RegistryDao(client, indexName);
    }
    
    
    /**
     * Initialize the singleton.
     * @param cfg Registry (Elasticsearch) configuration parameters.
     * @throws Exception Generic exception
     */
    public static void init(String connURL, String authFile, String index) throws Exception
    {
        instance = new RegistryManager(connURL, authFile, index);
    }
    
    
    /**
     * Clean up resources (close Elasticsearch client / connection).
     */
    public static void destroy()
    {
        if(instance == null) return;
        
        CloseUtils.close(instance.client);
        instance = null;
    }
    
    
    /**
     * Get the singleton instance.
     * @return Registry manager singleton
     */
    public static RegistryManager getInstance()
    {
        return instance;
    }
    
    
    /**
     * Get schema DAO object.
     * @return Schema DAO
     */
    public SchemaDao getSchemaDao()
    {
        return schemaDao;
    }

    
    /**
     * Get schema DAO object.
     * @return Schema DAO
     */
    public DataDictionaryDao getDataDictionaryDao()
    {
        return dataDictionaryDao;
    }

    
    /**
     * Get registry DAO object.
     * @return Registry DAO
     */
    public RegistryDao getRegistryDao()
    {
        return registryDao;
    }

}
