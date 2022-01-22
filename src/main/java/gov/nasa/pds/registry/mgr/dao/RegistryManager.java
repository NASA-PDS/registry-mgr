package gov.nasa.pds.registry.mgr.dao;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.registry.mgr.cfg.RegistryCfg;
import gov.nasa.pds.registry.mgr.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.mgr.dao.schema.SchemaDao;


/**
 * A singleton object to query Elasticsearch.
 *  
 * @author karpenko
 */
public class RegistryManager
{
    private static RegistryManager instance = null;
    
    private RestClient esClient;
    private SchemaDao schemaDao;
    private DataDictionaryDao dataDictionaryDao;
    
    
    /**
     * Private constructor. Use getInstance() instead.
     * @param cfg Registry (Elasticsearch) configuration parameters.
     * @throws Exception Generic exception
     */
    private RegistryManager(RegistryCfg cfg) throws Exception
    {
        if(cfg.url == null || cfg.url.isEmpty()) throw new IllegalArgumentException("Missing Registry URL");
        
        esClient = EsClientFactory.createRestClient(cfg.url, cfg.authFile);
        
        String indexName = cfg.indexName;
        if(indexName == null || indexName.isEmpty()) 
        {
            indexName = "registry";
        }

        Logger log = LogManager.getLogger(this.getClass());
        log.info("Registry URL: " + cfg.url);
        log.info("Registry index: " + indexName);
        
        schemaDao = new SchemaDao(esClient, indexName);
        dataDictionaryDao = new DataDictionaryDao(esClient, indexName);                
    }
    
    
    /**
     * Initialize the singleton.
     * @param cfg Registry (Elasticsearch) configuration parameters.
     * @throws Exception Generic exception
     */
    public static void init(RegistryCfg cfg) throws Exception
    {
        instance = new RegistryManager(cfg);
    }
    
    
    /**
     * Clean up resources (close Elasticsearch client / connection).
     */
    public static void destroy()
    {
        if(instance == null) return;
        
        CloseUtils.close(instance.esClient);
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

}
