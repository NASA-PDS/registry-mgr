package gov.nasa.pds.registry.mgr.dao;

import java.io.File;

public class SchemaUpdaterConfig
{
    public String indexName;
    public String lddCfgUrl;
    public File tempDir;
    
    
    public SchemaUpdaterConfig(String indexName, String lddCfgUrl)
    {
        this.indexName = indexName;
        this.lddCfgUrl = lddCfgUrl;
        this.tempDir = new File(System.getProperty("java.io.tmpdir"));
    }
}
