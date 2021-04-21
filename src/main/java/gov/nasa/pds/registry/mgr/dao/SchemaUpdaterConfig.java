package gov.nasa.pds.registry.mgr.dao;

import java.io.File;

public class SchemaUpdaterConfig
{
    public String indexName;
    public String dataDictionaryRepoUrl;
    public File tempDir;
    
    
    public SchemaUpdaterConfig(String indexName)
    {
        this.indexName = indexName;
        this.tempDir = new File(System.getProperty("java.io.tmpdir"));
    }
}
