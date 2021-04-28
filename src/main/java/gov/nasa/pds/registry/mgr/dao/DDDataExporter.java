package gov.nasa.pds.registry.mgr.dao;

public class DDDataExporter extends DataExporter
{
    public DDDataExporter(String esUrl, String indexName, String authConfigFile)
    {
        super(esUrl, indexName + "-dd", authConfigFile);
    }

    
    @Override
    protected String createRequest(int batchSize, String searchAfter) throws Exception
    {
        RegistryRequestBuilder reqBld = new RegistryRequestBuilder();
        String json = reqBld.createExportAllDataRequest("es_field_name", batchSize, searchAfter);
        return json;
    }

}
