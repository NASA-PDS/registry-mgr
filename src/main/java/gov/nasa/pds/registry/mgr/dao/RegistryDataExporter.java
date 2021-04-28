package gov.nasa.pds.registry.mgr.dao;

public class RegistryDataExporter extends DataExporter
{
    private String filterFieldName;
    private String filterFieldValue;

    
    public RegistryDataExporter(String esUrl, String indexName, String authConfigFile)
    {
        super(esUrl, indexName, authConfigFile);
    }
    

    public void setFilterField(String name, String value)
    {
        this.filterFieldName = name;
        this.filterFieldValue = value;
    }
    
    
    @Override
    protected String createRequest(int batchSize, String searchAfter) throws Exception
    {
        RegistryRequestBuilder reqBld = new RegistryRequestBuilder();
        
        String json = (filterFieldName == null) ? 
                reqBld.createExportAllDataRequest("lidvid", batchSize, searchAfter) :
                reqBld.createExportDataRequest(filterFieldName, filterFieldValue, "lidvid", batchSize, searchAfter);

        return json;
    }

}
