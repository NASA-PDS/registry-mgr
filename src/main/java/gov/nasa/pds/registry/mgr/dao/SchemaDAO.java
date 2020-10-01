package gov.nasa.pds.registry.mgr.dao;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.mgr.util.DebugUtils;


public class SchemaDAO
{
    private RestClient client;
    
    
    public SchemaDAO(RestClient client)
    {
        this.client = client;
    }
    
    
    public Set<String> getFieldNames(String indexName) throws Exception
    {
        Request req = new Request("GET", "/" + indexName + "/_mappings");
        Response resp = client.performRequest(req);
        
        MappingsParser parser = new MappingsParser(indexName);
        return parser.parse(resp.getEntity());
    }
    
    
    public void updateMappings(String indexName, String json) throws Exception
    {
        Request req = new Request("PUT", "/" + indexName + "/_mapping");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);
    }
    
    
    public boolean indexExists(String indexName) throws Exception
    {
        Request req = new Request("HEAD", "/" + indexName);
        Response resp = client.performRequest(req);
        return resp.getStatusLine().getStatusCode() == 200;
    }

    
    public Map<String, String> getDataTypes(String indexName, List<String> ids) throws Exception
    {
        if(indexName == null) throw new IllegalArgumentException("Index name is null");

        Map<String, String> map = new TreeMap<>();
        if(ids == null || ids.isEmpty()) return map;
        
        // Create request
        indexName = indexName + "-dd";
        Request req = new Request("GET", "/" + indexName + "/_mget?_source=es_data_type");
        
        // Create request body
        SchemaRequestBld bld = new SchemaRequestBld();
        String json = bld.createMgetRequest(ids);
        req.setJsonEntity(json);
        
        // Call ES
        Response resp = client.performRequest(req);
        
        DebugUtils.dumpResponseBody(resp);
        
        return map;
    }
}
