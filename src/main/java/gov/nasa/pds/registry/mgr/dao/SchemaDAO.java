package gov.nasa.pds.registry.mgr.dao;

import java.util.Set;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;


public class SchemaDAO
{
    public static Set<String> getFieldNames(RestClient client, String indexName) throws Exception
    {
        Request req = new Request("GET", "/" + indexName + "/_mappings");
        Response resp = client.performRequest(req);
        
        MappingsParser parser = new MappingsParser(indexName);
        return parser.parse(resp.getEntity());
    }
    
    
    public static void updateMappings(RestClient client, String indexName, String json) throws Exception
    {
        Request req = new Request("PUT", "/" + indexName + "/_mapping");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);
    }
    
    
    public static boolean indexExists(RestClient client, String indexName) throws Exception
    {
        Request req = new Request("HEAD", "/" + indexName);
        Response resp = client.performRequest(req);
        return resp.getStatusLine().getStatusCode() == 200;
    }

    
    
}
