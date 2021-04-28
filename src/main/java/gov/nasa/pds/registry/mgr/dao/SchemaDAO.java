package gov.nasa.pds.registry.mgr.dao;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.SearchResponseParser;
import gov.nasa.pds.registry.mgr.util.Tuple;


/**
 * Elasticsearch schema DAO (Data Access Object).
 * 
 * @author karpenko
 */
public class SchemaDAO
{
    private RestClient client;
    
    
    /**
     * Constructor
     * @param client Elasticsearch client
     */
    public SchemaDAO(RestClient client)
    {
        this.client = client;
    }
    
    
    /**
     * Call Elasticsearch "mappings" API to get a list of field names for an index.
     * @param indexName Elasticsearch index name
     * @return a collection of field names
     * @throws Exception
     */
    public Set<String> getFieldNames(String indexName) throws Exception
    {
        Request req = new Request("GET", "/" + indexName + "/_mappings");
        Response resp = client.performRequest(req);
        
        MappingsParser parser = new MappingsParser(indexName);
        return parser.parse(resp.getEntity());
    }
    
    
    private static class GetLddDateRespParser extends SearchResponseParser implements SearchResponseParser.Callback
    {
        public Instant date = null;
        
        @Override
        public void onRecord(String id, Object rec) throws Exception
        {
            if(rec instanceof Map)
            {
                @SuppressWarnings("rawtypes")
                Map map = (Map)rec;
                String strDate = (String)map.get("date");
                if(strDate == null) return;

                date = Instant.parse(strDate);
            }
        }
    }
    
    public Instant getLddDate(String indexName, String namespace) throws Exception
    {
        SchemaRequestBuilder bld = new SchemaRequestBuilder();
        String json = bld.createGetLddInfoRequest(namespace);

        Request req = new Request("GET", "/" + indexName + "-dd/_search");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);
        
        GetLddDateRespParser parser = new GetLddDateRespParser();
        parser.parseResponse(resp, parser); 
        return parser.date;
    }
    
    
    public void updateMappings(String indexName, Collection<String> ids, 
            MissingDataTypeCallback cb) throws Exception
    {
        if(ids == null || ids.isEmpty()) return;
        
        List<Tuple> fields = getDataTypes(indexName, ids, cb);
        SchemaRequestBuilder bld = new SchemaRequestBuilder();
        String json = bld.createUpdateSchemaRequest(fields);
        
        Request req = new Request("PUT", "/" + indexName + "/_mapping");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);
    }
    
    
    public static interface MissingDataTypeCallback
    {
        public String getDataType(String fieldId);
    }
    

    public List<Tuple> getDataTypes(String indexName, Collection<String> ids, 
            MissingDataTypeCallback cb) throws Exception
    {
        if(indexName == null) throw new IllegalArgumentException("Index name is null");

        List<Tuple> results = new ArrayList<>();
        if(ids == null || ids.isEmpty()) return results;
        
        // Create request
        indexName = indexName + "-dd";
        Request req = new Request("GET", "/" + indexName + "/_mget?_source=es_data_type");
        
        // Create request body
        SchemaRequestBuilder bld = new SchemaRequestBuilder();
        String json = bld.createMgetRequest(ids);
        req.setJsonEntity(json);
        
        // Call ES
        Response resp = client.performRequest(req);
        MgetParser parser = new MgetParser();
        List<MgetParser.Record> records = parser.parse(resp.getEntity());
        
        for(MgetParser.Record rec: records)
        {
            if(rec.found)
            {
                results.add(new Tuple(rec.id, rec.esDataType));
            }
            else
            {
                String dataType = cb.getDataType(rec.id);
                if(dataType == null)
                {
                    throw new Exception("Could not find datatype for field '" + rec.id + "'.\n" 
                            + "See 'https://nasa-pds.github.io/pds-registry-app/operate/common-ops.html#Load' for more information.");
                }
                else
                {
                    results.add(new Tuple(rec.id, dataType));
                }
            }
        }
        
        return results;
    }
    
}
