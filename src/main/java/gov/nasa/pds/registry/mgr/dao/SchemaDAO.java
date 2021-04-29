package gov.nasa.pds.registry.mgr.dao;

import java.time.Instant;
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
     * Call Elasticsearch "mappings" API to get a list of field names.
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
    
    /**
     * 
     * @param indexName
     * @param namespace
     * @return
     * @throws Exception
     */
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
    
    
    public void updateSchema(String indexName, List<Tuple> fields) throws Exception
    {
        if(fields == null || fields.isEmpty()) return;
        
        SchemaRequestBuilder bld = new SchemaRequestBuilder();
        String json = bld.createUpdateSchemaRequest(fields);
        
        Request req = new Request("PUT", "/" + indexName + "/_mapping");
        req.setJsonEntity(json);
        client.performRequest(req);
    }
    
    
    public DataTypesInfo getDataTypes(String indexName, Collection<String> ids, 
            boolean stopOnFirstMissing) throws Exception
    {
        if(indexName == null) throw new IllegalArgumentException("Index name is null");

        DataTypesInfo dtInfo = new DataTypesInfo();
        if(ids == null || ids.isEmpty()) return dtInfo;
        
        // Create request
        Request req = new Request("GET", "/" + indexName + "-dd/_mget?_source=es_data_type");
        
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
                dtInfo.newFields.add(new Tuple(rec.id, rec.esDataType));
            }
            // There is no data type for this field in ES registry-dd index
            else
            {
                // Automatically assign data type for known fields
                if(rec.id.startsWith("ref_lid_") || rec.id.startsWith("ref_lidvid_") 
                        || rec.id.endsWith("_Area")) 
                {
                    dtInfo.newFields.add(new Tuple(rec.id, "keyword"));
                    continue;
                }
                
                if(stopOnFirstMissing) throw new DataTypeNotFoundException(rec.id);
                
                // Get field namespace
                String ns = getFieldNamespace(rec.id);
                if(ns == null) throw new DataTypeNotFoundException(rec.id);
                
                dtInfo.missingNamespaces.add(ns);
                dtInfo.lastMissingField = rec.id;
            }
        }
        
        return dtInfo;
    }
    
    
    private static String getFieldNamespace(String fieldId)
    {
        int idx = fieldId.indexOf(':');
        if(idx < 1) return null;
        
        return fieldId.substring(0, idx);
    }

}
