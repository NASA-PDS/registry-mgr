package gov.nasa.pds.registry.mgr.dao;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import gov.nasa.pds.registry.common.Request;
import gov.nasa.pds.registry.common.Response;
import gov.nasa.pds.registry.common.RestClient;
import gov.nasa.pds.registry.common.es.dao.BulkResponseParser;
import gov.nasa.pds.registry.common.util.SearchResponseParser;
import gov.nasa.pds.registry.common.util.Tuple;
import gov.nasa.pds.registry.mgr.dao.resp.GetAltIdsParser;

/**
 * Data access object
 * @author karpenko
 */
public class RegistryDao
{    
    private RestClient client;
    private String indexName;

    private boolean pretty = false;

    /**
     * Constructor
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index
     */
    public RegistryDao(RestClient client, String indexName)
    {        
        this.client = client;
        this.indexName = indexName;
    }

    
    /**
     * Generate pretty JSONs for debugging
     * @param b boolean flag
     */
    public void setPretty(boolean b)
    {
        this.pretty = b;
    }
    
    
    /**
     * Get product's alternative IDs by primary key 
     * @param ids primary keys (usually LIDVIDs)
     * @return ID map: key = product primary key (usually LIDVID), value = set of alternate IDs 
     * @throws Exception an exception
     */
    public Map<String, Set<String>> getAlternateIds(Collection<String> ids) throws Exception
    {
        if(ids == null || ids.isEmpty()) return null;
                
        Request.Search req = client.createSearchRequest()
            .buildAlternativeIds(ids)
            .setIndex(this.indexName)
            .setPretty(pretty);
        Response resp = client.performRequest(req);

        //DebugUtils.dumpResponseBody(resp);
        
        GetAltIdsParser cb = new GetAltIdsParser();
        SearchResponseParser parser = new SearchResponseParser();
        parser.parseResponse(resp, cb);
        
        return cb.getIdMap();
    }
    
    
    /**
     * Update alternate IDs by primary keys
     * @param newIds ID map: key = product primary key (usually LIDVID), 
     * value = additional alternate IDs to be added to existing alternate IDs.
     * @throws Exception an exception
     */
    public void updateAlternateIds(Map<String, Set<String>> newIds) throws Exception
    {
        if(newIds == null || newIds.isEmpty()) return;
        
        RegistryRequestBuilder bld = new RegistryRequestBuilder();        
        Request.Bulk req = client.createBulkRequest()
            .setIndex(this.indexName)
            .setRefresh(Request.Bulk.Refresh.WaitFor);
        for (Tuple t : bld.createUpdateAltIdsRequest(newIds)) {
          req.add(t.item1, t.item2);
        }
        Response.Bulk resp = client.performRequest(req);
        new BulkResponseParser().parse(resp);
    }

}
