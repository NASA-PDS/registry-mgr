package gov.nasa.pds.registry.mgr.dao.dd;

import gov.nasa.pds.registry.common.Request.Search;
import gov.nasa.pds.registry.mgr.dao.DataExporter;

/**
 * Exports data dictionary records from Elasticsearch into a file.
 *  
 * @author karpenko
 */
public class DDDataExporter extends DataExporter
{
    private final String namespace;

    /**
     * Constructor
     * @param esUrl Elasticsearch URL
     * @param authConfigFile authentication configuration file
     * @param namespace optional namespace filter (e.g. "lro"); null exports all
     */
    public DDDataExporter(String esUrl, String authConfigFile, String namespace)
    {
        super(esUrl, "-dd", authConfigFile);
        this.namespace = namespace;
    }

    @Override
    protected Search createRequest(Search req, int batchSize, String searchAfter) {
      if (namespace != null && !namespace.isBlank()) {
        return req.all("attr_ns", namespace, "es_field_name", batchSize, searchAfter);
      }
      return req.all("es_field_name", batchSize, searchAfter);
    }

}
