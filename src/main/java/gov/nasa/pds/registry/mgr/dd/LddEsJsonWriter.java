package gov.nasa.pds.registry.mgr.dd;

import java.io.File;
import java.util.Map;
import java.util.Set;

import gov.nasa.pds.registry.mgr.dd.parser.DDAttribute;
import gov.nasa.pds.registry.mgr.dd.parser.ClassAttrAssociationParser;


/**
 * Writes Elasticsearch JSON data file to be loaded into data dictionary index.
 * 
 * @author karpenko
 */
public class LddEsJsonWriter implements ClassAttrAssociationParser.Callback
{
    private DDNJsonWriter writer;
    private DDRecord ddRec = new DDRecord();
    
    private Pds2EsDataTypeMap dtMap;
    private Map<String, DDAttribute> ddAttrCache;
    private Set<String> nsFilter;
    

    /**
     * Constructor
     * @param outFile Elasticsearch JSON data file
     * @param dtMap PDS to Elasticsearch data type map
     * @param ddAttrCache LDD attribute cache
     * @throws Exception
     */
    public LddEsJsonWriter(File outFile, Pds2EsDataTypeMap dtMap, Map<String, DDAttribute> ddAttrCache) throws Exception
    {
        writer = new DDNJsonWriter(outFile);
        this.dtMap = dtMap;
        this.ddAttrCache = ddAttrCache;
    }

    
    /**
     * Set namespace filter. Only process classes having these namespaces.
     * @param filter
     */
    public void setNamespaceFilter(Set<String> filter)
    {
        this.nsFilter = (filter != null && filter.size() > 0) ? filter : null;
    }
    
    
    /**
     * Close output file
     * @throws Exception
     */
    public void close() throws Exception
    {
        writer.close();
    }

    
    @Override
    public void onAssociation(String classNs, String className, String attrId) throws Exception
    {
        // Apply namespace filter
        if(nsFilter != null && !nsFilter.contains(classNs)) return;        

        DDAttribute attr = ddAttrCache.get(attrId);
        if(attr == null)
        {
            System.out.println("[WARNING] Missing attribute " + attrId);
        }
        else
        {
            writeRecord(classNs, className, attr);
        }
    }

        
    private void writeRecord(String classNs, String className, DDAttribute dda) throws Exception
    {
        // Assign values
        ddRec.classNs = classNs;
        ddRec.className = className;
        ddRec.attrNs = dda.attrNs;
        ddRec.attrName = dda.attrName;
        
        ddRec.dataType = dda.dataType;
        ddRec.esDataType = dtMap.getEsType(dda.dataType);
        
        ddRec.description = dda.description;

        // Write
        writer.write(ddRec.esFieldNameFromComponents(), ddRec);
    
        if(!classNs.equals(dda.attrNs))
        {
            ddRec.attrNs = classNs;
            writer.write(ddRec.esFieldNameFromComponents(), ddRec);
        }
    }
        
}
