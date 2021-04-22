package gov.nasa.pds.registry.mgr.dd;

import gov.nasa.pds.registry.mgr.Constants;

/**
 * Data dictionary record in Elasticsearch
 * 
 * @author karpenko
 */
public class DDRecord
{
    public String esFieldName;
    public String esDataType;
    
    public String classNs;
    public String className;
    
    public String attrNs;
    public String attrName;
    
    public String description;
    public String dataType;

    public String version;
    public String date;
    
    
    /**
     * Constructor
     */
    public DDRecord()
    {        
    }
    
    
    public String esFieldNameFromComponents()
    {
        return classNs + Constants.NS_SEPARATOR + className + Constants.ATTR_SEPARATOR 
                + attrNs + Constants.NS_SEPARATOR + attrName;
    }
}
