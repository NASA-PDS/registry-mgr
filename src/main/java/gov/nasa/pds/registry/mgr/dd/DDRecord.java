package gov.nasa.pds.registry.mgr.dd;

public class DDRecord
{
    public String classNs;
    public String className;
    
    public String attrNs;
    public String attrName;
    
    public String description;

    public String dataType;
    public String esDataType;
    
    
    public DDRecord()
    {        
    }
    
    
    public String getEsFieldName()
    {
        return classNs + "/" + className + "/" + attrNs + "/" + attrName;
    }
}
