package gov.nasa.pds.registry.mgr.dd.parser;

public class DDClass
{
    public String classNs;
    public String className;
    public String parentId;

    
    public DDClass()
    {
    }

    
    public String getClassNsName()
    {
        return classNs + "." + className;
    }

    
    public String getId()
    {
        return classNs + "." + className;
    }

}
