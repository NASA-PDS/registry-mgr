package gov.nasa.pds.registry.mgr.dao;


@SuppressWarnings("serial")
public class DataTypeNotFoundException extends Exception
{
    public DataTypeNotFoundException(String fieldName)
    {
        super("Could not find datatype for field '" + fieldName + "'.\n" 
                + "See 'https://nasa-pds.github.io/pds-registry-app/operate/common-ops.html#Load' for more information.");
    }
}
