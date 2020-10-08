package gov.nasa.pds.registry.mgr.dd;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import gov.nasa.pds.registry.mgr.dd.parser.DDAttribute;

public class DDAttributeCache
{
    private Map<String, List<DDAttribute>> cache = new TreeMap<>();

    
    public DDAttributeCache()
    {
    }


    public void add(String classId, DDAttribute attr)
    {
        List<DDAttribute> list = cache.get(classId);
        if(list == null)
        {
            list = new ArrayList<>();
            cache.put(classId, list);
        }
        
        list.add(attr);
    }
    
    
    public void put(String classId, List<DDAttribute> list)
    {
        cache.put(classId, list);
    }
    
    
    public List<DDAttribute> get(String classId)
    {
        return cache.get(classId);
    }
}
