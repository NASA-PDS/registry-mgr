package gov.nasa.pds.registry.mgr.dd;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import gov.nasa.pds.registry.mgr.dd.parser.DDAttribute;
import gov.nasa.pds.registry.mgr.dd.parser.DDClass;
import gov.nasa.pds.registry.mgr.dd.parser.DDParser;


public class DDProcessor implements DDParser.Callback
{
    private DDNJsonWriter writer;
    private DDRecord ddRec = new DDRecord();
    
    private Pds2EsDataTypeMap dtMap;
    private Set<String> nsFilter;
    
    private Map<String, DDClass> ddClassMap = new TreeMap<>();
    private Set<String> classIdsToCache = new TreeSet<>();
    private DDAttributeCache ddAttrCache = new DDAttributeCache();
    
    
    public DDProcessor(File outFile, File typeMapFile, Set<String> nsFilter) throws Exception
    {
        System.out.println("Will export data dictionary to ES NJSON " + outFile.getAbsolutePath());
        
        if(nsFilter != null && nsFilter.size() > 0)
        {
            this.nsFilter = nsFilter;
        }

        dtMap = new Pds2EsDataTypeMap();
        if(typeMapFile != null)
        {
            dtMap.load(typeMapFile);
        }
        
        writer = new DDNJsonWriter(outFile);
    }

    
    public void close() throws Exception
    {
        writer.close();
    }

    
    @Override
    public void onClass(DDClass ddClass) throws Exception
    {
        if(ddClass.parentId != null)
        {
            ddClassMap.put(ddClass.getId(), ddClass);
            classIdsToCache.add(ddClass.parentId);
        }
    }

    
    @Override
    public void onAttribute(DDAttribute dda) throws Exception
    {
        // Apply namespace filter
        if(nsFilter != null && !nsFilter.contains(dda.classNs)) return;        
        
        String classId = dda.getClassNsName();
        if(classIdsToCache.contains(classId))
        {
            ddAttrCache.add(classId, dda);
        }
        
        writeRecord(dda.classNs, dda.className, dda);
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
        writer.write(ddRec.getEsFieldName(), ddRec);
    }
    
    
    public void processParentClasses() throws Exception
    {
        if(ddClassMap.isEmpty()) return;
        
        System.out.println("Processing parent classes...");
        
        for(DDClass ddClass: ddClassMap.values())
        {
            processClass(ddClass);
        }
    }

    
    private void processClass(DDClass ddClass) throws Exception
    {
        while(ddClass != null)
        {
            List<DDAttribute> list = ddAttrCache.get(ddClass.parentId);
            
            if(list == null)
            {
                //TODO: Load from ES
                
                System.out.println("[WARNING] There are no attributes for " + ddClass.parentId);
                list = new ArrayList<>();
                ddAttrCache.put(ddClass.parentId, list);
            }
        
            for(DDAttribute attr: list)
            {
                writeRecord(ddClass.classNs, ddClass.className, attr);
            }
    
            ddClass = ddClassMap.get(ddClass.parentId);
        }
    }
}
