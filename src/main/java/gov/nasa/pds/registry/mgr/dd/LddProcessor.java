package gov.nasa.pds.registry.mgr.dd;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import gov.nasa.pds.registry.mgr.dd.parser.DDAttribute;
import gov.nasa.pds.registry.mgr.dd.parser.AttributeDictionaryParser;
import gov.nasa.pds.registry.mgr.dd.parser.ClassAttrAssociationParser;


public class LddProcessor implements AttributeDictionaryParser.Callback, ClassAttrAssociationParser.Callback
{
    private DDNJsonWriter writer;
    private DDRecord ddRec = new DDRecord();
    
    private Pds2EsDataTypeMap dtMap;
    private Set<String> nsFilter;
    
    private Map<String, DDAttribute> ddAttrCache = new TreeMap<>();
    
    
    public LddProcessor(File outFile, Pds2EsDataTypeMap dtMap, Set<String> nsFilter) throws Exception
    {
        System.out.println("[INFO] Exporting data dictionary to ES NJSON " + outFile.getAbsolutePath());
        
        if(nsFilter != null && nsFilter.size() > 0)
        {
            this.nsFilter = nsFilter;
        }

        this.dtMap = dtMap;
        writer = new DDNJsonWriter(outFile);
    }

    
    public void close() throws Exception
    {
        writer.close();
    }

    
    @Override
    public void onAttribute(DDAttribute dda) throws Exception
    {
        ddAttrCache.put(dda.id, dda);
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
