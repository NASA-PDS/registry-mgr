package gov.nasa.pds.registry.mgr.util;

public class Logger
{
    public static void info(String msg)
    {
        System.out.println("[INFO] " + msg);
    }

    
    public static void warn(String msg)
    {
        System.out.println("[WARN] " + msg);
    }


    public static void error(String msg)
    {
        System.out.println("[ERROR] " + msg);
    }

}
