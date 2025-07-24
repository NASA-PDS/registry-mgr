package gov.nasa.pds.registry.mgr;

public class Version extends gov.nasa.pds.registry.common.Version {
  private class SubVersion extends Version {
    private final String name;
    SubVersion(Version v, String cmdname) {
      this.name = v.getName() + "-" + cmdname;
    }
    protected String getName() {
      return this.name;
    }
  }
  private static Version self = null;
  public static synchronized Version instance() {
    if (self == null) {
      self = new Version();
    }
    return self;
  }
  protected String getName() {
    return "registry-manager";
  }
  Version subcommand (String cmdname) {
    return new SubVersion(this,cmdname);
  }
}
