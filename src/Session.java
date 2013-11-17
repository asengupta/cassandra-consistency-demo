public class Session {

  private final String sessionid;
  private final int version;

  public String getSessionid() {
    return sessionid;
  }

  public int getVersion() {
    return version;
  }

  public Session(String sessionid, int version) {

    this.sessionid = sessionid;
    this.version = version;

  }
}
