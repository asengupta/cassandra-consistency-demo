public class ConsistentSession {

  private final String sessionid;
  private int version;
  private String data;

  public String getSessionid() {
    return sessionid;
  }

  public int getVersion() {
    return version;
  }

  public String getData() {
    return data;
  }

  public ConsistentSession(String sessionid, int version, String data) {

    this.sessionid = sessionid;
    this.version = version;
    this.data = data;
  }

  public void setData(String data) {
    this.data = data;
  }

  public void setVersion(int version) {
    this.version = version;
  }
}
