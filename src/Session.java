public class Session {

  private final int empid;
  private final int deptid;
  private final String first_name;
  private final String last_name;

  public int getEmpid() {
    return empid;
  }

  public int getDeptid() {
    return deptid;
  }

  public String getFirst_name() {
    return first_name;
  }

  public String getLast_name() {
    return last_name;
  }

  public Session(int empid, int deptid, String first_name, String last_name) {

    this.empid = empid;
    this.deptid = deptid;
    this.first_name = first_name;
    this.last_name = last_name;

  }
}
