package membership;

public class Introducers {

    private static String introducer1 = "172.22.147.96";
    private static String introducer2 = "172.22.147.97";
    private static String introducer3 = "172.22.147.98";
    private static String introcuder4 = "172.22.147.99";
    private static String introducer5 = "172.22.147.100";

//    private static String introducer1 = "10.194.193.97";
//    private static String introducer2 = "10.195.9.90";

    public String[] getIntroducers() {
        return new String[]{introducer1, introducer2, introducer3, introcuder4, introducer5};
        //return new String[]{introducer1, introducer2};
    }

}
