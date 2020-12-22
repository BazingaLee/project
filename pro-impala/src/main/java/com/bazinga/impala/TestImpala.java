package com.bazinga.impala;

import java.sql.*;

public class TestImpala {
    public static void test() {
        Connection con = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
        String CONNECTION_URL = "jdbc:impala://x.x.x.x:21050;AuthMech=3;request_pool=development;UID=xxx;PWD=xxx";
        try {
            Class.forName(JDBC_DRIVER);
            con = DriverManager.getConnection(CONNECTION_URL);
            ps = con.prepareStatement("SELECT * FROM db.tb LIMIT 100;");
            rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                rs.close();
                ps.close();
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        test();
    }

}

