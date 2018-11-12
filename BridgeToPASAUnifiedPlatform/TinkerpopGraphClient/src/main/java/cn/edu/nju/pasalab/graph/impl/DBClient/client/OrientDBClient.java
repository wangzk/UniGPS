package cn.edu.nju.pasalab.graph.impl.DBClient.client;

import com.orientechnologies.orient.jdbc.OrientJdbcConnection;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class OrientDBClient extends AbstractClient {


    @Override
    public Graph openDB() {
        return OrientGraph.open(getParam().getProperty("uri"),
                getParam().getProperty("username"),
                getParam().getProperty("password"));
    }

    @Override
    public void clearGraph() throws SQLException {
        Properties info = new Properties();
        info.put("user", getParam().getProperty("username"));
        info.put("password", getParam().getProperty("password"));
        Connection conn = (OrientJdbcConnection)
                DriverManager.getConnection("jdbc:orient:" + getParam().getProperty("uri"), info);
        Statement stmt = conn.createStatement();
        stmt.executeQuery("DELETE VERTEX FROM V");
        stmt.close();
    }
}
