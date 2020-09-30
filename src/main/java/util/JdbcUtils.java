package util;

import org.jsoup.select.Evaluator;

import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.Properties;

/**
 * @author peichendong
 */
public class JdbcUtils {

    private static String url;
    private static String user;
    private static String password;

    static {
        try {
            //创建Properties集合类
            Properties properties=new Properties();
            //获取resources路径下的文件的方式-->ClassLoader类加载器
            ClassLoader classLoader=JdbcUtils.class.getClassLoader();
            URL res=classLoader.getResource("jdbc.properties");
            assert res != null;
            String path=res.getPath();

            properties.load(new FileReader(path));

            url=properties.getProperty("url");
            user=properties.getProperty("user");
            password=properties.getProperty("password");
            String driver = properties.getProperty("driver");
            try {
                Class.forName(driver);
            }catch (ClassNotFoundException e){
                e.printStackTrace();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * @return 返回连接对象
     * @throws SQLException 数据库连接异常
     */
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url,user,password);
    }

    /**
     * 释放资源
     * @param statement  sql执行对象
     * @param connection 数据库连接对象
     */
    public static void close(Statement statement,Connection connection){
        if (statement!=null){
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection!=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param resultSet
     * @param statement
     * @param connection
     */
    public static void close(ResultSet resultSet,Statement statement,Connection connection){

        if (resultSet!=null){
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (statement!=null){
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (connection!=null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}
