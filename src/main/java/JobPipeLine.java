import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;
import util.JdbcUtils;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * @author peichendong
 */
public class JobPipeLine  implements Pipeline {
    @Override
    public void process(ResultItems resultItems, Task task) {

//        save(resultItems,"area","province");
//        save(resultItems,"county");
//        save(resultItems,"county","county");
    }




    /**
     * 储存信息
     * @param resultItems 储存的结果项
     */
    private void save(ResultItems resultItems,String areas) {

        List<Map<String,Object>> area=resultItems.get(areas);

        if (area.size()==0){
            System.out.println(resultItems.getRequest().getUrl()+"此页面为爬取数据,请稍后重试");
        }else {

            area.forEach(stringObjectMap -> {

                String name= (String) stringObjectMap.get("P_NAME");
                String    code=(String)stringObjectMap.get("P_CODE");
                int    level=(int) stringObjectMap.get("P_LEVEL");
                String    cascade=(String)stringObjectMap.get("P_CASCADE");
                Long parentCode=(Long) stringObjectMap.get("P_PARENT_CODE");
                String year=(String)stringObjectMap.get("P_YEAR");

                String sql=null;
                switch (areas){
                    case "city":
                        sql="insert into city(P_NAME,P_CODE,P_LEVEL,P_CASCADE,P_PARENT_CODE,P_YEAR) values(?,?,?,?,?,?)";
                        break;
                    case "area":
                        sql="insert into province(P_NAME,P_CODE,P_LEVEL,P_CASCADE,P_PARENT_CODE,P_YEAR) values(?,?,?,?,?,?)";
                    case "county":
                        sql="insert into county(P_NAME,P_CODE,P_LEVEL,P_CASCADE,P_PARENT_CODE,P_YEAR) values(?,?,?,?,?,?)";
                        break;
                    default:
                        break;
                }


                try {
                    Connection connection = JdbcUtils.getConnection();
                    PreparedStatement statement=connection.prepareStatement(sql);
                    statement.setString(1,name);
                    statement.setString(2,code);
                    statement.setInt(3,level);
                    statement.setString(4,cascade);
                    statement.setLong(5,parentCode);
                    statement.setString(6,year);
                    statement.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }

        System.out.println(areas +"已存入至"+ "city" +"中");

    }
}
