import com.sun.org.apache.xalan.internal.xsltc.cmdline.Compile;
import jdk.internal.org.objectweb.asm.tree.analysis.Value;
import org.apache.commons.logging.impl.WeakHashtable;
import us.codecraft.webmagic.*;
import us.codecraft.webmagic.pipeline.ConsolePipeline;
import us.codecraft.webmagic.pipeline.ResultItemsCollectorPipeline;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.scheduler.BloomFilterDuplicateRemover;
import us.codecraft.webmagic.scheduler.QueueScheduler;
import us.codecraft.webmagic.selector.Selectable;

import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.regex.Pattern.compile;

/**
 * @author peichendong
 */
public class JobProcessor implements PageProcessor {

    private static String SPIDER_URL;

    private String year;
    public JobProcessor(String year) {
        this.year = year;
    }

    public JobProcessor() {
    }

    /**
     * 解析页面
     * @param page 页面
     */

    @Override
    public void process(Page page) {

        this.getProvince(page);
        this.getCity(page);
        this.getCounty(page);

    }


    /**
     * 获取省份的信息
     * @param page 全国省份的页面
     */
    private void getProvince(Page page) {

        /*
        * 先获取省级tr便签下的td标签的的标签列表,转换为Stream流,后对该流中的标签进行操作
        *
        *
        *
        * */

        List< Map<String,Object>> provinces =  page.getHtml().xpath("//tr[@class='provincetr']/td").nodes().stream()
                //过滤文本为空的标签
                .filter(selectable -> selectable.xpath("//a/text()")!=null)
                //确定链接
                .filter(selectable -> selectable.links().all().size()!=0)
                //对标签进行操作
                .map(selectable -> {
                    //获取标签内容为:省(直辖市)代码+省名
                    String name=selectable.xpath("//a/text()").toString();
                    //获取该城市的链接
                    String newUrl=selectable.links().all().get(0);
                    //将链接加入到爬取队列中
                   page.addTargetRequest(newUrl);

                   //通过取代链接中的内容获取省(直辖市)级代码
                   String replace=newUrl.replace(SPIDER_URL,"").replace(".html","");
                   String areaCode=replace+"0000";  //省级代码

                    HashMap<String,Object> map=new HashMap<>();

                    map.put("P_NAME",name);
                    map.put("P_CODE",areaCode);
                    map.put("P_LEVEL",1);
                    map.put("P_CASCADE","/");
                    map.put("P_PARENT_CODE",0L);
                    map.put("P_YEAR",year);

                    return map;
                }).collect(Collectors.toList());


        page.putField("area",provinces);

    }

    /**
     * 获取市级的信息
     * @param page 市级页面
     */
    private void getCity(Page page) {


        //获取市级中的tr下的td的标签列表
        List< Selectable > cityNodes = page.getHtml().xpath("//tr[@class='citytr']/td").nodes();
        List<Map<String,Object>> city=new ArrayList<>();
        cityNodes.forEach(node->{
            String name=node.xpath("//a/text()").toString();
            if (!compile("[0-9]*").matcher(name).matches()){
                //获取连接因为两个连接一样所以得到第一个
                String newUrl=node.links().all().get(0);
                page.addTargetRequest(newUrl);
                String replace=newUrl.replace(SPIDER_URL,"").replace(".html","");
                String[] spilt=replace.split("/");
                String parentId=spilt[0]+"0000";
                String areaCode=spilt[spilt.length-1]+"00";
                HashMap<String,Object> map=new HashMap<>();
                map.put("P_NAME",name);
                map.put("P_CODE",areaCode);
                map.put("P_LEVEL",2);
                map.put("P_CASCADE","/"+parentId+"/"+areaCode);
                map.put("P_PARENT_CODE",Long.valueOf(parentId));
                map.put("P_YEAR",year);
                city.add(map);
            }
        });

        page.putField("city",city);
    }

    /**
     * 获取县级数据
     * @param page 县级的页面
     */
    public void getCounty(Page page){
        List<Map<String,Object>> county=new LinkedList<>();
        List<Selectable> countyNodes=page.getHtml().xpath("//tr[@class='countytr']/td").nodes();
        for (int i = 0; i < countyNodes.size(); i += 2) {
            List<String> code=countyNodes.get(i).xpath("//*/text()").all();
            List<String> name=countyNodes.get(i+1).xpath("//*/text()").all();
            String countyCode=code.get(0);
            String countyName=name.get(0);
            if (code.size() > 1) {
                countyCode = code.get(1);
                countyName = name.get(1);
                String newUrl = countyNodes.get(i).links().all().get(0);

            }
            countyCode=countyCode.substring(0,6);
            String parentId=countyCode.substring(0,4)+"00";
            HashMap<String,Object> map=new HashMap<>();
            map.put("P_NAME",countyName);
            map.put("P_CODE",countyCode);
            map.put("P_LEVEL",3);
            map.put("P_CASCADE","/"+countyCode.substring(0,2)+"0000/"+parentId+"/"+code);
            map.put("P_PARENT_CODE",Long.valueOf(parentId));
            map.put("P_YEAR",year);
            county.add(map);
        }

        page.putField("county",county);
    }

    private Site site=Site.me().setRetrySleepTime(3000).setSleepTime(1000).setRetryTimes(3);
    @Override
    public Site getSite() {
        return site;
    }

    /**
     * 执行爬虫
     * @param args 程序进口
     */

    public static void main(String[] args) {

        SPIDER_URL="http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2017/";

        System.out.println("爬取开始......");
        long begin=System.currentTimeMillis();
        Spider.create(new JobProcessor())
                //设置爬取的页面
                .addUrl(SPIDER_URL)
                .addPipeline(new ConsolePipeline())
                .addPipeline(new JobPipeLine())
                .setScheduler(new QueueScheduler().setDuplicateRemover(new BloomFilterDuplicateRemover(500)))
                .thread(6)
                .run();
        long end=System.currentTimeMillis();
        System.out.println("爬取结束......");
        long time=end-begin;
       System.out.println("经过"+time+"秒");
    }


}
