package cao.bg.crawler;

import cao.bg.DatasourceApplication;
import cao.bg.bean.CovidBean;
import cao.bg.utils.HttpUtils;
import cao.bg.utils.TimeUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author : CGL
 * @Date : 2022 2022/1/4 18:24
 * @Desc : 实现疫情数据爬取
 */

//@RunWith(SpringJUnit4ClassRunner.class)
//@SpringBootTest(classes = DatasourceApplication.class)
@Component
public class Covid19DataCrawler {
    @Autowired
    private KafkaTemplate kafkaTemplate;

/*    @Test //测试kafka与springboot的整合
    public void testKafkaTemplate() throws Exception {
        kafkaTemplate.send("test", 1, "abc");
        Thread.sleep(10000000);
    }
*/


    //@Test
    //quartz表达式
    @Scheduled(initialDelay = 1000,fixedDelay = 1000*60*60*24)      // 每一天爬一次
    //@Scheduled(cron="0/1 * * * * ?")//每隔1s执行
    //@Scheduled(cron="0 0 8 * * ?")//每天的8点定时执行
    //@Scheduled(cron="0/5 * * * * ?")//每隔5s执行
    public void testCrawling() throws Exception {
        //System.out.println("每隔5s执行一次");
        String datetime = TimeUtils.format(System.currentTimeMillis(), "yyyy-MM-dd");

        // 1.爬取指定页面
        String html = HttpUtils.getHtml("https://ncov.dxy.cn/ncovh5/view/pneumonia");
        //System.out.println(html);

        // 2.解析页面中的指定内容-即id为getAreastat的标签中的全国疫情数据
        Document doc = Jsoup.parse(html);
        String text = doc.select("script[id=getAreaStat]").toString();
        //System.out.println(text);

        // 3.使用正则表达式获取json格式的疫情数据
        String pattern = "\\[(.*)\\]";              // 定义正则规则:"["开始，"]"结尾，中间任意
        Pattern reg = Pattern.compile(pattern);     // 编译成正则对象
        Matcher matcher = reg.matcher(text);        // 去text中进行匹配
        String jsonStr = "";
        if (matcher.find()) {                       // 如果text中的内容和正则规则匹配上就去出来，赋值给jsonStr变量
            jsonStr = matcher.group(0);
            //System.out.println(jsonStr);            // 打印疫情的json格式数据
        } else {
            System.out.println("no match");
        }

        //对json数据进行更近一步的解析
        // 4.将第一层json(省份数据)解析为JavaBean
        List<CovidBean> pCovidBeans = JSON.parseArray(jsonStr,CovidBean.class);     // 将json数组转成CovidBean的实体类
        for (CovidBean pBean : pCovidBeans) {
            //System.out.println(pBean);

            //先设置时间字段
            pBean.setDatetime(datetime);
            //获取cities
            String citiesStr = pBean.getCities();
            //5.将第二层json(城市数据)解析为JavaBean
            List<CovidBean> covidBeans = JSON.parseArray(citiesStr, CovidBean.class);
            for (CovidBean bean : covidBeans) {//bean为城市
                //System.out.println(cao.bg.bean);
                bean.setDatetime(datetime);
                bean.setPid(pBean.getLocationId());//把省份的id作为城市的pid
                bean.setProvinceShortName(pBean.getProvinceShortName());
                //System.out.println(bean);

                //后续需要将城市疫情数据发送给Kafka
                //将JavaBean转为beanStr再发送给Kafka
                String beanStr = JSON.toJSONString(bean);
                kafkaTemplate.send("covid19", bean.getPid(), beanStr);  // 同一省份的数据到同一个分区
            }

            //6.获取第一层json(省份数据)中的每一天的统计数据
            String statisticsDataUrl = pBean.getStatisticsData();
            String statisticsDataStr = HttpUtils.getHtml(statisticsDataUrl);
            //获取statisticsDataStr中的data字段对应的数据
            JSONObject jsonObject = JSON.parseObject(statisticsDataStr);
            String dataStr = jsonObject.getString("data");  // dataStr为每一天的数据
            //System.out.println(dataStr);

            //将爬取解析出来的每一天的数据设置回省份pBean中的StatisticsData字段中(之前该字段只是一个URL)
            pBean.setStatisticsData(dataStr);
            pBean.setCities(null);
            //System.out.println(pBean);
            //后续需要将省份疫情数据发送给Kafka
            String pBeanStr = JSON.toJSONString(pBean);
            kafkaTemplate.send("covid19", pBean.getLocationId(), pBeanStr);
        }

        // 在测试每隔5s爬取一次的时候就要把线程休眠注掉
        //Thread.sleep(10000000);

    }

}

