package cao.bg;

/**
 * @Author : CGL
 * @Date : 2022 2022/1/4 13:45
 * @Desc :
 */

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;

import java.io.File;
import java.net.URL;

public class JsoupTest {
    @Test
    public void testGetDoument() throws Exception {
        // connect(): 获取网页源码
        //Document doc = Jsoup.connect("https://www.bilibili.com/").get();
        //System.out.println(doc);

        // parse 解析html或xml文档，返回Document对象
        //Document doc = Jsoup.parse(new URL("https://www.bilibili.com/"), 1000);
        //Document doc = Jsoup.parse(new File("jsoup.html"), "UTF-8");
        String htmlStr = FileUtils.readFileToString(new File("jsoup.html"), "UTF-8");
        Document doc = Jsoup.parse(htmlStr);
        System.out.println(doc);
        Element titleElement = doc.getElementsByTag("title").first();
        String title = titleElement.text();
        System.out.println(title);
    }
    @Test
    public void testGetElement() throws Exception {
        Document doc = Jsoup.parse(new File("jsoup.html"), "UTF-8");
        //System.out.println(doc);

        //根据id获取元素getElementById
        Element element = doc.getElementById("app");
        String text = element.text();
        //System.out.println(text);
        //根据标签获取元素getElementsByTag
        Elements elements = doc.getElementsByTag("title");
        Element titleElement = elements.first();
        String title = titleElement.text();
        //System.out.println(title);
        //根据class获取元素getElementsByClass
        Element element1 = doc.getElementsByClass("international-home").last();
        //System.out.println(element1.text());
        //根据属性获取元素
        String alt = doc.getElementsByAttribute("alt").first().text();
        System.out.println(alt);
    }

    @Test
    public void testElementOperator() throws Exception {
        Document doc = Jsoup.parse(new File("jsoup.html"), "UTF-8");

        //getElementsByAttributeValue():寻找属性为指定值的元素
        Element element = doc.getElementsByAttributeValue("class", "sub-item").first();

        //获取元素中的id
        String id = element.id();
        //System.out.println(id);
        //获取元素中的classname
        String className = element.className();
        //System.out.println(className);
        //获取元素中的属性值
        String id1 = element.attr("id");
        //System.out.println(id1);
        //获取元素中所有的属性
        String attrs = element.attributes().toString();
        System.out.println(attrs);
        //获取元素中的文本内容
        String text = element.text();
        //System.out.println(text);
    }

    @Test
    public void testSelect() throws Exception {
        Document doc = Jsoup.parse(new File("jsoup.html"), "UTF-8");
        //根据标签名获取元素
        Elements spans = doc.select("div");
        for (Element span : spans) {
            //System.out.println(span.text());
        }
        //根据id获取元素
        String text = doc.select("#app").text();
        //System.out.println(text);
        //根据class获取元素
        String text1 = doc.select(".sub-container").text();
        //System.out.println(text1);
        //根据属性获取元素
        String text2 = doc.select("[alt]").text();
        //System.out.println(text2);
        //根据属性值获取元素
        String text3 = doc.select("[class=sub-container]").text();
        System.out.println(text3);

    }

    @Test
    public void testSelect2() throws Exception {
        Document doc = Jsoup.parse(new File("jsoup.html"), "UTF-8");
        //根据标签名+id组合选取元素
        String text = doc.select("div#app").text();
        //System.out.println(text);
        //根据标签名+class
        String text1 = doc.select("div.international-home").text();
        //System.out.println(text1);
        //根据标签名+元素名
        String text2 = doc.select("p[class]").text();
        //System.out.println(text2);
        //任意组合
        String text3 = doc.select("a[href]._blank").text();
        //System.out.println(text3);
        //查找某个元素下的直接子元素
        String text4 = doc.select(".nav-link > ul > li").text();
        System.out.println(text4);
        //查找某个元素下的所有子元素
        String text5 = doc.select(".nav-link li").text();
        //System.out.println(text5);
        //查找某个元素下的所有直接子元素
        String text6 = doc.select(".nav-link > *").text();
        System.out.println(text6);
    }
}
