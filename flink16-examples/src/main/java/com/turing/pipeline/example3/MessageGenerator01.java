package com.turing.pipeline.example3;

import com.alibaba.fastjson2.JSON;
import com.turing.bean.Message01;
import com.turing.common.DateUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * @descri 电商场景实战之实时PV和UV曲线的数据模拟
 *
 * @author lj.michale
 * @date 2023-03-10
 */
public class MessageGenerator01 {

    /**
     * 位置
     */
    private static String[] position = new String[]{"北京","天津","抚州","长沙","南京","深圳","杭州","常德","吉林"};
    /**
     *网络方式
     */
    private static String[] networksUse = new String[]{"4G","2G","WIFI","5G"};
    /**
     * 来源方式
     */
    private static String[] sources = new String[]{"直接输入","百度跳转","360搜索跳转","必应跳转"};
    /**
     * 浏览器
     */
    private static String[] brower = new String[]{"火狐浏览器","qq浏览器","360浏览器","谷歌浏览器"};
    /**
     * 客户端信息
     */
    private static String[] clientInfo = new String[]{"Android","IPhone OS","None","Windows Phone","Mac"};

    /**
     * 客户端IP
     */
    private static String[] clientIp = new String[]{"172.24.103.101","172.24.103.102","172.24.103.103","172.24.103.104","172.24.103.1","172.24.103.2","172.24.103.3","172.24.103.4","172.24.103.5","172.24.103.6"};
    /**
     * 埋点链路
     */
    private static String[] gpm = new String[]{"http://172.24.103.102:7088/controlmanage/platformOverView","http://172.24.103.103:7088/datamanagement/metadata","http://172.24.103.104:7088/projectmanage/projectList"};
    /**
    account_id                        VARCHAR,--用户ID。
    client_ip                         VARCHAR,--客户端IP。
    client_info                       VARCHAR,--设备机型信息。
    platform                          VARCHAR,--系统版本信息。
    imei                              VARCHAR,--设备唯一标识。
    `version`                         VARCHAR,--版本号。
    `action`                          VARCHAR,--页面跳转描述。
    gpm                               VARCHAR,--埋点链路。
    c_time                            VARCHAR,--请求时间。
    target_type                       VARCHAR,--目标类型。
    target_id                         VARCHAR,--目标ID。
    udata                             VARCHAR,--扩展信息，JSON格式。
    session_id                        VARCHAR,--会话ID。
    product_id_chain                  VARCHAR,--商品ID串。
    cart_product_id_chain             VARCHAR,--加购商品ID。
    tag                               VARCHAR,--特殊标记。
    `position`                        VARCHAR,--位置信息。
    network                           VARCHAR,--网络使用情况。
    p_dt                              VARCHAR,--时间分区天。
    p_platform                        VARCHAR --系统版本信息。
    **/

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.10.102:9092");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        //创建生产者实例
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);
        //每 1 秒请模拟求一次
        Random random = new Random();

        while(true) {
            Message01 message01 = new Message01();
            message01.setAccount_id(UUID.randomUUID().toString());
            message01.setClient_ip("172.24.103." + random.nextInt(255));
            message01.setClient_info(clientInfo[random.nextInt(clientInfo.length)]);
            message01.setAction(sources[random.nextInt(sources.length)]);
            message01.setGpm(gpm[random.nextInt(gpm.length)]);
            message01.setC_time(System.currentTimeMillis()/1000);
            message01.setUdata("json格式扩展信息");
            message01.setPosition(position[random.nextInt(position.length)]);
            message01.setNetwork(networksUse[random.nextInt(networksUse.length)]);
            message01.setP_dt(DateUtils.getCurrentDateOfPattern("yyyy-MM-dd"));
            String json = JSON.toJSONString(message01);

            try {
                Thread.sleep(800);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            ProducerRecord record = new ProducerRecord<String, String>("turing-massage-test",json);
            producer.send(record);

            System.out.println(json);
        }

    }
}
