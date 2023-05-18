package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

@SpringBootTest
public class HotelDocumentTest {
    @Autowired
    private IHotelService hotelService;

    private RestHighLevelClient client;

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.203.128:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }

    /**
     * 拿数据库的存到ES
     * 1.具体步骤：先拿出来，然后转DOC，转了之后要用json序列化
     * 2.准备request对象，存到ES就用Index，删除ES就用Delete，修改就是Update，查看是Get，后加上Request
     * 3.注意存入对象的id要与数据库一致，不建议使用ES自动生成的ID
     * 4.存入request的API是source，里面传两个参数，一个是json格式的doc对象，另一个是确认json格式？
     * 5.用client，客户端对象发送到ES，如果分类见2.
     * @throws IOException
     */
    @Test
    void testAddDocument() throws IOException {
        //查就酒店
        Hotel hotel = hotelService.getById(61083L);
        //转doc对象准备存ES
        HotelDoc hotelDoc = new HotelDoc(hotel);
        //转json准备存ES
        String json = JSON.toJSONString(hotelDoc);

        //准备Request对象
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());//这里设置ID是为了让ES中的ID与数据库的ID映射一致。
        //准备Json文档
        request.source(json, XContentType.JSON);//将json数据存入到request中
        client.index(request,RequestOptions.DEFAULT);//发送到ES。


    }

    /**
     * 怎么存就怎么取，思维逆过来就行，
     * 先准备request对象，然后去取指定id的hotel
     * 接收用response对象。
     * 取了然后反序列化，再转doc对象
     * @throws IOException
     */
    @Test
    void testGetDocumentById() throws IOException {
        //从ES那边拿
        //准备request
        GetRequest request = new GetRequest("hotel", "61083");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        String json = response.getSourceAsString();
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    /**
     * delete
     * @throws IOException
     */
    @Test
    void testDeleteDocument() throws IOException {
        //直接发request
        DeleteRequest request = new DeleteRequest("hotel", "61083");
        client.delete(request,RequestOptions.DEFAULT);
    }

    /**
     * 注意修改uptade的语句，用
     * .doc准备新添加的数据
     * @throws IOException
     */
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        //2.准备发送请求
        request.doc(
                "price","952",
                "starName","四钻"
        );
        //3.发送请求
        client.update(request,RequestOptions.DEFAULT);
    }

    @Test
    void testBulkRequest() throws IOException {
        //mybatis-plus 去查数据库的hotel每条数据，list封装
        //然后创建一个BulkRequest对象，从命名可以推断bulk指大量，request是请求，及这个对象可以存储大量请求
        BulkRequest bulkRequest = new BulkRequest();
        for (Hotel hotel : hotelService.list()) {
            //原来是怎么添加的，现在就怎么做
            HotelDoc hotelDoc = new HotelDoc(hotel);
            String hotelDocJson = JSONObject.toJSONString(hotelDoc);
            //转json之后就存入bulk里面
            bulkRequest.add(new IndexRequest("hotel").id(hotelDoc.getId().toString())
                    .source(hotelDocJson,XContentType.JSON));//这里链式编程，将设置id和用source添加单个json的hoteldoc对象到request中
            //利用bulkrequest的add方法可以添加多个（Index）request对象

            //添加完了就发送bulk对象
            client.bulk(bulkRequest,RequestOptions.DEFAULT);
        }

    }

    /**
     * 线程池完成批量添加操作
     * 获取操作耗时
     * @throws IOException
     */

    @Test
    void testThreadPoolBulkRequest() throws IOException {
        // 批量查询酒店数据
        List<Hotel> hotels = hotelService.list();

        long startTime = System.currentTimeMillis();

        // 创建线程池,线程数为10
        ExecutorService executor = new ThreadPoolExecutor(// 核心线程池大小
                5,
                // 最大线程池大小
                10,
                // 空闲线程存活时间
                60,
                // 存活时间单位
                TimeUnit.SECONDS,
                // 线程池所使用的缓冲队列
                new ArrayBlockingQueue<>(20),
                // 线程工厂
                Executors.defaultThreadFactory(),
                // 拒绝策略
                new ThreadPoolExecutor.CallerRunsPolicy());

        // 计算总数据条数并均分为10条/线程
        int total = hotels.size();
        int count = total / 10;

        // 创建所有任务并执行
        for (int i = 0; i < 10; i++) {
            int start = i * count;
            int end = (i + 1) * count;
            if (i == 9) end = total;

            int finalEnd = end;
            executor.execute(() -> {
                List<Hotel> subHotels;
                subHotels = hotels.subList(start, finalEnd);

                // 创建Request
                BulkRequest request = new BulkRequest();

                // 添加Request到线程本地的Request
                for (Hotel hotel : subHotels) {
                    HotelDoc hotelDoc = new HotelDoc(hotel);
                    // 创建新增文档的Request对象
                    request.add(new IndexRequest("hotel")
                            .id(hotelDoc.getId().toString())
                            .source(JSON.toJSONString(hotelDoc), XContentType.JSON));
                }

                // 发送请求
                try {
                    client.bulk(request, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        executor.shutdown();

        long endTime = System.currentTimeMillis();
        long costTime = endTime - startTime;
        System.out.println("Time consumed: " + costTime + "ms");
    }
}