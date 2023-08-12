package cn.powernode.hotel;


import cn.powernode.hotel.constans.HotelConstans;
import cn.powernode.hotel.pojo.Hotel;
import cn.powernode.hotel.pojo.HotelDoc;
import cn.powernode.hotel.service.impl.HotelServiceImpl;
import com.alibaba.fastjson.JSON;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.ParsedStats;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @ClassName HotelDemoApplicationTest
 * @Author Zhiyu Wu
 * @Date 2023/8/8 17:13
 */
@SpringBootTest
public class HotelDemoApplicationTest {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //创建索引
    @Test
    public void creatIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("hotel");
        createIndexRequest.mapping(HotelConstans.HOTEL, XContentType.JSON);
        restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    //删除索引
    @Test
    public void deleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("hotel");
        restHighLevelClient.indices().delete(deleteIndexRequest,RequestOptions.DEFAULT);
    }

    //修改索引
    @Test
    public void updateIndex() throws IOException {
        PutMappingRequest putMappingRequest = new PutMappingRequest();

        restHighLevelClient.indices().putMapping(putMappingRequest,RequestOptions.DEFAULT);
    }

    //判断索引是否存在
    @Test
    public void existsIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("hotel");
        boolean exists = restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists ? "索引库已经存在" : "索引库不存在");
    }

    @Autowired
    private HotelServiceImpl hotelServiceImpl;

    //新增文档
    @Test
    public void insertdoc() throws IOException {
        Hotel hotel = hotelServiceImpl.getById(60398);
        HotelDoc hotelDoc = new HotelDoc(hotel);
        IndexRequest indexRequest = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        indexRequest.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        restHighLevelClient.index(indexRequest,RequestOptions.DEFAULT);
    }

    //查询文档
    @Test
    public void getdoc() throws IOException {
        GetRequest getRequest = new GetRequest("hotel","60398");
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        String source = response.getSourceAsString();
        HotelDoc hotelDoc = JSON.parseObject(source, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    //删除文档
    @Test
    public void deletedoc() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("hotel","60398");
        DeleteResponse delete = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    //修改文档
    @Test
    public void updatedoc() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("hotel","60398");
        updateRequest.doc(
          "price","952",
                "starName","四钻"
        );
        restHighLevelClient.update(updateRequest,RequestOptions.DEFAULT);
    }

    //批量导入文档
    @Test
    public void bulkRequest() throws IOException {
        List<Hotel> hotels = hotelServiceImpl.list();
        BulkRequest bulkRequest = new BulkRequest();
        for ( Hotel hotel : hotels ){
            HotelDoc hotelDoc = new HotelDoc(hotel);
            bulkRequest.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        restHighLevelClient.bulk(bulkRequest,RequestOptions.DEFAULT);
    }

    //查询所有
    @Test
    public void matchAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchRequest.source().query(matchAllQueryBuilder);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        extracted(response);
    }

    //返回值操作
    private void extracted(SearchResponse response) {
        //解析响应
        SearchHits hits = response.getHits();
        //获取总条数
        TotalHits totalHits = hits.getTotalHits();
        long value = totalHits.value;
        System.out.println(value);
        //获取文档数组
        SearchHit[] hits1 = hits.getHits();
        ArrayList<HotelDoc> hotelDocs = new ArrayList<>();
        //遍历
        for ( SearchHit documentFields : hits1 ){
            //获取文档source
            String sourceAsString = documentFields.getSourceAsString();
            //反序列化
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            hotelDocs.add(hotelDoc);
        }
        System.out.println(hotelDocs);
    }

    //全文检索查询
    @Test
    public void match() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.matchQuery("business","四川北路商业区"));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        extracted(response);
    }

    //精准查询
    @Test
    public void ids() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.idsQuery().addIds("1765008760","413460"));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        extracted(response);
    }

    //范围查询
    @Test
    public void range() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.rangeQuery("price").gte(400).lte(800));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        extracted(response);
    }

    //词条查询
    @Test
    public void term() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.termQuery("city","上海"));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        extracted(response);
    }

    //地理坐标查询
    @Test
    public void geoInstance() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.geoDistanceQuery("location")
                .distance("10", DistanceUnit.KILOMETERS)
                .point(31.21,121.5));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        extracted(response);
    }

    //地理边界范围查询
    @Test
    public void geoBoundingBox() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.geoBoundingBoxQuery("location")
                .setCorners(31,121,30.5,121.5));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        extracted(response);
    }

    //算分查询
    @Test
    public void functionScore() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.functionScoreQuery(QueryBuilders
                //match查询
                .matchQuery("name","嘉定凯悦"),
                //定义算分函数
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        //构建每一个算分函数
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.idsQuery().addIds("624417"),
                        //指定权重
                        ScoreFunctionBuilders.weightFactorFunction(10))
        }).boostMode(CombineFunction.SUM));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        extracted(response);
    }

    //bool查询
    @Test
    public void bool() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("name","嘉定凯悦"))
                .mustNot(QueryBuilders.rangeQuery("price").gt(400))
                .filter(QueryBuilders.geoDistanceQuery("location")
                        .distance("30km")
                        .point(31.21,121.5)
                ));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        extracted(response);
    }

    //排序
    @Test
    public void sort() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.matchAllQuery())
                .sort("score",SortOrder.DESC)
                .sort("price", SortOrder.ASC);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        extracted(response);
    }

    //分页
    @Test
    public void page() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.matchAllQuery())
                .from(0)
                .size(5)
                .sort("price",SortOrder.ASC);
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        extracted(response);
    }

    //高亮
    @Test
    public void highLight() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.matchQuery("name","凯悦"));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        for ( SearchHit hit : response.getHits().getHits() ){
            //获取文档source
            String sourceAsString = hit.getSourceAsString();
            //反序列化
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            //获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)) {
                //根据名字获取高亮结果
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null) {
                    //获取高亮值
                    String name = Arrays.toString(highlightField.getFragments());
                    //覆盖非高亮结果
                    hotelDoc.setName(name);
                }
            }
        }
    }

    //桶聚合
    @Test
    public void bucket() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source()
                        .size(0)
                        .aggregation(AggregationBuilders
                                .terms("brandAgg")
                                .field("brand")
                                .size(20)
                                .order(BucketOrder.count(true)));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        ParsedStringTerms brandAgg = response.getAggregations().get("brandAgg");
        List<? extends Terms.Bucket> buckets = brandAgg.getBuckets();
        for ( Terms.Bucket bucket : buckets ){
            String key = bucket.getKey().toString();
            long docCount = bucket.getDocCount();
            System.out.println(key + ":" + docCount);
        }
    }

    //度量聚合
    @Test
    public void metric() throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source()
                .size(0)
                .aggregation(AggregationBuilders
                        .terms("brandAgg")
                        .field("brand")
                        .size(20)
                        .order(BucketOrder.aggregation("scoreAgg.avg",false))
                        .subAggregation(AggregationBuilders.stats("scoreAgg").field("score")));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        ParsedStringTerms brandAgg = response.getAggregations().get("brandAgg");
        List<? extends Terms.Bucket> buckets = brandAgg.getBuckets();
        for ( Terms.Bucket bucket : buckets ){
            String key = bucket.getKey().toString();
            long docCount = bucket.getDocCount();
            Aggregations aggregations = bucket.getAggregations();
            ParsedStats scoreAgg = aggregations.get("scoreAgg");
            long count = scoreAgg.getCount();
            double min = scoreAgg.getMin();
            double max = scoreAgg.getMax();
            double avg = scoreAgg.getAvg();
            double sum = scoreAgg.getSum();
            System.out.println(key + ":" + docCount + ":" + count+ ":" + min+ ":" + max + ":" + avg + ":" + sum);
        }
    }

    //自动补全
    @Test
    public void completion() throws IOException {
        SearchRequest searchRequest = new SearchRequest("test1");
        searchRequest.source()
                .suggest(new SuggestBuilder().addSuggestion("title_suggest", SuggestBuilders.completionSuggestion("title")
                        .text("s")
                        .skipDuplicates(true)
                        .size(10)));
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Suggest suggest = response.getSuggest();
        ArrayList<String> list = new ArrayList<>();
        Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> titleSuggest = suggest.getSuggestion("title_suggest");
        for ( Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option> options : titleSuggest ){
            List<? extends Suggest.Suggestion.Entry.Option> option = options.getOptions();
            for ( Suggest.Suggestion.Entry.Option option1 : option ){
                String s = option1.getText().toString();
                list.add(s);
            }
        }
        System.out.println(list);
    }
}



