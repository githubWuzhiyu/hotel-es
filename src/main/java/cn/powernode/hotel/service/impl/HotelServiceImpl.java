package cn.powernode.hotel.service.impl;

import cn.powernode.hotel.dto.ParamDto;
import cn.powernode.hotel.mapper.HotelMapper;
import cn.powernode.hotel.pojo.Hotel;
import cn.powernode.hotel.pojo.HotelDoc;
import cn.powernode.hotel.service.IHotelService;
import cn.powernode.vo.PageResult;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelServiceImpl extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 条件过滤查询
     * @param paramDto
     * @return
     * @throws IOException
     */
    @Override
    public PageResult queryList(ParamDto paramDto) throws IOException {
        SearchRequest searchRequest = new SearchRequest("hotel");
        FunctionScoreQueryBuilder functionScoreQueryBuilder = getQueryBuilder(paramDto, searchRequest);

        //根据需求查询
        searchRequest.source().query(functionScoreQueryBuilder);
        //根据传入参数设置排序
        switch (paramDto.getSortBy()) {
            case "score":
                searchRequest.source().sort("score",SortOrder.DESC);
                break;
            case "price":
                searchRequest.source().sort("price");
                break;
        }
        //分页
        Integer page = paramDto.getPage();
        Integer size = paramDto.getSize();
        searchRequest.source().from((page-1) * size)
                        .size(size);
        //查询我周围的酒店
        String location = paramDto.getLocation();
        if (!StringUtils.isEmpty(location)) {
            searchRequest.source().sort(SortBuilders.geoDistanceSort("location",new GeoPoint(location))
                    .order(SortOrder.ASC)
                    .unit(DistanceUnit.KILOMETERS));
        }
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        //返回结果
        return handleResponse(response);
    }

    /**
     * 聚合操作方法
     * @param paramDto
     * @return
     * @throws IOException
     */
    @Override
    public Map<String, List<String>> filerList(ParamDto paramDto) throws IOException {
        //构建查询对象
        SearchRequest searchRequest = new SearchRequest("hotel");
        //获取boolQueryBuilder
        FunctionScoreQueryBuilder functionScoreQueryBuilder = getQueryBuilder(paramDto, searchRequest);
        //根据需求查询
        searchRequest.source().query(functionScoreQueryBuilder);
        //聚合操作
        buildAggregation(searchRequest);
        //获取聚合操作查询数据
        List<String> city = getBucket(searchRequest, "cityAgg");
        List<String> starName = getBucket(searchRequest, "starNameAgg");
        List<String> brand = getBucket(searchRequest, "brandAgg");
        //将聚合操作查询的数据存入map返回
        Map<String, List<String>> map = new HashMap<>();
        map.put("city",city);
        map.put("starName",starName);
        map.put("brand",brand);
        return map;
    }

    /**
     * 自动补全操作
     * @param key
     * @return
     * @throws IOException
     */
    @Override
    public List<String> getSuggestion(String key) {
        try {
            SearchRequest searchRequest = new SearchRequest("hotel");
            searchRequest.source().suggest(new SuggestBuilder()
                    .addSuggestion("suggestions", SuggestBuilders
                            .completionSuggestion("suggestion")
                            .text(key)
                            //跳过重复项
                            .skipDuplicates(true)
                            .size(10)));
            //发起请求
            SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            //解析结果
            Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> suggestions = response.getSuggest().getSuggestion("suggestions");
            ArrayList<String> list = new ArrayList<>();
            for ( Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option> suggestion : suggestions ){
                for ( Suggest.Suggestion.Entry.Option option : suggestion ){
                    String test = option.getText().toString();
                    list.add(test);
                }
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertById(Long id) {
        try {
            //根据id查询酒店数据
            Hotel hotel = getById(id);
            //转为文档类型
            HotelDoc hotelDoc = new HotelDoc(hotel);
            //获取request对象
            IndexRequest indexRequest = new IndexRequest("hotel").id(hotelDoc.getId().toString());
            //准备json文档
            indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            restHighLevelClient.index(indexRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteByI(Long id) {
        try {
            DeleteRequest deleteRequest = new DeleteRequest("hotel", id.toString());
            restHighLevelClient.delete(deleteRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //获取聚合操作查询数据
    private List<String> getBucket(SearchRequest searchRequest,String aggName) throws IOException {
        SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        ParsedStringTerms aggregation = response.getAggregations().get(aggName);
        List<? extends Terms.Bucket> buckets = aggregation.getBuckets();
        ArrayList<String> list = new ArrayList<>();
        for ( Terms.Bucket bucket : buckets ){
            String key = bucket.getKey().toString();
            list.add(key);
        }
        return list;
    }

    //聚合
    private void buildAggregation(SearchRequest searchRequest) {
        searchRequest.source().aggregation(AggregationBuilders.terms("cityAgg").field("city").size(100))
                .aggregation(AggregationBuilders.terms("starNameAgg").field("starName").size(100))
                .aggregation(AggregationBuilders.terms("brandAgg").field("brand").size(100))
                //设置size
                .size(0);
    }

    //查询需求过滤
    private FunctionScoreQueryBuilder getQueryBuilder(ParamDto paramDto, SearchRequest searchRequest) {
        String key = paramDto.getKey();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (StringUtils.isEmpty(key)) {
            //如果输入框没有值，则查询所有
            searchRequest.source().query(QueryBuilders.matchAllQuery());
        }else {
            //如果输入框有值，则根据输入内容查询
            boolQueryBuilder.must(QueryBuilders.matchQuery("all",key));
        }
        //根据条件选择查询
        //根据城市查询
        if (!StringUtils.isEmpty(paramDto.getCity())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("city", paramDto.getCity()));
        }
        //根据星级查询
        if (!StringUtils.isEmpty(paramDto.getStarName())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("starName", paramDto.getStarName()));
        }
        //根据品牌查询
        if (!StringUtils.isEmpty(paramDto.getBrand())) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("brand", paramDto.getBrand()));
        }
        //根据价格区间查询
        if (!StringUtils.isEmpty(paramDto.getCity())) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price")
                    .gte(paramDto.getMinPrice())
                    .lte(paramDto.getMaxPrice()));
        }
        //算分
        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(
                //原始查询，相关性算分的查询
                boolQueryBuilder,
                //function score的数组
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                        // 其中的一个function score 元素
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                //过滤条件
                                QueryBuilders.termQuery("isAD", true),
                                //算分函数
                                ScoreFunctionBuilders.weightFactorFunction(10)
                        )
                }
        ).boostMode(CombineFunction.SUM);
        return functionScoreQueryBuilder;
    }

    //结果解析
    private PageResult handleResponse(SearchResponse response) {
        PageResult pageResult = new PageResult();
        //返回值操作
        SearchHits hits = response.getHits();
        //获取总条数
        TotalHits totalHits = hits.getTotalHits();
        pageResult.setTotal(totalHits.value);
        //获取所有数据
        SearchHit[] hitsHits = hits.getHits();
        ArrayList<HotelDoc> hotelDocs = new ArrayList<>();
        for ( SearchHit hitsHit : hitsHits ){
            String sourceAsString = hitsHit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            //获取距离显示
            Object[] sortValues = hitsHit.getSortValues();
            if (sortValues.length > 0) {
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            hotelDocs.add(hotelDoc);
        }
        pageResult.setHotels(hotelDocs);


        return pageResult;
    }
}
