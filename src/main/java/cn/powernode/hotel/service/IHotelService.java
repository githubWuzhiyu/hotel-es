package cn.powernode.hotel.service;

import cn.powernode.hotel.dto.ParamDto;
import cn.powernode.hotel.pojo.Hotel;
import cn.powernode.vo.PageResult;
import com.baomidou.mybatisplus.extension.service.IService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IHotelService extends IService<Hotel> {

    /**
     * 条件过滤查询
     * @param paramDto
     * @return
     */
    PageResult queryList(ParamDto paramDto) throws IOException;

    /**
     * 聚合接口
     * @param paramDto
     * @return
     * @throws IOException
     */
    Map<String, List<String>> filerList(ParamDto paramDto) throws IOException;

    /**
     * 自动补全接口
     * @param key
     * @return
     */
    List<String> getSuggestion(String key) ;

    void insertById(Long id);

    void deleteByI(Long id);
}
