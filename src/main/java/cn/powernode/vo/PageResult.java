package cn.powernode.vo;

import cn.powernode.hotel.pojo.HotelDoc;
import lombok.Data;

import java.util.List;

/**
 * @ClassName PageData
 * @Author Zhiyu Wu
 * @Date 2023/8/10 14:52
 */
@Data
public class PageResult {

    private List<HotelDoc> hotels;
    private long total;
}
