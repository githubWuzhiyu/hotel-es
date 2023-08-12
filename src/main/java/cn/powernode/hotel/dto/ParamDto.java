package cn.powernode.hotel.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName QueryDto
 * @Author Zhiyu Wu
 * @Date 2023/8/10 14:54
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParamDto {
    private String key;
    private Integer page;
    private Integer size;
    private String sortBy;
    private String city;
    private String starName;
    private String brand;
    private Integer maxPrice;
    private Integer minPrice;
    // 我当前的地理坐标
    private String location;
}
