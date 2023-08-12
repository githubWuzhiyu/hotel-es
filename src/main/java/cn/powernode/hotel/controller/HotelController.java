package cn.powernode.hotel.controller;

import cn.powernode.hotel.dto.ParamDto;
import cn.powernode.hotel.service.IHotelService;
import cn.powernode.vo.PageResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @ClassName HotelListController
 * @Author Zhiyu Wu
 * @Date 2023/8/10 14:58
 */
@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Autowired
    private IHotelService hotelService;

    @PostMapping("/list")
    public PageResult queryList(@RequestBody ParamDto paramDto) throws IOException {
        return hotelService.queryList(paramDto);
    }

    @PostMapping("/filters")
    public Map<String, List<String>> filerList(@RequestBody ParamDto paramDto) throws IOException {
        return hotelService.filerList(paramDto);
    }

    @GetMapping("/suggestion")
    public List<String> getSuggestion(@RequestParam("key") String key){
        return hotelService.getSuggestion(key);
    }
}
