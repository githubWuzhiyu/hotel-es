package cn.powernode.hotel.service.impl;

import cn.powernode.hotel.mapper.HotelMapper;
import cn.powernode.hotel.pojo.Hotel;
import cn.powernode.hotel.service.IHotelService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
}
