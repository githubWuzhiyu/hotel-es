package cn.powernode.hotel.listener;

import cn.powernode.hotel.service.IHotelService;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @ClassName HotelInsertListener
 * @Author Zhiyu Wu
 * @Date 2023/8/11 20:11
 */
@Component
@RocketMQMessageListener(topic = "insert",
        consumerGroup = "${rocketmq.consumer.group}",
        messageModel = MessageModel.CLUSTERING)
public class HotelInsertListener implements RocketMQListener<String> {

    @Autowired
    private IHotelService hotelService;

    /**
     * 处理新增或修改业务的方法
     * @param message
     */
    @Override
    public void onMessage(String message) {
    hotelService.insertById(Long.parseLong(message));
    }
}
