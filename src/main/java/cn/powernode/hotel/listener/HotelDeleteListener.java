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
@RocketMQMessageListener(topic = "delete",
        consumerGroup = "${rocketmq.consumer.group}",
        messageModel = MessageModel.CLUSTERING)
public class HotelDeleteListener implements RocketMQListener<String> {

    @Autowired
    private IHotelService hotelService;

    /**
     * 处理删除业务的方法
     * @param message
     */
    @Override
    public void onMessage(String message) {
    hotelService.deleteByI(Long.parseLong(message));
    }
}
