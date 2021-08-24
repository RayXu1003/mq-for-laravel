<?php

/**
 * RabbitMQ 消费这示例
 * @desc   PhpStorm
 * @author Neng.Tian
 * @time   2021/8/22 6:06 PM
 */

use Rex\MessageQueue\Facades\MQ;

// 拉消费模式
consumePull();

// 推消费模式
consumePush();

/**
 * 拉消费模式
 */
function consumePull()
{
    $channel = MQ::connection('default');
    #=================================================
    #  消费者单条地获取消息，多并发下可能出现意外情况
    #  此模式影响RabbitMQ性能。在意高吞吐量，建议用推模式
    #  $queue可以为空，默认取config.php中的defaultQueue
    #=================================================
    while ($channel->size() > 0) {
        try {
            $message = $channel->pop();
            $data = json_decode($message->getBody(), true);
            if ($data['key'] % 2 == 0) {
                # todo 正常消费后ack
                echo $data['key'] . PHP_EOL;
                $channel->ack();
            } else {
                // 拒绝消息
                $channel->reject();
                // 转入死信
                MQ::connection('dlx')->push($data);
            }
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }
}

/**
 * 推消费模模式
 *
 * @throws Exception
 */
function consumePush()
{
    // 推模式
    $channel = MQ::connection('default');

    #=================================================
    # $queue可以为空，默认取config.php中的defaultQueue
    # $callback需返回true/false，队列才能明确是否删除消息
    #=================================================
    $channel->consume(function ($message) {
        try {
            echo $message->getBody() . PHP_EOL;
            return true; // 回调为true，队列才删除消息
        } catch (\Exception $e) {
            return false; // 回调为false，消息重新入队
        }
    });

    $channel->start();
}

