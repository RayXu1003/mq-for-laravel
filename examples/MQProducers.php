<?php

/**
 * rabbitmq 生产者示例
 *
 * @desc   PhpStorm
 * @author Neng.Tian
 * @time   2021/8/22 6:06 PM
 */

use Rex\MessageQueue\Facades\MQ;
use Rex\MessageQueue\Objects\PublishModel;


// 普通发送消息
pushMQ();

// 插件实现延迟队列
pushEasyDelayMQ();

// 经典的延迟队列实现
pushDelayMQ();

/**
 * 发送消息
 */
function pushMQ()
{
    try {
        $mq = MQ::connection('default');
        $mq->setModel(PublishModel::RETURN);

        for ($i = 1; $i <= 100; $i++) {
            $data = ['key' => $i];

            $rk = 'rex1.#';
            $correlation_id = $mq->push($data, $rk);
            echo $i . ' - ' . $correlation_id . PHP_EOL;
        }
    } catch (\Exception $e) {
        echo 'HaHa~, ' . $e->getMessage();
        echo $e->getTraceAsString();
    }
}

/**
 * 延迟队列（插件版本-简易）
 */
function pushEasyDelayMQ()
{
    try {
        $mq = MQ::connection('easy_delayed');

        #=====================================================================
        #  设置channel模式
        #  延迟消息暂存在exchange，return模式无效
        #=====================================================================
        $mq->setModel(PublishModel::RETURN);

        #=====================================================================
        #  发送延迟消息
        #  @param int    $expired 过期时间，单位s
        #  @param array  $data    消息
        #  @param string $rk      建议指定routing_key，方便消费脚本区分
        #  @return string 返回唯一值，如6100d7b9324531.68322900；为空代表消息丢失
        #=====================================================================
        $expired = 100;
        $data = ['key' => 'value'];
        $rk = 'routing.key';
        $correlation_id = $mq->easy_delay($expired, $data, $rk);
        echo $correlation_id . PHP_EOL;
    } catch (\Exception $e) {
        echo 'HaHa~, ' . $e->getMessage();
    }
}

/**
 * 延迟队列（经典版本-高可用）
 */
function pushDelayMQ()
{
    try {
        $mq = MQ::connection('delayed');
        $mq->setModel(PublishModel::RETURN);

        #=====================================================================
        #  发送延迟消息
        #  @param int    $expired 过期时间，单位s
        #  @param array  $data    消息
        #  @param string $rk      建议指定routing_key，方便消费脚本区分
        #  @return string 返回唯一值，如6100d7b9324531.68322900；为空代表消息丢失
        #=====================================================================
        $expired = 10;
        $data = ['key' => 'value'];
        $rk = '11s';
        $correlation_id = $mq->delay($expired, $data, $rk);
        echo $correlation_id . PHP_EOL;
    } catch (\Exception $e) {
        echo $e->getMessage();
    }
}