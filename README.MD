使用 Laravel Facade 的形式定义 RabbitMQ ，提供**简洁、易记的语法，超简单**。

## 更新

* 增加check heartbeat
* 增加高可用策略配置命令
* 增加延迟队列插件（`RabbitMQ 3.7+` 以上适用）
* 增加传统延迟消息方案（`delayExchange` + `DlxExchange`）
* 增加消息 `flag`，用于 `confirm` 模式下 `ack` 回调成功


## 配置

* 添加 `config/mq.php`

```
return [

    'default' => env('MQ_DRIVER', 'amqp'),

    'connections' => [

        'default' => [
            'driver' => 'amqp',

            'host'     => env('AMQP_HOST', '127.0.0.1'),
            'port'     => env('AMQP_PORT', 5672),
            'login'    => env('AMQP_USER', 'admin'),
            'password' => env('AMQP_PASS', null),
            'vhost'    => env('AMQP_VHOST', '/'),

            #=====================================================================
            # 考虑场景通用化，默认启用Topic模式，可兼容Direct模式
            #=====================================================================
            'default_exchange' => 'mms.cc.exchange',
            'default_queue'    => 'mms.cc.queue1',

            #=====================================================================
            # exchange - queues maps
            # 支持exchange对应多个queue
            # exchange与queue一对一，则route中的queue可以不填，自动填充default_queue
            # eg: exchange => queue OR exchange => [queue1, queue2, queue3]
            #=====================================================================
            'route' => [
                'mms.cc.exchange' => [
                    'mms.cc.queue1',
                    'mms.cc.queue2',
                    'mms.cc.queue3'
                ],
            ],

            #=====================================================================
            # exchange - queue binding
            # queue => binding_key
            #=====================================================================
            'binding' => [
                'mms.cc.queue1' => 'rex1.#',
                'mms.cc.queue2' => 'rex2.#',
                'mms.cc.queue3' => 'rex3.#'
            ],
        ],

        #=====================================================================
        # 死信
        # 使用场景：consumer消费队列中存在异常消息或不可处理消息时，
        #         将这个消息重新发布到设置的DLX，用于后续处理
        #=====================================================================
        'dlx' => [
            'driver' => 'amqp',

            'host'     => env('AMQP_HOST', '127.0.0.1'),
            'port'     => env('AMQP_PORT', 5672),
            'login'    => env('AMQP_USER', 'admin'),
            'password' => env('AMQP_PASS', null),
            'vhost'    => env('AMQP_VHOST', '/'),

            'exchange_params' => [
                'type'        => 'fanout',
                'passive'     => false,
                'durable'     => true,
                'auto_delete' => false,
            ],
            'default_exchange' => 'dlx.exchange',
            'default_queue'    => 'dlx.queue',

            // exchange - queues maps
            'route' => [
                'dlx.exchange' => 'dlx.queue'
            ],
        ],

        #=====================================================================
        # 延迟消息 - 需安装插件
        # 使用场景：延迟消息量不大，且不在乎高可用
        #=====================================================================
        'easy_delayed' => [
            'driver' => 'amqp',

            'host'     => env('AMQP_HOST', '127.0.0.1'),
            'port'     => env('AMQP_PORT', 5672),
            'login'    => env('AMQP_USER', 'admin'),
            'password' => env('AMQP_PASS', null),
            'vhost'    => env('AMQP_VHOST', '/'),

            'default_exchange' => 'easy.delayed.exchange',
            'default_queue'    => 'easy.delayed.queue',

            'route' => [
                'easy.delayed.exchange' => 'easy.delayed.queue'
            ],
        ],

        #=====================================================================
        # 经典延迟队列
        # 根据用于延迟的exchange、queue，
        # 会对应自动生成相同属性，带前缀'dlx.'的死信exchange、queue
        #=====================================================================
        'delayed' => [
            'driver' => 'amqp',

            'host'     => env('AMQP_HOST', '127.0.0.1'),
            'port'     => env('AMQP_PORT', 5672),
            'login'    => env('AMQP_USER', 'admin'),
            'password' => env('AMQP_PASS', null),
            'vhost'    => env('AMQP_VHOST', '/'),

            'default_exchange' => 'delayed.exchange',

            'route' => [
                'delayed.exchange' => [
                    'delayed.queue.10s',
                    'delayed.queue.10min',
                    'delayed.queue.1h',
                ]
            ],
            'binding' => [
                'delayed.queue.10s'   => '10s',
                'delayed.queue.10min' => '10min',
                'delayed.queue.1h'    => '1h'
            ]
        ],
    ],
];
```

* 增加配置项，`config/app.php`

```
'providers' => [
    // MQ
    \Rex\MessageQueue\MQServiceProvider::class,
],

'aliases' => [
    // MQ
    'MQ' => \Rex\MessageQueue\Facades\MQ::class,
],
```


## Demo

* 生产消息

```
use Rex\MessageQueue\Facades\MQ;
use Rex\MessageQueue\Objects\PublishModel;

try {
    $mq = MQ::connection('default');

    #=====================================================================
    #  设置channel模式
    #  不关心消息丢失则屏蔽；开启confirm或return模式，速率下降250倍左右
    #=====================================================================
    $mq->setModel(PublishModel::CONFIRM);

    #=====================================================================
    #  发送消息
    #  @param array  $data 消息
    #  @param string $rk   默认topic模式，需指定routing_key
    #  @return string 唯一值，如6100d7b9324531.68322900；为空代表消息丢失
    #=====================================================================
    $data = ['key' => 'value'];
    $rk   = 'routing.key';
    $correlation_id = $mq->push($data, $rk);
} catch (\Exception $e) {
    echo 'HaHa~ ' . $e->getMessage();
}

```
* 生产延迟消息（经典版）

```
use Rex\MessageQueue\Facades\MQ;
use Rex\MessageQueue\Objects\PublishModel;

try {
    $mq = MQ::connection('delayed');
    $mq->setModel(PublishModel::CONFIRM);

    #=====================================================================
    #  发送延迟消息
    #  @param int    $expired 过期时间，单位s
    #  @param array  $data    消息
    #  @param string $rk      建议指定routing_key，方便消费脚本区分
    #  @return string 返回唯一值，如6100d7b9324531.68322900；为空代表消息丢失
    #=====================================================================
    $expired = 10;
    $data    = ['key' => 'value'];
    $rk      = '10s';
    $correlation_id = $mq->delay($expired, $data, $rk);
} catch (\Exception $e) {
    echo 'HaHa~ ' . $e->getMessage();
}

```

* 生产延迟消息（插件版）

```
use Rex\MessageQueue\Facades\MQ;
use Rex\MessageQueue\Objects\PublishModel;

try {
    $mq = MQ::connection('easy_delayed');

    #=====================================================================
    #  设置channel模式
    #  延迟消息暂存在exchange，return模式无效
    #=====================================================================
    $mq->setModel(PublishModel::CONFIRM);

    #=====================================================================
    #  发送延迟消息
    #  @param int    $expired 过期时间，单位s
    #  @param array  $data    消息
    #  @param string $rk      建议指定routing_key，方便消费脚本区分
    #  @return string 返回唯一值，如6100d7b9324531.68322900；为空代表消息丢失
    #=====================================================================
    $expired = 10;
    $data    = ['key' => 'value'];
    $rk      = 'routing.key';
    $correlation_id = $mq->easy_delay($expired, $data, $rk);
} catch (\Exception $e) {
    echo 'HaHa~ ' . $e->getMessage();
}

```

* 消费消息（推模式）

```
use Rex\MessageQueue\Facades\MQ;

$channel = MQ::connection('default');	

#=================================================
#  消费者持续订阅获取单条消息
#  $callback需返回true/false，队列才能明确是否删除消息    
#=================================================
$queue = 'queue'; // 可以不传$queue，默认取 config/mq.php 中的defaultQueue
$channel->consume($queue, $is_ack = true, function($message){
    try {
        echo $message->getBody() . PHP_EOL;
        return true; // 回调为true，队列删除消息
    } catch (\Exception $e) {
        return false; // 回调为false，消息重新入队
    }	
});

$channel->start();	
```

* 消费消息（拉模式）

```
use MQ;

$channel = MQ::connection('default');	

#===================================================================
#  消费者单条地获取消息，多并发下可能出现意外情况
#  此模式影响RabbitMQ性能，高吞吐量时建议用推模式
#===================================================================
$queue = 'queue'; // 可以不传$queue，默认取 config/mq.php 中的defaultQueue
while ($channel->size($queue) > 0) {
    try {
        $data = json_decode($channel->pop($queue)->getBody(), true);
        if ($data == 'good') {
            # todo 正常消费后ack
            $channel->ack();
        } else {
            $channel->reject(); // 拒绝消息
            MQ::connection('dlx')->push($data); // 转入死信
        }
    } catch (\Exception $e) {
        echo $e->getMessage();
    }
}
```

* 消费消息（推模式+心跳 仅限有pcntl扩展）

```
use Rex\MessageQueue\Facades\MQ;

$channel = MQ::connection('default');	
// 注册心跳
$heartbeatHandler = new PCNTLHeartbeatSender($channel->getConnection());  // Windows下没有pcntl扩展 , 在Windows测试代码可以注释掉这行,上测试或者生产把这行加回来
$heartbeatHandler->register(); // Windows下没有pcntl扩展 , 在Windows测试代码可以注释掉这行,上测试或者生产把这行加回来
#=================================================
#  消费者持续订阅获取单条消息
#  $callback需返回true/false，队列才能明确是否删除消息    
#=================================================
$queue = 'queue'; // 可以不传$queue，默认取 config/mq.php 中的defaultQueue
$channel->consume($queue, $is_ack = true, function($message){
    try {
        echo $message->getBody() . PHP_EOL;
        return true; // 回调为true，队列删除消息
    } catch (\Exception $e) {
        return false; // 回调为false，消息重新入队
    }	
});

$channel->start();	
```

* 消费消息（拉模式+心跳 仅限pcntl扩展）

```
use MQ;

$channel = MQ::connection('default');	
// 注册心跳
$heartbeatHandler = new PCNTLHeartbeatSender($channel->getConnection());  //Windows下没有pcntl扩展 , 在Windows测试代码可以注释掉这行,上测试或者生产把这行加回来
$heartbeatHandler->register();  // Windows下没有pcntl扩展 , 在Windows测试代码可以注释掉这行,上测试或者生产把这行加回来
#===================================================================
#  消费者单条地获取消息，多并发下可能出现意外情况
#  此模式影响RabbitMQ性能，高吞吐量时建议用推模式
#===================================================================
$queue = 'queue'; // 可以不传$queue，默认取 config/mq.php 中的defaultQueue
while ($channel->size($queue) > 0) {
    try {
        $data = json_decode($channel->pop($queue)->getBody(), true);
        if ($data == 'good') {
            # todo 正常消费后ack
            $channel->ack();
        } else {
            $channel->reject(); // 拒绝消息
            MQ::connection('dlx')->push($data); // 转入死信
        }
    } catch (\Exception $e) {
        echo $e->getMessage();
    }
}
```

## 其它重要说明
* please make sure you upgrade to **Composer 2+**.
* 设置 RabbitMQ **高可用策略** 
```
# vhost填写对应的AMQP_VHOST
rabbitmqctl set_policy -p vhost ha "^" '{"ha-mode":"all", "ha-sync-mode":"automatic"}'
```
* [延迟队列官网插件](https://www.rabbitmq.com/community-plugins.html)
