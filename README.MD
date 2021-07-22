使用Laravel Facade的形式定义RabbitMQ，提供简洁、易记的语法，超简单。

## 更新

- 第一次正式提交

## 配置

* 添加config/mq.php

```
return [

    'default' => env('MQ_DRIVER', 'amqp'),

    'connections' => [

		 // 业务名称
        'default' => [
            'driver' => 'amqp',

            'host'     => env('AMQP_HOST', '127.0.0.1'),
            'port'     => env('AMQP_PORT', 5672),
            'login'    => env('AMQP_USER', 'admin'),
            'password' => env('AMQP_PASS', null),
            'vhost'    => env('AMQP_VHOST', '/'),

            #============================================================================================
            # 一般而言，exchange与queue一一对应，即Direct模式。此时，route中queue可以不填，会自动填充default_queue
            # 考虑场景通用化，默认启用Topic模式，可兼容Direct模式。最终route需支持exchange对应多个queue
            #============================================================================================
            'default_exchange' => 'mms.cc.exchange',
            'default_queue'    => 'mms.cc.queue',

            // exchange - queues maps
            'route' => [
                // Exchange => Queue
                //'mms.cc.exchange' => 'mms.cc.queue',
                'mms.cc.exchange' => [
                    'mms.cc.queue',
                    'mms.cc.queue2',
                    'mms.cc.queue3'
                ],
            ],

            // exchange - queue binding
            'binding' => [
                // queue => binding_key
                'mms.cc.queue' => 'rex.#',
                'mms.cc.queue2' => 'rex2.#',
                'mms.cc.queue3' => 'rex3.#'
            ],
        ],
    ],
];
```

* 增加配置项，config/app.php

```
'providers' => [
	// MQ
	\Rex\MessageQueue\MQServiceProvider::class,
],

'aliases' => [
	// MQ
   'MQ' => \Rex\MessageQueue\MQFacade::class,
],
```


## Demo

* 生产消息

```
use MQ;

try {
	$mq = MQ::connection('default');
	// 若关心消息丢失情况，开启confirm模式或return模式，但会影响速率；不关心则不调用
	$mq->setModel('confirm');
	// 发送消息
	$data = ['test' => 'value'];	// 数组格式
	$routing_key = 'rex.test';      // 强制使用topic模式，需指定routing_key
	$correlation_id = $mq->push($data, $routing_key);     // correlation_id为空代表消息丢失
} catch (\Exception $e) {
	echo $e->getMessage();
}

```

* 消费消息（推模式）

```
use MQ;

$channel = MQ::connection('default');	

#=================================================
# $queue可以为空，默认取config.php中的defaultQueue
# $callback需返回true/false，队列才能明确是否删除消息    
#=================================================
$channel->consume($queue = '', $is_ack = true, function($message){
	try {
		echo $message->getBody() . PHP_EOL;
		return true;	// 回调为true，队列才删除消息
	} catch (\Exception $e) {
		return false;
	}	
});

#=================================================
# 默认不设置$count，等待消费完队列消息
# 若设置$count，则消费指定个数后，脚本主动退出
#=================================================
$channel->start($count = '');	
```

* 消费消息（拉模式）

```
use MQ;

$channel = MQ::connection('default');	

#=================================================
# $queue可以为空，默认取config.php中的defaultQueue
# $message可以为空，默认ack最近一条message
#=================================================
while ($channel->size($queue) > 0) {
	$message = $channel->pop($queue);
	$channel->ack($queue, $message);
}
```

## 其它
* please make sure you upgrade to Composer 2+.




