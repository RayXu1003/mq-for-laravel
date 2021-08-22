<?php
/**
 * 消息发布模式
 * @desc   PhpStorm
 * @author Neng.Tian
 * @time   2021/8/22 3:55 PM
 */

namespace Rex\MessageQueue\Objects;

class PublishModel extends AbstractObject
{

    /**
     * 默认模式
     */
    const DEFAULT = 'default';

    /**
     * 返回模式
     */
    const RETURN = 'return';

    /**
     * 确认模式
     */
    const CONFIRM = 'confirm';

}