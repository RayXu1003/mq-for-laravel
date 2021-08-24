<?php
/**
 * 枚举类型的抽象类
 *
 * @desc   PhpStorm
 * @author Neng.Tian
 * @time   2021/8/22 4:28 PM
 */
namespace Rex\MessageQueue\Objects;


abstract class AbstractObject
{

    /**
     * @var null|array
     */
    protected $map = null;

    /**
     * @var null|array
     */
    protected $values = null;

    /**
     * @var null|array
     */
    protected $names = null;


    /**
     * @return array|null
     * @throws \ReflectionException
     */
    public function getArrayCopy() {
        if ($this->map === null) {
            $this->map = (new \ReflectionClass(get_called_class()))
                ->getConstants();
        }

        return $this->map;
    }

    /**
     * @return array|int[]|string[]|null
     * @throws \ReflectionException
     */
    public function getNames() {
        if ($this->names === null) {
            $this->names = array_keys($this->getArrayCopy());
        }

        return $this->names;
    }

    /**
     * @return array|null
     * @throws \ReflectionException
     */
    public function getValues() {
        if ($this->values === null) {
            $this->values = array_values($this->getArrayCopy());
        }

        return $this->values;
    }

    /**
     * @param $value
     * @return bool
     * @throws \ReflectionException
     */
    public function isValid($value) {
        return in_array($value, $this->getValues());
    }
}