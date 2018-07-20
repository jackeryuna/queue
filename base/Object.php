<?php
/**
 * 对象基本操作
 * User: fang fangz@2345.com
 * Date: 2018/07/19
 * Time: 19:23
 */
namespace Octopus\queue\base;

use Octopus\queue\base\Container;

class Object
{
    public static $container;

    public static function createObject($type, array $params = [])
    {
        if( empty(static::$container) ) {
            static::$container = new Container();
        }

        if ( is_array($type) && isset($type['class']) ) {
            $class = $type['class'];
            unset($type['class']);
            return static::$container->get($class, $params, $type);
        }

        throw new InvalidConfigException('Unsupported configuration type: ' . gettype($type));
    }
}