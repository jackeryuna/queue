<?php
/**
 * 处理jobhandler为closure类型的的任务
 * User: fangz fangz@2345.com
 * Date: 2018/07/20
 * Time: 08:57
 */

namespace jackeryuna\queue\helper;

use jackeryuna\queue\base\JobHandler;

class QueueClosure extends JobHandler
{
    /**
     * @var \Closure
     */
    public $closure;

    /**
     * 执行任务
     * @param   $job
     * @param  array $data
     * @return void
     * @throws \Exception
     */
    public function handle($job, $data)
    {
        if ($this->closure instanceof \Closure) {
            $closure = $this->closure;
            $closure($job, $data);
        } else {
            throw new \Exception("closure is wrong!");
        }
    }
}