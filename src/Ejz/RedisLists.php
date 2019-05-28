<?php

namespace Ejz;

use Ejz\RedisClient;

class RedisLists
{
    /** @var RedisClient */
    private $redisClient;

    private $prefix = 'RedisLists.';

    /**
     * @param RedisClient $redisClient
     */
    public function __construct(RedisClient $redisClient)
    {
        $this->redisClient = $redisClient;
    }

    /**
     * @param string     $list
     * @param string     $item
     * @param string|int $ttl  (optional)
     */
    public function insert(string $list, string $item, $ttl)
    {
        $time = is_numeric($ttl) ? time() + $ttl : strtotime($ttl);
        $this->redis->ZADD($this->prefix . $list, $time, $item);
    }

    /**
     * @param string $list
     * @param string $item
     */
    public function remove(string $list, string $item)
    {
        $this->redis->ZREM($this->prefix . $list . '.taken', $item);
        $this->redis->ZREM($this->prefix . $list, $item);
    }

    /**
     * @param string $list
     */
    public function removeAll(string $list)
    {
        $this->redis->DEL($this->prefix . $list . '.taken');
        $this->redis->DEL($this->prefix . $list);
    }

    /**
     * @param string     $list
     * @param string|int $ttl
     *
     * @return string
     */
    public function take(string $list, $ttl): string
    {
        $time = is_numeric($ttl) ? time() + $ttl : strtotime($ttl);
        do {
            $item = $this->takeBackend($list, $time);
        } while ($item === null && sleep(3) === 0);
        return $item;
    }

    /**
     * @param string $list
     * @param int    $time
     *
     * @return ?string
     */
    private function takeBackend(string $list, int $ttl): ?string
    {
        return $this->redis->EVAL('
            local prefix = ARGV[1]
            local time = ARGV[2]
            math.randomseed(tonumber(ARGV[3]))
            local list = ARGV[4]
            local ttl = tonumber(ARGV[5])
            redis.call("ZREMRANGEBYSCORE", prefix .. list, "-inf", time)
            redis.call("ZREMRANGEBYSCORE", prefix .. list .. ".taken", "-inf", time)
            local items = redis.call("ZRANGE", prefix .. list, 0, -1)
            local taken = redis.call("ZRANGE", prefix .. list .. ".taken", 0, -1)
            local _ = {}
            for i = 1, #taken do _[taken[i]] = true end
            taken = _
            _ = {}
            for i = 1, #items do
                _[#_ + 1] = {sort = math.random(), value = items[i]}
            end
            items = _
            table.sort(items, function (a, b)
                return a.sort < b.sort
            end)
            for i = 1, #items do
                if not taken[items[i].value] then
                    redis.call("ZADD", prefix .. list .. ".taken", ttl, items[i].value)
                    return items[i].value
                end
            end
        ', 0, ...[
            $this->prefix,
            time(),
            mt_rand(),
            $list,
            $ttl
        ]);
    }
}
