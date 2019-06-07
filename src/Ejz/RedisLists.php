<?php

namespace Ejz;

use Ejz\RedisClient;

class RedisLists
{
    /** @var RedisClient */
    private $redis;

    private $prefix = 'RedisLists.';

    /**
     * @param RedisClient $redis
     */
    public function __construct(RedisClient $redis)
    {
        $this->redis = $redis;
    }

    /**
     * @param string     $list
     * @param mixed      $item
     * @param string|int $ttl  (optional)
     */
    public function insert(string $list, $item, $ttl)
    {
        $item = $this->pack($item);
        $time = is_numeric($ttl) ? time() + $ttl : strtotime($ttl);
        $this->redis->ZADD($this->prefix . $list, $time, $item);
    }

    /**
     * @param string $list
     * @param mixed  $item
     */
    public function remove(string $list, $item)
    {
        $item = $this->pack($item);
        $this->redis->ZREM($this->prefix . $list . '.taken', $item);
        $this->redis->ZREM($this->prefix . $list, $item);
    }

    /**
     * @param string $list
     *
     * @return mixed|null
     */
    public function popMin(string $list)
    {
        $this->redis->ZREMRANGEBYSCORE($this->prefix . $list, '-inf', time());
        $items = $this->redis->ZPOPMIN($this->prefix . $list);
        return isset($items[0]) ? $this->unpack($items[0]) : null;
    }

    /**
     * @param string $list
     *
     * @return mixed|null
     */
    public function popMax(string $list)
    {
        $this->redis->ZREMRANGEBYSCORE($this->prefix . $list, '-inf', time());
        $items = $this->redis->ZPOPMAX($this->prefix . $list);
        return isset($items[0]) ? $this->unpack($items[0]) : null;
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
     * @return mixed
     */
    public function take(string $list, $ttl)
    {
        $time = is_numeric($ttl) ? time() + $ttl : strtotime($ttl);
        do {
            $item = $this->takeBackend($list, $time);
        } while ($item === null && sleep(3) === 0);
        return $this->unpack($item);
    }

    /**
     * @param string $list
     * @param int    $time
     *
     * @return ?string
     */
    private function takeBackend(string $list, int $ttl): ?string
    {
        $list = explode(':', $list);
        [$list, $virtual] = [$list[0], $list[1] ?? ''];
        return $this->redis->EVAL('
            local prefix = ARGV[1]
            local time = ARGV[2]
            math.randomseed(tonumber(ARGV[3]))
            local list = ARGV[4]
            local virtual = ARGV[5]
            local ttl = tonumber(ARGV[6])
            local taken_prefix = ".taken"
            if virtual ~= "" then
                taken_prefix = "." .. virtual .. taken_prefix
            end
            redis.call("ZREMRANGEBYSCORE", prefix .. list, "-inf", time)
            redis.call("ZREMRANGEBYSCORE", prefix .. list .. taken_prefix, "-inf", time)
            local items = redis.call("ZRANGE", prefix .. list, 0, -1)
            local taken = redis.call("ZRANGE", prefix .. list .. taken_prefix, 0, -1)
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
                    redis.call("ZADD", prefix .. list .. taken_prefix, ttl, items[i].value)
                    return items[i].value
                end
            end
        ', 0, ...[
            $this->prefix,
            time(),
            mt_rand(),
            $list,
            $virtual,
            $ttl
        ]);
    }

    /**
     * @param string $list
     *
     * @return array
     */
    public function all(string $list): array
    {
        $this->redis->ZREMRANGEBYSCORE($this->prefix . $list, '-inf', time());
        $ret = (array) $this->redis->ZRANGE($this->prefix . $list, 0, -1);
        return array_map([$this, 'unpack'], $ret);
    }

    /**
     * @param mixed $value
     *
     * @return string
     */
    private function pack($value): string
    {
        if (is_string($value)) {
            return 'r' . $value;
        }
        return 's' . serialize($value);
    }

    /**
     * @param string $value
     *
     * @return mixed
     */
    private function unpack(string $value)
    {
        $c = $value[0];
        $value = substr($value, 1);
        if ($c === 'r') {
            return $value;
        }
        return unserialize($value);
    }
}
