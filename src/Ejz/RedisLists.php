<?php

namespace Ejz;

class RedisLists
{
    /**
     * PREFIX
     */
    private const PREFIX = 'list_';

    /** @var RedisClient */
    private $client;

    /** @var string */
    private $prefix;

    /**
     * @param RedisClient $client
     * @param ?string     $prefix (optional)
     */
    public function __construct(RedisClient $client, ?string $prefix = null)
    {
        $this->client = $client;
        $this->prefix = $prefix ?? self::PREFIX;
    }

    /**
     * @param string $list
     * @param mixed  $item
     * @param int    $ttl
     */
    public function insert(string $list, $item, int $ttl)
    {
        $item = $this->pack($item);
        $this->client->ZADD($this->prefix . $list, time() + $ttl, $item);
    }

    /**
     * @param string $list
     * @param mixed  $item
     */
    public function remove(string $list, $item)
    {
        $item = $this->pack($item);
        $this->client->ZREM($this->prefix . $list, $item);
        $this->client->ZREM($this->prefix . $list . '.taken', $item);
    }

    /**
     * @param string $list
     *
     * @return mixed
     */
    public function lru(string $list)
    {
        $this->gc($list);
        $items = $this->client->ZPOPMIN($this->prefix . $list);
        $item = $items[0] ?? null;
        return $item !== null ? $this->unpack($item) : null;
    }

    /**
     * @param string $list
     */
    private function gc(string $list)
    {
        $time = time();
        $this->client->ZREMRANGEBYSCORE($this->prefix . $list, '-inf', $time);
        $this->client->ZREMRANGEBYSCORE($this->prefix . $list . '.taken', '-inf', $time);
    }

    /**
     * @param string $list
     */
    public function removeAll(string $list)
    {
        $this->client->DEL($this->prefix . $list);
        $this->client->DEL($this->prefix . $list . '.taken');
    }

    /**
     * @param string $list
     * @param int    $ttl
     *
     * @return mixed
     */
    public function take(string $list, int $ttl)
    {
        $time = time() + $ttl;
        $virtual = '';
        if (strpos($list, ':') !== false) {
            [$list, $virtual] = explode(':', $list, 2);
        }
        $args = [$this->prefix, $time, mt_rand(), $list, $virtual];
        $item = $this->client->EVAL(self::SCRIPT_TAKE, 0, ...$args);
        return $item !== null ? $this->unpack($item) : null;
    }

    /**
     * @param string $list
     *
     * @return array
     */
    public function all(string $list): array
    {
        $this->gc($list);
        $ret = (array) $this->client->ZRANGE($this->prefix . $list, 0, -1);
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

    /**
     * SCRIPT_TAKE
     */
    private const SCRIPT_TAKE = '
        local prefix = ARGV[1]
        local time = ARGV[2]
        math.randomseed(tonumber(ARGV[3]))
        local list = ARGV[4]
        local virtual = ARGV[5]
        local taken_prefix = ".taken"
        if virtual ~= "" then
            taken_prefix = "." .. virtual .. taken_prefix
        end
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
            return b.sort < a.sort
        end)
        for i = 1, #items do
            if not taken[items[i].value] then
                redis.call("ZADD", prefix .. list .. taken_prefix, time, items[i].value)
                return items[i].value
            end
        end
    ';
}
