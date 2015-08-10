<?php namespace Msz;

class Cache
{
    protected static $connected = null;
    protected static $cachekey = '__CACHE__';
    protected static $host = 'localhost';

    /** @var \Memcache */
    static $memcache = null;
    static $flag = MEMCACHE_COMPRESSED;
    static $prefix = 'club';
    static $useCleanQueue = true;

    /**
     * @param  array $options host, connect, flag, prefix
     * @return void
     */
    static function initialize(array $options)
    {

        if (isset($options['host'])) self::$host = $options['host'];
        if (isset($options['flag'])) self::$flag = $options['flag'];
        if (isset($options['useCleanQueue'])) self::$useCleanQueue = $options['useCleanQueue'];

        if (isset($options['prefix'])) {

            self::$prefix = $options['prefix'];
            self::$cachekey = self::$prefix . self::$cachekey;
        }
    }

    /**
     * Connect to the memcached server(s)
     * @param null $host
     * @return bool|null
     */
    static function connect($host = null)
    {

        if (self::$connected) return true;
        if (self::$connected === false) return false;

        if (!$host) $host = self::$host;
        self::$memcache = new \Memcache();

        self::$connected = false;
        $parts = explode(':', $host);

        $host = $parts[0];
        if (isset($parts[1])) $port = $parts[1]; else $port = 11211;

        if (!(self::$connected = (@self::$memcache->connect($host, $port)))) {

            error_log('Cache::connect failed to connect ' . $host . ':' . $port . '.');
        }

        return self::$connected;
    }

    /**
     * Flush all existing items at the servers
     * @static
     * @return void
     */
    static function flush()
    {

        if (!self::connect()) return;

        self::$memcache->flush();
    }

    /**
     * Set a value in the cache
     * Expiration time is one hour if not set
     * @param $key
     * @param $var
     * @param int $expires
     * @param bool $compress
     * @return bool
     */
    static function set($key, $var, $expires = 3600, $compress = false)
    {

        if (!self::connect()) return false;

        if (!is_numeric($expires)) die('Cache class: set expires is not numeric!');

        if ($compress) $flag = self::$flag; else $flag = null;

        return self::$memcache->set(self::$prefix . $key, $var, $flag, $expires);
    }

    /**
     * stores variable var with key only if such key doesn't exist at the server yet
     * @static
     * @param string $key
     * @param string $var
     * @param int $expires
     * @param bool $compress
     * @return bool
     */
    static function add($key, $var, $expires = 3600, $compress = false)
    {

        if (!self::connect()) return false;

        if (!is_numeric($expires)) die('Cache class: set expires is not numeric!');

        if ($compress) $flag = self::$flag; else $flag = null;

        return self::$memcache->add(self::$prefix . $key, $var, $flag, $expires);
    }

    /**
     * replace value of existing item with key. In case if item with such key doesn't exists, returns FALSE.
     * @static
     * @param string $key
     * @param mixed $var
     * @param int $expires
     * @param bool $compress
     * @return mixed
     */
    static function replace($key, $var, $expires = 3600, $compress = false)
    {

        if (!self::connect()) return false;

        if (!is_numeric($expires)) die('Cache class: set expires is not numeric!');

        if ($compress) $flag = self::$flag; else $flag = null;

        return self::$memcache->replace(self::$prefix . $key, $var, $flag, $expires);
    }

    /**
     * Get a value from cache
     * @param $key
     * @return array|bool|string
     */
    static function get($key)
    {

        if (!self::connect()) return false;

        if (self::$useCleanQueue && self::inCleanQueue($key)) return false;
        return self::$memcache->get(self::$prefix . $key);
    }

    /**
     * Remove value from cache
     * @param $key
     * @return bool
     */
    static function delete($key)
    {

        if (!self::connect()) return false;

        return self::$memcache->delete(self::$prefix . $key, 0);
    }

    static function increment($key, $value = 1)
    {

        if (!self::connect()) return false;

        return self::$memcache->increment(self::$prefix . $key, $value);
    }

    /**
     * @static
     * clear key value on get after $sec seconds
     * @param  string $key
     * @param  int $seconds number of seconds
     * @return void
     */
    static function cleanQueue($key, $seconds = 0)
    {

        if (!self::connect()) return;

        $r = self::$memcache->get(self::$cachekey);
        if (!is_array($r)) $r = array();
        $time = time();

        if (!isset($r[$key]) || (isset($r[$key]) && $time + $seconds < $r[$key])) {

            $r[$key] = $time + $seconds;
            self::$memcache->set(self::$cachekey, $r, 0, 0);
        }
    }

    static function inCleanQueue($key)
    {

        if (!self::connect()) return false;

        $r = self::$memcache->get(self::$cachekey);

        if (is_array($r) && isset($r[$key]) && $r[$key] <= time()) {

            unset($r[$key]);
            self::$memcache->set(self::$cachekey, $r, 0, 0);
            self::delete($key);
            return 1;
        }

        return 0;
    }

    static function flushQueue()
    {

        if (!self::connect()) return;

        self::$memcache->delete(self::$cachekey, 0);
    }

    static function getCacheKeys()
    {

        if (!self::connect()) return false;

        return self::$memcache->get(self::$cachekey);
    }

    /**
     * adds value to the list (atomic operation), if no list create it
     * @static
     * @param  string $list_name
     * @param  mixed $value
     * @param  mixed $expires
     * @param  mixed $cnt
     * @return boolean
     */
    static function listPush($list_name, $value, $expires = 3600, $cnt = 0)
    {

        if (empty($list_name)) return false;

        $klst = 'list:' . $list_name . ':';
        $kidx = 'list:' . $list_name . ':idx';
        $klck = 'list:' . $list_name . ':lock';

        if (static::get($klck)) {

            if ($cnt > 100) return false;
            usleep(1);
            self::listPush($list_name, $value, $expires, $cnt + 1);
        }

        $idx = self::listIncrementKey($kidx);
        if (empty($idx)) return false;

        self::set($klst . $idx, $value, $expires);
        return true;
    }

    /**
     * return all values of the list and clears it
     * @static
     * @param  string $list_name
     * @return array|null
     */
    static function listTrim($list_name)
    {

        $klst = 'list:' . $list_name . ':';
        $kidx = 'list:' . $list_name . ':idx';
        $klck = 'list:' . $list_name . ':lock';

        self::set($klck, 1, 0);
        usleep(1000);
        $idx = self::get($kidx);

        $r = array();

        if ($idx) {
            for ($i = 1; $i <= $idx; $i++) {
                $r[$i] = self::get($klst . $i);
                //self::delete($klst.$i);
            }
        }
        //self::set($kidx, 0, 0);
        self::delete($klck);
        return $r;
    }

    protected static function listIncrementKey($k, $cnt = 0)
    {

        $idx = self::increment($k);
        if ($idx !== false && is_numeric($idx)) return $idx;
        $i = self::add($k, 1, 0);
        if ($i === true) return 1;

        // maximal execution time 1 sec
        if ($cnt >= 1000) {
            error_log('Cache:listIncrementKey cant increment ' . $k);
            return null;
        }
        usleep(1);
        self::listIncrementKey($k, $cnt + 1);

        return null;
    }
}