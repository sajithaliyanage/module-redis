/*
 * Copyright (c) 2018, WSO2 Inc. (http:www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http:www.apache.orglicensesLICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.ei.module.redis.actions;

import io.lettuce.core.KeyValue;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisListCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.ballerinalang.jvm.values.ArrayValue;
import org.wso2.ei.module.redis.RedisDataSource;
import org.ballerinalang.jvm.util.exceptions.BallerinaException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * {@code {@link AbstractRedisAction}} is the base class for all Redis connector actions.
 *
 * @since 0.5.0
 */
public class AbstractRedisAction {
    protected static final String MUST_NOT_BE_NULL = "must not be null";
    private static final String KEY_MUST_NOT_BE_NULL = "Key " + MUST_NOT_BE_NULL;
    private static final String KEYS_MUST_NOT_BE_NULL = "Key(s) " + MUST_NOT_BE_NULL;
    private static final String ARGUMENTS_MUST_NOT_BE_NULL = "Arguments " + MUST_NOT_BE_NULL;

    protected AbstractRedisAction() {
    }
    
    private static <K, V> Object getRedisCommands(RedisDataSource<K, V> redisDataSource) {
        if (isClusterConnection(redisDataSource)) {
            return redisDataSource.getRedisClusterCommands();
        } else {
            return redisDataSource.getRedisCommands();
        }
    }

    //String Commands

    protected <K, V> String set(K key, V value, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            return new String(redisCommands.set(key, value));
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String get(K key, RedisDataSource<K, String> redisDataSource) {
        RedisStringCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.get(key);
            return result == null ? null : new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected Integer append(String key, String value, RedisDataSource<String, String> redisDataSource) {
        RedisStringCommands<String, String> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<String, String>) getRedisCommands(redisDataSource);
            Long result = redisCommands.append(key, value);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected Integer bitCount(String key, RedisDataSource<String, String> redisDataSource) {
        RedisStringCommands<String, String> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<String, String>) getRedisCommands(redisDataSource);
            Long result = redisCommands.bitcount(key);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer bitopAnd(K destination, RedisDataSource<K, V> redisDataSource, K... keys) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.bitopAnd(destination, keys);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEYS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer bitopOr(K destination, RedisDataSource<K, V> redisDataSource, K... keys) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.bitopOr(destination, keys);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEYS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer bitopNot(K destination, K key, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.bitopNot(destination, key);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEYS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer bitopXor(K destination, RedisDataSource<K, V> redisDataSource, K... keys) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.bitopXor(destination, keys);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer decr(K key, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.decr(key);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer decrBy(K key, int value, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.decrby(key, value);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer getBit(K key, int offset, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.getbit(key, offset);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String getRange(K key, int start, int end, RedisDataSource<K, String> redisDataSource) {
        RedisStringCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.getrange(key, start, end);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String getSet(K key, String value, RedisDataSource<K, String> redisDataSource) {
        RedisStringCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.getset(key, value);
            return result == null ? null : new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer incr(K key, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.incr(key);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer incrBy(K key, int value, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.incrby(key, value);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Float incrByFloat(K key, double value, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            Double result = redisCommands.incrbyfloat(key, value);
            return new Float(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Map mGet(RedisDataSource<K, String> redisDataSource, K... key) {
        RedisStringCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, String>) getRedisCommands(redisDataSource);
            List<KeyValue<K, String>> result = redisCommands.mget(key);
            return createBMapFromKeyValueList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEYS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> String mSet(Map<K, V> map, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            String result = redisCommands.mset(map);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Boolean mSetnx(Map<K, V> map, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.msetnx(map);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String pSetex(K key, String value, long expirationPeriodMS,
            RedisDataSource<K, String> redisDataSource) {
        RedisStringCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.psetex(key, expirationPeriodMS, value);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer setBit(K key, int value, long offset, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.setbit(key, offset, value);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String setEx(K key, String value, long expirationPeriodSeconds,
            RedisDataSource<K, String> redisDataSource) {
        RedisStringCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.setex(key, expirationPeriodSeconds, value);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Boolean setNx(K key, String value, RedisDataSource<K, String> redisDataSource) {
        RedisStringCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, String>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.setnx(key, value);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer setRange(K key, long offset, V value, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            long result = redisCommands.setrange(key, offset, value);
            return new Integer((int)result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer strln(K key, RedisDataSource<K, V> redisDataSource) {
        RedisStringCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisStringCommands<K, V>) getRedisCommands(redisDataSource);
            long result = redisCommands.strlen(key);
            return new Integer((int)result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    //List Commands
    protected <K, V> Integer lPush(K key, RedisDataSource<K, V> redisDataSource, V... value) {
        RedisListCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.lpush(key, value);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String lPop(K key, RedisDataSource<K, String> redisDataSource) {
        RedisListCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.lpop(key);
            return result == null ? null : new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer lPushX(K key, RedisDataSource<K, V> redisDataSource, V... values) {
        RedisListCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.lpushx(key, values);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Map<K, String> bLPop(long timeout, RedisDataSource<K, String> redisDataSource, K... keys) {
        RedisListCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, String>) getRedisCommands(redisDataSource);
            KeyValue<K, String> result = redisCommands.blpop(timeout, keys);
            if (result != null) {
                Map<K, String> bMap = new HashMap<>();
                bMap.put(result.getKey(), new String(result.getValue()));
                return bMap;
            } else {
                return null;
            }
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEYS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Map<K, String> bRPop(long timeout, RedisDataSource<K, String> redisDataSource, K... keys) {
        RedisListCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, String>) getRedisCommands(redisDataSource);
            KeyValue<K, String> result = redisCommands.brpop(timeout, keys);
            if (result != null) {
                Map<K, String> bMap = new HashMap<>();
                bMap.put(result.getKey(), new String(result.getValue()));
                return bMap;
            } else {
                return null;
            }
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String brPopLPush(K source, K destination, long timeout,
            RedisDataSource<K, String> redisDataSource) {
        RedisListCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.brpoplpush(timeout, source, destination);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEYS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String lIndex(K key, long index, RedisDataSource<K, String> redisDataSource) {
        RedisListCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.lindex(key, index);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer lInsert(K key, boolean before, V pivot, V value, RedisDataSource<K, V> redisDataSource) {
        RedisListCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.linsert(key, before, pivot, value);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer lLen(K key, RedisDataSource<K, V> redisDataSource) {
        RedisListCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.llen(key);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue lRange(K key, long start, long stop, RedisDataSource<K, String> redisDataSource) {
        RedisListCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, String>) getRedisCommands(redisDataSource);
            List<String> result = redisCommands.lrange(key, start, stop);
            return createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer lRem(K key, long count, V value, RedisDataSource<K, V> redisDataSource) {
        RedisListCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.lrem(key, count, value);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> String lSet(K key, long index, V value, RedisDataSource<K, V> redisDataSource) {
        RedisListCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, V>) getRedisCommands(redisDataSource);
            String result = redisCommands.lset(key, index, value);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> String lTrim(K key, long start, long stop, RedisDataSource<K, V> redisDataSource) {
        RedisListCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, V>) getRedisCommands(redisDataSource);
            String result = redisCommands.ltrim(key, start, stop);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String rPop(K key, RedisDataSource<K, String> redisDataSource) {
        RedisListCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.rpop(key);
            return result == null ? null : new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String rPopLPush(K source, K destination, RedisDataSource<K, String> redisDataSource) {
        RedisListCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.rpoplpush(source, destination);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer rPush(K key, RedisDataSource<K, V> redisDataSource, V... values) {
        RedisListCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.rpush(key, values);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer rPushX(K key, RedisDataSource<K, V> redisDataSource, V... values) {
        RedisListCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisListCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.rpushx(key, values);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    //Set Commands

    protected <K, V> Integer sAdd(K key, RedisDataSource<K, V> redisDataSource, V... values) {
        RedisSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.sadd(key, values);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer sCard(K key, RedisDataSource<K, V> redisDataSource) {
        RedisSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.scard(key);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue sDiff(RedisDataSource<K, String> redisDataSource, K... keys) {
        RedisSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, String>) getRedisCommands(redisDataSource);
            Set<String> result = redisCommands.sdiff(keys);
            ArrayValue valueArray = createStringArrayFromSet(result);
            return valueArray;
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Integer sDiffStore(K dest, RedisDataSource<K, String> redisDataSource, K... keys) {
        RedisSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, String>) getRedisCommands(redisDataSource);
            Long result = redisCommands.sdiffstore(dest, keys);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue sInter(RedisDataSource<K, String> redisDataSource, K... keys) {
        RedisSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, String>) getRedisCommands(redisDataSource);
            Set<String> result = redisCommands.sinter(keys);
            return createStringArrayFromSet(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Integer sInterStore(K dest, RedisDataSource<K, String> redisDataSource, K... keys) {
        RedisSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, String>) getRedisCommands(redisDataSource);
            Long result = redisCommands.sinterstore(dest, keys);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Boolean sIsMember(K key, V value, RedisDataSource<K, V> redisDataSource) {
        RedisSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, V>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.sismember(key, value);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue sMembers(K key, RedisDataSource<K, String> redisDataSource) {
        RedisSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, String>) getRedisCommands(redisDataSource);
            Set<String> result = redisCommands.smembers(key);
            return createStringArrayFromSet(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Boolean sMove(K source, K dest, V member, RedisDataSource<K, V> redisDataSource) {
        RedisSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, V>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.smove(source, dest, member);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEYS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue sPop(K key, int count, RedisDataSource<K, String> redisDataSource) {
        RedisSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, String>) getRedisCommands(redisDataSource);
            Set<String> result = redisCommands.spop(key, count);
            return (result == null || result.isEmpty()) ? null : createStringArrayFromSet(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue sRandMember(K key, int count, RedisDataSource<K, String> redisDataSource) {
        RedisSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, String>) getRedisCommands(redisDataSource);
            List<String> result = redisCommands.srandmember(key, count);
            return (result == null || result.isEmpty()) ? null : createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Integer sRem(K key, RedisDataSource<K, String> redisDataSource, String... members) {
        RedisSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, String>) getRedisCommands(redisDataSource);
            Long result = redisCommands.srem(key, members);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue sUnion(RedisDataSource<K, String> redisDataSource, K... keys) {
        RedisSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, String>) getRedisCommands(redisDataSource);
            Set<String> result = redisCommands.sunion(keys);
            return createStringArrayFromSet(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEYS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Integer sUnionStore(K dest, RedisDataSource<K, String> redisDataSource, K... keys) {
        RedisSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSetCommands<K, String>) getRedisCommands(redisDataSource);
            Long result = redisCommands.sunionstore(dest, keys);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Argements " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    public static <K, V> void close(RedisDataSource<K, V> redisDataSource) {
        if (isClusterConnection(redisDataSource)) {
            if (redisDataSource.isPoolingEnabled()) {
                redisDataSource.closeConnectionPool();
            } else {
                redisDataSource.getRedisClusterCommands().quit();
            }
        } else {
            if (redisDataSource.isPoolingEnabled()) {
                redisDataSource.closeConnectionPool();
            } else {
                redisDataSource.getRedisCommands().quit();
            }
        }
    }

    // Sorted Set Commands

    protected <K, V> Integer zAdd(K key, RedisDataSource<K, V> redisDataSource, Map<V, Double> valueScoreMap) {
        ScoredValue<V>[] scoredValues = createArrayFromScoredValueMap(valueScoreMap);
        RedisSortedSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.zadd(key, scoredValues);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Members " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer zCard(K key, RedisDataSource<K, V> redisDataSource) {
        RedisSortedSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.zcard(key);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer zCount(K key, double min, double max, RedisDataSource<K, V> redisDataSource) {
        Range<Double> range = Range.create(min, max);
        RedisSortedSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.zcount(key, range);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Float zIncrBy(K key, double amount, V member, RedisDataSource<K, V> redisDataSource) {
        RedisSortedSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, V>) getRedisCommands(redisDataSource);
            Double result = redisCommands.zincrby(key, amount, member);
            return new Float(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer zInterStore(K dest, RedisDataSource<K, V> redisDataSource, K... keys) {
        RedisSortedSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.zinterstore(dest, keys);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer zLexCount(K key, V min, V max, RedisDataSource<K, V> redisDataSource) {
        RedisSortedSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, V>) getRedisCommands(redisDataSource);
            Range<V> range = Range.create(min, max);
            Long result = redisCommands.zlexcount(key, range);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue zRange(K key, long min, long max, RedisDataSource<K, String> redisDataSource) {
        RedisSortedSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, String>) getRedisCommands(redisDataSource);
            List<String> result = redisCommands.zrange(key, min, max);
            return createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue zRangeByLex(K key, String min, String max, RedisDataSource<K, String> redisDataSource) {
        RedisSortedSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, String>) getRedisCommands(redisDataSource);
            Range<String> range = Range.create(min, max);
            List<String> result = redisCommands.zrangebylex(key, range);
            return createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue zRevRangeByLex(K key, String min, String max,
            RedisDataSource<K, String> redisDataSource) {
        RedisSortedSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, String>) getRedisCommands(redisDataSource);
            Range<String> range = Range.create(min, max);
            List<String> result = redisCommands.zrevrangebylex(key, range);
            return createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Arguments" + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue zRangeByScore(K key, double min, double max,
            RedisDataSource<K, String> redisDataSource) {
        RedisSortedSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, String>) getRedisCommands(redisDataSource);
            Range<Double> range = Range.create(min, max);
            List<String> result = redisCommands.zrangebyscore(key, range);
            return createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer zRank(K key, V member, RedisDataSource<K, V> redisDataSource) {
        RedisSortedSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.zrank(key, member);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer zRem(K key, RedisDataSource<K, V> redisDataSource, V... members) {
        RedisSortedSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.zrem(key, members);
            return new Integer(result.intValue());
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Integer zRemRangeByLex(K key, String min, String max, RedisDataSource<K, String> redisDataSource) {
        RedisSortedSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, String>) getRedisCommands(redisDataSource);
            Range<String> range = Range.create(min, max);
            Long result = redisCommands.zremrangebylex(key, range);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Integer zRemRangeByRank(K key, long min, long max, RedisDataSource<K, String> redisDataSource) {
        RedisSortedSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, String>) getRedisCommands(redisDataSource);
            Long result = redisCommands.zremrangebyrank(key, min, max);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Integer zRemRangeByScore(K key, double min, double max, RedisDataSource<K, String> redisDataSource) {
        RedisSortedSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, String>) getRedisCommands(redisDataSource);
            Range<Double> range = Range.create(min, max);
            Long result = redisCommands.zremrangebyscore(key, range);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue zRevRange(K key, long min, long max, RedisDataSource<K, String> redisDataSource) {
        RedisSortedSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, String>) getRedisCommands(redisDataSource);
            List<String> result = redisCommands.zrevrange(key, min, max);
            return createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue zRevRangeByScore(K key, double min, double max,
            RedisDataSource<K, String> redisDataSource) {
        RedisSortedSetCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, String>) getRedisCommands(redisDataSource);
            Range<Double> range = Range.create(min, max);
            List<String> result = redisCommands.zrevrangebyscore(key, range);
            return createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(ARGUMENTS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer zRevRank(K key, V member, RedisDataSource<K, V> redisDataSource) {
        RedisSortedSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.zrevrank(key, member);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Float zScore(K key, V member, RedisDataSource<K, V> redisDataSource) {
        RedisSortedSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, V>) getRedisCommands(redisDataSource);
            double result = redisCommands.zscore(key, member);
            return new Float(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer zUnionStore(K dest, RedisDataSource<K, V> redisDataSource, K... keys) {
        RedisSortedSetCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisSortedSetCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.zunionstore(dest, keys);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Destination key/source key(s) " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    // Hash Commands

    protected <K, V> Integer hDel(K key, RedisDataSource<K, V> redisDataSource, K... fields) {
        RedisHashCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.hdel(key, fields);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Key/field(s) " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Boolean hExists(K key, K field, RedisDataSource<K, V> redisDataSource) {
        RedisHashCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, V>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.hexists(key, field);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Key/field(s) " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String hGet(K key, K field, RedisDataSource<K, String> redisDataSource) {
        RedisHashCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.hget(key, field);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Key/field(s) " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Map<K, String> hGetAll(K key, RedisDataSource<K, String> redisDataSource) {
        RedisHashCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, String>) getRedisCommands(redisDataSource);
            Map<K, String> result = redisCommands.hgetall(key);
            return createBMapFromMap(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Integer hIncrBy(K key, K field, long amount, RedisDataSource<K, String> redisDataSource) {
        RedisHashCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, String>) getRedisCommands(redisDataSource);
            Long result = redisCommands.hincrby(key, field, amount);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Key/field(s) " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Float hIncrByFloat(K key, K field, double amount, RedisDataSource<K, String> redisDataSource) {
        RedisHashCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, String>) getRedisCommands(redisDataSource);
            Double result = redisCommands.hincrbyfloat(key, field, amount);
            return new Float(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Key/field " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected ArrayValue hKeys(String key, RedisDataSource<String, String> redisDataSource) {
        RedisHashCommands<String, String> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<String, String>) getRedisCommands(redisDataSource);
            List<String> result = redisCommands.hkeys(key);
            return createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Integer hLen(K key, RedisDataSource<K, String> redisDataSource) {
        RedisHashCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, String>) getRedisCommands(redisDataSource);
            Long result = redisCommands.hlen(key);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Key/field " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Map<K, String> hMGet(K key, RedisDataSource<K, String> redisDataSource, K... fields) {
        RedisHashCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, String>) getRedisCommands(redisDataSource);
            List<KeyValue<K, String>> result = redisCommands.hmget(key, fields);
            return createBMapFromKeyValueList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Key/field(s) " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> String hMSet(K key, Map<K, V> fieldValueMap, RedisDataSource<K, V> redisDataSource) {
        RedisHashCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, V>) getRedisCommands(redisDataSource);
            String result = redisCommands.hmset(key, fieldValueMap);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Key/field " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Boolean hSet(K key, K field, V value, RedisDataSource<K, V> redisDataSource) {
        RedisHashCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, V>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.hset(key, field, value);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Key/field " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Boolean hSetNx(K key, K field, V value, RedisDataSource<K, V> redisDataSource) {
        RedisHashCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, V>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.hsetnx(key, field, value);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Key/field " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer hStrln(K key, K field, RedisDataSource<K, V> redisDataSource) {
        RedisHashCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, V>) getRedisCommands(redisDataSource);
            long result = redisCommands.hstrlen(key, field);
            return new Integer((int)result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Key/field " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue hVals(K key, RedisDataSource<K, String> redisDataSource) {
        RedisHashCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisHashCommands<K, String>) getRedisCommands(redisDataSource);
            List<String> result = redisCommands.hvals(key);
            return createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    // Key commands
    protected <K, V> Integer del(RedisDataSource<K, V> redisDataSource, K... keys) {
        RedisKeyCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.del(keys);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    // TODO: Add as a native action once byte type is supported in ballerina. When doing so retrun a BType
    protected <K, V> byte[] dump(K key, RedisDataSource<K, V> redisDataSource) {
        RedisKeyCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, V>) getRedisCommands(redisDataSource);
            byte[] result = redisCommands.dump(key);
            return result;
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer exists(RedisDataSource<K, V> redisDataSource, K... keys) {
        RedisKeyCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, V>) getRedisCommands(redisDataSource);
            Long result = redisCommands.exists(keys);
            return new Integer(result.intValue());
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEYS_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Boolean expire(K key, long seconds, RedisDataSource<K, V> redisDataSource) {
        RedisKeyCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, V>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.expire(key, seconds);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <V> ArrayValue keys(String pattern, RedisDataSource<String, V> redisDataSource) {
        RedisKeyCommands<String, V> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<String, V>) getRedisCommands(redisDataSource);
            List<String> result = redisCommands.keys(pattern);
            return createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Boolean move(K key, int db, RedisDataSource<K, V> redisDataSource) {
        RedisKeyCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, V>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.move(key, db);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Boolean persist(K key, RedisDataSource<K, V> redisDataSource) {
        RedisKeyCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, V>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.persist(key);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Boolean pExpire(K key, long milliSeconds, RedisDataSource<K, V> redisDataSource) {
        RedisKeyCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, V>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.pexpire(key, milliSeconds);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer pTtl(K key, RedisDataSource<K, V> redisDataSource) {
        RedisKeyCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, V>) getRedisCommands(redisDataSource);
            long result = redisCommands.pttl(key);
            return new Integer((int)result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String randomKey(RedisDataSource<K, String> redisDataSource) {
        RedisKeyCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.randomkey();
            return result == null ? null : new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String rename(K key, K newName, RedisDataSource<K, String> redisDataSource) {
        RedisKeyCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.rename(key, newName);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> Boolean renameNx(K key, K newName, RedisDataSource<K, String> redisDataSource) {
        RedisKeyCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, String>) getRedisCommands(redisDataSource);
            boolean result = redisCommands.renamenx(key, newName);
            return new Boolean(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> ArrayValue sort(K key, RedisDataSource<K, String> redisDataSource) {
        RedisKeyCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, String>) getRedisCommands(redisDataSource);
            List<String> result = redisCommands.sort(key);
            return createStringArrayFromList(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> Integer ttl(K key, RedisDataSource<K, V> redisDataSource) {
        RedisKeyCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, V>) getRedisCommands(redisDataSource);
            long result = redisCommands.ttl(key);
            return new Integer((int)result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K, V> String type(K key, RedisDataSource<K, V> redisDataSource) {
        RedisKeyCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisKeyCommands<K, V>) getRedisCommands(redisDataSource);
            String result = redisCommands.type(key);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException(KEY_MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    //Connection commands

    protected <K, V> String auth(String password, RedisDataSource<K, V> redisDataSource) {
        RedisCommands<K, V> redisCommands = null;
        try {
            redisCommands = (RedisCommands<K, V>) getRedisCommands(redisDataSource);
            String result = redisCommands.auth(password);
            return new String(result);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Password " + MUST_NOT_BE_NULL);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    protected <K> String echo(String message, RedisDataSource<K, String> redisDataSource) {
        RedisCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.echo(message);
            return new String(result);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    public static <K> String ping(RedisDataSource<K, String> redisDataSource) {
        RedisCommands<K, String> redisCommands = null;
        try {
            redisCommands = (RedisCommands<K, String>) getRedisCommands(redisDataSource);
            String result = redisCommands.ping();
            return new String(result);
        } finally {
            releaseResources(redisCommands, redisDataSource);
        }
    }

    private static boolean isClusterConnection(RedisDataSource redisDataSource) {
        return redisDataSource.isClusterConnection();
    }

    private ArrayValue createStringArrayFromSet(Set<String> set) {
        ArrayValue bStringArray = new ArrayValue(new String[set.size()]);
        int i = 0;
        for (String item : set) {
            bStringArray.add(i, item);
            i++;
        }
        return bStringArray;
    }

    private ArrayValue createStringArrayFromList(List<String> list) {
        ArrayValue bStringArray = new ArrayValue(new String[list.size()]);
        int i = 0;
        for (String item : list) {
            bStringArray.add(i, item);
            i++;
        }
        return bStringArray;
    }

    private <V> ScoredValue<V>[] createArrayFromScoredValueMap(Map<V, Double> valueScoreMap) {
        ScoredValue<V>[] scoredValues = new ScoredValue[valueScoreMap.size()];
        int i = 0;
        for (Map.Entry<V, Double> entry : valueScoreMap.entrySet()) {
            scoredValues[i] = ScoredValue.fromNullable(entry.getValue(), entry.getKey());
            i++;
        }
        return scoredValues;
    }

    private <K> Map<K, String> createBMapFromMap(Map<K, String> map) {
        Map<K, String> bMap = new HashMap<>();
        map.forEach((key, value) -> bMap.put(key, new String(value)));
        return bMap;
    }

    private <K> Map<K, String> createBMapFromKeyValueList(List<KeyValue<K, String>> list) {
        Map<K, String> bMap = new HashMap<>();
        for (KeyValue<K, String> item : list) {
            String value;
            try {
                value = item.getValue();
            } catch (NoSuchElementException e) {
                value = null;
            }
            bMap.put(item.getKey(), new String(value));
        }
        return bMap;
    }

    protected String[] createArrayFromStringArray(ArrayValue bStringArray) {
        String[] stringArray = new String[(int) bStringArray.size()];
        for (int i = 0; i < bStringArray.size(); i++) {
            stringArray[i] = bStringArray.getString(i);
        }
        return stringArray;
    }

    protected Map<String, String> createMapFromBMap(Map<String, String> bMap) {
        Map<String, String> map = new HashMap<>();
        bMap.keySet().forEach(item -> map.put(item, bMap.get(item)));
        return map;
    }

//    protected void setNullableReturnValues(Object result, Context context) {
//        if (result == null) {
//            context.setReturnValues();
//        } else {
//            context.setReturnValues(result);
//        }
//    }

    private static <K, V> void releaseResources(Object redisCommands, RedisDataSource<K, V> redisDataSource) {
        if (redisDataSource.isPoolingEnabled() && redisCommands != null) {
            redisDataSource.releaseResources(redisCommands);
        }
    }
}
