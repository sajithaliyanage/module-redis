/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.

 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at

 *      http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.ei.module.redis.endpoint;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.codec.Utf8StringCodec;
import org.ballerinalang.jvm.util.exceptions.BallerinaException;
import org.ballerinalang.jvm.values.HandleValue;
import org.ballerinalang.jvm.values.MapValue;
import org.wso2.ei.module.redis.Constants;
import org.wso2.ei.module.redis.RedisDataSource;
import org.wso2.ei.module.redis.actions.AbstractRedisAction;

public class RedisClient {

    private RedisClient(){}

    public static HandleValue initClient(MapValue config) {
        String host = config.getStringValue("host");
        String password = config.getStringValue("password");
        MapValue options = config.getMapValue("options");

        RedisCodec<String, String> codec = retrieveRedisCodec(Constants.Codec.STRING_CODEC.getCodecName());
        boolean clusteringEnabled = options.getBooleanValue(Constants.EndpointConfig.CLUSTERING_ENABLED);
        boolean poolingEnabled = options.getBooleanValue(Constants.EndpointConfig.POOLING_ENABLED);

        RedisDataSource<String, String> redisDataSource;
        redisDataSource = new RedisDataSource<>(codec, clusteringEnabled, poolingEnabled);
        redisDataSource.init(host, password, options);

        return new HandleValue(redisDataSource);
    }

    private static RedisCodec retrieveRedisCodec(String codecString) {
        Constants.Codec codec = retrieveCodec(codecString);
        switch (codec) {
            case BYTE_ARRAY_CODEC:
                return new ByteArrayCodec();
            case STRING_CODEC:
                return new StringCodec();
            case UTF8_STRING_CODEC:
                return new Utf8StringCodec();
            default:
                throw new UnsupportedOperationException("Support for RedisCodec " + codec + " is not implemented yet");
        }
    }

    protected static Constants.Codec retrieveCodec(String codecString) {
        try {
            return Constants.Codec.fromCodecName(codecString);
        } catch (IllegalArgumentException e) {
            throw new BallerinaException("Unsupported Codec: " + codecString);
        }
    }

    public static String ping(HandleValue redisConnection) {
        RedisDataSource<String, String> redisDataSource = (RedisDataSource)redisConnection.getValue();
        return AbstractRedisAction.ping(redisDataSource);
    }

    public static void close(HandleValue redisConnection) {
        RedisDataSource<String, String> redisDataSource = (RedisDataSource)redisConnection.getValue();
        AbstractRedisAction.close(redisDataSource);
    }
}
