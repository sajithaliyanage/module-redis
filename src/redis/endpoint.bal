// Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerinax/java;


# Represents Redis client endpoint.
public type Client client object {
    private ClientEndpointConfiguration clientEndpointConfig;

    private handle redisConnection;

    # Gets called when the endpoint is being initialized during the module initialization.
    public function __init(ClientEndpointConfiguration config) {
        self.redisConnection = initClient(config);
    }

    public function close() returns error? {
        return close(self.redisConnection);
    }

    public remote function ping() returns string|error {
        return ping(self.redisConnection);
    }

    //public function echo(string message) returns string | error {
    //    return echo(java:fromString(message));
    //}
};

//function echo(handle message) returns  string | error = @java:Method {
//    class: "org.wso2.ei.module.redis.basic.BasicCommands"
//} external;

function initClient(ClientEndpointConfiguration clientEndpointConfig) returns handle = @java:Method {
    class: "org.wso2.ei.module.redis.endpoint.RedisClient"
} external;

function close(handle redisClient) = @java:Method {
    class: "org.wso2.ei.module.redis.endpoint.RedisClient"
} external;

function ping(handle redisClient) returns string = @java:Method {
    class: "org.wso2.ei.module.redis.endpoint.RedisClient"
} external;


//function close (Client redisClient) = external;

//function initClient(Client redisClient, ClientEndpointConfiguration clientEndpointConfig) = external;


# The Client endpoint configuration for Redis databases.
#
# + host - The host of the Redis database
# + password - Password for the database connection
# + options - Properties for the connection configuration
public type ClientEndpointConfiguration record {|
    string host = "";
    string password = "";
    Options options = {};
|};

# Connection options for Redis Client Endpoint.
#
# + clientName - The clientName of the connection
# + connectionPooling - Boolean value depending on whether the connection
#   pooling is enabled or not
# + isClusterConnection - Whether to enable cluster connection or not
# + ssl - Boolean value depending on whether SSL is enabled or not
# + startTls - Boolean value depending on whether startTLS is enabled or not
# + verifyPeer - Boolean value depending on whether peer verification is
#   enabled or not
# + database - The database to be used with the connection
# + connectionTimeout - The timeout value for the connection
public type Options record {|
    string clientName = "";
    boolean connectionPooling = false;
    boolean isClusterConnection = false;
    boolean ssl = false;
    boolean startTls = false;
    boolean verifyPeer = false;
    int database = -1;
    int connectionTimeout = -1;
|};


