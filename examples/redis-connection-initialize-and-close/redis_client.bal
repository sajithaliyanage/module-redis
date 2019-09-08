import ballerina/io;

public function main() {
        redis:Client conn = new({
            host: "localhost",
            password: "",
            options: { connectionPooling: true, isClusterConnection: false, ssl: false,
                startTls: false, verifyPeer: false, connectionTimeout: 500 }
        });

        io:println("Redis Server Started...");

        conn.stop();
        io:println("\nRedis connection closed!");
}
