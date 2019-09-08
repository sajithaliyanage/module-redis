import ballerina/io;
import ballerina/test;

# Before Suite Function

//@test:BeforeSuite
//function beforeSuiteFunc() {
//    io:println("I'm the before suite function!");
//}

//# Before test function

//function beforeFunc() {
//    io:println("I'm the before function!");
//}

# Test function
Client conn = new ({
    host: "localhost",
    password: "",
    options: {
        connectionPooling: true,
        isClusterConnection: false,
        ssl: false,
        startTls: false,
        verifyPeer: false,
        connectionTimeout: 500
    }
});

@test:Config {}
function testFunction() {
    io:println("Pinging Redis Server...");
         //Ping Server
         var result = conn->ping();
         string assertionResponse = "";
         if (result is string) {
             assertionResponse = "Hello";
             io:println(result);
         } else {
             io:println(result.reason());
         }

    test:assertEquals(assertionResponse, "Hello", msg = "Failed!");
}

//function afterFunc() {
//    io:println("I'm the after function!");
//}
//
//# After Suite Function
//
//@test:AfterSuite
//function afterSuiteFunc() {
//    io:println("I'm the after suite function!");
//}
