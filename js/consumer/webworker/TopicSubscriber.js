importScripts('../../lib/solclient-debug.js');

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Solace Web Messaging API for JavaScript
 * Publish/Subscribe tutorial - Topic Subscriber
 * Demonstrates subscribing to a topic for direct messages and receiving messages
 */

/*jslint es6 browser devel:true*/
/*global solace*/

var TopicSubscriber = function (topicName) {
    'use strict';
    
    var subscriber = {};
    subscriber.session = null;
    subscriber.topicName = topicName;
    subscriber.subscribed = false;

    subscriber.log = function (line) {
        var now = new Date();
        var time = [('0' + now.getHours()).slice(-2), ('0' + now.getMinutes()).slice(-2),
            ('0' + now.getSeconds()).slice(-2)];
        var timestamp = '[' + time.join(':') + '] ';
        let msg =  timestamp + line
        console.log('ww |', msg);
        postMessage(msg);
    };

    subscriber.log('\n*** Subscriber to topic "' + subscriber.topicName + '" is ready to connect ***');

    // Establishes connection to Solace router
    subscriber.connect = function () {
        // extract params
        if (subscriber.session !== null) {
            subscriber.log('Already connected and ready to subscribe.');
            return;
        }

        // create session
        let username = "default";
        let pass = "default";
        let vpn = "pingu-VPN";
        let hosturl = "ws://shared-rcssolace-node01.otc-test.sbb.ch";

        solace.SessionProperties.connectTimeoutInMsecs = 30 * 1000;
        solace.SessionProperties.connectRetries = -1;
        solace.SessionProperties.reconnectRetries = -1;
        solace.SessionProperties.reconnectRetryWaitInMsecs = 10 * 1000;
        solace.SolclientFactory.setLogLevel(solace.LogLevel.DEBUG);

        subscriber.log('Connecting to Solace message router using url: ' + hosturl);
        subscriber.log('Client username: ' + username);
        subscriber.log('Solace message router VPN name: ' + vpn);
    
        try {
            subscriber.session = solace.SolclientFactory.createSession({
                // solace.SessionProperties
                url: hosturl,
                vpnName: vpn,
                userName: username,
                password: pass,
            });
        } catch (error) {
            subscriber.log(error.toString());
        }
        // define session event listeners
        subscriber.session.on(solace.SessionEventCode.UP_NOTICE, function (sessionEvent) {
            subscriber.log('=== Successfully connected and ready to subscribe. ===');
        });
        subscriber.session.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, function (sessionEvent) {
            subscriber.log('Connection failed to the message router: ' + sessionEvent.infoStr +
                ' - check correct parameter values and connectivity!');
        });
        subscriber.session.on(solace.SessionEventCode.DISCONNECTED, function (sessionEvent) {
            subscriber.log('Disconnected.');
            subscriber.subscribed = false;
            if (subscriber.session !== null) {
                subscriber.session.dispose();
                subscriber.session = null;
            }
        });
        subscriber.session.on(solace.SessionEventCode.SUBSCRIPTION_ERROR, function (sessionEvent) {
            subscriber.log('Cannot subscribe to topic: ' + sessionEvent.correlationKey);
        });
        subscriber.session.on(solace.SessionEventCode.SUBSCRIPTION_OK, function (sessionEvent) {
            if (subscriber.subscribed) {
                subscriber.subscribed = false;
                subscriber.log('Successfully unsubscribed from topic: ' + sessionEvent.correlationKey);
            } else {
                subscriber.subscribed = true;
                subscriber.log('Successfully subscribed to topic: ' + sessionEvent.correlationKey);
                subscriber.log('=== Ready to receive messages. ===');
            }
        });
        // define message event listener
        subscriber.session.on(solace.SessionEventCode.MESSAGE, function (message) {
            let now = Date.now()
            let attachment = message.getBinaryAttachment();
            console.log(arguments)
            let splits = attachment.split(';');
            let timeWhenSent = splits[0].substr(4);
            let elapsedTime = now - parseInt(timeWhenSent);
            subscriber.log("Elapsed Time in ms: " + elapsedTime + " | " + splits[1].substr(0, 40) + " | " + attachment.length + " bytes");
        });
        subscriber.connectToSolace();
    };

    // Actually connects the session triggered when the iframe has been loaded - see in html code
    subscriber.connectToSolace = function () {
        try {
            subscriber.session.connect();
        } catch (error) {
            subscriber.log(error.toString());
        }
    };

    // Subscribes to topic on Solace message router
    subscriber.subscribe = function () {
        if (subscriber.session !== null) {
            if (subscriber.subscribed) {
                subscriber.log('Already subscribed to "' + subscriber.topicName
                    + '" and ready to receive messages.');
            } else {
                subscriber.log('Subscribing to topic: ' + subscriber.topicName);
                try {
                    subscriber.session.subscribe(
                        solace.SolclientFactory.createTopicDestination(subscriber.topicName),
                        true, // generate confirmation when subscription is added successfully
                        subscriber.topicName, // use topic name as correlation key
                        10000 // 10 seconds timeout for this operation
                    );
                } catch (error) {
                    subscriber.log(error.toString());
                }
            }
        } else {
            subscriber.log('Cannot subscribe because not connected to Solace message router.');
        }
    };

    // Unsubscribes from topic on Solace message router
    subscriber.unsubscribe = function () {
        if (subscriber.session !== null) {
            if (subscriber.subscribed) {
                subscriber.log('Unsubscribing from topic: ' + subscriber.topicName);
                try {
                    subscriber.session.unsubscribe(
                        solace.SolclientFactory.createTopicDestination(subscriber.topicName),
                        true, // generate confirmation when subscription is removed successfully
                        subscriber.topicName, // use topic name as correlation key
                        10000 // 10 seconds timeout for this operation
                    );
                } catch (error) {
                    subscriber.log(error.toString());
                }
            } else {
                subscriber.log('Cannot unsubscribe because not subscribed to the topic "'
                    + subscriber.topicName + '"');
            }
        } else {
            subscriber.log('Cannot unsubscribe because not connected to Solace message router.');
        }
    };

    // Gracefully disconnects from Solace message router
    subscriber.disconnect = function () {
        subscriber.log('Disconnecting from Solace message router...');
        if (subscriber.session !== null) {
            try {
                subscriber.session.disconnect();
            } catch (error) {
                subscriber.log(error.toString());
            }
        } else {
            subscriber.log('Not connected to Solace message router.');
        }
    };

    subscriber.start = function () {
        subscriber.connect();
        setTimeout(function(){ subscriber.subscribe(); }, 2000);        
    }

    return subscriber;
};

// Initialize factory with the most recent API defaults
var factoryProps = new solace.SolclientFactoryProperties();
factoryProps.profile = solace.SolclientFactoryProfiles.version10;
solace.SolclientFactory.init(factoryProps);

// enable logging to JavaScript console at WARN level
// NOTICE: works only with "solclientjs-debug.js"
solace.SolclientFactory.setLogLevel(solace.LogLevel.WARN);

// create the subscriber, specifying name of the subscription topic
let TOPIC = "demo";
subscriber = new TopicSubscriber(TOPIC);
subscriber.start();