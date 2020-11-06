# Flowchart

```
+----------------+---------------------------------------------------------------------------------------+
| caller         |   rabbitmq-client                                                                     |
+----------------+---------------------------------------------------------------------------------------+
| Connect -------------> Connect()---=+                                                                  |
|                                     |                                                                  |
|                                     +=> connect()                                                      |
|                                     |                     +- done => close()                           |
|                                     +=> create GOROUTE#1 -+                          + done => return  |
|                                                           +- channelClosed => run() -+ connect()       |
|                                                                                      + subscribe()     |
|                                                                                                        |
| Subscribe -----------> Subscribe ---+                                                                  |
|                                     |                                  +- done => return               |
|                                     +=> create GOROUTE#2 subscribe() --+- msg => GOROUTE#3 handleMsg   |
|                                                                        +- channelclosed => return      |
+--------------------------------------------------------------------------------------------------------+
```