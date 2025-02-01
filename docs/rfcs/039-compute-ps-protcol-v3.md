# V3 version of compute-page server protocol

Created on: 2024-08-15

Author: Konstantin Knizhnik

## Summary

Current version of compute-PS protocol doesn't allow to verify that received response actually corresponds to the request.
In most cases it should not cause any problems, because Neon SMGR follows classical server request-response pattern.
If response is not received due to some reasons (network error, ...), then connection is dropped and error is reported.

But we also actively use prefetch, which allows to minimize network round-trip overhead.
In case of prefetch compute sends multiple getpage requests and then gets responses to all of them.
It is expected that responses are received in the same order as requests are sent, i.e. for each request we receive response.

Unfortunately it can be violated in case of errors. In this case it can happen that connection is reset but prefetch ring - not.
As a result we treat response of new request as response to some older prefetch request and place wrong page image in shared buffer.
If this page is modified, then it is saved in WAL and database file. So we are not able to recover original (correct) page image by applying WAL.
So we can mix pages of one relation or even pages of different relations. Most frequently such corruption is detected for indexes,
but just because there are more invariants which can be checked.  And there is no good universal way to detect and recover such
corruption.

## Motivation

This bug in prefetch was fixed, but we want to prevent similar problems in future.
For example prewarm is also similar with prefetch and sens several requests and only
after it wait for response.

## Previous work

We already changed protocol version from V1 to V2 when replaced single request LSN with pair
(request LSN,not modified since LSN). It was done by introducing new command codes.
So there was no explicit check for protocol version: if server receives new command,
it assumes that it is new protocol version. After both clients and servers were upgraded to new version,
new command codes were removed. Then version was added to "pagestream" command used to perform handshake.
Client has `neon.protocol_version` GUC specifying which protocol version it should use.
So client informs server about protocol version it is going to use, but server can not ask the client to use some other protocol version,
it can only reject connection request if it is not supporting this protocol version.


## Requirements

- Be able to verify that page server response corresponds to the requests
- Provide backward compatibility: old clients should work with new server.

## Non Goals

- Detect page corruption (include CRC)
- Support of vector operation (merge several requests into one)
- Forward compatibility: support new clients with old page server


## Solution

Include in response extra fields making it possible to verify that response corresponds to the particular request.
Such extra fields include:

- tablespace OID
- database OID
- relation OID
- fork number
- block id (for getpage)
- request LSN
- last modified LSN

In addition to this fields, we also introduce unique auto-incremented `request_id`.
It is combined from `backend_id` and local auto-incremented counter.
There is some probability of collision if backend is restarted, but it is not critical as far as we have all other fields included in response.
`request_id` can be used for better tracking and associating log messages produced by client and page server.

Although only mismatch of `getpage` request can cause data corruption, we want to extend responses for all other commands: get  relation/db size, check presence of relation.


## Compatibility

We will change handshake command freom "pagestream_v2" to "pagestream_v3". With V3 version of protocol server should
reply with extended responses. Request/response tags will not be changed.

To prevent forward compatibility issues (when new client tries to access old server), deploy of this PR should be done in three steps:
1. Deploy of new server recognizing V3 protocol version
2. Deploy of new client which is able to send V3 commands, but by default still using V2.
3. After one release cycle when no rollback to previous PS version is possible, we can switch default version of protocol to V3, by changing `neon.protocol_version` GUC in project settings.

