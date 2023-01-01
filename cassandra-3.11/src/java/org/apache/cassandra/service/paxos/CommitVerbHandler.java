/*
 * 
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
 * 
 */
package org.apache.cassandra.service.paxos;

//HK
import org.apache.cassandra.utils.FBUtilities;

import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;

public class CommitVerbHandler implements IVerbHandler<Commit>
{
    public void doVerb(MessageIn<Commit> message, int id)
    {
//        if(!FBUtilities.getLocalAddress().equals(message.from)) {
//            System.out.println("===============HK-TEST=====local: " + FBUtilities.getLocalAddress() + "====message from:  " +message.from);
//            System.out.println(message.toString());
//            PaxosState.commit(message.payload);
//        }
        //HK
        System.out.println("==================HK-TEST============commit message:"+message);
        PaxosState.commit(message.payload);

        Tracing.trace("Enqueuing acknowledge to {}", message.from);
        MessagingService.instance().sendReply(WriteResponse.createPaxosMessage(), id, message.from); // HK
        //MessagingService.instance().sendReply(WriteResponse.createMessage(), id, message.from);
    }
}
