package com.Auzmor.Netty;

import io.netty.channel.ChannelHandler.Sharable;

import java.nio.charset.Charset;

import com.Auzmor.Protobuf.PersonElem;
import com.Auzmor.Protobuf.PersonElem.Person;
import com.Auzmor.kafkaQueue.kafkapost;
import com.google.gson.Gson;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler implementation for the echo server.
 */
@Sharable
public class EchoServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
    	System.out.println("Request received...");
    	
    	ByteBuf  bytebuff= (ByteBuf)msg;
   	 	kafkapost kafkaproducer=new kafkapost();
   	 	Gson gson = new Gson();
   	 	String json =bytebuff.toString(Charset.defaultCharset());
   	 	Person personobj = gson.fromJson(json, Person.class);  
   	 	Person personobject= PersonElem.Person.newBuilder()
                 .setAge(personobj.getAge())
                 .setName(personobj.getName()) 
                 .build();
       System.out.println("--------------------------------------");
       System.out.println(personobject);
       System.out.println("--------------------------------------");
      
	   	 ctx.write(msg);
		   	 if(personobject!=null){
		   		 try{
		   			System.out.println("Posting into kafka Queue");
		   			kafkaproducer.sendMessage(personobject.toString());
		   		 	} catch (Exception e)
		   		 	{
					e.printStackTrace();
		   		 	}
		     	   	channelReadComplete(ctx);
		     	   	ctx.close();
		   	 	} 	
    		}

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}