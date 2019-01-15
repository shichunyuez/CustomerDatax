package com.alibaba.datax.web;

import com.alibaba.datax.web.protocol.Protocol;
import com.alibaba.datax.web.reqresp.*;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

public class MasterHandler extends SimpleChannelUpstreamHandler{

    private static final Logger LOG = LoggerFactory.getLogger(MasterHandler.class);
    private MasterContext context=null;

    private CompletionService<ChannelResponse> completionService=new ExecutorCompletionService<ChannelResponse>(Executors.newCachedThreadPool());

    private class ChannelResponse{
        Channel channel;
        Protocol.WebResponse resp;
        public ChannelResponse(Channel channel,Protocol.WebResponse resp){
            this.channel=channel;
            this.resp=resp;
        }
    }

    public MasterHandler(MasterContext context){
        this.context=context;

        new Thread(){
            public void run() {
                while(true){
                    try {
                        Future<ChannelResponse> f=completionService.take();
                        ChannelResponse resp=f.get();
                        resp.channel.write(wapper(resp.resp));
                    } catch (Exception e) {
                        LOG.error("处理消息错误: ", e);
                    }
                }
            };
        }.start();
    }
    private Protocol.SocketMessage wapper(Protocol.WebResponse resp){
        return Protocol.SocketMessage.newBuilder().setKind(Protocol.SocketMessage.Kind.WEB_RESPONSE).setBody(resp.toByteString()).build();
    }
    private HeartBeatReqResp heartBeatReqResp=new HeartBeatReqResp();
    private UpdateReqResp updateReqResp=new UpdateReqResp();
    private WebCancelReqResp webCancelReqResp=new WebCancelReqResp();
    private WebExecuteReqResp webExecuteReqResp=new WebExecuteReqResp();
    private WebDebugReqResp debugReqResp=new WebDebugReqResp();

    /*@Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        final Channel channel=ctx.getChannel();
        Protocol.SocketMessage sm=(Protocol.SocketMessage) e.getMessage();
        if(sm.getKind()== Protocol.SocketMessage.Kind.REQUEST){
            final Protocol.Request request=Protocol.Request.newBuilder().mergeFrom(sm.getBody()).build();
            if(request.getOperate()== Protocol.Operate.HeartBeat){
                heartBeatReqResp.beHeartBeat(context, channel, request);
            }
        }else if(sm.getKind()== Protocol.SocketMessage.Kind.WEB_REUQEST){
            final Protocol.WebRequest request= Protocol.WebRequest.newBuilder().mergeFrom(sm.getBody()).build();
            if(request.getOperate()== Protocol.WebOperate.ExecuteJob){
                completionService.submit(new Callable<ChannelResponse>() {
                    public ChannelResponse call() throws Exception {
                        return new ChannelResponse(channel,webExecuteReqResp.beWebExecute(context,request));
                    }
                });
            }else if(request.getOperate()== Protocol.WebOperate.CancelJob){
                completionService.submit(new Callable<ChannelResponse>() {
                    public ChannelResponse call() throws Exception {
                        return new ChannelResponse(channel,webCancelReqResp.beWebCancel(context,request));
                    }
                });
            }else if(request.getOperate()== Protocol.WebOperate.UpdateJob){
                completionService.submit(new Callable<ChannelResponse>() {
                    public ChannelResponse call() throws Exception {
                        return  new ChannelResponse(channel,updateReqResp.beWebUpdate(context,request));
                    }
                });
            }else if(request.getOperate()== Protocol.WebOperate.ExecuteDebug){
                completionService.submit(new Callable<ChannelResponse>() {
                    public ChannelResponse call() throws Exception {
                        return new ChannelResponse(channel, debugReqResp.beWebExecute(context, request));
                    }
                });
            }
        }else if(sm.getKind()== Protocol.SocketMessage.Kind.RESPONSE){
            for(ResponseListener lis:new ArrayList<ResponseListener>(listeners)){
                lis.onResponse(Protocol.Response.newBuilder().mergeFrom(sm.getBody()).build());
            }
        }else if(sm.getKind()== Protocol.SocketMessage.Kind.WEB_RESPONSE){
            for(ResponseListener lis:new ArrayList<ResponseListener>(listeners)){
                lis.onWebResponse(Protocol.WebResponse.newBuilder().mergeFrom(sm.getBody()).build());
            }
        }

        super.messageReceived(ctx, e);
    }
    @Override
    public void channelConnected(ChannelHandlerContext ctx,
                                 ChannelStateEvent e) throws Exception {
        context.getWorkers().put(ctx.getChannel(), new WorkerHolder(context,ctx.getChannel()));
        Channel channel=ctx.getChannel();
        SocketAddress addr=channel.getRemoteAddress();
        LOG.info("收到从节点的连接:"+addr.toString());
        super.channelConnected(ctx, e);
    }*/
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx,
                                    ChannelStateEvent e) throws Exception {
        LOG.info("从节点关闭连接 :"+ctx.getChannel().getRemoteAddress().toString());
        super.channelDisconnected(ctx, e);
    }
    private List<ResponseListener> listeners=new CopyOnWriteArrayList<ResponseListener>();
    public void addListener(ResponseListener listener){
        listeners.add(listener);
    }
    public void removeListener(ResponseListener listener){
        listeners.remove(listener);
    }
    public static interface ResponseListener{
        public void onResponse(Protocol.Response resp);
        public void onWebResponse(Protocol.WebResponse resp);
    }
}
