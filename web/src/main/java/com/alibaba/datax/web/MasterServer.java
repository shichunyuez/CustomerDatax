package com.alibaba.datax.web;

import com.alibaba.datax.web.protocol.Protocol;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * Created by Administrator on 2017/7/17 0017.
 */
public class MasterServer {

    private static final Logger LOG = LoggerFactory.getLogger(MasterServer.class);
    private ServerBootstrap bootstrap;
    private ChannelPipelineFactory pipelineFactory;

    public MasterServer(final ChannelHandler handler){
        NioServerSocketChannelFactory channelFactory=
                new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
        bootstrap=new ServerBootstrap(channelFactory);
        pipelineFactory=new ChannelPipelineFactory(){
            private final ProtobufVarint32LengthFieldPrepender frameEncoder = new ProtobufVarint32LengthFieldPrepender();
            private final ProtobufEncoder protobufEncoder = new ProtobufEncoder();
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline p = pipeline();
                p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
                p.addLast("protobufDecoder",new ProtobufDecoder(Protocol.SocketMessage.getDefaultInstance()));
                p.addLast("frameEncoder", frameEncoder);
                p.addLast("protobufEncoder", protobufEncoder);
                p.addLast("handler", handler);
                return p;
            }

        };
        try {
            bootstrap.setPipeline(pipelineFactory.getPipeline());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void start(int port){
        bootstrap.bind(new InetSocketAddress(port));
        LOG.info("主节点网络服务启动成功,端口为: "+port);
    }

    public synchronized void shutdown(){
        bootstrap.releaseExternalResources();
        LOG.info("主节点网络服务关闭.");
    }

}