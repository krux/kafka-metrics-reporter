package com.krux.metrics.http.status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * An HTTP server that sends back the content of the received HTTP request in a
 * pretty plaintext form.
 */
public class HttpServer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(HttpServer.class.getName());

    private int _port;
    EventLoopGroup _bossGroup = null;
    EventLoopGroup _workerGroup = null;

    public HttpServer(int port) {
        _port = port;

        // Runtime.getRuntime().addShutdownHook( new Thread() {
        // @Override
        // public void run() {
        // System.out.println("Shutting down http status listener" );
        // _bossGroup.shutdownNow();
        // _workerGroup.shutdownNow();
        // }
        // });
    }

    @Override
    public void run() {

        try {

            System.out.println("Starting HTTP Status listener on port " + _port);

            // Configure the server.
            _bossGroup = new NioEventLoopGroup(1);
            _workerGroup = new NioEventLoopGroup();
            try {
                ServerBootstrap b = new ServerBootstrap();
                b.option(ChannelOption.SO_BACKLOG, 1024);
                b.group(_bossGroup, _workerGroup).channel(NioServerSocketChannel.class)
                        .childHandler(new HttpServerInitializer());

                Channel ch = b.bind(_port).sync().channel();
                ch.closeFuture().sync();
            } finally {
                _bossGroup.shutdownGracefully();
                _workerGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            log.error("HTTP server failed", e);
        }
    }
}