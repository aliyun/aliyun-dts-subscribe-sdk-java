package com.taobao.drc.client.network;

import com.taobao.drc.client.DRCClientException;
import com.taobao.drc.client.Listener;
import com.taobao.drc.client.cm.ClusterManagerFacade;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.enums.TransportType;
import com.taobao.drc.client.network.congestion.CongestionController;
import com.taobao.drc.client.network.congestion.CongestionControllers;
import com.taobao.drc.client.sql.SqlEngine;
import com.taobao.drc.client.sql.SqlHandler;
import com.taobao.drc.client.utils.NetworkUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.ConcurrentSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import static com.taobao.drc.client.utils.Constant.*;
import static com.taobao.drc.client.utils.NetworkConstant.*;


/**
 * Created by jianjundeng on 10/2/14.
 */
public class NetworkEndpoint {

    private static final Log log = LogFactory.getLog(NetworkEndpoint.class);

    private Bootstrap bootstrap;

    private EventLoopGroup workerGroup;
    
    private DRCClientEventExecutorGroup sqlWorkerGroup;
    
    private Set<Channel> channelSet = new ConcurrentSet<Channel>();

    private Listener listener;

    private boolean close;

    private ThreadFactory parseFactory;

    private ThreadFactory notifyFactory;
    
    private SqlEngine engine;

    private TransportType transportType;

    private EventExecutorGroup eventExecutorGroup;

    private final AtomicReference<Channel> writeChannelRef = new AtomicReference<Channel>();

    public void start(final boolean multi, final UserConfig userConfig){
        bootstrap = new Bootstrap();
		workerGroup = new NioEventLoopGroup(multi?0:1,parseFactory);
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        eventExecutorGroup=new NioEventLoopGroup(multi?0:1,notifyFactory);
        switch (transportType) {
            // TODO: read timeout handler may be needed instead of heartbeat thread detect
            case HTTP:
                bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        CongestionController congestionController = CongestionControllers.createFromConfig(userConfig);
                        ch.pipeline().addLast(new AutoReadAwareReadTimeoutHandler(userConfig.getReadTimeoutSeconds()));
                        ch.pipeline().addLast(new HttpResponseDecoder());
                        ch.pipeline().addLast(new HttpRequestEncoder());
                        ch.pipeline().addLast(new HttpMessageHandler());
                        ch.pipeline().addLast(new RecordCRC32Handler());
                        ch.pipeline().addLast(new DataFlowLimitHandler());
                        ch.pipeline().addLast(new RecordPreNotifyHandler(congestionController));
                        if(engine!=null) {
                            sqlWorkerGroup = new DRCClientEventExecutorGroup(multi?0:1,new DRCClientThreadFactory("SqlHandlerPrefix"));
                            ch.pipeline().addLast(sqlWorkerGroup, "SqlFilter", new SqlHandler(engine));
                        }
                        ch.pipeline().addLast(eventExecutorGroup,new RecordNotifyHandler(congestionController));
                    }
                });
                break;
            case DRCNET:
                bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        CongestionController congestionController = CongestionControllers.createFromConfig(userConfig);
                        ch.pipeline().addLast(new AutoReadAwareReadTimeoutHandler(userConfig.getReadTimeoutSeconds()));
                        ch.pipeline().addLast(new DRCNetMessageHandler());
                        ch.pipeline().addLast(new RecordCRC32Handler());
                        ch.pipeline().addLast(new DataFlowLimitHandler());
                        ch.pipeline().addLast(new RecordPreNotifyHandler(congestionController));
                        ch.pipeline().addLast(new UserCtrlMsgHandler());
                        if(engine!=null) {
                            sqlWorkerGroup = new DRCClientEventExecutorGroup(multi?0:1,new DRCClientThreadFactory("SqlHandlerPrefix"));
                            ch.pipeline().addLast(sqlWorkerGroup, "SqlFilter", new SqlHandler(engine));
                        }
                        ch.pipeline().addLast(eventExecutorGroup,new RecordNotifyHandler(congestionController));
                    }
                });
                break;
        }
    }

    public void setTransportType(TransportType transportType) {
        this.transportType=transportType;
    }

    private void setBootstrapAttributes(final Bootstrap bootstrapLocal, final String host, final  String port, final NetworkEndpoint networkEndpoint, final UserConfig userConfig, final ConnectionStateChangeListener stateChangeListener) {
        log.info("set attributes for bootstrap connecting to [" + host + ":" + port + "]");
        bootstrapLocal.attr(configKey, userConfig);
        bootstrapLocal.attr(channelSetKey, channelSet);
        bootstrapLocal.attr(networkKey, networkEndpoint);
        if (null != stateChangeListener) {
            bootstrapLocal.attr(ConnectionStateChangeListener.CONNECTION_STATE_CHANGE_LISTENER_ATTRIBUTE_KEY, stateChangeListener);
        }
        bootstrapLocal.attr(listenerKey, listener);
    }



    public ChannelFuture connectStoreByHTTP(final String host, final String port, final UserConfig userConfig,
                                            final ConnectionStateChangeListener stateChangeListener) throws Exception {
        final String uri = "http://" + host + ":" + port + "/" + userConfig.getSubTopic();
        Bootstrap localBootstrap = bootstrap.clone();
        // set attribute before connect
        setBootstrapAttributes(localBootstrap, host, port, this, userConfig, stateChangeListener);
        //connect
        ChannelFuture future = localBootstrap.connect(host, Integer.parseInt(port));
        final NetworkEndpoint networkEndpoint=this;
        future.addListener(new ChannelFutureListener(){
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                Channel channel=future.channel();
                channelSet.add(channel);
                if(future.isSuccess()){
                    //multi is null,auto retry
                    //send param
                    HttpDataFactory factory = new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE);
                    HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
                    HttpPostRequestEncoder bodyRequestEncoder = new HttpPostRequestEncoder(factory, request, false);
                    for (Map.Entry<String, String> entry : userConfig.getParams().entrySet()) {
                        bodyRequestEncoder.addBodyAttribute(entry.getKey(), entry.getValue());
                    }
                    request = bodyRequestEncoder.finalizeRequest();
                    //TODO: fix body size greater than chunk size
                    channel.writeAndFlush(request);
                    if (bodyRequestEncoder.isChunked()) {
                        while (true) {
                            HttpContent chunkContent = bodyRequestEncoder.readChunk(null);
                            if (chunkContent == null) {
                                break;
                            }
                            channel.write(chunkContent);
                        }
                    }
                    channel.flush();
                    log.info("client start,subTopic:"+userConfig.getSubTopic()+" init checkpoint:"+userConfig.getCheckpoint().toString());
                } else {
                    log.info("subTopic:"+userConfig.getSubTopic()+",client connect store failed, store address [" + host + ":" + port + "]", future.cause());
                    future.channel().pipeline().fireChannelInactive();
                }
            }
        });
        return future;
    }

    public ChannelFuture connectStoreByDRCNet(final String host, final String port, final UserConfig userConfig,
                                              final ConnectionStateChangeListener stateChangeListener) throws Exception {
        Bootstrap localBootstrap = bootstrap.clone();
        // set attribute before connect
        setBootstrapAttributes(localBootstrap, host, port, this, userConfig, stateChangeListener);
        final ChannelFuture channelFuture = localBootstrap.connect(host, Integer.parseInt(port));
        final NetworkEndpoint networkEndpoint=this;
        channelFuture.addListener(new ChannelFutureListener(){
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                Channel channel=future.channel();
                channelSet.add(channel);
                if(future.isSuccess()){
                    //multi is null,auto retry
                    log.info("client start,subTopic:"+userConfig.getSubTopic()+" init checkpoint:"+userConfig.getCheckpoint().toString());
                }else{
                    log.info("subTopic:"+userConfig.getSubTopic()+",client connect store failed, store address [" + host + ":" + port + "]", future.cause());
                    future.channel().pipeline().fireChannelInactive();
                }
                trySetChannelToWrite(channel);
            }
        });
        return channelFuture;
    }

    public void setMessageListener(Listener listener) {
        this.listener = listener;
    }

    public void close() {
        close=true;
        for (Channel channel : channelSet) {
            ChannelFuture channelFuture=channel.close();
            channelFuture.syncUninterruptibly();
        }
        workerGroup.shutdownGracefully();
        if (sqlWorkerGroup != null) {
            sqlWorkerGroup.shutdownGracefully();
        }
        eventExecutorGroup.shutdownGracefully();
    }

    public boolean isClose(){
        return close;
    }

    public ThreadFactory getParseFactory() {
        return parseFactory;
    }

    public void setParseFactory(ThreadFactory parseFactory) {
        this.parseFactory = parseFactory;
    }

    public ThreadFactory getNotifyFactory() {
        return notifyFactory;
    }

    public void setNotifyFactory(ThreadFactory notifyFactory) {
        this.notifyFactory = notifyFactory;
    }

	public void setSqlEngine(SqlEngine engine){
		this.engine=engine;
	}

	private void trySetChannelToWrite(final Channel channel) {
        if (writeChannelRef.compareAndSet(null, channel)) {
            log.info("User control data will be written to [" + channel.remoteAddress() + "]");
            channel.closeFuture().addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    log.info("Connection [" + channel.remoteAddress() + "] to write user control data closed");
                    writeChannelRef.set(null);

                    for (Channel chan : channelSet) {
                        if (chan.isActive()) {
                            trySetChannelToWrite(chan);
                            break;
                        }
                    }
                }
            });
        }
    }

    public Future writeUserCtlMessage(byte[] userCtlBytes) {
	    if (userCtlBytes == null) {
	        throw new NullPointerException("User control bytes cannot be null!");
        }
	    if (transportType != TransportType.DRCNET) {
	        throw new UnsupportedOperationException("Supported in DRCNET mode only");
        }
        Channel channel = writeChannelRef.get();
	    if (channel == null) {
	        return new DefaultPromise(eventExecutorGroup.next())
                    .setFailure(new IllegalStateException("Channel not ready"));
        }
        return channel.write(new UserCtrlMessage(userCtlBytes));
    }

    public ClusterManagerFacade.StoreInfo findStore(UserConfig userConfig, boolean doValidateFilter) throws Exception {
        ClusterManagerFacade.askToken(userConfig);
        ClusterManagerFacade.askTopic(userConfig);
        ClusterManagerFacade.askRegionInfo(userConfig);
        if (doValidateFilter) {
            DBType dbType = NetworkUtils.retrieveDBTypeFromRemote(userConfig, userConfig.getSubTopic());
            if (!userConfig.getDataFilter().validateFilter(dbType)) {
                throw new DRCClientException("Data filter check failed: for oceanbase 1.0, the filter format must be " +
                        " [tenant.dbname.tbname.clos]");
            }
        }

        return ClusterManagerFacade.fetchStoreInfo(userConfig, userConfig.getTransportType() == TransportType.DRCNET);
    }

    public ChannelFuture connectToStore(ClusterManagerFacade.StoreInfo storeInfo, UserConfig userConfig,
                                         ConnectionStateChangeListener stateChangeListener) throws Exception {
        switch (userConfig.getTransportType()) {
            case HTTP:
                return connectStoreByHTTP(storeInfo.getHost(), String.valueOf(storeInfo.getPort()),
                        userConfig, stateChangeListener);
            case DRCNET:
                return connectStoreByDRCNet(storeInfo.getHost(), String.valueOf(storeInfo.getDrcNetPort()),
                        userConfig, stateChangeListener);
            default:
                throw new IllegalArgumentException("Unsupported transport type: [" + transportType + "]");
        }
    }
}
