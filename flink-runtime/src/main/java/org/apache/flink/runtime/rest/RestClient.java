/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.io.network.netty.OutboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.SSLHandlerFactory;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rest.util.RestConstants;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultSelectStrategyFactory;
import org.apache.flink.shaded.netty4.io.netty.channel.SelectStrategyFactory;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.TooLongFrameException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpClientCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.Attribute;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.MemoryAttribute;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.channels.spi.SelectorProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.SecurityOptions.SSL_REST_ENABLED;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;

/** This client is the counter-part to the {@link RestServerEndpoint}. */
public class RestClient implements AutoCloseableAsync {
    private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

    private static final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();
    private static final ObjectMapper flexibleObjectMapper =
            RestMapperUtils.getFlexibleObjectMapper();

    // used to open connections to a rest server endpoint
    private final Executor executor;

    private final Bootstrap bootstrap;

    private final CompletableFuture<Void> terminationFuture;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static final String VERSION_PLACEHOLDER = "{{VERSION}}";

    private final String urlPrefix;

    // Used to track unresolved request futures in case they need to be resolved when the client is
    // closed
    private final Collection<CompletableFuture<Channel>> responseChannelFutures =
            ConcurrentHashMap.newKeySet();

    private final List<OutboundChannelHandlerFactory> outboundChannelHandlerFactories;

    /**
     * Creates a new RestClient for the provided root URL. If the protocol of the URL is "https",
     * then SSL is automatically enabled for the REST client.
     */
    public static RestClient forUrl(Configuration configuration, Executor executor, URL rootUrl)
            throws ConfigurationException {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(rootUrl);
        if ("https".equals(rootUrl.getProtocol())) {
            configuration = configuration.clone();
            configuration.set(SSL_REST_ENABLED, true);
        }
        return new RestClient(configuration, executor, rootUrl.getHost(), rootUrl.getPort());
    }

    public RestClient(Configuration configuration, Executor executor)
            throws ConfigurationException {
        this(configuration, executor, null, -1);
    }

    public RestClient(Configuration configuration, Executor executor, String host, int port)
            throws ConfigurationException {
        this(configuration, executor, host, port, DefaultSelectStrategyFactory.INSTANCE);
    }

    @VisibleForTesting
    RestClient(
            Configuration configuration,
            Executor executor,
            SelectStrategyFactory selectStrategyFactory)
            throws ConfigurationException {
        this(configuration, executor, null, -1, selectStrategyFactory);
    }

    private RestClient(
            Configuration configuration,
            Executor executor,
            String host,
            int port,
            SelectStrategyFactory selectStrategyFactory)
            throws ConfigurationException {
        Preconditions.checkNotNull(configuration);
        this.executor = Preconditions.checkNotNull(executor);
        this.terminationFuture = new CompletableFuture<>();
        outboundChannelHandlerFactories = new ArrayList<>();
        ServiceLoader<OutboundChannelHandlerFactory> loader =
                ServiceLoader.load(OutboundChannelHandlerFactory.class);
        final Iterator<OutboundChannelHandlerFactory> factories = loader.iterator();
        while (factories.hasNext()) {
            try {
                final OutboundChannelHandlerFactory factory = factories.next();
                if (factory != null) {
                    outboundChannelHandlerFactories.add(factory);
                    LOG.info("Loaded channel outbound factory: {}", factory);
                }
            } catch (Throwable e) {
                LOG.error("Could not load channel outbound factory.", e);
                throw e;
            }
        }
        outboundChannelHandlerFactories.sort(
                Comparator.comparingInt(OutboundChannelHandlerFactory::priority).reversed());

        urlPrefix = configuration.get(RestOptions.URL_PREFIX);
        Preconditions.checkArgument(
                urlPrefix.startsWith("/") && urlPrefix.endsWith("/"),
                "urlPrefix must start and end with '/'");

        final RestClientConfiguration restConfiguration =
                RestClientConfiguration.fromConfiguration(configuration);
        final SSLHandlerFactory sslHandlerFactory = restConfiguration.getSslHandlerFactory();
        ChannelInitializer<SocketChannel> initializer =
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) {
                        try {
                            // SSL should be the first handler in the pipeline
                            if (sslHandlerFactory != null) {
                                SslHandler nettySSLHandler =
                                        host == null
                                                ? sslHandlerFactory.createNettySSLHandler(
                                                        socketChannel.alloc())
                                                : sslHandlerFactory.createNettySSLHandler(
                                                        socketChannel.alloc(), host, port);
                                socketChannel.pipeline().addLast("ssl", nettySSLHandler);
                            }
                            socketChannel
                                    .pipeline()
                                    .addLast(new HttpClientCodec())
                                    .addLast(
                                            new HttpObjectAggregator(
                                                    restConfiguration.getMaxContentLength()));

                            for (OutboundChannelHandlerFactory factory :
                                    outboundChannelHandlerFactories) {
                                Optional<ChannelHandler> channelHandler =
                                        factory.createHandler(configuration);
                                if (channelHandler.isPresent()) {
                                    socketChannel.pipeline().addLast(channelHandler.get());
                                }
                            }

                            socketChannel
                                    .pipeline()
                                    .addLast(new ChunkedWriteHandler()) // required for
                                    // multipart-requests
                                    .addLast(
                                            new IdleStateHandler(
                                                    restConfiguration.getIdlenessTimeout(),
                                                    restConfiguration.getIdlenessTimeout(),
                                                    restConfiguration.getIdlenessTimeout(),
                                                    TimeUnit.MILLISECONDS))
                                    .addLast(new ClientHandler());
                        } catch (Throwable t) {
                            t.printStackTrace();
                            ExceptionUtils.rethrow(t);
                        }
                    }
                };

        // No NioEventLoopGroup constructor available that allows passing nThreads, threadFactory,
        // and selectStrategyFactory without also passing a SelectorProvider, so mimicking its
        // default value seen in other constructors
        NioEventLoopGroup group =
                new NioEventLoopGroup(
                        1,
                        new ExecutorThreadFactory("flink-rest-client-netty"),
                        SelectorProvider.provider(),
                        selectStrategyFactory);

        bootstrap = new Bootstrap();
        bootstrap
                .option(
                        ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        Math.toIntExact(restConfiguration.getConnectionTimeout()))
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(initializer);

        LOG.debug("Rest client endpoint started.");
    }

    @VisibleForTesting
    Collection<CompletableFuture<Channel>> getResponseChannelFutures() {
        return responseChannelFutures;
    }

    @VisibleForTesting
    List<OutboundChannelHandlerFactory> getOutboundChannelHandlerFactories() {
        return outboundChannelHandlerFactories;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return shutdownInternally(Time.seconds(10L));
    }

    public void shutdown(Time timeout) {
        final CompletableFuture<Void> shutDownFuture = shutdownInternally(timeout);

        try {
            shutDownFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
            LOG.debug("Rest endpoint shutdown complete.");
        } catch (Exception e) {
            LOG.warn("Rest endpoint shutdown failed.", e);
        }
    }

    private CompletableFuture<Void> shutdownInternally(Time timeout) {
        if (isRunning.compareAndSet(true, false)) {
            LOG.debug("Shutting down rest endpoint.");

            if (bootstrap != null) {
                if (bootstrap.config().group() != null) {
                    bootstrap
                            .config()
                            .group()
                            .shutdownGracefully(0L, timeout.toMilliseconds(), TimeUnit.MILLISECONDS)
                            .addListener(
                                    finished -> {
                                        notifyResponseFuturesOfShutdown();

                                        if (finished.isSuccess()) {
                                            terminationFuture.complete(null);
                                        } else {
                                            terminationFuture.completeExceptionally(
                                                    finished.cause());
                                        }
                                    });
                }
            }
        }
        return terminationFuture;
    }

    private void notifyResponseFuturesOfShutdown() {
        responseChannelFutures.forEach(
                future ->
                        future.completeExceptionally(
                                new IllegalStateException(
                                        "RestClient closed before request completed")));
        responseChannelFutures.clear();
    }

    public <
                    M extends MessageHeaders<EmptyRequestBody, P, EmptyMessageParameters>,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(String targetAddress, int targetPort, M messageHeaders)
                    throws IOException {
        return sendRequest(
                targetAddress,
                targetPort,
                messageHeaders,
                EmptyMessageParameters.getInstance(),
                EmptyRequestBody.getInstance());
    }

    public <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(
                    String targetAddress,
                    int targetPort,
                    M messageHeaders,
                    U messageParameters,
                    R request)
                    throws IOException {
        return sendRequest(
                targetAddress,
                targetPort,
                messageHeaders,
                messageParameters,
                request,
                Collections.emptyList());
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 方法重载继续调用sendRequest方法
    */
    public <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(
                    String targetAddress,
                    int targetPort,
                    M messageHeaders,
                    U messageParameters,
                    R request,
                    Collection<FileUpload> fileUploads)
                    throws IOException {
        Collection<? extends RestAPIVersion> supportedAPIVersions =
                messageHeaders.getSupportedAPIVersions();
        return sendRequest(
                targetAddress,
                targetPort,
                messageHeaders,
                messageParameters,
                request,
                fileUploads,
                RestAPIVersion.getLatestVersion(supportedAPIVersions));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 发送请求
    */
    public <
                    M extends MessageHeaders<R, P, U>,
                    U extends MessageParameters,
                    R extends RequestBody,
                    P extends ResponseBody>
            CompletableFuture<P> sendRequest(
                    String targetAddress,
                    int targetPort,
                    M messageHeaders,
                    U messageParameters,
                    R request,
                    Collection<FileUpload> fileUploads,
                    RestAPIVersion<? extends RestAPIVersion<?>> apiVersion)
                    throws IOException {
        /**
         * 参数校验
         */
        Preconditions.checkNotNull(targetAddress);
        Preconditions.checkArgument(
                NetUtils.isValidHostPort(targetPort),
                "The target port " + targetPort + " is not in the range [0, 65535].");
        Preconditions.checkNotNull(messageHeaders);
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(messageParameters);
        Preconditions.checkNotNull(fileUploads);
        Preconditions.checkState(
                messageParameters.isResolved(), "Message parameters were not resolved.");

        /**
         * 首先检查请求的apiVersion是否包含在messageHeaders所支持的API版本中。如果不包含，则抛出一个IllegalArgumentException，
         * 并附带一个描述性的错误消息，指明请求的版本不受支持，同时列出支持的所有版本
         */
        if (!messageHeaders.getSupportedAPIVersions().contains(apiVersion)) {
            throw new IllegalArgumentException(
                    String.format(
                            "The requested version %s is not supported by the request (method=%s URL=%s). Supported versions are: %s.",
                            apiVersion,
                            messageHeaders.getHttpMethod(),
                            messageHeaders.getTargetRestEndpointURL(),
                            messageHeaders.getSupportedAPIVersions().stream()
                                    .map(RestAPIVersion::getURLVersionPrefix)
                                    .collect(Collectors.joining(","))));
        }
        /**
         * 首先调用constructVersionedHandlerUrl方法，使用消息头、API版本前缀和URL前缀来构建一个版本化的处理器URL。
         * 然后，它使用MessageParameters.resolveUrl方法来解析并构建目标URL。
         */
        String versionedHandlerURL =
                constructVersionedHandlerUrl(
                        messageHeaders, apiVersion.getURLVersionPrefix(), this.urlPrefix);
        String targetUrl = MessageParameters.resolveUrl(versionedHandlerURL, messageParameters);

        LOG.debug(
                "Sending request of class {} to {}:{}{}",
                request.getClass(),
                targetAddress,
                targetPort,
                targetUrl);
        // serialize payload
        StringWriter sw = new StringWriter();
        /**
         * 使用objectMapper将request对象序列化为JSON格式的字符串，并将其转换为ByteBuf对象，
         * 这是Netty网络库用来处理字节数据的类。
         */
        objectMapper.writeValue(sw, request);
        ByteBuf payload =
                Unpooled.wrappedBuffer(sw.toString().getBytes(ConfigConstants.DEFAULT_CHARSET));
        /**
         * 调用createRequest方法来创建一个新的HTTP请求，并传入目标地址、URL、HTTP方法、负载、文件上传和自定义头。
         */
        Request httpRequest =
                createRequest(
                        targetAddress + ':' + targetPort,
                        targetUrl,
                        messageHeaders.getHttpMethod().getNettyHttpMethod(),
                        payload,
                        fileUploads,
                        messageHeaders.getCustomHeaders());
        /** 定义了一个JavaType类型的变量responseType，用于存储最终构建的响应类型 */
        final JavaType responseType;
        /** 从messageHeaders对象中获取响应类型的参数，这些参数用于泛型类型的构建。 */
        final Collection<Class<?>> typeParameters = messageHeaders.getResponseTypeParameters();
        /**
         * 如果typeParameters集合为空，那么直接通过objectMapper的constructType方法根据messageHeaders中的响应类
         * （getResponseClass）来构建响应类型。
         */
        if (typeParameters.isEmpty()) {
            responseType = objectMapper.constructType(messageHeaders.getResponseClass());
        } else {
            /**
             * 如果typeParameters集合不为空，那么使用objectMapper的getTypeFactory方法来获取类型工厂，
             * 并调用constructParametricType方法构建带参数的泛型类型。这里需要传入泛型的原始类型（messageHeaders.getResponseClass()）
             * 和泛型参数数组（typeParameters.toArray(...)）。
             */
            responseType =
                    objectMapper
                            .getTypeFactory()
                            .constructParametricType(
                                    messageHeaders.getResponseClass(),
                                    typeParameters.toArray(new Class<?>[typeParameters.size()]));
        }
        /**
         * 调用submitRequest方法，传入目标地址、端口、构建好的HTTP请求以及响应类型，来发送请求并可能返回响应。
         */
        return submitRequest(targetAddress, targetPort, httpRequest, responseType);
    }

    private static <M extends MessageHeaders<?, ?, ?>> String constructVersionedHandlerUrl(
            M messageHeaders, String urlVersionPrefix, String urlPrefix) {
        String targetUrl = messageHeaders.getTargetRestEndpointURL();
        if (targetUrl.contains(VERSION_PLACEHOLDER)) {
            return targetUrl.replace(VERSION_PLACEHOLDER, urlVersionPrefix);
        } else {
            return urlPrefix + urlVersionPrefix + messageHeaders.getTargetRestEndpointURL();
        }
    }

    private static Request createRequest(
            String targetAddress,
            String targetUrl,
            HttpMethod httpMethod,
            ByteBuf jsonPayload,
            Collection<FileUpload> fileUploads,
            Collection<HttpHeader> customHeaders)
            throws IOException {
        if (fileUploads.isEmpty()) {

            HttpRequest httpRequest =
                    new DefaultFullHttpRequest(
                            HttpVersion.HTTP_1_1, httpMethod, targetUrl, jsonPayload);

            HttpHeaders headers = httpRequest.headers();
            headers.set(HttpHeaderNames.HOST, targetAddress)
                    .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
                    .add(HttpHeaderNames.CONTENT_LENGTH, jsonPayload.capacity())
                    .add(HttpHeaderNames.CONTENT_TYPE, RestConstants.REST_CONTENT_TYPE);
            customHeaders.forEach(ch -> headers.set(ch.getName(), ch.getValue()));

            return new SimpleRequest(httpRequest);
        } else {
            HttpRequest httpRequest =
                    new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, httpMethod, targetUrl);

            HttpHeaders headers = httpRequest.headers();
            headers.set(HttpHeaderNames.HOST, targetAddress)
                    .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            customHeaders.forEach(ch -> headers.set(ch.getName(), ch.getValue()));

            // takes care of splitting the request into multiple parts
            HttpPostRequestEncoder bodyRequestEncoder;
            try {
                // we could use mixed attributes here but we have to ensure that the minimum size is
                // greater than
                // any file as the upload otherwise fails
                DefaultHttpDataFactory httpDataFactory = new DefaultHttpDataFactory(true);
                // the FileUploadHandler explicitly checks for multipart headers
                bodyRequestEncoder = new HttpPostRequestEncoder(httpDataFactory, httpRequest, true);

                Attribute requestAttribute =
                        new MemoryAttribute(FileUploadHandler.HTTP_ATTRIBUTE_REQUEST);
                requestAttribute.setContent(jsonPayload);
                bodyRequestEncoder.addBodyHttpData(requestAttribute);

                int fileIndex = 0;
                for (FileUpload fileUpload : fileUploads) {
                    Path path = fileUpload.getFile();
                    if (Files.isDirectory(path)) {
                        throw new IllegalArgumentException(
                                "Upload of directories is not supported. Dir=" + path);
                    }
                    File file = path.toFile();
                    LOG.trace("Adding file {} to request.", file);
                    bodyRequestEncoder.addBodyFileUpload(
                            "file_" + fileIndex, file, fileUpload.getContentType(), false);
                    fileIndex++;
                }
            } catch (HttpPostRequestEncoder.ErrorDataEncoderException e) {
                throw new IOException("Could not encode request.", e);
            }

            try {
                httpRequest = bodyRequestEncoder.finalizeRequest();
            } catch (HttpPostRequestEncoder.ErrorDataEncoderException e) {
                throw new IOException("Could not finalize request.", e);
            }

            return new MultipartRequest(httpRequest, bodyRequestEncoder);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * submitRequest方法，该方法用于异步提交HTTP请求并返回一个CompletableFuture对象，该对象将在请求完成时持有响应体
     * private: 这是一个私有方法，只能在当前类中被调用。
     * <P extends ResponseBody>: 这是一个泛型方法，其中P是ResponseBody的一个子类或实现。这表示返回的CompletableFuture将持有某种类型的响应体，该类型继承自ResponseBody。
     * CompletableFuture<P>: 方法返回一个CompletableFuture对象，该对象将在请求完成时包含响应体。
     * 参数包括目标地址、目标端口、HTTP请求和响应类型。
    */
    private <P extends ResponseBody> CompletableFuture<P> submitRequest(
            String targetAddress, int targetPort, Request httpRequest, JavaType responseType) {
        /**
         * 如果isRunning的值为false，表示RestClient已经关闭，
         * 那么方法将立即返回一个异常完成的CompletableFuture。
         */
        if (!isRunning.get()) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException("RestClient is already closed"));
        }
        /**
         * 创建一个新的CompletableFuture对象，用于异步获取连接通道（Channel）。
         * 将这个channelFuture添加到responseChannelFutures中，以便稍后可能对其进行管理或跟踪。
         */
        final CompletableFuture<Channel> channelFuture = new CompletableFuture<>();
        responseChannelFutures.add(channelFuture);
        /**
         * 使用bootstrap来尝试连接到指定的地址和端口。
         * 返回的ChannelFuture表示一个异步的I/O操作，当连接建立成功或失败时，它会得到通知。
         */
        final ChannelFuture connectFuture = bootstrap.connect(targetAddress, targetPort);
        /** 为connectFuture添加一个监听器，当连接操作完成时，监听器会被触发。 */
        connectFuture.addListener(
                /**
                 * 在监听器内部，首先从responseChannelFutures中移除channelFuture，因为它不再需要被跟踪（因为连接操作已经完成）。
                 */
                (ChannelFuture future) -> {
                    responseChannelFutures.remove(channelFuture);

                    if (future.isSuccess()) {
                        /** 如果连接成功，则使用成功的Channel完成channelFuture。 */
                        channelFuture.complete(future.channel());
                    } else {
                        /** 如果连接失败，则使用导致失败的异常来完成channelFuture。 */
                        channelFuture.completeExceptionally(future.cause());
                    }
                });
        /**
         * 在成功建立连接之后，它开始发送HTTP请求，并处理响应。
         */
        return channelFuture
                /**
                 * channelFuture.thenComposeAsync(...): 这是一个CompletableFuture的链式调用，
                 * 它接受一个函数作为参数，这个函数会在channelFuture完成时（即连接成功建立后）被调用。
                 * thenComposeAsync用于组合多个异步操作，并允许你返回一个新的CompletableFuture。
                 */
                .thenComposeAsync(
                        channel -> {
                            /**
                             * ClientHandler Netty中的一个ChannelInboundHandler实现，用于处理入站的I/O事件和消息。
                             * 通过从ChannelPipeline中获取ClientHandler的实例，代码可以访问与特定Channel关联的处理器。
                             */
                            ClientHandler handler = channel.pipeline().get(ClientHandler.class);

                            CompletableFuture<JsonResponse> future;
                            boolean success = false;

                            try {
                                if (handler == null) {
                                    throw new IOException(
                                            "Netty pipeline was not properly initialized.");
                                } else {
                                    /**
                                     * 使用writeTo方法将httpRequest写入到Channel中，这通常会触发Netty的写操作，将请求发送到服务器。
                                     */
                                    httpRequest.writeTo(channel);
                                    future = handler.getJsonFuture();
                                    success = true;
                                }
                            /**
                             * executor: 这个参数是一个Executor，用于异步执行thenComposeAsync中的函数。
                             * 这允许你在一个单独的线程中执行耗时的操作，而不会阻塞主线程。
                             */
                            } catch (IOException e) {
                                future =
                                        FutureUtils.completedExceptionally(
                                                new ConnectionException(
                                                        "Could not write request.", e));
                            } finally {
                                if (!success) {
                                    channel.close();
                                }
                            }

                            return future;
                        },
                        executor)
                .thenComposeAsync(
                        (JsonResponse rawResponse) -> parseResponse(rawResponse, responseType),
                        executor);
    }

    private static <P extends ResponseBody> CompletableFuture<P> parseResponse(
            JsonResponse rawResponse, JavaType responseType) {
        CompletableFuture<P> responseFuture = new CompletableFuture<>();
        final JsonParser jsonParser = objectMapper.treeAsTokens(rawResponse.json);
        try {
            // We make sure it fits to ErrorResponseBody, this condition is enforced by test in
            // RestClientTest
            if (rawResponse.json.size() == 1 && rawResponse.json.has("errors")) {
                ErrorResponseBody error =
                        objectMapper.treeToValue(rawResponse.getJson(), ErrorResponseBody.class);
                responseFuture.completeExceptionally(
                        new RestClientException(
                                error.errors.toString(), rawResponse.getHttpResponseStatus()));
            } else {
                P response = flexibleObjectMapper.readValue(jsonParser, responseType);
                responseFuture.complete(response);
            }
        } catch (IOException ex) {
            // if this fails it is either the expected type or response type was wrong, most
            // likely caused
            // by a client/search MessageHeaders mismatch
            LOG.error(
                    "Received response was neither of the expected type ({}) nor an error. Response={}",
                    responseType,
                    rawResponse,
                    ex);
            responseFuture.completeExceptionally(
                    new RestClientException(
                            "Response was neither of the expected type("
                                    + responseType
                                    + ") nor an error.",
                            ex,
                            rawResponse.getHttpResponseStatus()));
        }
        return responseFuture;
    }

    private interface Request {
        void writeTo(Channel channel) throws IOException;
    }

    private static final class SimpleRequest implements Request {
        private final HttpRequest httpRequest;

        SimpleRequest(HttpRequest httpRequest) {
            this.httpRequest = httpRequest;
        }

        @Override
        public void writeTo(Channel channel) {
            channel.writeAndFlush(httpRequest);
        }
    }

    private static final class MultipartRequest implements Request {
        private final HttpRequest httpRequest;
        private final HttpPostRequestEncoder bodyRequestEncoder;

        MultipartRequest(HttpRequest httpRequest, HttpPostRequestEncoder bodyRequestEncoder) {
            this.httpRequest = httpRequest;
            this.bodyRequestEncoder = bodyRequestEncoder;
        }

        @Override
        public void writeTo(Channel channel) {
            ChannelFuture future = channel.writeAndFlush(httpRequest);
            // this should never be false as we explicitly set the encoder to use multipart messages
            if (bodyRequestEncoder.isChunked()) {
                future = channel.writeAndFlush(bodyRequestEncoder);
            }

            // release data and remove temporary files if they were created, once the writing is
            // complete
            future.addListener((ignored) -> bodyRequestEncoder.cleanFiles());
        }
    }

    private static class ClientHandler extends SimpleChannelInboundHandler<Object> {

        private final CompletableFuture<JsonResponse> jsonFuture = new CompletableFuture<>();

        CompletableFuture<JsonResponse> getJsonFuture() {
            return jsonFuture;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpResponse
                    && ((HttpResponse) msg).status().equals(REQUEST_ENTITY_TOO_LARGE)) {
                jsonFuture.completeExceptionally(
                        new RestClientException(
                                String.format(
                                        REQUEST_ENTITY_TOO_LARGE + ". Try to raise [%s]",
                                        RestOptions.CLIENT_MAX_CONTENT_LENGTH.key()),
                                ((HttpResponse) msg).status()));
            } else if (msg instanceof FullHttpResponse) {
                readRawResponse((FullHttpResponse) msg);
            } else {
                LOG.error(
                        "Implementation error: Received a response that wasn't a FullHttpResponse.");
                if (msg instanceof HttpResponse) {
                    jsonFuture.completeExceptionally(
                            new RestClientException(
                                    "Implementation error: Received a response that wasn't a FullHttpResponse.",
                                    ((HttpResponse) msg).status()));
                } else {
                    jsonFuture.completeExceptionally(
                            new RestClientException(
                                    "Implementation error: Received a response that wasn't a FullHttpResponse.",
                                    HttpResponseStatus.INTERNAL_SERVER_ERROR));
                }
            }
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            jsonFuture.completeExceptionally(
                    new ConnectionClosedException("Channel became inactive."));
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                jsonFuture.completeExceptionally(
                        new ConnectionIdleException("Channel became idle."));
                ctx.close();
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            if (cause instanceof TooLongFrameException) {
                jsonFuture.completeExceptionally(
                        new TooLongFrameException(
                                String.format(
                                        cause.getMessage() + " Try to raise [%s]",
                                        RestOptions.CLIENT_MAX_CONTENT_LENGTH.key())));
            } else {
                jsonFuture.completeExceptionally(cause);
            }
            ctx.close();
        }

        private void readRawResponse(FullHttpResponse msg) {
            ByteBuf content = msg.content();

            JsonNode rawResponse;
            try (InputStream in = new ByteBufInputStream(content)) {
                rawResponse = objectMapper.readTree(in);
                LOG.debug("Received response {}.", rawResponse);
            } catch (JsonProcessingException je) {
                LOG.error("Response was not valid JSON.", je);
                // let's see if it was a plain-text message instead
                content.readerIndex(0);
                try (ByteBufInputStream in = new ByteBufInputStream(content)) {
                    byte[] data = new byte[in.available()];
                    in.readFully(data);
                    String message = new String(data);
                    LOG.error("Unexpected plain-text response: {}", message);
                    jsonFuture.completeExceptionally(
                            new RestClientException(
                                    "Response was not valid JSON, but plain-text: " + message,
                                    je,
                                    msg.status()));
                } catch (IOException e) {
                    jsonFuture.completeExceptionally(
                            new RestClientException(
                                    "Response was not valid JSON, nor plain-text.",
                                    je,
                                    msg.status()));
                }
                return;
            } catch (IOException ioe) {
                LOG.error("Response could not be read.", ioe);
                jsonFuture.completeExceptionally(
                        new RestClientException("Response could not be read.", ioe, msg.status()));
                return;
            }
            jsonFuture.complete(new JsonResponse(rawResponse, msg.status()));
        }
    }

    private static final class JsonResponse {
        private final JsonNode json;
        private final HttpResponseStatus httpResponseStatus;

        private JsonResponse(JsonNode json, HttpResponseStatus httpResponseStatus) {
            this.json = Preconditions.checkNotNull(json);
            this.httpResponseStatus = Preconditions.checkNotNull(httpResponseStatus);
        }

        public JsonNode getJson() {
            return json;
        }

        public HttpResponseStatus getHttpResponseStatus() {
            return httpResponseStatus;
        }

        @Override
        public String toString() {
            return "JsonResponse{"
                    + "json="
                    + json
                    + ", httpResponseStatus="
                    + httpResponseStatus
                    + '}';
        }
    }
}
