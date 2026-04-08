package com.openexchange.oms.api.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * WebSocket handler for real-time order/execution/balance updates.
 *
 * Subscribe: { "op": "subscribe", "channels": ["orders","executions","balances"], "userId": "..." }
 * Push events: ORDER_UPDATE, EXECUTION_REPORT, BALANCE_UPDATE
 */
public class WebSocketHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    private static final Logger log = LoggerFactory.getLogger(WebSocketHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // userId → set of channels per connection
    private final Map<ChannelHandlerContext, Long> ctxToUserId = new ConcurrentHashMap<>();
    private final Map<ChannelHandlerContext, Set<String>> ctxToChannels = new ConcurrentHashMap<>();

    // userId → set of contexts (for broadcasting updates to all connections of a user)
    private final Map<Long, Set<ChannelHandlerContext>> userConnections = new ConcurrentHashMap<>();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) throws Exception {
        if (frame instanceof TextWebSocketFrame textFrame) {
            handleTextMessage(ctx, textFrame.text());
        }
    }

    private void handleTextMessage(ChannelHandlerContext ctx, String text) {
        try {
            JsonNode msg = MAPPER.readTree(text);
            String op = msg.has("op") ? msg.get("op").asText() : "";

            if ("subscribe".equals(op)) {
                long userId = msg.get("userId").asLong();
                Set<String> channels = new CopyOnWriteArraySet<>();
                if (msg.has("channels")) {
                    for (JsonNode ch : msg.get("channels")) {
                        channels.add(ch.asText());
                    }
                }

                ctxToUserId.put(ctx, userId);
                ctxToChannels.put(ctx, channels);
                userConnections.computeIfAbsent(userId, k -> new CopyOnWriteArraySet<>()).add(ctx);

                ObjectNode ack = MAPPER.createObjectNode();
                ack.put("type", "SUBSCRIBED");
                ack.put("userId", userId);
                ctx.writeAndFlush(new TextWebSocketFrame(MAPPER.writeValueAsString(ack)));
            } else if ("unsubscribe".equals(op)) {
                removeConnection(ctx);
            }
        } catch (Exception e) {
            log.warn("Invalid WebSocket message: {}", e.getMessage());
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        removeConnection(ctx);
    }

    private void removeConnection(ChannelHandlerContext ctx) {
        Long userId = ctxToUserId.remove(ctx);
        ctxToChannels.remove(ctx);
        if (userId != null) {
            Set<ChannelHandlerContext> connections = userConnections.get(userId);
            if (connections != null) {
                connections.remove(ctx);
                if (connections.isEmpty()) {
                    userConnections.remove(userId);
                }
            }
        }
    }

    /**
     * Push an event to all WebSocket connections for a given user.
     */
    public void pushToUser(long userId, String channel, Object event) {
        Set<ChannelHandlerContext> connections = userConnections.get(userId);
        if (connections == null || connections.isEmpty()) return;

        try {
            String json = MAPPER.writeValueAsString(event);
            TextWebSocketFrame frame = new TextWebSocketFrame(json);

            for (ChannelHandlerContext ctx : connections) {
                Set<String> subscribedChannels = ctxToChannels.get(ctx);
                if (subscribedChannels != null && subscribedChannels.contains(channel)) {
                    if (ctx.channel().isActive()) {
                        ctx.writeAndFlush(frame.retainedDuplicate());
                    }
                }
            }
            frame.release();
        } catch (Exception e) {
            log.error("Error pushing WebSocket event to user {}", userId, e);
        }
    }

    public int getConnectionCount() {
        return ctxToUserId.size();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("WebSocket error", cause);
        ctx.close();
    }
}
