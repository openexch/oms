package com.openexchange.oms.risk;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Manages per-market risk configurations, supporting hot-reload via admin API.
 */
public class RiskConfigManager {

    private final RiskEngine riskEngine;
    private final RiskConfig[] configs;
    private final int maxMarkets;

    public RiskConfigManager(RiskEngine riskEngine, int maxMarkets) {
        this.riskEngine = riskEngine;
        this.maxMarkets = maxMarkets;
        this.configs = new RiskConfig[maxMarkets];
    }

    public void setConfig(int marketId, RiskConfig config) {
        if (marketId < 0 || marketId >= maxMarkets) {
            throw new IllegalArgumentException("marketId out of range: " + marketId);
        }
        configs[marketId] = config;
        riskEngine.setMarketConfig(marketId, config);
    }

    public RiskConfig getConfig(int marketId) {
        if (marketId < 0 || marketId >= maxMarkets) return null;
        return configs[marketId];
    }

    public Map<String, Object> getConfigAsMap(int marketId) {
        RiskConfig config = getConfig(marketId);
        if (config == null) return null;
        return configToMap(marketId, config);
    }

    public Map<String, Map<String, Object>> getAllConfigs() {
        Map<String, Map<String, Object>> all = new LinkedHashMap<>();
        for (int i = 0; i < maxMarkets; i++) {
            if (configs[i] != null) {
                all.put(String.valueOf(i), configToMap(i, configs[i]));
            }
        }
        return all;
    }

    public void updateConfig(int marketId, Map<String, Object> fields) {
        RiskConfig existing = getConfig(marketId);
        RiskConfig.Builder builder = RiskConfig.builder();

        if (existing != null) {
            builder.minQuantity(existing.getMinQuantity())
                   .maxQuantity(existing.getMaxQuantity())
                   .minNotional(existing.getMinNotional())
                   .maxNotional(existing.getMaxNotional())
                   .priceCollarPercent(existing.getPriceCollarPercent())
                   .circuitBreakerPercent(existing.getCircuitBreakerPercent())
                   .circuitBreakerWindowMs(existing.getCircuitBreakerWindowMs())
                   .maxOrdersPerSec(existing.getMaxOrdersPerSec())
                   .maxOrdersPerMin(existing.getMaxOrdersPerMin())
                   .maxOpenOrders(existing.getMaxOpenOrders())
                   .maxPositionPerMarket(existing.getMaxPositionPerMarket());
        }

        if (fields.containsKey("minQuantity")) builder.minQuantity(toLong(fields.get("minQuantity")));
        if (fields.containsKey("maxQuantity")) builder.maxQuantity(toLong(fields.get("maxQuantity")));
        if (fields.containsKey("minNotional")) builder.minNotional(toLong(fields.get("minNotional")));
        if (fields.containsKey("maxNotional")) builder.maxNotional(toLong(fields.get("maxNotional")));
        if (fields.containsKey("priceCollarPercent")) builder.priceCollarPercent(toInt(fields.get("priceCollarPercent")));
        if (fields.containsKey("circuitBreakerPercent")) builder.circuitBreakerPercent(toInt(fields.get("circuitBreakerPercent")));
        if (fields.containsKey("circuitBreakerWindowMs")) builder.circuitBreakerWindowMs(toLong(fields.get("circuitBreakerWindowMs")));
        if (fields.containsKey("maxOrdersPerSec")) builder.maxOrdersPerSec(toInt(fields.get("maxOrdersPerSec")));
        if (fields.containsKey("maxOrdersPerMin")) builder.maxOrdersPerMin(toInt(fields.get("maxOrdersPerMin")));
        if (fields.containsKey("maxOpenOrders")) builder.maxOpenOrders(toInt(fields.get("maxOpenOrders")));
        if (fields.containsKey("maxPositionPerMarket")) builder.maxPositionPerMarket(toLong(fields.get("maxPositionPerMarket")));

        setConfig(marketId, builder.build());
    }

    private static Map<String, Object> configToMap(int marketId, RiskConfig config) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("marketId", marketId);
        map.put("minQuantity", config.getMinQuantity());
        map.put("maxQuantity", config.getMaxQuantity());
        map.put("minNotional", config.getMinNotional());
        map.put("maxNotional", config.getMaxNotional());
        map.put("priceCollarPercent", config.getPriceCollarPercent());
        map.put("circuitBreakerPercent", config.getCircuitBreakerPercent());
        map.put("circuitBreakerWindowMs", config.getCircuitBreakerWindowMs());
        map.put("maxOrdersPerSec", config.getMaxOrdersPerSec());
        map.put("maxOrdersPerMin", config.getMaxOrdersPerMin());
        map.put("maxOpenOrders", config.getMaxOpenOrders());
        map.put("maxPositionPerMarket", config.getMaxPositionPerMarket());
        return map;
    }

    private static long toLong(Object val) {
        if (val instanceof Number n) return n.longValue();
        return Long.parseLong(val.toString());
    }

    private static int toInt(Object val) {
        if (val instanceof Number n) return n.intValue();
        return Integer.parseInt(val.toString());
    }
}
