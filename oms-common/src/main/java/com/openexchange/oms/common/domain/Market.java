package com.openexchange.oms.common.domain;

import com.match.domain.MarketInfo;
import com.openexchange.oms.common.enums.Asset;

/**
 * Market definition mapping marketId to base/quote asset pair.
 * Identity (id, symbol) derived from {@link MarketInfo} — single source of truth.
 */
public record Market(int marketId, String symbol, Asset baseAsset, Asset quoteAsset) {

    public static final Market BTC_USD  = fromInfo(MarketInfo.BTC_USD,  Asset.BTC,  Asset.USD);
    public static final Market ETH_USD  = fromInfo(MarketInfo.ETH_USD,  Asset.ETH,  Asset.USD);
    public static final Market SOL_USD  = fromInfo(MarketInfo.SOL_USD,  Asset.SOL,  Asset.USD);
    public static final Market XRP_USD  = fromInfo(MarketInfo.XRP_USD,  Asset.XRP,  Asset.USD);
    public static final Market DOGE_USD = fromInfo(MarketInfo.DOGE_USD, Asset.DOGE, Asset.USD);

    public static final Market[] ALL = {BTC_USD, ETH_USD, SOL_USD, XRP_USD, DOGE_USD};

    private static Market fromInfo(MarketInfo info, Asset base, Asset quote) {
        return new Market(info.id(), info.symbol(), base, quote);
    }

    private static final Market[] BY_ID = new Market[6];
    static {
        for (Market m : ALL) {
            BY_ID[m.marketId] = m;
        }
    }

    public static Market fromId(int marketId) {
        if (marketId < 1 || marketId >= BY_ID.length || BY_ID[marketId] == null) {
            throw new IllegalArgumentException("Unknown market ID: " + marketId);
        }
        return BY_ID[marketId];
    }
}
